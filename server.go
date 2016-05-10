package main

import (
    "flag"
    "io"
    "log"
    "net"
    "os"
    "os/signal"
    "time"
)

const MAX_UNPROCESSED_INCOMING_MESSAGES = 1000
const MAX_READ_SIZE = 65535
const DEFAULT_UDP_ADDRESS = ":8125"
const DEFAULT_FLUSH_INTERVAL_MILLISECONDS = 10000

func main() {
    udpServerAddress := flag.String("udpAddr", DEFAULT_UDP_ADDRESS, "UDP server address")
    tcpServerAddress := flag.String("tcpAddr", "", "TCP server address")
    flushInterval := flag.Int("flushInterval", DEFAULT_FLUSH_INTERVAL_MILLISECONDS,
        "Metrics flush interval (milliseconds)")

    flag.Parse()

    sigChan := make(chan os.Signal)
    signal.Notify(sigChan, os.Interrupt)

    incomingMessages := make(chan string, MAX_UNPROCESSED_INCOMING_MESSAGES)

    go udpListener(*udpServerAddress, incomingMessages)

    if (*tcpServerAddress != "") {
        go tcpListener(*tcpServerAddress, incomingMessages)
    }

    mainLoop(time.Duration(*flushInterval) * time.Millisecond, incomingMessages, sigChan)
}

func mainLoop(flushInterval time.Duration, incomingMessages <-chan string, signal <-chan os.Signal) {
    messages := make([]string, 0, 1000)
    flushTicker := time.NewTicker(flushInterval) 

    for {
        select {
            case message := <-incomingMessages:
                messages = append(messages, message)

            case <-flushTicker.C:
                flushMessages(messages)
                messages = make([]string, 0, 1000)

            case <-signal:
                log.Print("Shutting down the server")                
                return
        }
    }
}

func udpListener(serverAddress string, incomingMessages chan<- string) {
    udpAddr, err := net.ResolveUDPAddr("udp", serverAddress)

    if (err != nil) {
        log.Fatalf("Error resolving UDP server address: %s", err)
    }

    udpConn, err := net.ListenUDP("udp", udpAddr)

    if (err != nil) {        
        log.Fatalf("Error listening UDP: %s", err)
    }

    defer udpConn.Close()

    log.Printf("Listening for UDP connections on %s", udpAddr)

    readMessages(udpConn, incomingMessages) 
}

func tcpListener(serverAddress string, incomingMessages chan<- string) {
    tcpAddr, err := net.ResolveTCPAddr("tcp", serverAddress)

    if (err != nil) {
        log.Fatalf("Error resolving TCP server address: %s", err)
    }

    tcpListener, err := net.ListenTCP("tcp", tcpAddr)

    if (err != nil) {        
        log.Fatalf("Error listening TCP: %s", err)
    }

    defer tcpListener.Close()

    log.Printf("Listening for TCP connections on %s", tcpAddr)

    for {
        tcpConn, err := tcpListener.AcceptTCP()

        if (err != nil) {
            log.Fatalf("Error accepting TCP connection: %s", err)
        }

        go readMessages(tcpConn, incomingMessages)
    }
}

func readMessages(src io.ReadCloser, incomingMessages chan<- string) {
    defer src.Close()    

    for {
        buf := make([]byte, MAX_READ_SIZE)

        if _, err := src.Read(buf); err != nil {
            if (err != io.EOF) {
                log.Printf("Error reading message: %s", err)
            }

            break;
        }

        log.Println("incoming: ", string(buf))
        incomingMessages <- string(buf)
    }
}

func flushMessages(messages []string) {
    log.Println("Flushing messages")

    for _, message := range messages {
        log.Println(message)
    }
}