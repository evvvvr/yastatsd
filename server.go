package main

import (
	"flag"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	// "strings"
	"time"
	// "github.com/evvvvr/yastatsd/internal/metric"
	// "github.com/evvvvr/yastatsd/internal/parser"
)

const MAX_UNPROCESSED_INCOMING_METRICS = 1000
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

	incomingMetrics := make(chan string, MAX_UNPROCESSED_INCOMING_METRICS)

	go udpListener(*udpServerAddress, incomingMetrics)

	if *tcpServerAddress != "" {
		go tcpListener(*tcpServerAddress, incomingMetrics)
	}

	mainLoop(time.Duration(*flushInterval)*time.Millisecond, incomingMetrics, sigChan)
}

func mainLoop(flushInterval time.Duration, incomingMetrics <-chan string, signal <-chan os.Signal) {
	metrics := make([]string, 0, 1000)
	flushTicker := time.NewTicker(flushInterval)

	for {
		select {
		case metric := <-incomingMetrics:
			metrics = append(metrics, metric)

		case <-flushTicker.C:
			flushMetrics(metrics)
			metrics = make([]string, 0, 1000)

		case <-signal:
			log.Print("Shutting down the server")
			return
		}
	}
}

func udpListener(serverAddress string, incomingMetrics chan<- string) {
	udpAddr, err := net.ResolveUDPAddr("udp", serverAddress)

	if err != nil {
		log.Fatalf("Error resolving UDP server address: %s", err)
	}

	udpConn, err := net.ListenUDP("udp", udpAddr)

	if err != nil {
		log.Fatalf("Error listening UDP: %s", err)
	}

	defer udpConn.Close()

	log.Printf("Listening for UDP connections on %s", udpAddr)

	readMetrics(udpConn, incomingMetrics)
}

func tcpListener(serverAddress string, incomingMetrics chan<- string) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", serverAddress)

	if err != nil {
		log.Fatalf("Error resolving TCP server address: %s", err)
	}

	tcpListener, err := net.ListenTCP("tcp", tcpAddr)

	if err != nil {
		log.Fatalf("Error listening TCP: %s", err)
	}

	defer tcpListener.Close()

	log.Printf("Listening for TCP connections on %s", tcpAddr)

	for {
		tcpConn, err := tcpListener.AcceptTCP()

		if err != nil {
			log.Fatalf("Error accepting TCP connection: %s", err)
		}

		go readMetrics(tcpConn, incomingMetrics)
	}
}

func readMetrics(src io.ReadCloser, incomingMetrics chan<- string) {
	defer src.Close()

	buf := make([]byte, MAX_READ_SIZE)

	for {
		numRead, err := src.Read(buf)

		if err != nil {
			if err != io.EOF {
				log.Printf("Error reading: %s", err)
			}

			break
		}

		incoming := buf[:numRead]
		log.Println("incoming: ", string(incoming))
		incomingMetrics <- string(incoming)
		// parseMetrics(string(buf), incomingMetrics)
	}
}

// func parseMetrics(msg string, incomingMetrics chan<- string) {
//     for _, msgLine := range strings.Split(msg, "\n") {
//         metric, err := parser.ParseLine(msgLine)

//         if (err != nil) {
//             log.Printf("Error reading metric: %s. Line is: %s", err, msgLine)
//             continue
//         }

//         incomingMetrics <- metric
//     }
// }

func flushMetrics(metrics []string) {
	log.Println("Flushing metrics")

	for _, metric := range metrics {
		log.Println(metric)
	}
}
