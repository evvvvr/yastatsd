package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/evvvvr/yastatsd/internal/metric"
	"github.com/evvvvr/yastatsd/internal/parser"
)

const MAX_UNPROCESSED_INCOMING_METRICS = 1000
const MAX_READ_SIZE = 65535
const DEFAULT_UDP_ADDRESS = ":8125"
const DEFAULT_FLUSH_INTERVAL_MILLISECONDS = 10000

var counters = make(map[string]float64)
var timers = make(map[string][]float64)
var timersCount = make(map[string]float64)
var gauges = make(map[string]float64)
var sets = make(map[string]map[string]struct{})

func main() {
	udpServerAddress := flag.String("udpAddr", DEFAULT_UDP_ADDRESS, "UDP server address")
	tcpServerAddress := flag.String("tcpAddr", "", "TCP server address")
	flushInterval := flag.Int("flushInterval", DEFAULT_FLUSH_INTERVAL_MILLISECONDS,
		"Metrics flush interval (milliseconds)")

	flag.Parse()

	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, os.Interrupt)

	incomingMetrics := make(chan *metric.Metric, MAX_UNPROCESSED_INCOMING_METRICS)
	errorsChan := make(chan *error, MAX_UNPROCESSED_INCOMING_METRICS)

	go udpListener(*udpServerAddress, incomingMetrics, errorsChan)

	if *tcpServerAddress != "" {
		go tcpListener(*tcpServerAddress, incomingMetrics, errorsChan)
	}

	mainLoop(time.Duration(*flushInterval)*time.Millisecond, incomingMetrics, errorsChan, sigChan)
}

func mainLoop(flushInterval time.Duration, incomingMetrics <-chan *metric.Metric, errors <-chan *error, signal <-chan os.Signal) {
	metrics := make([]*metric.Metric, 0, 1000)
	flushTicker := time.NewTicker(flushInterval)
	errorCount := 0

	for {
		select {
		case metric := <-incomingMetrics:
			processMetric(metric)
			// metrics = append(metrics, metric)

		case <-errors:
			errorCount += 1

		case <-flushTicker.C:
			flushMetrics(metrics, errorCount)
			errorCount = 0
			metrics = make([]*metric.Metric, 0, 1000)

		case <-signal:
			log.Print("Shutting down the server")
			return
		}
	}
}

func udpListener(serverAddress string, incomingMetrics chan<- *metric.Metric, errors chan<- *error) {
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

	readMetrics(udpConn, incomingMetrics, errors)
}

func tcpListener(serverAddress string, incomingMetrics chan<- *metric.Metric, errors chan<- *error) {
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

		go readMetrics(tcpConn, incomingMetrics, errors)
	}
}

func readMetrics(src io.ReadCloser, incomingMetrics chan<- *metric.Metric, errorsChan chan<- *error) {
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

		metrics, errors := parser.Parse(string(buf[:numRead]))

		for _, metric := range metrics {
			incomingMetrics <- metric
		}

		for _, error := range errors {
			errorsChan <- &error
		}
	}
}

func flushMetrics(metrics []*metric.Metric, errorCount int) {
	log.Printf("Flushing metrics. Error count is %d\n", errorCount)

	var buf = bytes.NewBufferString("Counters: ")
	for bucket, counter := range counters {
		buf.WriteString(fmt.Sprintf("%s: %s | ", bucket, strconv.FormatFloat(counter, 'f', -1, 64)))
	}

	log.Printf("%s\n", buf.String())

	buf = bytes.NewBufferString("Timers: ")
	for bucket, timer := range timers {
		buf.WriteString(fmt.Sprintf("%s: ", bucket))

		for _, val := range timer {
			buf.WriteString(fmt.Sprintf("%s ", strconv.FormatFloat(val, 'f', -1, 64)))
		}

		buf.WriteString(fmt.Sprintf("@%s", strconv.FormatFloat(timersCount[bucket], 'f', -1, 64)))

		buf.WriteString(" | ")
	}

	log.Printf("%s\n", buf.String())

	buf = bytes.NewBufferString("Gauges: ")
	for bucket, gauge := range gauges {
		buf.WriteString(fmt.Sprintf("%s: %s | ", bucket, strconv.FormatFloat(gauge, 'f', -1, 64)))
	}

	log.Printf("%s\n", buf.String())

	buf = bytes.NewBufferString("Sets: ")

	for bucket, set := range sets {
		buf.WriteString(fmt.Sprintf("%s: ", bucket))

		for val, _ := range set {
			buf.WriteString(fmt.Sprintf("%s ", val))
		}

		buf.WriteString("| ")
	}

	log.Printf("%s\n", buf.String())

	counters = make(map[string]float64)
	timers = make(map[string][]float64)
	timersCount = make(map[string]float64)
	gauges = make(map[string]float64)
	sets = make(map[string]map[string]struct{})
}

func processMetric(m *metric.Metric) {
	switch m.Type {
	case metric.Counter:
		_, exists := counters[m.Bucket]

		if !exists {
			counters[m.Bucket] = 0
		}

		counters[m.Bucket] += m.FloatValue * float64(1/m.Sampling)

	case metric.Timer:
		_, exists := timers[m.Bucket]

		if !exists {
			timers[m.Bucket] = make([]float64, 0, 100)
		}

		timers[m.Bucket] = append(timers[m.Bucket], m.FloatValue)

		if !exists {
			timersCount[m.Bucket] = 0
		}

		timersCount[m.Bucket] += float64(1 / m.Sampling)

	case metric.Gauge:
		_, exists := gauges[m.Bucket]

		if !exists {
			gauges[m.Bucket] = 0
		}

		if m.DoesGaugeHasOperation {
			gauges[m.Bucket] += m.FloatValue
		} else {
			gauges[m.Bucket] = m.FloatValue
		}

	case metric.Set:
		_, exists := sets[m.Bucket]

		if !exists {
			sets[m.Bucket] = make(map[string]struct{})
		}

		sets[m.Bucket][m.StringValue] = struct{}{}
	}
}
