package main

import (
	// "bytes"
	"flag"
	// "fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	// "strconv"
	// "strings"
	"time"

	"github.com/evvvvr/yastatsd/internal/metric"
	"github.com/evvvvr/yastatsd/internal/parser"
)

const MAX_UNPROCESSED_INCOMING_METRICS = 1000
const MAX_READ_SIZE = 65535
const DEFAULT_UDP_ADDRESS = ":8125"
const DEFAULT_FLUSH_INTERVAL_MILLISECONDS = 10000
const DEFAULT_TIMER_CAPACITY = 100

var errorCount = 0
var metrics = metric.Metrics{Counters: make(map[string]float64), Timers: make(map[string][]float64), TimersCount: make(map[string]float64), Gauges: make(map[string]float64), Sets: make(map[string]map[string]struct{})}
var percentiles = []float64{90.0}
var flushInterval *int

func main() {
	udpServerAddress := flag.String("udpAddr", DEFAULT_UDP_ADDRESS, "UDP server address")
	tcpServerAddress := flag.String("tcpAddr", "", "TCP server address")
	flushInterval = flag.Int("flushInterval", DEFAULT_FLUSH_INTERVAL_MILLISECONDS,
		"Metrics flush interval (milliseconds)")
	debug := flag.Bool("debug", false, "Should print metric values on flush")

	flag.Parse()

	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, os.Interrupt)

	incomingMetrics := make(chan *metric.Metric, MAX_UNPROCESSED_INCOMING_METRICS)
	errorsChan := make(chan *error, MAX_UNPROCESSED_INCOMING_METRICS)

	go udpListener(*udpServerAddress, incomingMetrics, errorsChan)

	if *tcpServerAddress != "" {
		go tcpListener(*tcpServerAddress, incomingMetrics, errorsChan)
	}

	mainLoop(*flushInterval, incomingMetrics, errorsChan, sigChan, *debug)
}

func mainLoop(flushInterval int, incomingMetrics <-chan *metric.Metric, errors <-chan *error, signal <-chan os.Signal, debug bool) {
	flushTicker := time.NewTicker(time.Duration(flushInterval) * time.Millisecond)

	for {
		select {
		case metric := <-incomingMetrics:
			saveMetric(metric)

		case <-errors:
			errorCount++

		case <-flushTicker.C:
			metric.Calculate(&metrics, flushInterval, percentiles)

			flushMetrics(errorCount)

			/*			if debug {
							outputMetricsForDebug()
						}
			*/
			resetMetrics()

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

func flushMetrics(errorCount int) {
	log.Printf("Flushing metrics. Error count is %d\n", errorCount)
}

// func outputMetricsForDebug() {
// 	var buf = bytes.NewBufferString("Counters: ")

// 	countersStrings := make([]string, 0, len(counters))
// 	for bucket, counter := range counters {
// 		counterString := fmt.Sprintf("%s: %s", bucket, strconv.FormatFloat(counter, 'f', -1, 64))

// 		countersStrings = append(countersStrings, counterString)
// 	}

// 	buf.WriteString(strings.Join(countersStrings, ", "))

// 	buf.WriteString("\nTimers: ")
// 	timersStrings := make([]string, 0, len(timers))
// 	for bucket, timer := range timers {
// 		timerBuf := bytes.NewBufferString(fmt.Sprintf("%s: ", bucket))

// 		timerStrings := make([]string, 0, len(timer))

// 		for _, val := range timer {
// 			timerStrings = append(timerStrings, strconv.FormatFloat(val, 'f', -1, 64))
// 		}

// 		timerBuf.WriteString(strings.Join(timerStrings, ", "))
// 		timerBuf.WriteString(fmt.Sprintf(" @%s", strconv.FormatFloat(timersCount[bucket], 'f', -1, 64)))

// 		timersStrings = append(timersStrings, timerBuf.String())
// 	}
// 	buf.WriteString(strings.Join(timersStrings, "; "))

// 	buf.WriteString("\nGauges: ")
// 	gaugesStrings := make([]string, 0, len(gauges))
// 	for bucket, gauge := range gauges {
// 		gaugesStrings = append(gaugesStrings, fmt.Sprintf("%s: %s", bucket, strconv.FormatFloat(gauge, 'f', -1, 64)))
// 	}
// 	buf.WriteString(strings.Join(gaugesStrings, ", "))

// 	buf.WriteString("\nSets:")
// 	for bucket, set := range sets {
// 		buf.WriteString(fmt.Sprintf(" %s: ", bucket))

// 		keys := make([]string, 0, len(set))
// 		for key := range set {
// 			keys = append(keys, key)
// 		}

// 		buf.WriteString(strings.Join(keys, "|"))
// 	}

// 	fmt.Println(buf.String())
// }

func resetMetrics() {
	errorCount = 0
	metrics = metric.Metrics{Counters: make(map[string]float64), Timers: make(map[string][]float64), TimersCount: make(map[string]float64), Gauges: make(map[string]float64), Sets: make(map[string]map[string]struct{})}
}

func saveMetric(m *metric.Metric) {
	switch m.Type {
	case metric.Counter:
		_, exists := metrics.Counters[m.Bucket]

		if !exists {
			metrics.Counters[m.Bucket] = 0
		}

		metrics.Counters[m.Bucket] += m.FloatValue * float64(1/m.Sampling)

	case metric.Timer:
		_, exists := metrics.Timers[m.Bucket]

		if !exists {
			metrics.Timers[m.Bucket] = make([]float64, 0, DEFAULT_TIMER_CAPACITY)
		}

		metrics.Timers[m.Bucket] = append(metrics.Timers[m.Bucket], m.FloatValue)

		if !exists {
			metrics.TimersCount[m.Bucket] = 0
		}

		metrics.TimersCount[m.Bucket] += float64(1 / m.Sampling)

	case metric.Gauge:
		_, exists := metrics.Gauges[m.Bucket]

		if !exists {
			metrics.Gauges[m.Bucket] = 0
		}

		if m.DoesGaugeHaveOperation {
			metrics.Gauges[m.Bucket] += m.FloatValue
		} else {
			metrics.Gauges[m.Bucket] = m.FloatValue
		}

	case metric.Set:
		_, exists := metrics.Sets[m.Bucket]

		if !exists {
			metrics.Sets[m.Bucket] = make(map[string]struct{})
		}

		metrics.Sets[m.Bucket][m.StringValue] = struct{}{}
	}
}
