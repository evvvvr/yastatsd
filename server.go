package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/evvvvr/yastatsd/internal/metric"
	"github.com/evvvvr/yastatsd/internal/parser"
)

type calculatedMetrics struct {
	Counters map[string]counterData
	Timers   map[string]timerData
	Gauges   map[string]float64
	Sets     map[string]map[string]struct{}
}

type counterData struct {
	Value float64
	Rate  float64
}

type timerData struct {
	Points            []float64
	Lower             float64
	Upper             float64
	Count             float64
	CountPerSecond    float64
	Sum               float64
	Mean              float64
	Median            float64
	StandardDeviation float64
	standardDeviation float64
	PercentilesData   map[float64]percentileData
}

type percentileData struct {
	Count int
	Max   float64
	Sum   float64
	Mean  float64
}

const MAX_UNPROCESSED_INCOMING_METRICS = 1000
const MAX_READ_SIZE = 65535
const DEFAULT_UDP_ADDRESS = ":8125"
const DEFAULT_FLUSH_INTERVAL_MILLISECONDS = 10000

var errorCount = 0
var counters = make(map[string]float64)
var timers = make(map[string][]float64)
var timersCount = make(map[string]float64)
var gauges = make(map[string]float64)
var sets = make(map[string]map[string]struct{})
var percentiles = []float64{float64(90)}

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

	mainLoop(time.Duration(*flushInterval)*time.Millisecond, incomingMetrics, errorsChan, sigChan, *debug)
}

func mainLoop(flushInterval time.Duration, incomingMetrics <-chan *metric.Metric, errors <-chan *error, signal <-chan os.Signal, debug bool) {
	flushTicker := time.NewTicker(flushInterval)

	for {
		select {
		case metric := <-incomingMetrics:
			saveMetric(metric)

		case <-errors:
			errorCount++

		case <-flushTicker.C:
			calculateMetrics()

			flushMetrics(errorCount)

			if debug {
				outputMetricsForDebug()
			}

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

func calculateMetrics() calculatedMetrics {
	res := calculatedMetrics{Counters: make(map[string]counterData), Timers: make(map[string]timerData), Gauges: gauges, Sets: sets}

	for bucket, counter := range counters {
		res.Counters[bucket] = counterData{Value: counter, Rate: counter / float64(*flushInterval / 1000)}
	}

	for bucket, timer := range timers {
		points := timer

		if len(points) > 0 {
			lower := points[0]
			upper := points[len(points)-1]
			count := timersCount	[bucket]
			seen := len(points)
			countPerSecond := timersCount[bucket] / float64(*flushInterval / 1000)

			sum := float64(0)
			for _, val := range points {
				sum += val
			}

			mean := sum / float64(seen)
			mid := seen / 2.0

			median := float64(0)
			if seen%2 == 1 {
				median = points[mid]
			} else {
				median = (points[mid-1] + points[mid]) / 2.0
			}

			numerator := 0.0

			for _, val := range points {
				numerator += math.Pow(val-mean, 2.0)
			}

			standardDeviation := math.Sqrt(numerator / float64(seen))

			percentilesData := make(map[float64]percentileData)

			for _, percentile := range percentiles {
				pctSum := points[0]
				pctMean := points[0]
				pctMax := points[seen-1]

				if len(points) > 1 {
					pctCount := int(math.Floor(((math.Abs(percentile) / 100.0) * float64(count)) + 0.5))

					if pctCount == 0 {
						continue
					}

					if percentile > 0 {
						pctMax = points[pctCount-1]

						pctSum = 0

						for _, val := range points[:pctCount-1] {
							pctSum += val
						}
					} else {
						pctMax = points[seen-pctCount]

						pctSum = 0
						for _, val := range points[seen-pctCount-1:] {
							pctSum += val
						}
					}

					pctMean = pctSum / float64(pctCount)

					percentilesData[percentile] = percentileData{Count: pctCount, Max: pctMax, Sum: pctSum, Mean: pctMean}
				}
			}

			//TODO: create func to init new timerData instance
			res.Timers[bucket] = timerData{Points: points, Lower: lower, Upper: upper, Count: count, CountPerSecond: countPerSecond, Sum: sum, Mean: mean, Median: median, StandardDeviation: standardDeviation, PercentilesData: percentilesData}
		} else {
			res.Timers[bucket] = timerData{Points: points}
		}
	}

	return res
}

func flushMetrics(errorCount int) {
	log.Printf("Flushing metrics. Error count is %d\n", errorCount)
}

func outputMetricsForDebug() {
	var buf = bytes.NewBufferString("Counters: ")

	countersStrings := make([]string, 0, len(counters))
	for bucket, counter := range counters {
		counterString := fmt.Sprintf("%s: %s", bucket, strconv.FormatFloat(counter, 'f', -1, 64))

		countersStrings = append(countersStrings, counterString)
	}

	buf.WriteString(strings.Join(countersStrings, ", "))

	buf.WriteString("\nTimers: ")
	timersStrings := make([]string, 0, len(timers))
	for bucket, timer := range timers {
		timerBuf := bytes.NewBufferString(fmt.Sprintf("%s: ", bucket))

		timerStrings := make([]string, 0, len(timer))

		for _, val := range timer {
			timerStrings = append(timerStrings, strconv.FormatFloat(val, 'f', -1, 64))
		}

		timerBuf.WriteString(strings.Join(timerStrings, ", "))
		timerBuf.WriteString(fmt.Sprintf(" @%s", strconv.FormatFloat(timersCount[bucket], 'f', -1, 64)))

		timersStrings = append(timersStrings, timerBuf.String())
	}
	buf.WriteString(strings.Join(timersStrings, "; "))

	buf.WriteString("\nGauges: ")
	gaugesStrings := make([]string, 0, len(gauges))
	for bucket, gauge := range gauges {
		gaugesStrings = append(gaugesStrings, fmt.Sprintf("%s: %s", bucket, strconv.FormatFloat(gauge, 'f', -1, 64)))
	}
	buf.WriteString(strings.Join(gaugesStrings, ", "))

	buf.WriteString("\nSets:")
	for bucket, set := range sets {
		buf.WriteString(fmt.Sprintf(" %s: ", bucket))

		keys := make([]string, 0, len(set))
		for key := range set {
			keys = append(keys, key)
		}

		buf.WriteString(strings.Join(keys, "|"))
	}

	fmt.Println(buf.String())
}

func resetMetrics() {
	errorCount = 0
	counters = make(map[string]float64)
	timers = make(map[string][]float64)
	timersCount = make(map[string]float64)
	gauges = make(map[string]float64)
	sets = make(map[string]map[string]struct{})
}

func saveMetric(m *metric.Metric) {
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

		if m.DoesGaugeHaveOperation {
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
