package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/go-yaml/yaml"

	"github.com/evvvvr/yastatsd/internal/metric"
	"github.com/evvvvr/yastatsd/internal/parser"
)

type Config struct {
	UdpServerAddress    string `yaml:"udpServerAddress"`
	TcpServerAddress    string `yaml:"tcpServerAddress"`
	FlushInterval       int    `yaml:"flushInterval"`
	GraphiteAddress     string `yaml:"graphiteAddress"`
	GraphiteIPV6        bool   `yaml:"graphiteIPV6"`
	PrefixStats         string `yaml:"prefixStats"`
	SanitizeBucketNames bool   `yaml:"sanitizeBucketNames"`
	Percentiles         []float64
	DeleteCounters      bool `yaml:"deleteCounters"`
	DeleteTimers        bool `yaml:"deleteTimers"`
	DeleteGauges        bool `yaml:"deleteGauges"`
	DeleteSets          bool `yaml:"deleteSets"`
	Debug               bool
}

const (
	DEFAULT_UDP_ADDRESS                 = ":8125"
	DEFAULT_FLUSH_INTERVAL_MILLISECONDS = 10000
	MAX_UNPROCESSED_INCOMING_METRICS    = 1000
	MAX_READ_SIZE                       = 65535
	DEFAULT_TIMER_CAPACITY              = 100
	PACKETS_RECIEVED_COUNTER            = "packets_recieved"
	METRICS_RECIEVED_COUNTER            = "metrics_recieved"
	ERRORS_COUNTER                      = "bad_lines_seen"
)

var (
	config = Config{
		UdpServerAddress:    DEFAULT_UDP_ADDRESS,
		TcpServerAddress:    "",
		FlushInterval:       DEFAULT_FLUSH_INTERVAL_MILLISECONDS,
		GraphiteAddress:     "",
		PrefixStats:         "statsd",
		SanitizeBucketNames: true,
		Percentiles:         []float64{90.0}}

	metrics = metric.Metrics{
		Counters:    make(map[string]float64),
		Timers:      make(map[string][]float64),
		TimersCount: make(map[string]float64),
		Gauges:      make(map[string]float64),
		Sets:        make(map[string]map[string]struct{})}

	packetsRecievedCounter string
	metricsRecievedCounter string
	errorsCounter          string
)

func main() {
	configFile, err := ioutil.ReadFile("config.yaml")
	if err != nil {
		log.Fatalf("Error reading config file: %s", err)
	}

	err = yaml.Unmarshal(configFile, &config)
	if err != nil {
		log.Fatalf("Error reading config file: %s", err)
	}

	packetsRecievedCounter := processBucketName(PACKETS_RECIEVED_COUNTER)
	metricsRecievedCounter := processBucketName(METRICS_RECIEVED_COUNTER)
	errorsCounter := processBucketName(ERRORS_COUNTER)

	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, os.Interrupt)

	incomingMetrics := make(chan *metric.Metric, MAX_UNPROCESSED_INCOMING_METRICS)

	metrics.Counters[packetsRecievedCounter] = 0
	metrics.Counters[metricsRecievedCounter] = 0
	metrics.Counters[errorsCounter] = 0

	go udpListener(incomingMetrics)

	if config.TcpServerAddress != "" {
		go tcpListener(incomingMetrics)
	}

	mainLoop(incomingMetrics, sigChan)
}

func mainLoop(incomingMetrics <-chan *metric.Metric, signal <-chan os.Signal) {
	flushIntervalDuration := time.Duration(config.FlushInterval) * time.Millisecond
	flushTicker := time.NewTicker(flushIntervalDuration)

	for {
		select {
		case metric := <-incomingMetrics:
			metric.Bucket = processBucketName(metric.Bucket)
			saveMetric(metric)

		case <-flushTicker.C:
			calculatedMetrics := metric.Calculate(&metrics, config.FlushInterval, config.Percentiles)

			if config.GraphiteAddress != "" {
				if config.Debug {
					log.Printf("Flushing metrics to Graphite server: %s", config.GraphiteAddress)
				}

				flushMetrics(flushIntervalDuration, calculatedMetrics, config.GraphiteIPV6, config.GraphiteAddress)
			}

			if config.Debug {
				debugPrint(calculatedMetrics)
			}

			resetMetrics()

		case <-signal:
			log.Print("Shutting down the server")
			return
		}
	}
}

func udpListener(incomingMetrics chan<- *metric.Metric) {
	udpAddr, err := net.ResolveUDPAddr("udp", config.UdpServerAddress)

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

func tcpListener(incomingMetrics chan<- *metric.Metric) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", config.TcpServerAddress)

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

func readMetrics(src io.ReadCloser, incomingMetrics chan<- *metric.Metric) {
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

		metrics.Counters[packetsRecievedCounter]++
		parsedMetrics, errors := parser.Parse(string(buf[:numRead]))

		metrics.Counters[metricsRecievedCounter] += float64(len(parsedMetrics))
		metrics.Counters[errorsCounter] += float64(len(errors))

		for _, metric := range parsedMetrics {
			incomingMetrics <- metric
		}
	}
}

func processBucketName(bucket string) string {
	if config.SanitizeBucketNames {
		bucket = sanitizeBucketName(bucket)
	}

	if config.PrefixStats != "" {
		bucket = fmt.Sprintf("%s.%s", config.PrefixStats, bucket)
	}

	return bucket
}

func sanitizeBucketName(bucket string) string {
	res := make([]byte, len(bucket))
	var resLength int

	for i := 0; i < len(bucket); i++ {
		c := bucket[i]
		switch {
		case (c >= byte('a') && c <= byte('z')) || (c >= byte('A') && c <= byte('Z')) || (c >= byte('0') && c <= byte('9')) || c == byte('-') || c == byte('.') || c == byte('_'):
			res[resLength] = c
			resLength++
		case c == byte(' '):
			res[resLength] = byte('_')
			resLength++
		case c == byte('/'):
			res[resLength] = byte('-')
			resLength++
		}
	}

	return string(res[:resLength])
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

func resetMetrics() {
	if config.DeleteCounters {
		metrics.Counters = make(map[string]float64)

		metrics.Counters[packetsRecievedCounter] = 0
		metrics.Counters[metricsRecievedCounter] = 0
		metrics.Counters[errorsCounter] = 0
	} else {
		setMetricsToZeroes(metrics.Counters)
	}

	if config.DeleteTimers {
		metrics.Timers = make(map[string][]float64)
	} else {
		for bucket, _ := range metrics.Timers {
			metrics.Timers[bucket] = []float64{}
		}
	}

	if config.DeleteGauges {
		metrics.Gauges = make(map[string]float64)
	}

	if config.DeleteSets {
		metrics.Sets = make(map[string]map[string]struct{})
	} else {
		for bucket, _ := range metrics.Sets {
			metrics.Sets[bucket] = make(map[string]struct{})
		}
	}
}

func setMetricsToZeroes(m map[string]float64) {
	for bucket, _ := range m {
		m[bucket] = 0
	}
}
