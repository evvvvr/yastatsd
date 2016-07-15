package main

import (
	"bytes"
	"fmt"
	"log"
	"math/big"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/evvvvr/yastatsd/internal/metric"
)

func flushMetrics(deadline time.Duration, m *metric.CalculatedMetrics, graphiteAddress string) {
	var buf *bytes.Buffer = bytes.NewBuffer([]byte{})
	ts := time.Now().Unix()

	for bucket, counter := range m.Counters {
		valStr := strconv.FormatFloat(counter.Value, 'f', -1, 64)
		rateStr := strconv.FormatFloat(counter.Rate, 'f', -1, 64)
		fmt.Fprintf(buf, "%s.count %s %d\n", bucket, valStr, ts)
		fmt.Fprintf(buf, "%s.rate %s %d\n", bucket, rateStr, ts)
	}

	for bucket, timer := range m.Timers {
		lowerStr := strconv.FormatFloat(timer.Lower, 'f', -1, 64)
		upperStr := strconv.FormatFloat(timer.Upper, 'f', -1, 64)
		countStr := strconv.FormatFloat(timer.Count, 'f', -1, 64)
		countPsStr := strconv.FormatFloat(timer.CountPerSecond, 'f', -1, 64)
		sumStr := strconv.FormatFloat(timer.Sum, 'f', -1, 64)
		meanStr := strconv.FormatFloat(timer.Mean, 'f', -1, 64)
		medianStr := strconv.FormatFloat(timer.Median, 'f', -1, 64)
		stdStr := strconv.FormatFloat(timer.StandardDeviation, 'f', -1, 64)

		fmt.Fprintf(buf, "%s.lower %s %d\n", bucket, lowerStr, ts)
		fmt.Fprintf(buf, "%s.upper %s %d\n", bucket, upperStr, ts)
		fmt.Fprintf(buf, "%s.count %s %d\n", bucket, countStr, ts)
		fmt.Fprintf(buf, "%s.count_ps %s %d\n", bucket, countPsStr, ts)
		fmt.Fprintf(buf, "%s.sum %s %d\n", bucket, sumStr, ts)
		fmt.Fprintf(buf, "%s.mean %s %d\n", bucket, meanStr, ts)
		fmt.Fprintf(buf, "%s.median %s %d\n", bucket, medianStr, ts)
		fmt.Fprintf(buf, "%s.std %s %d\n", bucket, stdStr, ts)

		for pct, pctData := range timer.PercentilesData {
			pctStr := strconv.FormatFloat(pct, 'f', -1, 64)
			pctStr = strings.Replace(strings.Replace(pctStr, ".", "_", -1), "-", "top", -1)
			upperStr := strconv.FormatFloat(pctData.Upper, 'f', -1, 64)
			sumStr := strconv.FormatFloat(pctData.Sum, 'f', -1, 64)
			meanStr := strconv.FormatFloat(pctData.Mean, 'f', -1, 64)

			fmt.Fprintf(buf, "%s.count_%s %d %d\n", bucket, pctStr, pctData.Count, ts)

			if big.NewFloat(pct).Cmp(bigZero) > 0 {
				fmt.Fprintf(buf, "%s.upper_%s %s %d\n", bucket, pctStr, upperStr, ts)
			} else {
				fmt.Fprintf(buf, "%s.lower_%s %s %d\n", bucket, pctStr, upperStr, ts)
			}

			fmt.Fprintf(buf, "%s.sum_%s %s %d\n", bucket, pctStr, sumStr, ts)
			fmt.Fprintf(buf, "%s.mean_%s %s %d\n", bucket, pctStr, meanStr, ts)
		}
	}

	for bucket, gauge := range m.Gauges {
		valStr := strconv.FormatFloat(gauge, 'f', -1, 64)
		fmt.Fprintf(buf, "%s %s %d\n", bucket, valStr, ts)
	}

	for bucket, set := range m.Sets {
		fmt.Fprintf(buf, "%s %d %d\n", bucket, len(set), ts)
	}

	client, err := net.Dial("tcp", graphiteAddress)
	if err != nil {
		log.Printf("Error connecting Graphite server %s - %s", graphiteAddress, err)
		return
	}

	defer client.Close()

	err = client.SetDeadline(time.Now().Add(deadline))
	if err != nil {
		log.Printf("Error connecting Graphite server %s - %s", graphiteAddress, err)
		return
	}

	_, err = client.Write(buf.Bytes())
	if err != nil {
		log.Printf("Error submitting metrics to Graphite server %s - %s", graphiteAddress, err)
		return
	}
}
