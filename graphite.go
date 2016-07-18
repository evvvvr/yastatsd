package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/evvvvr/yastatsd/internal/metric"
	"github.com/evvvvr/yastatsd/internal/util"
)

func flushMetrics(deadline time.Duration, m *metric.CalculatedMetrics, graphiteIPV6 bool, graphiteAddress string) {
	var buf *bytes.Buffer = bytes.NewBuffer([]byte{})
	ts := time.Now().Unix()

	for bucket, counter := range m.Counters {
		valStr := util.FormatFloat(counter.Value)
		rateStr := util.FormatFloat(counter.Rate)
		fmt.Fprintf(buf, "%s.count %s %d\n", bucket, valStr, ts)
		fmt.Fprintf(buf, "%s.rate %s %d\n", bucket, rateStr, ts)
	}

	for bucket, timer := range m.Timers {
		lowerStr := util.FormatFloat(timer.Lower)
		upperStr := util.FormatFloat(timer.Upper)
		countStr := util.FormatFloat(timer.Count)
		countPsStr := util.FormatFloat(timer.CountPerSecond)
		sumStr := util.FormatFloat(timer.Sum)
		meanStr := util.FormatFloat(timer.Mean)
		medianStr := util.FormatFloat(timer.Median)
		stdStr := util.FormatFloat(timer.StandardDeviation)

		fmt.Fprintf(buf, "%s.lower %s %d\n", bucket, lowerStr, ts)
		fmt.Fprintf(buf, "%s.upper %s %d\n", bucket, upperStr, ts)
		fmt.Fprintf(buf, "%s.count %s %d\n", bucket, countStr, ts)
		fmt.Fprintf(buf, "%s.count_ps %s %d\n", bucket, countPsStr, ts)
		fmt.Fprintf(buf, "%s.sum %s %d\n", bucket, sumStr, ts)
		fmt.Fprintf(buf, "%s.mean %s %d\n", bucket, meanStr, ts)
		fmt.Fprintf(buf, "%s.median %s %d\n", bucket, medianStr, ts)
		fmt.Fprintf(buf, "%s.std %s %d\n", bucket, stdStr, ts)

		for pct, pctData := range timer.PercentilesData {
			pctStr := util.FormatFloat(pct)
			pctStr = strings.Replace(strings.Replace(pctStr, ".", "_", -1), "-", "top", -1)
			upperStr := util.FormatFloat(pctData.Upper)
			sumStr := util.FormatFloat(pctData.Sum)
			meanStr := util.FormatFloat(pctData.Mean)

			fmt.Fprintf(buf, "%s.count_%s %d %d\n", bucket, pctStr, pctData.Count, ts)

			if util.CmpToZero(pct) > 0 {
				fmt.Fprintf(buf, "%s.upper_%s %s %d\n", bucket, pctStr, upperStr, ts)
			} else {
				fmt.Fprintf(buf, "%s.lower_%s %s %d\n", bucket, pctStr, upperStr, ts)
			}

			fmt.Fprintf(buf, "%s.sum_%s %s %d\n", bucket, pctStr, sumStr, ts)
			fmt.Fprintf(buf, "%s.mean_%s %s %d\n", bucket, pctStr, meanStr, ts)
		}
	}

	for bucket, gauge := range m.Gauges {
		valStr := util.FormatFloat(gauge)
		fmt.Fprintf(buf, "%s %s %d\n", bucket, valStr, ts)
	}

	for bucket, set := range m.Sets {
		fmt.Fprintf(buf, "%s %d %d\n", bucket, len(set), ts)
	}

	network := "tcp"
	if graphiteIPV6 {
		network = "tcp6"
	}

	client, err := net.Dial(network, graphiteAddress)
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
