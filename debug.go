package main

import (
	"bytes"
	"fmt"
	"log"
	"math/big"
	"strconv"
	"strings"

	"github.com/evvvvr/yastatsd/internal/metric"
)

var bigZero = big.NewFloat(0)

func debugPrint(m *metric.CalculatedMetrics) {
	var buf = bytes.NewBufferString("Counters:\n")

	for _, bucket := range SortMapKeys(m.Counters) {
		counter := m.Counters[bucket]

		counterString := fmt.Sprintf("%s: value: %s, rate: %s\n",
			bucket,
			strconv.FormatFloat(counter.Value, 'f', -1, 64),
			strconv.FormatFloat(counter.Rate, 'f', -1, 64))

		buf.WriteString(counterString)
	}

	buf.WriteString("Timers:\n")
	for _, bucket := range SortMapKeys(m.Timers) {
		timer := m.Timers[bucket]
		timerBuf := bytes.NewBufferString(fmt.Sprintf("%s: ", bucket))
		timerPoints := make([]string, len(timer.Points))

		for i, point := range timer.Points {
			timerPoints[i] = strconv.FormatFloat(point, 'f', -1, 64)
		}

		timerPointsStr := fmt.Sprintf("[%s], ", strings.Join(timerPoints, ", "))
		timerBuf.WriteString(timerPointsStr)

		lowerStr := strconv.FormatFloat(timer.Lower, 'f', -1, 64)
		uppersStr := strconv.FormatFloat(timer.Upper, 'f', -1, 64)
		countStr := strconv.FormatFloat(timer.Count, 'f', -1, 64)
		countPerSecStr := strconv.FormatFloat(timer.CountPerSecond, 'f', -1, 64)
		sumStr := strconv.FormatFloat(timer.Sum, 'f', -1, 64)
		meanStr := strconv.FormatFloat(timer.Mean, 'f', -1, 64)
		medianStr := strconv.FormatFloat(timer.Median, 'f', -1, 64)
		standardDeviationStr := strconv.FormatFloat(timer.StandardDeviation, 'f', -1, 64)
		timerBuf.WriteString(fmt.Sprintf("lower: %s, upper: %s, count: %s, count per second: %s, sum: %s, mean: %s, median: %s, standard deviation: %s", lowerStr, uppersStr, countStr, countPerSecStr, sumStr, meanStr, medianStr, standardDeviationStr))

		pctStrings := make([]string, 0, len(timer.Points))
		for percentile, pctData := range timer.PercentilesData {
			percentileStr := strconv.FormatFloat(percentile, 'f', -1, 64)
			pctCountStr := fmt.Sprintf("%d", pctData.Count)
			pctLimitStr := "upper"

			if big.NewFloat(percentile).Cmp(bigZero) < 0 {
				pctLimitStr = "lower"
			}

			pctUpperStr := strconv.FormatFloat(pctData.Upper, 'f', -1, 64)
			pctSumStr := strconv.FormatFloat(pctData.Sum, 'f', -1, 64)
			pctMeanStr := strconv.FormatFloat(pctData.Mean, 'f', -1, 64)

			pctStrings = append(pctStrings, fmt.Sprintf("%s: count: %s, %s: %s, sum: %s, mean: %s", percentileStr, pctCountStr, pctLimitStr, pctUpperStr, pctSumStr, pctMeanStr))
		}

		timerBuf.WriteString(fmt.Sprintf(", percentiles: [%s]", strings.Join(pctStrings, "; ")))
		buf.WriteString(timerBuf.String() + "\n")
	}

	buf.WriteString("Gauges:\n")
	for _, bucket := range SortMapKeys(m.Gauges) {
		gauge := m.Gauges[bucket]
		gaugeString := fmt.Sprintf("%s: %s\n", bucket, strconv.FormatFloat(gauge, 'f', -1, 64))
		buf.WriteString(gaugeString)
	}

	buf.WriteString("Sets:\n")
	for _, bucket := range SortMapKeys(m.Sets) {
		set := m.Sets[bucket]
		buf.WriteString(fmt.Sprintf("%s: ", bucket))
		keys := SortMapKeys(set)
		buf.WriteString(strings.Join(keys, ", ") + "\n")
	}

	log.Print(buf.String())
}
