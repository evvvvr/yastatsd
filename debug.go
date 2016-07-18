package main

import (
	"bytes"
	"fmt"
	"log"
	"strings"

	"github.com/evvvvr/yastatsd/internal/metric"
	"github.com/evvvvr/yastatsd/internal/util"
)

func debugPrint(m *metric.CalculatedMetrics) {
	var buf = bytes.NewBufferString("Counters:\n")

	for _, bucket := range util.SortMapKeys(m.Counters) {
		counter := m.Counters[bucket]

		counterString := fmt.Sprintf("%s: value: %s, rate: %s\n",
			bucket,
			util.FormatFloat(counter.Value),
			util.FormatFloat(counter.Rate))

		buf.WriteString(counterString)
	}

	buf.WriteString("Timers:\n")
	for _, bucket := range util.SortMapKeys(m.Timers) {
		timer := m.Timers[bucket]
		timerBuf := bytes.NewBufferString(fmt.Sprintf("%s: ", bucket))
		timerPoints := make([]string, len(timer.Points))

		for i, point := range timer.Points {
			timerPoints[i] = util.FormatFloat(point)
		}

		timerPointsStr := fmt.Sprintf("[%s], ", strings.Join(timerPoints, ", "))
		timerBuf.WriteString(timerPointsStr)

		lowerStr := util.FormatFloat(timer.Lower)
		uppersStr := util.FormatFloat(timer.Upper)
		countStr := util.FormatFloat(timer.Count)
		countPerSecStr := util.FormatFloat(timer.CountPerSecond)
		sumStr := util.FormatFloat(timer.Sum)
		meanStr := util.FormatFloat(timer.Mean)
		medianStr := util.FormatFloat(timer.Median)
		standardDeviationStr := util.FormatFloat(timer.StandardDeviation)
		timerBuf.WriteString(fmt.Sprintf("lower: %s, upper: %s, count: %s, count per second: %s, sum: %s, mean: %s, median: %s, standard deviation: %s", lowerStr, uppersStr, countStr, countPerSecStr, sumStr, meanStr, medianStr, standardDeviationStr))

		pctStrings := make([]string, 0, len(timer.Points))
		for percentile, pctData := range timer.PercentilesData {
			percentileStr := util.FormatFloat(percentile)
			pctCountStr := fmt.Sprintf("%d", pctData.Count)

			pctLimitStr := "upper"
			if util.CmpToZero(percentile) < 0 {
				pctLimitStr = "lower"
			}

			pctUpperStr := util.FormatFloat(pctData.Upper)
			pctSumStr := util.FormatFloat(pctData.Sum)
			pctMeanStr := util.FormatFloat(pctData.Mean)

			pctStrings = append(pctStrings, fmt.Sprintf("%s: count: %s, %s: %s, sum: %s, mean: %s", percentileStr, pctCountStr, pctLimitStr, pctUpperStr, pctSumStr, pctMeanStr))
		}

		timerBuf.WriteString(fmt.Sprintf(", percentiles: [%s]", strings.Join(pctStrings, "; ")))
		buf.WriteString(timerBuf.String() + "\n")
	}

	buf.WriteString("Gauges:\n")
	for _, bucket := range util.SortMapKeys(m.Gauges) {
		gauge := m.Gauges[bucket]
		gaugeString := fmt.Sprintf("%s: %s\n", bucket, util.FormatFloat(gauge))
		buf.WriteString(gaugeString)
	}

	buf.WriteString("Sets:\n")
	for _, bucket := range util.SortMapKeys(m.Sets) {
		set := m.Sets[bucket]
		buf.WriteString(fmt.Sprintf("%s: ", bucket))
		keys := util.SortMapKeys(set)
		buf.WriteString(strings.Join(keys, ", ") + "\n")
	}

	log.Print(buf.String())
}
