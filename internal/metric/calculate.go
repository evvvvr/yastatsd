package metric

import (
	"math"
	"sort"

	"github.com/evvvvr/yastatsd/internal/util"
)

type CalculatedMetrics struct {
	Counters map[string]CounterData
	Timers   map[string]TimerData
	Gauges   map[string]float64
	Sets     map[string]map[string]struct{}
}

type CounterData struct {
	Value float64
	Rate  float64
}

type TimerData struct {
	Points            []float64
	Lower             float64
	Upper             float64
	Count             float64
	CountPerSecond    float64
	Sum               float64
	Mean              float64
	Median            float64
	StandardDeviation float64
	PercentilesData   map[float64]PercentileData
}

type PercentileData struct {
	Count int
	Upper float64
	Sum   float64
	Mean  float64
}

func Calculate(m *Metrics, flushInterval int, percentiles []float64) *CalculatedMetrics {
	res := CalculatedMetrics{Counters: make(map[string]CounterData),
		Timers: make(map[string]TimerData),
		Gauges: m.Gauges,
		Sets:   m.Sets}

	for bucket, counter := range m.Counters {
		res.Counters[bucket] = CounterData{Value: counter,
			Rate: counter / float64(flushInterval/1000)}
	}

	for bucket, timer := range m.Timers {
		points := timer
		sort.Float64s(points)
		seen := len(points)

		if seen > 0 {
			lower := points[0]
			upper := points[seen-1]
			count := m.TimersCount[bucket]

			countPerSecond := m.TimersCount[bucket] / float64(flushInterval/1000)

			cumulativeValues := []float64{lower}
			for i := 1; i < seen; i++ {
				cumulativeValues = append(cumulativeValues,
					points[i]+cumulativeValues[i-1])
			}

			sum := cumulativeValues[seen-1]
			mean := sum / float64(seen)
			mid := seen / 2

			median := float64(0)
			if seen%2 == 1 {
				median = points[mid]
			} else {
				median = (points[mid-1] + points[mid]) / 2.0
			}

			numerator := float64(0)

			for _, val := range points {
				numerator += math.Pow(val-mean, 2.0)
			}

			standardDeviation := math.Sqrt(numerator / float64(seen))

			percentilesData := make(map[float64]PercentileData)

			for _, percentile := range percentiles {
				pctSum := points[0]
				pctMean := points[0]
				pctUpper := points[seen-1]

				if len(points) > 1 {
					pctCount := int(math.Floor(((math.Abs(percentile) /
						100.0) * float64(seen)) + 0.5))

					if pctCount == 0 {
						continue
					}

					if util.CmpToZero(percentile) > 0 {
						pctUpper = points[pctCount-1]
						pctSum = cumulativeValues[pctCount-1]
					} else {
						pctUpper = points[seen-pctCount]
						pctSum = cumulativeValues[seen-1] - cumulativeValues[seen-pctCount-1]
					}

					pctMean = pctSum / float64(pctCount)

					percentilesData[percentile] = PercentileData{Count: pctCount,
						Upper: pctUpper,
						Sum:   pctSum,
						Mean:  pctMean}
				}
			}

			res.Timers[bucket] = TimerData{Points: points,
				Lower:             lower,
				Upper:             upper,
				Count:             count,
				CountPerSecond:    countPerSecond,
				Sum:               sum,
				Mean:              mean,
				Median:            median,
				StandardDeviation: standardDeviation,
				PercentilesData:   percentilesData}
		} else {
			res.Timers[bucket] = TimerData{Points: points}
		}
	}

	return &res
}
