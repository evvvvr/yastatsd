package metric_test

import (
	"fmt"
	"math/big"
	"strconv"
	"testing"

	"github.com/evvvvr/yastatsd/internal/metric"
)

func TestCounterCalculation(t *testing.T) {
	const flushInterval = 10000

	counters := map[string]float64{"a.a": 2, "a.b": 2.71, "c": 0.25}
	expectedCounterValues := map[string]*big.Float{"a.a": big.NewFloat(2), "a.b": big.NewFloat(2.71), "c": big.NewFloat(0.25)}
	expectedCounterRates := map[string]*big.Float{"a.a": big.NewFloat(0.2), "a.b": big.NewFloat(0.271), "c": big.NewFloat(0.025)}

	calculatedMetrics := metric.Calculate(&metric.Metrics{Counters: counters}, flushInterval, []float64{})

	if len(calculatedMetrics.Counters) != 3 {
		t.Fatalf("Invalid calculated counters length. Expected: %d, Actual: %d", 3, len(calculatedMetrics.Counters))
	}

	for bucket, counterData := range calculatedMetrics.Counters {
		expectedVal := expectedCounterValues[bucket]
		calculatedVal := big.NewFloat(counterData.Value)

		if expectedVal.Cmp(calculatedVal) != 0 {
			expectedValFloat, _ := expectedVal.Float64()
			t.Fatalf("Invalid calculated counter value for counter %s. Expected: %s, Actual: %s", bucket, strconv.FormatFloat(expectedValFloat, 'f', -1, 64), strconv.FormatFloat(counterData.Value, 'f', -1, 64))
		}

		expectedRate := expectedCounterRates[bucket]
		calculatedRate := big.NewFloat(counterData.Rate)

		if expectedRate.Cmp(calculatedRate) != 0 {
			expectedRateFloat, _ := expectedRate.Float64()
			t.Fatalf("Invalid calculated counter rate for counter %s. Expected: %s, Actual: %s", bucket, strconv.FormatFloat(expectedRateFloat, 'f', -1, 64), strconv.FormatFloat(counterData.Rate, 'f', -1, 64))
		}
	}
}

func TestTimersCalculation(t *testing.T) {
	const flushInterval = 10000

	percentiles := []float64{90, -50}

	timers := map[string][]float64{"a.a": []float64{0.7, 0.5, 3.1}, "a.b": []float64{1, 2, 1, 0}, "c": []float64{}, "d": []float64{1.75}}
	timersCount := map[string]float64{"a.a": 6, "a.b": 12, "c": 0, "d": 1}
	expectedLowers := map[string]float64{"a.a": 0.7, "a.b": 1, "c": 0, "d": 1.75}
	expectedUppers := map[string]float64{"a.a": 3.1, "a.b": 0, "c": 0, "d": 1.75}
	expectedCountsPerSecond := map[string]float64{"a.a": 0.6, "a.b": 1.2, "c": 0, "d": 0.1}
	expectedSums := map[string]float64{"a.a": 4.3, "a.b": 4, "c": 0, "d": 1.75}
	expectedMeans := map[string]float64{"a.a": 1.4333333333333333, "a.b": 1, "c": 0, "d": 1.75}
	expectedMedians := map[string]float64{"a.a": 0.5, "a.b": 1.5, "c": 0, "d": 1.75}
	expectedDeviations := map[string]float64{"a.a": 1.1813363431112902, "a.b": 0.7071067811865476, "c": 0, "d": 0}

	expectedPctLens := map[string]int{"a.a": 2, "a.b": 2, "c": 0, "d": 0}
	expectedPctCounts := map[string]map[float64]int{"a.a": map[float64]int{90: 3, -50: 2},
		"a.b": map[float64]int{90: 4, -50: 2}}
	expectedPctUppers := map[string]map[float64]float64{"a.a": map[float64]float64{90: 3.1, -50: 0.5},
		"a.b": map[float64]float64{90: 0, -50: 1}}
	expectedPctSums := map[string]map[float64]float64{"a.a": map[float64]float64{90: 1.2, -50: 3.6},
		"a.b": map[float64]float64{90: 4, -50: 1}}
	expectedPctMeans := map[string]map[float64]float64{"a.a": map[float64]float64{90: 0.39999999999999997, -50: 1.8},
		"a.b": map[float64]float64{90: 1, -50: 0.5}}

	calculatedTimers := metric.Calculate(&metric.Metrics{Timers: timers, TimersCount: timersCount}, flushInterval, percentiles).Timers

	timerNames := make([]string, len(timers))

	for bucket, _ := range timers {
		timerNames = append(timerNames, bucket)
	}

	for _, timerName := range timerNames {
		cmpFloats(timersCount[timerName], calculatedTimers[timerName].Count, fmt.Sprintf("Invalid count for timer %s ", timerName), t)
		cmpFloats(expectedLowers[timerName], calculatedTimers[timerName].Lower, fmt.Sprintf("Invalid calculated lower for timer %s ", timerName), t)
		cmpFloats(expectedUppers[timerName], calculatedTimers[timerName].Upper, fmt.Sprintf("Invalid calculated upper for timer %s ", timerName), t)
		cmpFloats(expectedCountsPerSecond[timerName], calculatedTimers[timerName].CountPerSecond, fmt.Sprintf("Invalid calculated count per second for timer %s ", timerName), t)
		cmpFloats(expectedSums[timerName], calculatedTimers[timerName].Sum, fmt.Sprintf("Invalid calculated sum for timer %s ", timerName), t)
		cmpFloats(expectedMeans[timerName], calculatedTimers[timerName].Mean, fmt.Sprintf("Invalid calculated mean for timer %s ", timerName), t)
		cmpFloats(expectedMedians[timerName], calculatedTimers[timerName].Median, fmt.Sprintf("Invalid calculated median for timer %s ", timerName), t)
		cmpFloats(expectedDeviations[timerName], calculatedTimers[timerName].StandardDeviation, fmt.Sprintf("Invalid calculated standard deviation for timer %s ", timerName), t)

		calculatedPercentiles := calculatedTimers[timerName].PercentilesData

		if expectedPctLens[timerName] != len(calculatedPercentiles) {
			t.Fatalf("Invalid calculated percentiles length for timer %s Expected: %d, Actual: %d", timerName, expectedPctLens[timerName], len(calculatedPercentiles))
		}

		for _, percentile := range percentiles {
			pctString := strconv.FormatFloat(percentile, 'f', -1, 64)

			if len(calculatedPercentiles) != 0 {

				if expectedPctCounts[timerName][percentile] != calculatedTimers[timerName].PercentilesData[percentile].Count {
					t.Fatalf("Invalid count for timer %s and percentile %s Expected: %d Actual: %d", timerName, pctString, expectedPctCounts[timerName][percentile], calculatedTimers[timerName].PercentilesData[percentile].Count)
				}

				cmpFloats(expectedPctUppers[timerName][percentile], calculatedTimers[timerName].PercentilesData[percentile].Upper, fmt.Sprintf("Invalid calculated upper value for timer %s and percentile %s ", timerName, pctString), t)
				cmpFloats(expectedPctSums[timerName][percentile], calculatedTimers[timerName].PercentilesData[percentile].Sum, fmt.Sprintf("Invalid calculated sum for timer %s and percentile %s ", timerName, pctString), t)
				cmpFloats(expectedPctMeans[timerName][percentile], calculatedTimers[timerName].PercentilesData[percentile].Mean, fmt.Sprintf("Invalid calculated mean for timer %s and percentile %s ", timerName, pctString), t)
			}
		}
	}
}

func cmpFloats(expected float64, actual float64, messagePrefix string, t *testing.T) {
	if big.NewFloat(expected).Cmp(big.NewFloat(actual)) != 0 {
		t.Fatalf("%sExpected: %s, Actual: %s", messagePrefix, strconv.FormatFloat(expected, 'f', -1, 64), strconv.FormatFloat(actual, 'f', -1, 64))
	}
}
