package metric_test

import (
	"testing"

	"github.com/evvvvr/yastatsd/internal/metric"
)

func TestEqual(t *testing.T) {
	metricA := metric.Metric{Bucket: "test", FloatValue: 9.8, Type: metric.Counter, Sampling: 1}
	metricB := metric.Metric{Bucket: "test", FloatValue: 9.8, Type: metric.Counter, Sampling: 1}
	metricC := metric.Metric{Bucket: "test", FloatValue: 9.8, Type: metric.Counter, Sampling: 1}
	metricD := metric.Metric{Bucket: "test2", FloatValue: 9.8, Type: metric.Gauge, Sampling: 1}

	if metricA.Equal(nil) {
		t.Error("Metric can't be equal to nil")
	}

	if !metricA.Equal(&metricA) {
		t.Error("Metric must be reflexive equal")
	}

	if !metricA.Equal(&metricB) {
		t.Error("Metrics must be equal")
	}

	if !metricB.Equal(&metricA) {
		t.Error("Metrics must be symmetrically equal")
	}

	if metricA.Equal(&metricB) && metricB.Equal(&metricC) && !metricA.Equal(&metricC) {
		t.Error("Metrics must be transitively equal")
	}

	if metricA.Equal(&metricD) {
		t.Error("Metrics must be not equal")
	}

	setA := metric.Metric{Bucket: "test", StringValue: "kooka", FloatValue: 9.8, Type: metric.Set, Sampling: 1}
	setB := metric.Metric{Bucket: "test", StringValue: "kooka", FloatValue: 0.8, Type: metric.Set, Sampling: 0.2}

	if !setA.Equal(&setB) {
		t.Error("Set metrics must be equal")
	}
}

func TestString(t *testing.T) {
	sampledTimer := metric.Metric{Bucket: "vo.ga", FloatValue: 2.71828, Type: metric.Timer, Sampling: 0.5}
	sampledTimerExpectedString := "vo.ga:2.71828|ms|@0.5"

	compareMetricStrings(t, sampledTimerExpectedString, &sampledTimer)

	gauge := metric.Metric{Bucket: "test", FloatValue: -3000, DoesGaugeHaveOperation: true, Type: metric.Gauge, Sampling: 1}
	gaugeExpectedString := "test:-3000|g"

	compareMetricStrings(t, gaugeExpectedString, &gauge)

	setMetric := metric.Metric{Bucket: "test", StringValue: "kooka", FloatValue: 9.8, Type: metric.Set, Sampling: 1}
	metricExpectedString := "test:kooka|s"

	compareMetricStrings(t, metricExpectedString, &setMetric)
}

func compareMetricStrings(t *testing.T, metricExpectedString string, m *metric.Metric) {
	metricString := m.String()

	if metricString != metricExpectedString {
		t.Errorf("Metric string representation is not equal to expected value. "+
			"Expected: %s, Actual: %s", metricExpectedString, metricString)
	}
}
