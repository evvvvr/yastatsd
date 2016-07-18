package parser_test

import (
	"testing"

	"github.com/evvvvr/yastatsd/internal/metric"
	"github.com/evvvvr/yastatsd/internal/parser"
)

func TestParseSingleMetric(t *testing.T) {
	sampledCounter := metric.Metric{Bucket: "voga", FloatValue: 3, Type: metric.Counter, Sampling: 0.1}

	metrics, errs := parser.Parse("voga:3|c|@0.1")

	if len(errs) > 0 {
		t.Fatalf("Error parsing single metric: %s", errs[0])
	}

	if len(metrics) != 1 {
		t.Fatalf("Wrong count of parsed metrics. Expected: %d, Actual: %d", 1, len(metrics))
	}

	compareMetrics(t, &sampledCounter, metrics[0])
}

func TestParseMultipleMetrics(t *testing.T) {
	timer := metric.Metric{Bucket: "voga", FloatValue: 3, Type: metric.Timer, Sampling: 1.0}
	gauge := metric.Metric{Bucket: "vo.ga", FloatValue: -3, DoesGaugeHaveOperation: true, Type: metric.Gauge, Sampling: 0.1}

	metrics, errs := parser.Parse("voga:3|ms\nvo.ga:-3|g|@0.1\nvo.ga:--3|g|@0.1\nvo.ga:--3|g|@0.1-\n:||@")

	if len(errs) != 3 {
		t.Fatalf("Wrong count of parsing errors. Expected: %d, Actual: %d", 3, len(errs))
	}

	if len(metrics) != 2 {
		t.Fatalf("Wrong count of parsed metrics. Expected: %d, Actual: %d", 2, len(metrics))
	}

	compareMetrics(t, &timer, metrics[0])
	compareMetrics(t, &gauge, metrics[1])
}

func BenchmarkParse(b *testing.B) {
	for n := 0; n < b.N; n++ {
		metrics, errs := parser.Parse("voga:3|ms\nvo.ga:-3|g|@0.1\nvo.ga:--3|g|@0.1\nvo.ga:--3|g|@0.1-\n:||@")

		if len(errs) != 3 {
			b.Fatalf("Wrong count of parsing errors. Expected: %d, Actual: %d", 3, len(errs))
		}

		if len(metrics) != 2 {
			b.Fatalf("Wrong count of parsed metrics. Expected: %d, Actual: %d", 2, len(metrics))
		}
	}
}

func compareMetrics(t *testing.T, expected, parsed *metric.Metric) {
	if !expected.Equal(parsed) {
		t.Errorf("Parsed metric is not equal to expected value. "+
			"Expected: %s, Actual: %s", expected, parsed)
	}
}
