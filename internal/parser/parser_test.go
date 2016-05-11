package parser_test

import (
	"testing"

	"github.com/evvvvr/yastatsd/internal/metric"
	"github.com/evvvvr/yastatsd/internal/parser"
)

func TestParseSampledCounter(t *testing.T) {
	sampledCounter := metric.Metric{Bucket: "voga", Value: 3, Type: metric.Counter, Sampling: 0.1}

	metrics, errs := parser.Parse("voga:3|c|@0.1")

	if len(errs) > 0 {
		t.Fatalf("Error parsing sampled counter: %s", errs[0])
	}

	if len(metrics) != 1 {
		t.Fatalf("Wrong count of parsed metrics: Expected: %d, Actual: %d", 1, len(metrics))
	}

	if !sampledCounter.Equal(metrics[0]) {
		t.Errorf("Parsed metric is not equal to expected value. Expected: %s, Actual: %s", &sampledCounter, metrics[1])
	}
}
