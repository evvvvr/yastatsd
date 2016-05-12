package parser

import (
	"errors"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/evvvvr/yastatsd/internal/metric"
)

var metricRegexp = regexp.MustCompile("^([[:alnum:]]|.+):(([+-])?\\d+\\.?\\d*)\\|([c|g|s]|ms)(?:\\|@([+-]?\\d+\\.?\\d*))?$")

func Parse(input string) ([]*metric.Metric, []error) {
	metrics := make([]*metric.Metric, 0, 1307)
	errors := make([]error, 0, 1307)

	for _, line := range strings.Split(input, "\n") {
		metric, err := parseLine(line)

		if err != nil {
			errors = append(errors, err)
		} else {
			metrics = append(metrics, metric)
		}
	}

	return metrics, errors
}

func parseLine(line string) (*metric.Metric, error) {
	submatches := metricRegexp.FindStringSubmatch(line)

	if submatches == nil {
		return nil, errors.New("Wrong metric format")
	}

	bucket := submatches[1]

	value, _ := strconv.ParseFloat(submatches[2], 64)

	metricType := metric.Counter
	switch submatches[4] {
	case "c":
		metricType = metric.Counter

	case "ms":
		metricType = metric.Timer

	case "g":
		metricType = metric.Gauge

	case "s":
		metricType = metric.Set
	}

	operation := metric.NoOperation
	if metricType == metric.Gauge && submatches[3] != "" {
		switch submatches[3] {
		case "+":
			operation = metric.Add

		case "-":
			operation = metric.Subtract
		}

		value = math.Abs(value)
	}

	sampling := 1.0
	if submatches[5] != "" {
		sampling, _ = strconv.ParseFloat(submatches[5], 64)
	}

	return &metric.Metric{Bucket: bucket, Value: value, Operation: operation, Type: metricType, Sampling: sampling}, nil
}
