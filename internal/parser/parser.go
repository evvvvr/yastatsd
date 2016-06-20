package parser

import (
	"errors"
	"strconv"
	"strings"

	"github.com/evvvvr/yastatsd/internal/metric"
)

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
	if len(line) < 5 {
		return nil, errors.New("Metric string is too short")
	}

	metricParts := strings.Split(line, ":")

	if len(metricParts) < 2 || len(metricParts[0]) == 0 || len(metricParts[1]) == 0 {
		return nil, errors.New("Invalid metric string format")
	}

	metricBucket := metricParts[0]
	moreMetricParts := strings.Split(metricParts[1], "|")

	if len(moreMetricParts) < 2 || len(moreMetricParts[0]) == 0 || len(moreMetricParts[1]) == 0 {
		return nil, errors.New("Invalid metric string format")
	}

	metricType := metric.Counter

	switch moreMetricParts[1] {
	case "c":
		metricType = metric.Counter

	case "ms":
		metricType = metric.Timer

	case "g":
		metricType = metric.Gauge

	case "s":
		metricType = metric.Set

	default:
		return nil, errors.New("Invalid metric type") 
	}

	metricValue := moreMetricParts[0]
	DoesGaugeHaveOperation := false
	var err error
	var metricStringValue string
	var metricFloatValue float64

	if (metricType == metric.Set) {
		metricStringValue = metricValue
	} else {
		if metricType == metric.Gauge {
			if strings.HasPrefix(metricValue, "+") || strings.HasPrefix(metricValue, "-") {
				DoesGaugeHaveOperation = true
			}
		} 

		metricFloatValue, err = strconv.ParseFloat(metricValue, 64)

		if err != nil {
			return nil, errors.New("Invalid metric value format") 
		}
	}

	metricSampling := 1.0
	if (metricType == metric.Counter || metricType == metric.Timer) && (len(moreMetricParts) == 3) {
		if len(moreMetricParts[2]) < 2 && !strings.HasPrefix(moreMetricParts[2], "@") {
			return nil,  errors.New("Invalid metric sampling format") 
		}

		metricSampling, err = strconv.ParseFloat(moreMetricParts[2][1:], 64) 

		if err != nil {
			return nil, errors.New("Invalid metric sampling value format") 
		}
	}

	return &metric.Metric{Bucket: metricBucket, StringValue: metricStringValue, FloatValue: metricFloatValue, Type: metricType, DoesGaugeHaveOperation: DoesGaugeHaveOperation, Sampling: metricSampling}, nil
}
