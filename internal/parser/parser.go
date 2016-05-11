package parser

import (
	"errors"
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
	return nil, errors.New("foo")
}

// ^([[:alnum:]]|.+):(([+-])?\d+\.?\d*)\|([c|g|s]|ms)(?:\|@([+-]?\d+\.?\d*))?$
