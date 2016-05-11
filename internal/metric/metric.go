package metric

import (
	"fmt"
	"math/big"
	"strings"
)

type MetricType int

const (
	Counter MetricType = iota
	Timer
	Gauge
	Set
)

type Operation int

const (
	NoOperation Operation = iota
	Add
	Subtract
)

type Metric struct {
	Bucket    string
	Value     float64
	Operation Operation
	Type      MetricType
	Sampling  float64
}

func (a *Metric) Equal(b *Metric) bool {
	if a == b {
		return true
	}

	if b == nil {
		return false
	}

	bigAValue, bigBValue := big.NewFloat(a.Value), big.NewFloat(b.Value)
	bigASampling, bigBSampling := big.NewFloat(a.Sampling), big.NewFloat(b.Sampling)

	return (a.Bucket == b.Bucket) && (bigAValue.Cmp(bigBValue) == 0) &&
		(a.Operation == b.Operation) && (a.Type == b.Type) &&
		(bigASampling.Cmp(bigBSampling) == 0)
}

func (m *Metric) String() string {
	typeString := ""

	switch m.Type {
	case Counter:
		typeString = "c"

	case Timer:
		typeString = "ms"

	case Gauge:
		typeString = "g"

	case Set:
		typeString = "s"
	}

	gaugeOpString := ""

	if m.Type == Gauge && m.Operation != NoOperation {
		switch m.Operation {
		case Add:
			gaugeOpString = "+"

		case Subtract:
			gaugeOpString = "-"
		}
	}

	one := big.NewFloat(1)
	sampleValue := big.NewFloat(m.Sampling)
	sampleString := ""

	if one.Cmp(sampleValue) != 0 {
		sampleString = strings.TrimRight(strings.TrimRight(fmt.Sprintf("|@%f", m.Sampling), "0"), ".")
	}

	valueString := strings.TrimRight(strings.TrimRight(fmt.Sprintf("%f", m.Value), "0"), ".")

	return fmt.Sprintf("%s:%s%s|%s%s", m.Bucket, gaugeOpString, valueString, typeString, sampleString)
}
