package metric

import (
	"fmt"
	"math/big"

	"github.com/evvvvr/yastatsd/internal/util"
)

var one = big.NewFloat(1)

type MetricType int

const (
	Counter MetricType = iota
	Timer
	Gauge
	Set
)

type Operation int

type Metric struct {
	Bucket                 string
	StringValue            string
	FloatValue             float64
	DoesGaugeHaveOperation bool
	Type                   MetricType
	Sampling               float64
}

type Metrics struct {
	Counters    map[string]float64
	Timers      map[string][]float64
	TimersCount map[string]float64
	Gauges      map[string]float64
	Sets        map[string]map[string]struct{}
}

func (a *Metric) Equal(b *Metric) bool {
	if a == b {
		return true
	}

	if b == nil {
		return false
	}

	if a.Type != b.Type {
		return false
	}

	areOperationsEqual := true
	if a.Type == Gauge {
		areOperationsEqual = a.DoesGaugeHaveOperation == b.DoesGaugeHaveOperation
	}

	areValuesEqual := false
	if a.Type == Set {
		areValuesEqual = a.StringValue == b.StringValue
	} else {
		bigAValue, bigBValue := big.NewFloat(a.FloatValue), big.NewFloat(b.FloatValue)
		areValuesEqual = bigAValue.Cmp(bigBValue) == 0
	}

	areSamplingsEqual := true
	if a.Type == Counter || a.Type == Timer {
		bigASampling, bigBSampling := big.NewFloat(a.Sampling), big.NewFloat(b.Sampling)
		areSamplingsEqual = bigASampling.Cmp(bigBSampling) == 0
	}

	return a.Bucket == b.Bucket && areValuesEqual && areOperationsEqual && areSamplingsEqual
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

	valueString := ""

	if m.Type == Set {
		valueString = m.StringValue
	} else {
		valueString = util.FormatFloat(m.FloatValue)
	}

	sampleString := ""
	sampleValue := big.NewFloat(m.Sampling)

	if (m.Type == Counter || m.Type == Timer) && (one.Cmp(sampleValue) != 0) {
		sampleString = fmt.Sprintf("|@%s", util.FormatFloat(m.Sampling))
	}

	return fmt.Sprintf("%s:%s|%s%s", m.Bucket, valueString, typeString, sampleString)
}
