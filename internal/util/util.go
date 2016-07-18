package util

import (
	"log"
	"math/big"
	"reflect"
	"sort"
	"strconv"
)

var bigZero = big.NewFloat(0)

func CmpToZero(f float64) int {
	return big.NewFloat(f).Cmp(bigZero)
}

func FormatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func SortMapKeys(m interface{}) []string {
	v := reflect.ValueOf(m)

	if v.Kind() != reflect.Map {
		log.Println(v.Kind())
		panic("m is not a map")
	}

	if v.Type().Key() != reflect.TypeOf("") {
		panic("m key type is not a string")
	}

	originalKeys := v.MapKeys()
	keys := make([]string, 0, len(originalKeys))

	for _, k := range originalKeys {
		keys = append(keys, k.String())
	}

	sort.Strings(keys)

	return keys
}
