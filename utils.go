package sqb

import (
	"reflect"
	"testing"
)

type SliceMap []any

func Map(kv ...any) SliceMap {
	if len(kv)&1 != 0 {
		kv = append(kv, nil)
	}
	return kv
}

func Keys(m SliceMap) []any {
	var keys = make([]any, 0, len(m)/2)
	for i, count := 0, len(m); i < count; i += 2 {
		keys = append(keys, m[i])
	}
	return keys
}

func Values(m SliceMap) []any {
	values := make([]any, 0, len(m)/2)
	for i, count := 0, len(m); i < count; i += 2 {
		values = append(values, m[i+1])
	}
	return values
}

func CheckSql(t *testing.T, expected string, actual string) {
	if expected != actual {
		t.Errorf("Expected SQL is %q, actual is %q", expected, actual)
	}
}

func CheckParams(t *testing.T, expected map[string]any, actual map[string]any) {
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected SQL params are %#v, actual is %#v", expected, actual)
	}
}
