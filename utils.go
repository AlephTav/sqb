package sqb

import "fmt"

type SliceMap []any

func ToSliceMap[K comparable, V any](values map[K]V) SliceMap {
	sm := make([]any, len(values))
	i := 0
	for k, v := range values {
		sm[i] = k
		i++
		sm[i] = v
		i++
	}
	return sm
}

func ToSliceMapSlice[K comparable, V any](values []map[K]V) []SliceMap {
	lst := make([]SliceMap, len(values))
	for i, mp := range values {
		lst[i] = ToSliceMap[K, V](mp)
	}
	return lst
}

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

func MapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func MapValues(m map[string]any) []any {
	values := make([]any, 0, len(m))
	for _, v := range m {
		values = append(values, v)
	}
	return values
}

func ToInt64(v any) (int64, error) {
	switch v.(type) {
	case uint8:
		return int64(v.(uint8)), nil
	case int8:
		return int64(v.(int8)), nil
	case uint16:
		return int64(v.(uint16)), nil
	case int16:
		return int64(v.(int16)), nil
	case uint:
		return int64(v.(uint)), nil
	case int:
		return int64(v.(int)), nil
	case uint64:
		return int64(v.(uint64)), nil
	case int64:
		return v.(int64), nil
	case float32:
		return int64(v.(float32)), nil
	case float64:
		return int64(v.(float64)), nil
	default:
		return 0, fmt.Errorf("value of type %T cannot be converted to int64", v)
	}
}
