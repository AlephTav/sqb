package sqb

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
