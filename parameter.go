package sqb

import (
	"strconv"
	"sync/atomic"
)

var parameterIndex atomic.Int64

func NextParameterName() string {
	parameterIndex.Add(1)
	return "p" + strconv.FormatInt(parameterIndex.Load(), 10)
}

func ResetParameterIndex() {
	for parameterIndex.Load() != 0 {	
		parameterIndex.Add(-parameterIndex.Load())
	}
}
