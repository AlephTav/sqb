package sqb

import (
	"strconv"
)

var parameterIndex = 0

func NextParameterName() string {
	parameterIndex++
	return "p" + strconv.Itoa(parameterIndex)
}

func ResetParameterIndex() {
	parameterIndex = 0
}
