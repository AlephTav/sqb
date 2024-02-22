package sql

import (
	"fmt"
	"github.com/AlephTav/sqb"
	"strings"
)

type ValueListExpression struct {
	Expression
}

func EmptyValueListExp() ValueListExpression {
	return ValueListExpression{EmptyExp()}
}

func NewValueListExp(values any) ValueListExpression {
	exp := EmptyValueListExp()
	exp.Append(values)
	return exp
}

func (e ValueListExpression) Copy() ValueListExpression {
	return ValueListExpression{e.Expression.Copy()}
}

func (e ValueListExpression) Append(values any) {
	if e.IsNotEmpty() {
		e.AddSql(", ")
	}
	e.AddSql(e.valueListToString(values))
}

func (e ValueListExpression) valueListToString(exp any) string {
	if exp == nil {
		return ""
	}
	switch exp.(type) {
	case Expression:
		return e.expressionToString(exp.(Expression))
	case sqb.Query:
		return e.queryToString(exp.(sqb.Query))
	case []any:
		return e.sliceToString(exp.([]any))
	case sqb.SliceMap:
		return e.mapToString(exp.(sqb.SliceMap))
	default:
		return fmt.Sprintf("%s", exp)
	}
}

func (e ValueListExpression) mapToString(exp sqb.SliceMap) string {
	return e.sliceToString(sqb.Values(exp))
}

func (e ValueListExpression) sliceToString(exp []any) string {
	if len(exp) == 0 {
		return ""
	}
	var separator string
	var result strings.Builder
	if _, ok := exp[0].([]any); ok {
		for _, value := range exp {
			result.WriteString(separator)
			result.WriteString(e.valueListToString(value))
			separator = ", "
		}
		return result.String()
	}
	for _, value := range exp {
		result.WriteString(separator)
		result.WriteString(e.valueToString(value))
		separator = ", "
	}
	return "(" + result.String() + ")"
}

func (e ValueListExpression) valueToString(exp any) string {
	if exp == nil {
		return "NULL"
	}
	switch exp.(type) {
	case Expression:
		return e.expressionToString(exp.(Expression))
	case sqb.Query:
		return e.queryToString(exp.(sqb.Query))
	case []any:
		return e.sliceOfValuesToString(exp.([]any))
	case map[string]any:
		return e.mapOfValuesToString(exp.(map[string]any))
	}
	return e.nextParameterName(exp)
}

func (e ValueListExpression) mapOfValuesToString(exp map[string]any) string {
	values := make([]any, 0, len(exp))
	for _, v := range exp {
		values = append(values, v)
	}
	return e.sliceOfValuesToString(values)
}

func (e ValueListExpression) sliceOfValuesToString(exp []any) string {
	var separator string
	var result strings.Builder
	for _, value := range exp {
		result.WriteString(separator)
		result.WriteString(e.valueToString(value))
		separator = ", "
	}
	return result.String()
}
