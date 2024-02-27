package sql

import (
	"fmt"
	"github.com/AlephTav/sqb"
	"strings"
)

type AssignmentExpression struct {
	Expression
}

func EmptyAssignmentExp() AssignmentExpression {
	return AssignmentExpression{EmptyExp()}
}

func (e AssignmentExpression) Copy() AssignmentExpression {
	return AssignmentExpression{e.Expression.Copy()}
}

func (e AssignmentExpression) Append(column any, args ...any) {
	if e.IsNotEmpty() {
		e.AddSql(", ")
	}
	if len(args) == 0 {
		e.AddSql(e.nameToString(column))
	} else {
		e.AddSql(e.mapToString(sqb.Map(column, args[0])))
	}
}

func (e AssignmentExpression) nameToString(exp any) string {
	if exp == nil {
		return "NULL"
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

func (e AssignmentExpression) sliceToString(exp []any) string {
	var separator string
	var result strings.Builder
	for _, value := range exp {
		result.WriteString(separator)
		result.WriteString(e.nameToString(value))
		separator = ", "
	}
	return result.String()
}

func (e AssignmentExpression) mapToString(exp sqb.SliceMap) string {
	var separator string
	var result strings.Builder
	for i, count := 0, len(exp); i < count; i += 2 {
		result.WriteString(separator)
		result.WriteString(e.nameToString(exp[i]))
		result.WriteString(" = ")
		result.WriteString(e.valueToString(exp[i+1]))
		separator = ", "
	}
	return result.String()
}

func (e AssignmentExpression) valueToString(exp any) string {
	if exp == nil {
		return "NULL"
	}
	switch exp.(type) {
	case Expression:
		return e.expressionToString(exp.(Expression))
	case sqb.Query:
		return e.queryToString(exp.(sqb.Query))
	}
	return e.nextParameterName(exp)
}
