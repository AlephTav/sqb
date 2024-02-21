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
		columnAsString, ok := column.(string)
		if !ok {
			columnAsString = e.nameToString(args[0])
		}
		e.AddSql(e.mapToString(map[string]any{columnAsString: args[1]}))
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
	case map[string]any:
		return e.mapToString(exp.(map[string]any))
	default:
		return fmt.Sprintf("%s", exp)
	}
}

func (e AssignmentExpression) sliceToString(exp []any) string {
	var separator string
	var result strings.Builder
	for value := range exp {
		result.WriteString(separator)
		result.WriteString(e.nameToString(value))
		separator = ", "
	}
	return result.String()
}

func (e AssignmentExpression) mapToString(exp map[string]any) string {
	var separator string
	var result strings.Builder
	for key, value := range exp {
		result.WriteString(separator)
		result.WriteString(e.nameToString(key))
		result.WriteString(" = ")
		result.WriteString(e.valueToString(value))
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
