package sql

import (
	"fmt"
	"github.com/AlephTav/sqb"
	"strings"
)

type WithExpression struct {
	Expression
}

func EmptyWithExp() WithExpression {
	return WithExpression{EmptyExp()}
}

func (e WithExpression) Copy() WithExpression {
	return WithExpression{e.Expression.Copy()}
}

func (e WithExpression) Append(recursive bool, query any, args ...any) {
	if e.IsNotEmpty() {
		e.AddSql(", ")
	}
	if recursive {
		e.AddSql("RECURSIVE ")
	}
	var exp, alias any
	if len(args) > 0 {
		alias = args[0]
	}
	if alias == nil {
		exp = query
	} else {
		exp = sqb.Map(alias, query)
	}
	e.AddSql(e.nameToString(exp))
}

func (e WithExpression) nameToString(exp any) string {
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

func (e WithExpression) sliceToString(exp []any) string {
	var separator string
	var result strings.Builder
	for _, value := range exp {
		var alias, query any
		if values, ok := value.([]any); ok && len(values) == 2 {
			alias = values[0]
			query = values[1]
		} else {
			alias = nil
			query = value
		}
		e.addToResult(alias, query, separator, &result)
		separator = " AND "
	}
	return result.String()
}

func (e WithExpression) mapToString(exp sqb.SliceMap) string {
	var separator string
	var result strings.Builder
	for i, count := 0, len(exp); i < count; i += 2 {
		result.WriteString(separator)
		e.addToResult(exp[i], exp[i+1], separator, &result)
		separator = " AND "
	}
	return result.String()
}

func (e WithExpression) addToResult(alias any, query any, separator string, result *strings.Builder) {
	var aliasAsString string
	if alias != nil {
		aliasAsString = e.nameToString(alias)
	}
	result.WriteString(separator)
	if aliasAsString != "" {
		result.WriteString(aliasAsString)
		result.WriteString(" AS ")
	}
	result.WriteString(e.nameToString(query))
}
