package sql

import (
	"fmt"
	"github.com/AlephTav/sqb"
	"strings"
)

type ListExpression struct {
	Expression
	reverseOrder bool
}

func EmptyListExp(reverseOrder bool) ListExpression {
	return ListExpression{EmptyExp(), reverseOrder}
}

func (e ListExpression) Copy() ListExpression {
	return ListExpression{e.Expression.Copy(), e.reverseOrder}
}

func (e ListExpression) AppendName(name any, alias any) {
	if e.IsNotEmpty() {
		e.AddSql(", ")
	}
	e.AddSql(e.nameToString(e.mapToExpression(name, alias)))
}

func (e ListExpression) mapToExpression(name any, alias any) any {
	if alias == nil && !e.reverseOrder {
		return name
	}
	if name == nil && e.reverseOrder {
		return alias
	}
	return sqb.Map(alias, name)
}

func (e ListExpression) nameToString(exp any) string {
	if exp == nil {
		return "NULL"
	}
	switch exp.(type) {
	case Expression:
		return e.expressionToString(exp.(Expression))
	case ConditionalExpression:
		return e.conditionToString(exp.(ConditionalExpression))
	case ValueListExpression:
		return e.valueListExpressionToString(exp.(ValueListExpression))
	case sqb.Query:
		return e.queryToString(exp.(sqb.Query))
	case []any:
		return e.sliceToString(exp.([]any))
	case map[string]any:
		return e.mapToString(exp.(map[string]any))
	case sqb.SliceMap:
		return e.sliceMapToString(exp.(sqb.SliceMap))
	default:
		return fmt.Sprintf("%s", exp)
	}
}

func (e ListExpression) valueListExpressionToString(exp ValueListExpression) string {
	e.AddParams(exp.Params())
	return "(VALUES " + exp.String() + ")"
}

func (e ListExpression) mapToString(exp map[string]any) string {
	var separator string
	var result strings.Builder
	for k, v := range exp {
		e.addToResult(k, v, separator, &result)
		separator = ", "
	}
	return result.String()
}

func (e ListExpression) sliceMapToString(exp sqb.SliceMap) string {
	var separator string
	var result strings.Builder
	for i, count := 0, len(exp); i < count; i += 2 {
		e.addToResult(exp[i], exp[i+1], separator, &result)
		separator = ", "
	}
	return result.String()
}

func (e ListExpression) sliceToString(exp []any) string {
	var separator string
	var result strings.Builder
	for _, value := range exp {
		var alias, name any
		if values, ok := value.([]any); ok && len(values) == 2 {
			alias = values[0]
			name = values[1]
		} else if e.reverseOrder {
			alias = value
			name = nil
		} else {
			alias = nil
			name = value
		}
		e.addToResult(alias, name, separator, &result)
		separator = ", "
	}
	return result.String()
}

func (e ListExpression) addToResult(alias any, name any, separator string, result *strings.Builder) {
	if e.reverseOrder {
		name, alias = alias, name
	}
	var aliasAsString string
	if alias != nil {
		aliasAsString = e.nameToString(alias)
	}
	result.WriteString(separator)
	result.WriteString(e.nameToString(name))
	if aliasAsString != "" {
		result.WriteByte(' ')
		result.WriteString(aliasAsString)
	}
}
