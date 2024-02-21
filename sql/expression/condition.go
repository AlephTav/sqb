package sql

import (
	"fmt"
	"github.com/AlephTav/sqb"
	"strings"
)

type ConditionalExpression struct {
	Expression
}

func EmptyCondExp() ConditionalExpression {
	return ConditionalExpression{EmptyExp()}
}

func NewCondExp(args ...any) ConditionalExpression {
	exp := ConditionalExpression{EmptyExp()}
	exp.Where(args...)
	return exp
}

func (e ConditionalExpression) Copy() ConditionalExpression {
	return ConditionalExpression{e.Expression.Copy()}
}

// AndWhere add "AND" condition to the expression:
//   - AndWhere(condition string)
//   - AndWhere(condition ConditionalExpression)
//   - AndWhere(column string, operator string, value any)
//   - AndWhere(operand any, operator string, value any)
//   - AndWhere(operator string, operand any)
func (e ConditionalExpression) AndWhere(args ...any) ConditionalExpression {
	switch len(args) {
	case 0:
		return e
	case 1:
		return e.Where(args[0], AND)
	case 2:
		return e.Where(args[0], args[1], AND)
	default:
		return e.Where(args[0], args[1], args[2], AND)
	}
}

// OrWhere add "OR" condition to the expression:
//   - OrWhere(condition string)
//   - OrWhere(condition ConditionalExpression)
//   - OrWhere(column string, operator string, value any)
//   - OrWhere(operand any, operator string, value any)
//   - OrWhere(operator string, operand any)
func (e ConditionalExpression) OrWhere(args ...any) ConditionalExpression {
	switch len(args) {
	case 0:
		return e
	case 1:
		return e.Where(args[0], OR)
	case 2:
		return e.Where(args[0], args[1], OR)
	default:
		return e.Where(args[0], args[1], args[2], OR)
	}
}

// Where add "OR" or "AND" condition to the expression:
//   - Where(condition string)
//   - Where(condition ConditionalExpression)
//   - Where(column string, operator string, value any)
//   - Where(operand any, operator string, value any)
//   - Where(operator string, operand any)
func (e ConditionalExpression) Where(args ...any) ConditionalExpression {
	argNum := len(args)
	if argNum == 0 {
		return e
	}
	connector, ok := args[argNum-1].(Connector)
	if !ok {
		connector = AND
	} else {
		argNum--
	}
	if e.IsNotEmpty() {
		e.AddSql(" ")
		e.AddSql(connector.String())
		e.AddSql(" ")
	}
	if argNum < 2 {
		e.AddSql(e.nameToString(args[0]))
	} else {
		var value any
		if argNum > 2 {
			value = args[2]
		}
		operator, operatorIsString := args[1].(string)
		if operatorIsString {
			e.AddSql(e.nameToString(args[0]))
			e.AddSql(" ")
			e.AddSql(operator)
			e.AddSql(" ")
			e.AddSql(e.valueToString(value, operator))
		} else {
			columnAsString := fmt.Sprintf("%s", args[1])
			e.AddSql(columnAsString)
			e.AddSql(" ")
			e.AddSql(e.valueToString(operator, columnAsString))
		}
	}
	return e
}

func (e ConditionalExpression) nameToString(exp any) string {
	if exp == nil {
		return "NULL"
	}
	switch exp.(type) {
	case Expression:
		return e.expressionToString(exp.(Expression))
	case ConditionalExpression:
		return e.conditionToString(exp.(ConditionalExpression))
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

func (e ConditionalExpression) sliceToString(exp []any) string {
	var separator string
	var result strings.Builder
	for value := range exp {
		result.WriteString(separator)
		result.WriteString(e.nameToString(value))
		separator = " AND "
	}
	return result.String()
}

func (e ConditionalExpression) mapToString(exp sqb.SliceMap) string {
	var separator string
	var result strings.Builder
	for i, count := 0, len(exp); i < count; i += 2 {
		result.WriteString(separator)
		result.WriteString(e.nameToString(exp[i]))
		result.WriteString(" = ")
		result.WriteString(e.valueToString(exp[i+1], "="))
		separator = " AND "
	}
	return result.String()
}

func (e ConditionalExpression) valueToString(exp any, operator string) string {
	if exp == nil {
		return "NULL"
	}
	switch exp.(type) {
	case Expression:
		return e.expressionToString(exp.(Expression))
	case sqb.Query:
		return e.queryToString(exp.(sqb.Query))
	case []any:
		return e.valueListToString(exp.([]any), operator)
	}
	return e.nextParameterName(exp)
}

func (e ConditionalExpression) valueListToString(values []any, operator string) string {
	var isBetween = e.isBetween(operator)
	var result strings.Builder
	var separator, sep string
	if isBetween {
		sep = " AND "
	} else {
		sep = ", "
	}
	for value := range values {
		result.WriteString(separator)
		result.WriteString(e.valueToString(value, operator))
		separator = sep
	}
	if isBetween {
		return result.String()
	}
	return "(" + result.String() + ")"
}

func (e ConditionalExpression) isBetween(operator string) bool {
	var op = strings.ToLower(operator)
	return op == "between" || op == "not between"
}
