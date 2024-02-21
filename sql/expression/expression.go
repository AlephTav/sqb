package sql

import (
	"github.com/AlephTav/sqb"
	"strings"
)

type Expression struct {
	sql    *strings.Builder
	params map[string]any
}

func EmptyExp() Expression {
	return Expression{
		sql:    &strings.Builder{},
		params: make(map[string]any),
	}
}

func NewExp(sql string) Expression {
	exp := EmptyExp()
	exp.sql.WriteString(sql)
	return exp
}

func NewExpWithParams(sql string, params map[string]any) Expression {
	exp := NewExp(sql)
	if params == nil {
		params = make(map[string]any)
	}
	exp.params = params
	return exp
}

func (e Expression) IsEmpty() bool {
	return e.sql.Len() == 0
}

func (e Expression) IsNotEmpty() bool {
	return e.sql.Len() != 0
}

func (e Expression) Params() map[string]any {
	return e.params
}

func (e Expression) String() string {
	return e.sql.String()
}

func (e Expression) AddSql(sql string) {
	e.sql.WriteString(sql)
}

func (e Expression) AddParams(params map[string]any) {
	for k, v := range params {
		e.params[k] = v
	}
}

func (e Expression) Clean() {
	e.sql.Reset()
	for param := range e.params {
		delete(e.params, param)
	}
}

func (e Expression) Copy() Expression {
	params := make(map[string]any, len(e.params))
	for k, v := range e.params {
		params[k] = v
	}
	return NewExpWithParams(e.sql.String(), params)
}

func (e Expression) nextParameterName(value any) string {
	var param = sqb.NextParameterName()
	e.params[param] = value
	return ":" + param
}

func (e Expression) expressionToString(exp Expression) string {
	e.AddParams(exp.Params())
	return exp.String()
}

func (e Expression) conditionToString(exp ConditionalExpression) string {
	e.AddParams(exp.Params())
	return "(" + exp.String() + ")"
}

func (e Expression) queryToString(exp sqb.Query) string {
	e.AddParams(exp.Params())
	return "(" + exp.String() + ")"
}
