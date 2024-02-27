package sql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type ValuesClause[T sqb.Statement[T]] struct {
	self T
	exp  sql.ValueListExpression
}

func NewValuesClause[T sqb.Statement[T]](self T) *ValuesClause[T] {
	return &ValuesClause[T]{self, sql.EmptyValueListExp()}
}

func (v *ValuesClause[T]) Values(values any) T {
	v.exp.Append(values)
	v.self.Dirty()
	return v.self
}

func (v *ValuesClause[T]) CleanValues() T {
	v.exp.Clean()
	v.self.Dirty()
	return v.self
}

func (v *ValuesClause[T]) CopyValues(self T) *ValuesClause[T] {
	return &ValuesClause[T]{self, v.exp.Copy()}
}

func (v *ValuesClause[T]) BuildValues() T {
	if v.exp.IsEmpty() {
		v.self.AddSql("VALUES")
	} else {
		v.self.AddParams(v.exp.Params())
		v.self.AddSql("VALUES ")
		v.self.AddSql(v.exp.String())
	}
	return v.self
}
