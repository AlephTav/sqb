package sql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type ValuesClause[T sqb.Statement[T]] struct {
	self T
	exp  sql.ColumnListExpression
}

func NewValuesClause[T sqb.Statement[T]](self T) *ValuesClause[T] {
	return &ValuesClause[T]{self, sql.EmptyColumnListExp()}
}

func (v *ValuesClause[T]) Values(values any) T {
	v.exp.Append(values)
	return v.self
}

func (v *ValueListClause[T]) CleanValues() T {
	v.exp.Clean()
	return v.self
}

func (v *ValuesClause[T]) CopyValues() *ValuesClause[T] {
	return &ValuesClause[T]{v.self, v.exp.Copy()}
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
