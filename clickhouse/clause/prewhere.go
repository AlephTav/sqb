package clickhouse

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type PrewhereClause[T sqb.Statement[T]] struct {
	self T
	exp  sql.DirectListExpression
}

func NewPrewhereClause[T sqb.Statement[T]](self T) *PrewhereClause[T] {
	return &PrewhereClause[T]{self, sql.EmptyDirectListExp()}
}

func (e *PrewhereClause[T]) Prewhere(expression any, args ...any) T {
	e.exp.Append(expression, args...)
	e.self.Dirty()
	return e.self
}

func (e *PrewhereClause[T]) CleanPrewhere() T {
	e.exp.Clean()
	e.self.Dirty()
	return e.self
}

func (e *PrewhereClause[T]) CopyPrewhere(self T) *PrewhereClause[T] {
	return &PrewhereClause[T]{self, e.exp.Copy()}
}

func (e *PrewhereClause[T]) BuildPrewhere() T {
	if e.exp.IsNotEmpty() {
		e.self.AddParams(e.exp.Params())

		e.self.AddSql(" PREWHERE ")
		e.self.AddSql(e.exp.String())
	}
	return e.self
}
