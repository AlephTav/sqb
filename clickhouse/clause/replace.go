package clickhouse

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type ReplaceClause[T sqb.Statement[T]] struct {
	self T
	exp  sql.DirectListExpression
}

func NewReplaceClause[T sqb.Statement[T]](self T) *ReplaceClause[T] {
	return &ReplaceClause[T]{self, sql.EmptyDirectListExp()}
}

func (e *ReplaceClause[T]) Replace(expression any, args ...any) T {
	e.exp.Append(expression, args...)
	e.self.Dirty()
	return e.self
}

func (e *ReplaceClause[T]) CleanReplace() T {
	e.exp.Clean()
	e.self.Dirty()
	return e.self
}

func (e *ReplaceClause[T]) CopyReplace(self T) *ReplaceClause[T] {
	return &ReplaceClause[T]{self, e.exp.Copy()}
}

func (e *ReplaceClause[T]) BuildReplace() T {
	if e.exp.IsNotEmpty() {
		e.self.AddParams(e.exp.Params())

		e.self.AddSql(" REPLACE(")
		e.self.AddSql(e.exp.String())
		e.self.AddSql(")")
	}
	return e.self
}
