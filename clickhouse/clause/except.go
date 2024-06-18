package clickhouse

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type ExceptClause[T sqb.Statement[T]] struct {
	self T
	exp  sql.DirectListExpression
}

func NewExceptClause[T sqb.Statement[T]](self T) *ExceptClause[T] {
	return &ExceptClause[T]{self, sql.EmptyDirectListExp()}
}

func (e *ExceptClause[T]) Except(expression any, args ...any) T {
	e.exp.Append(expression, args...)
	e.self.Dirty()
	return e.self
}

func (e *ExceptClause[T]) CleanExcept() T {
	e.exp.Clean()
	e.self.Dirty()
	return e.self
}

func (e *ExceptClause[T]) CopyExcept(self T) *ExceptClause[T] {
	return &ExceptClause[T]{self, e.exp.Copy()}
}

func (e *ExceptClause[T]) BuildExcept() T {
	if e.exp.IsNotEmpty() {
		e.self.AddParams(e.exp.Params())

		e.self.AddSql(" EXCEPT ")
		e.self.AddSql(e.exp.String())

	}
	return e.self
}
