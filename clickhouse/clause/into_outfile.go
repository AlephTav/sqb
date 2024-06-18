package clickhouse

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type IntoOutfileClause[T sqb.Statement[T]] struct {
	self T
	exp  sql.DirectListExpression
}

func NewIntoOutfileClause[T sqb.Statement[T]](self T) *IntoOutfileClause[T] {
	return &IntoOutfileClause[T]{self, sql.EmptyDirectListExp()}
}

func (e *IntoOutfileClause[T]) IntoOutfile(expression any, args ...any) T {
	e.exp.Append(expression, args...)
	e.self.Dirty()
	return e.self
}

func (e *IntoOutfileClause[T]) CleanIntoOutfile() T {
	e.exp.Clean()
	e.self.Dirty()
	return e.self
}

func (e *IntoOutfileClause[T]) CopyIntoOutfile(self T) *IntoOutfileClause[T] {
	return &IntoOutfileClause[T]{self, e.exp.Copy()}
}

func (e *IntoOutfileClause[T]) BuildIntoOutfile() T {
	if e.exp.IsNotEmpty() {
		e.self.AddParams(e.exp.Params())

		e.self.AddSql(" INTO OUTFILE ")
		e.self.AddSql(e.exp.String())

	}
	return e.self
}
