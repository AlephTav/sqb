package clickhouse

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type FormatClause[T sqb.Statement[T]] struct {
	self T
	exp  sql.DirectListExpression
}

func NewFormatClause[T sqb.Statement[T]](self T) *FormatClause[T] {
	return &FormatClause[T]{self, sql.EmptyDirectListExp()}
}

func (e *FormatClause[T]) Format(expression any, args ...any) T {
	e.exp.Append(expression, args...)
	e.self.Dirty()
	return e.self
}

func (e *FormatClause[T]) CleanFormat() T {
	e.exp.Clean()
	e.self.Dirty()
	return e.self
}

func (e *FormatClause[T]) CopyFormat(self T) *FormatClause[T] {
	return &FormatClause[T]{self, e.exp.Copy()}
}

func (e *FormatClause[T]) BuildFormat() T {
	if e.exp.IsNotEmpty() {
		e.self.AddParams(e.exp.Params())

		e.self.AddSql(" FORMAT ")
		e.self.AddSql(e.exp.String())

	}
	return e.self
}
