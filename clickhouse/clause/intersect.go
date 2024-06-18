package clickhouse

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type IntersectClause[T sqb.Statement[T]] struct {
	self T
	exp  sql.DirectListExpression
}

func NewIntersectClause[T sqb.Statement[T]](self T) *IntersectClause[T] {
	return &IntersectClause[T]{self, sql.EmptyDirectListExp()}
}

func (e *IntersectClause[T]) Intersect(expression any, args ...any) T {
	e.exp.Append(expression, args...)
	e.self.Dirty()
	return e.self
}

func (e *IntersectClause[T]) CleanIntersect() T {
	e.exp.Clean()
	e.self.Dirty()
	return e.self
}

func (e *IntersectClause[T]) CopyIntersect(self T) *IntersectClause[T] {
	return &IntersectClause[T]{self, e.exp.Copy()}
}

func (e *IntersectClause[T]) BuildIntersect() T {
	if e.exp.IsNotEmpty() {
		e.self.AddParams(e.exp.Params())

		e.self.AddSql(" INTERSECT ")
		e.self.AddSql(e.exp.String())
	}
	return e.self
}
