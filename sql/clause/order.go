package sql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type OrderClause[T sqb.Statement[T]] struct {
	self T
	exp  sql.ReversedListExpression
}

func NewOrderClause[T sqb.Statement[T]](self T) *OrderClause[T] {
	return &OrderClause[T]{self, sql.EmptyReversedListExp()}
}

// OrderBy adds column name and its order to the order clause:
//   - OrderBy(column any)
//   - OrderBy(column any, order any)
func (o *OrderClause[T]) OrderBy(column any, args ...any) T {
	o.exp.Append(column, args...)
	o.self.Dirty()
	return o.self
}

func (o *OrderClause[T]) CleanOrder() T {
	o.exp.Clean()
	o.self.Dirty()
	return o.self
}

func (o *OrderClause[T]) CopyOrder() *OrderClause[T] {
	return &OrderClause[T]{o.self, o.exp.Copy()}
}

func (o *OrderClause[T]) BuildOrder() T {
	if o.exp.IsNotEmpty() {
		o.self.AddParams(o.exp.Params())
		o.self.AddSql(" ORDER BY ")
		o.self.AddSql(o.exp.String())
	}
	return o.self
}
