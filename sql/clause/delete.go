package sql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type DeleteClause[T sqb.Statement[T]] struct {
	self T
	exp  sql.DirectListExpression
}

func NewDeleteClause[T sqb.Statement[T]](self T) *DeleteClause[T] {
	return &DeleteClause[T]{self, sql.EmptyDirectListExp()}
}

// From adds table name and its alias to the delete clause:
//   - From(table any)
//   - From(table any, alias any)
func (d *DeleteClause[T]) From(table any, args ...any) T {
	d.exp.Append(table, args...)
	d.self.Dirty()
	return d.self
}

func (d *DeleteClause[T]) CleanDelete() T {
	d.exp.Clean()
	d.self.Dirty()
	return d.self
}

func (d *DeleteClause[T]) CopyDelete(self T) *DeleteClause[T] {
	return &DeleteClause[T]{self, d.exp.Copy()}
}

func (d *DeleteClause[T]) BuildDelete() (T, sql.DirectListExpression) {
	return d.self, d.exp
}
