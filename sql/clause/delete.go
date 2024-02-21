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
	return d.self
}

func (d *DeleteClause[T]) CleanDelete() T {
	d.exp.Clean()
	return d.self
}
