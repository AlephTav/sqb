package sql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type InsertClause[T sqb.Statement[T]] struct {
	self T
	exp  sql.DirectListExpression
}

func NewInsertClause[T sqb.Statement[T]](self T) *InsertClause[T] {
	return &InsertClause[T]{self, sql.EmptyDirectListExp()}
}

// Into adds table name and its alias to the insert clause:
//   - Into(table any)
//   - Into(table any, alias any)
func (i *InsertClause[T]) Into(table any, args ...any) T {
	i.exp.Append(table, args...)
	return i.self
}

func (i *InsertClause[T]) CleanInsert() T {
	i.exp.Clean()
	return i.self
}
