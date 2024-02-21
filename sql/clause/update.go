package sql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type UpdateClause[T sqb.Statement[T]] struct {
	self T
	exp  sql.DirectListExpression
}

func NewUpdateClause[T sqb.Statement[T]](self T) *UpdateClause[T] {
	return &UpdateClause[T]{self, sql.EmptyDirectListExp()}
}

// Table adds table name and its alias to the update clause:
//   - Table(table any)
//   - Table(table any, alias any)
func (u *UpdateClause[T]) Table(table any, args ...any) T {
	u.exp.Append(table, args...)
	return u.self
}

func (u *UpdateClause[T]) CleanUpdate() T {
	u.exp.Clean()
	return u.self
}
