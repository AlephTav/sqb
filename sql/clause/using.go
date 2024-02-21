package sql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type UsingClause[T sqb.Statement[T]] struct {
	self T
	exp  sql.DirectListExpression
}

func NewUsingClause[T sqb.Statement[T]](self T) *UsingClause[T] {
	return &UsingClause[T]{self, sql.EmptyDirectListExp()}
}

// Using adds table name and its alias to the using clause:
//   - Using(table any)
//   - Using(table any, alias any)
func (u *UsingClause[T]) Using(table any, args ...any) T {
	u.exp.Append(table, args...)
	return u.self
}

func (u *UsingClause[T]) CleanUsing() T {
	u.exp.Clean()
	return u.self
}

func (u *UsingClause[T]) CopyUsing() *UsingClause[T] {
	return &UsingClause[T]{u.self, u.exp.Copy()}
}

func (u *UsingClause[T]) BuildUsing() T {
	if u.exp.IsNotEmpty() {
		u.self.AddParams(u.exp.Params())
		u.self.AddSql(" USING ")
		u.self.AddSql(u.exp.String())
	}
	return u.self
}
