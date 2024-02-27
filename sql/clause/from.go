package sql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type FromClause[T sqb.Statement[T]] struct {
	self T
	exp  sql.DirectListExpression
}

func NewFromClause[T sqb.Statement[T]](self T) *FromClause[T] {
	return &FromClause[T]{self, sql.EmptyDirectListExp()}
}

// From adds table name and its alias to the "from" clause:
//   - From(table any)
//   - From(table any, alias any)
func (f *FromClause[T]) From(table any, args ...any) T {
	f.exp.Append(table, args...)
	f.self.Dirty()
	return f.self
}

func (f *FromClause[T]) CleanFrom() T {
	f.exp.Clean()
	f.self.Dirty()
	return f.self
}

func (f *FromClause[T]) CopyFrom(self T) *FromClause[T] {
	return &FromClause[T]{self, f.exp.Copy()}
}

func (f *FromClause[T]) BuildFrom() T {
	if f.exp.IsNotEmpty() {
		f.self.AddParams(f.exp.Params())
		f.self.AddSql(" FROM ")
		f.self.AddSql(f.exp.String())
	}
	return f.self
}
