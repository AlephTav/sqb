package sql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type ReturningClause[T sqb.Statement[T]] struct {
	self T
	exp  sql.DirectListExpression
}

func NewReturningClause[T sqb.Statement[T]](self T) *ReturningClause[T] {
	return &ReturningClause[T]{self, sql.EmptyDirectListExp()}
}

// Returning adds column name and its alias to the returning clause:
//   - Returning(column any)
//   - Returning(column any, alias any)
func (r *ReturningClause[T]) Returning(column any, args ...any) T {
	r.exp.Append(column, args...)
	r.self.Dirty()
	return r.self
}

func (r *ReturningClause[T]) CleanReturning() T {
	r.exp.Clean()
	r.self.Dirty()
	return r.self
}

func (r *ReturningClause[T]) CopyReturning() *ReturningClause[T] {
	return &ReturningClause[T]{r.self, r.exp.Copy()}
}

func (r *ReturningClause[T]) BuildReturning() T {
	if r.exp.IsNotEmpty() {
		r.self.AddParams(r.exp.Params())
		r.self.AddSql(" RETURNING ")
		r.self.AddSql(r.exp.String())
	}
	return r.self
}
