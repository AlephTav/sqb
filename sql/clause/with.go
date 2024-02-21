package sql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type WithClause[T sqb.Statement[T]] struct {
	self T
	exp  sql.WithExpression
}

func NewWithClause[T sqb.Statement[T]](self T) *WithClause[T] {
	return &WithClause[T]{self, sql.EmptyWithExp()}
}

// With adds query to the with clause:
// With(query any)
// With(query any, alias any)
func (w *WithClause[T]) With(query any, args ...any) T {
	w.exp.Append(false, query, args...)
	return w.self
}

// WithRecursive adds recursive query to the with clause:
// WithRecursive(query any)
// WithRecursive(query any, alias any)
func (w *WithClause[T]) WithRecursive(query any, args ...any) T {
	w.exp.Append(true, query, args...)
	return w.self
}

func (w *WithClause[T]) CleanWith() T {
	w.exp.Clean()
	return w.self
}

func (w *WithClause[T]) CopyWith() *WithClause[T] {
	return &WithClause[T]{w.self, w.exp.Copy()}
}

func (w *WithClause[T]) BuildWith() T {
	if w.exp.IsNotEmpty() {
		w.self.AddParams(w.exp.Params())
		w.self.AddSql("WITH ")
		w.self.AddSql(w.exp.String())
	}
	return w.self
}
