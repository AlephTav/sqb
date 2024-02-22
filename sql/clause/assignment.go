package sql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type AssignmentClause[T sqb.Statement[T]] struct {
	self T
	exp  sql.AssignmentExpression
}

func NewAssignmentClause[T sqb.Statement[T]](self T) *AssignmentClause[T] {
	return &AssignmentClause[T]{self, sql.EmptyAssignmentExp()}
}

// Assign adds column and value to the SET clause.
//   - Assign(column any)
//   - Assign(column any, value any)
func (a *AssignmentClause[T]) Assign(column any, args ...any) T {
	a.exp.Append(column, args...)
	a.self.Dirty()
	return a.self
}

func (a *AssignmentClause[T]) CleanAssignment() T {
	a.exp.Clean()
	a.self.Dirty()
	return a.self
}

func (a *AssignmentClause[T]) CopyAssignment() *AssignmentClause[T] {
	return &AssignmentClause[T]{a.self, a.exp.Copy()}
}

func (a *AssignmentClause[T]) BuildAssignment() T {
	if a.exp.IsNotEmpty() {
		a.self.AddParams(a.exp.Params())
		a.self.AddSql(" SET ")
		a.self.AddSql(a.exp.String())
	}
	return a.self
}
