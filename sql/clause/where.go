package sql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type WhereClause[T sqb.Statement[T]] struct {
	self T
	exp  sql.ConditionalExpression
}

func NewWhereClause[T sqb.Statement[T]](self T) *WhereClause[T] {
	return &WhereClause[T]{self, sql.NewCondExp()}
}

// AndWhere adds "AND" condition to the statement:
//   - AndWhere(condition string)
//   - AndWhere(condition ConditionalExpression)
//   - AndWhere(column string, operator string, value any)
//   - AndWhere(operand any, operator string, value any)
//   - AndWhere(operator string, operand any)
func (w *WhereClause[T]) AndWhere(args ...any) T {
	w.exp.AndWhere(args...)
	return w.self
}

// OrWhere adds "OR" condition to the statement:
//   - OrWhere(condition string)
//   - OrWhere(condition ConditionalExpression)
//   - OrWhere(column string, operator string, value any)
//   - OrWhere(operand any, operator string, value any)
//   - OrWhere(operator string, operand any)
func (w *WhereClause[T]) OrWhere(args ...any) T {
	w.exp.OrWhere(args...)
	return w.self
}

// Where adds "OR" or "AND" condition to the statement:
//   - Where(condition string)
//   - Where(condition ConditionalExpression)
//   - Where(column string, operator string, value any)
//   - Where(operand any, operator string, value any)
//   - Where(operator string, operand any)
func (w *WhereClause[T]) Where(args ...any) T {
	w.exp.Where(args...)
	return w.self
}

func (w *WhereClause[T]) CleanWhere() T {
	w.exp.Clean()
	return w.self
}

func (w *WhereClause[T]) CopyWhere() *WhereClause[T] {
	return &WhereClause[T]{w.self, w.exp.Copy()}
}

func (w *WhereClause[T]) BuildWhere() T {
	if w.exp.IsNotEmpty() {
		w.self.AddParams(w.exp.Params())
		w.self.AddSql(" WHERE ")
		w.self.AddSql(w.exp.String())
	}
	return w.self
}
