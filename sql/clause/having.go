package sql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type HavingClause[T sqb.Statement[T]] struct {
	self T
	exp  sql.ConditionalExpression
}

func NewHavingClause[T sqb.Statement[T]](self T) *HavingClause[T] {
	return &HavingClause[T]{self, sql.NewCondExp()}
}

// AndHaving adds "AND" exp condition to the statement:
//   - AndHaving(condition string)
//   - AndHaving(condition ConditionalExpression)
//   - AndHaving(column string, operator string, value any)
//   - AndHaving(operand any, operator string, value any)
//   - AndHaving(operator string, operand any)
func (h *HavingClause[T]) AndHaving(args ...any) T {
	h.exp.AndWhere(args...)
	h.self.Dirty()
	return h.self
}

// OrHaving adds "OR" exp condition to the statement:
//   - OrHaving(condition string)
//   - OrHaving(condition ConditionalExpression)
//   - OrHaving(column string, operator string, value any)
//   - OrHaving(operand any, operator string, value any)
//   - OrHaving(operator string, operand any)
func (h *HavingClause[T]) OrHaving(args ...any) T {
	h.exp.OrWhere(args...)
	h.self.Dirty()
	return h.self
}

// Having adds "OR" or "AND" condition to the statement:
//   - Having(condition string)
//   - Having(condition ConditionalExpression)
//   - Having(column string, operator string, value any)
//   - Having(operand any, operator string, value any)
//   - Having(operator string, operand any)
func (h *HavingClause[T]) Having(args ...any) T {
	h.exp.Where(args...)
	h.self.Dirty()
	return h.self
}

func (h *HavingClause[T]) CleanHaving() T {
	h.exp.Clean()
	h.self.Dirty()
	return h.self
}

func (h *HavingClause[T]) CopyHaving(self T) *HavingClause[T] {
	return &HavingClause[T]{self, h.exp.Copy()}
}

func (h *HavingClause[T]) BuildHaving() T {
	if h.exp.IsNotEmpty() {
		h.self.AddParams(h.exp.Params())
		h.self.AddSql(" HAVING ")
		h.self.AddSql(h.exp.String())
	}
	return h.self
}
