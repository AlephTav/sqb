package sql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type GroupClause[T sqb.Statement[T]] struct {
	self T
	exp  sql.ReversedListExpression
}

func NewGroupClause[T sqb.Statement[T]](self T) *GroupClause[T] {
	return &GroupClause[T]{self, sql.EmptyReversedListExp()}
}

// GroupBy adds column name and its order to the group clause:
//   - GroupBy(column any)
//   - GroupBy(column any, order any)
func (g *GroupClause[T]) GroupBy(column any, args ...any) T {
	g.exp.Append(column, args...)
	g.self.Dirty()
	return g.self
}

func (g *GroupClause[T]) CleanGroup() T {
	g.exp.Clean()
	g.self.Dirty()
	return g.self
}

func (g *GroupClause[T]) CopyGroup(self T) *GroupClause[T] {
	return &GroupClause[T]{self, g.exp.Copy()}
}

func (g *GroupClause[T]) BuildGroup() T {
	if g.exp.IsNotEmpty() {
		g.self.AddParams(g.exp.Params())
		g.self.AddSql(" GROUP BY ")
		g.self.AddSql(g.exp.String())
	}
	return g.self
}
