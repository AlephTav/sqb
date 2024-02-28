package sql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type ColumnsClause[T sqb.Statement[T]] struct {
	self T
	exp  sql.ColumnListExpression
}

func NewColumnsClause[T sqb.Statement[T]](self T) *ColumnsClause[T] {
	return &ColumnsClause[T]{self, sql.EmptyColumnListExp()}
}

func (c *ColumnsClause[T]) Columns(columns any) T {
	c.exp.Append(columns)
	c.self.Dirty()
	return c.self
}

func (c *ColumnsClause[T]) CleanColumns() T {
	c.exp.Clean()
	c.self.Dirty()
	return c.self
}

func (c *ColumnsClause[T]) CopyColumns(self T) *ColumnsClause[T] {
	return &ColumnsClause[T]{self, c.exp.Copy()}
}

func (c *ColumnsClause[T]) BuildColumns() T {
	if c.exp.IsNotEmpty() {
		c.self.AddParams(c.exp.Params())
		c.self.AddSql(" (")
		c.self.AddSql(c.exp.String())
		c.self.AddSql(")")
	}
	return c.self
}
