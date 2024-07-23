package postgresql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type OnClause[T sqb.Statement[T]] struct {
	self T
	exp  sql.ConditionalExpression
}

func NewOnClause[T sqb.Statement[T]](self T) *OnClause[T] {
	return &OnClause[T]{self, sql.EmptyCondExp()}
}

func (on *OnClause[T]) On(args ...any) T {
	on.exp.Where(args...)
	on.self.Dirty()
	return on.self
}

func (on *OnClause[T]) AndOn(args ...any) T {
	on.exp.AndWhere(args...)
	on.self.Dirty()
	return on.self
}

func (on *OnClause[T]) OrOn(args ...any) T {
	on.exp.OrWhere(args...)
	on.self.Dirty()
	return on.self
}

func (on *OnClause[T]) CleanOn() T {
	on.exp.Clean()
	on.self.Dirty()
	return on.self
}

func (on *OnClause[T]) CopyOn(self T) *OnClause[T] {
	return &OnClause[T]{self, on.exp.Copy()}
}

func (on *OnClause[T]) BuildOn() T {
	if on.exp.IsNotEmpty() {
		on.self.AddParams(on.exp.Params())
		on.self.AddSql(" ON")
		on.self.AddSql(" ")
		on.self.AddSql(on.exp.String())
	}

	return on.self
}
