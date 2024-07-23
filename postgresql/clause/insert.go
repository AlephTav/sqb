package postgresql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/clause"
)

type InsertClause[T sqb.Statement[T]] struct {
	*sql.InsertClause[T]
}

func NewInsertClause[T sqb.Statement[T]](self T) *InsertClause[T] {
	return &InsertClause[T]{sql.NewInsertClause[T](self)}
}

func (i *InsertClause[T]) CopyInsert(self T) *InsertClause[T] {
	return &InsertClause[T]{i.InsertClause.CopyInsert(self)}
}

func (i *InsertClause[T]) BuildInsert() T {
	self, exp := i.InsertClause.BuildInsert()
	if exp.IsEmpty() {
		self.AddSql("INSERT")
	} else {
		self.AddParams(exp.Params())
		self.AddSql("INSERT INTO ")
		self.AddSql(exp.String())
	}
	return self
}
