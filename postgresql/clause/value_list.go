package postgresql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/clause"
)

type ValueListClause[T sqb.ColumnsAwareStmt[T]] struct {
	*sql.ValueListClause[T]
}

func NewValueListClause[T sqb.ColumnsAwareStmt[T]](self T) *ValueListClause[T] {
	return &ValueListClause[T]{sql.NewValueListClause[T](self)}
}

func (v *ValueListClause[T]) CopyValueList(self T) *ValueListClause[T] {
	return &ValueListClause[T]{v.ValueListClause.CopyValueList(self)}
}

func (v *ValueListClause[T]) BuildValueList() T {
	self, exp := v.ValueListClause.BuildValueList()
	if exp.IsEmpty() {
		self.AddSql(" DEFAULT VALUES")
	} else {
		self.AddParams(exp.Params())
		self.AddSql(" VALUES ")
		self.AddSql(exp.String())
	}
	return self
}
