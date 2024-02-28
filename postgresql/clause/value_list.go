package postgresql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/clause"
)

type ValueListClause[T sqb.ColumnsAwareStmt[T], Q sqb.QueryStmt[Q]] struct {
	*sql.ValueListClause[T, Q]
}

func NewValueListClause[T sqb.ColumnsAwareStmt[T], Q sqb.QueryStmt[Q]](self T) *ValueListClause[T, Q] {
	return &ValueListClause[T, Q]{sql.NewValueListClause[T, Q](self)}
}

func (v *ValueListClause[T, Q]) CopyValueList(self T) *ValueListClause[T, Q] {
	return &ValueListClause[T, Q]{v.ValueListClause.CopyValueList(self)}
}

func (v *ValueListClause[T, Q]) BuildValueList() T {
	self, query, exp := v.ValueListClause.BuildValueList()
	if query != nil {
		self.AddParams((*query).Params())
		self.AddSql(" ")
		self.AddSql((*query).String())
	} else if exp.IsEmpty() {
		self.AddSql(" DEFAULT VALUES")
	} else {
		self.AddParams(exp.Params())
		self.AddSql(" VALUES ")
		self.AddSql(exp.String())
	}
	return self
}
