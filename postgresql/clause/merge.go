package postgresql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type MergeClause[T sqb.Statement[T]] struct {
	self T
	exp  sql.DirectListExpression
}

func NewMergeClause[T sqb.Statement[T]](self T) *MergeClause[T] {
	return &MergeClause[T]{self, sql.EmptyDirectListExp()}
}

func (m *MergeClause[T]) Into(table any, args ...any) T {
	m.exp.Append(table, args...)
	m.self.Dirty()
	return m.self
}

func (m *MergeClause[T]) CleanMerge() T {
	m.exp.Clean()
	m.self.Dirty()
	return m.self
}

func (m *MergeClause[T]) CopyMerge(self T) *MergeClause[T] {
	return &MergeClause[T]{self, m.exp.Copy()}
}

func (m *MergeClause[T]) BuildMerge() T {
	if m.exp.IsEmpty() {
		m.self.AddSql("MERGE ")
	} else {
		m.self.AddParams(m.exp.Params())
		m.self.AddSql("MERGE INTO ")
		m.self.AddSql(m.exp.String())
	}
	return m.self
}
