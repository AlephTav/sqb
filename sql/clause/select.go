package sql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type SelectClause[T sqb.Statement[T]] struct {
	self T
	exp  sql.DirectListExpression
}

func NewSelectClause[T sqb.Statement[T]](self T) *SelectClause[T] {
	return &SelectClause[T]{self, sql.EmptyDirectListExp()}
}

// Select adds column name and its alias to the select clause:
//   - Select(column any)
//   - Select(column any, alias any)
func (s *SelectClause[T]) Select(column any, args ...any) T {
	s.exp.Append(column, args...)
	s.self.Dirty()
	return s.self
}

func (s *SelectClause[T]) CleanSelect() T {
	s.exp.Clean()
	s.self.Dirty()
	return s.self
}

func (s *SelectClause[T]) CopySelect(self T) *SelectClause[T] {
	return &SelectClause[T]{self, s.exp.Copy()}
}

func (s *SelectClause[T]) BuildSelect() T {
	if s.exp.IsEmpty() {
		s.self.AddSql("SELECT *")
	} else {
		s.self.AddParams(s.exp.Params())
		s.self.AddSql("SELECT ")
		s.self.AddSql(s.exp.String())
	}
	return s.self
}
