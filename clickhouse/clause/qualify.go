package clickhouse

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type QualifyClause[T sqb.Statement[T]] struct {
	self T
	exp  sql.DirectListExpression
}

func NewQualifyClause[T sqb.Statement[T]](self T) *QualifyClause[T] {
	return &QualifyClause[T]{self, sql.EmptyDirectListExp()}
}

func (a *QualifyClause[T]) Qualify(expression any, args ...any) T {
	a.exp.Append(expression, args...)
	a.self.Dirty()
	return a.self
}

func (a *QualifyClause[T]) CleanQualify() T {
	a.exp.Clean()
	a.self.Dirty()
	return a.self
}

func (a *QualifyClause[T]) CopyQualify(self T) *QualifyClause[T] {
	return &QualifyClause[T]{self, a.exp.Copy()}
}

func (a *QualifyClause[T]) BuildQualify() T {
	if a.exp.IsNotEmpty() {
		a.self.AddParams(a.exp.Params())
		a.self.AddSql(" QUALIFY ")
		a.self.AddSql(a.exp.String())

	}
	return a.self
}
