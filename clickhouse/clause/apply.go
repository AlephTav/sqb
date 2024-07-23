package clickhouse

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type ApplyClause[T sqb.Statement[T]] struct {
	self T
	exps []sql.DirectListExpression
}

func NewApplyClause[T sqb.Statement[T]](self T) *ApplyClause[T] {
	return &ApplyClause[T]{self, []sql.DirectListExpression{}}
}

func (a *ApplyClause[T]) Apply(expression any, args ...any) T {
	exp := sql.EmptyDirectListExp()
	exp.Append(expression, args...)
	a.exps = append(a.exps, exp)
	a.self.Dirty()
	return a.self
}

func (a *ApplyClause[T]) CleanApply() T {
	a.exps = []sql.DirectListExpression{}
	a.self.Dirty()
	return a.self
}

func (a *ApplyClause[T]) CopyApply(self T) *ApplyClause[T] {
	expsCopy := make([]sql.DirectListExpression, len(a.exps))
	for i, exp := range a.exps {
		expsCopy[i] = exp.Copy()
	}
	return &ApplyClause[T]{self, expsCopy}
}

func (a *ApplyClause[T]) BuildApply() T {
	for _, exp := range a.exps {
		if exp.IsNotEmpty() {
			a.self.AddParams(exp.Params())
			a.self.AddSql(" APPLY(")
			a.self.AddSql(exp.String())
			a.self.AddSql(")")
		}
	}
	return a.self
}
