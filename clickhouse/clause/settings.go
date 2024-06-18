package clickhouse

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type SettingsClause[T sqb.Statement[T]] struct {
	self T
	exp  sql.DirectListExpression
}

func NewSettingsClause[T sqb.Statement[T]](self T) *SettingsClause[T] {
	return &SettingsClause[T]{self, sql.EmptyDirectListExp()}
}

func (a *SettingsClause[T]) Settings(expression any, args ...any) T {
	a.exp.Append(expression, args...)
	a.self.Dirty()
	return a.self
}

func (a *SettingsClause[T]) CleanSettings() T {
	a.exp.Clean()
	a.self.Dirty()
	return a.self
}

func (a *SettingsClause[T]) CopySettings(self T) *SettingsClause[T] {
	return &SettingsClause[T]{self, a.exp.Copy()}
}

func (a *SettingsClause[T]) BuildSettings() T {
	if a.exp.IsNotEmpty() {
		a.self.AddParams(a.exp.Params())
		a.self.AddSql(" SETTINGS ")
		a.self.AddSql(a.exp.String())

	}
	return a.self
}
