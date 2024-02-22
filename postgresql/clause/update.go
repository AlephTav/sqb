package postgresql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/clause"
)

type UpdateClause[T sqb.Statement[T]] struct {
	*sql.UpdateClause[T]
	only bool
}

func NewUpdateClause[T sqb.Statement[T]](self T) *UpdateClause[T] {
	return &UpdateClause[T]{sql.NewUpdateClause[T](self), false}
}

// OnlyTable adds table name and its alias to the "update only" clause:
//   - OnlyTable(table any)
//   - OnlyTable(table any, alias any)
func (u *UpdateClause[T]) OnlyTable(table any, args ...any) T {
	u.only = true
	return u.UpdateClause.Table(table, args...)
}

func (u *UpdateClause[T]) CleanUpdate() T {
	u.only = false
	return u.UpdateClause.CleanUpdate()
}

func (u *UpdateClause[T]) CopyUpdate() *UpdateClause[T] {
	return &UpdateClause[T]{u.UpdateClause.CopyUpdate(), u.only}
}

func (u *UpdateClause[T]) BuildUpdate() T {
	self, exp := u.UpdateClause.BuildUpdate()
	if exp.IsEmpty() {
		self.AddSql("UPDATE")
	} else {
		self.AddParams(exp.Params())
		self.AddSql("UPDATE ")
		if u.only {
			self.AddSql("ONLY ")
		}
		self.AddSql(exp.String())
	}
	return self
}
