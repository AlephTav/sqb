package postgresql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/clause"
)

type DeleteClause[T sqb.Statement[T]] struct {
	*sql.DeleteClause[T]
	only bool
}

func NewDeleteClause[T sqb.Statement[T]](self T) *DeleteClause[T] {
	return &DeleteClause[T]{sql.NewDeleteClause[T](self), false}
}

// FromOnly adds table name and its alias to the "delete from only" clause:
//   - FromOnly(table any)
//   - FromOnly(table any, alias any)
func (d *DeleteClause[T]) FromOnly(table any, args ...any) T {
	d.only = true
	return d.DeleteClause.From(table, args...)
}

func (d *DeleteClause[T]) CleanDelete() T {
	d.only = false
	return d.DeleteClause.CleanDelete()
}

func (d *DeleteClause[T]) CopyDelete() *DeleteClause[T] {
	return &DeleteClause[T]{d.DeleteClause.CopyDelete(), d.only}
}

func (d *DeleteClause[T]) BuildDelete() T {
	self, exp := d.DeleteClause.BuildDelete()
	if exp.IsEmpty() {
		self.AddSql("DELETE FROM")
	} else {
		self.AddParams(exp.Params())
		self.AddSql("DELETE FROM ")
		if d.only {
			self.AddSql("ONLY ")
		}
		self.AddSql(exp.String())
	}
	return self
}
