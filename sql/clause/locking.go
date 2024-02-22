package sql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type LockingClause[T sqb.Statement[T]] struct {
	self         T
	lockStrength string
	lockOption   string
	lockOf       sql.DirectListExpression
}

func NewLockingClause[T sqb.Statement[T]](self T) *LockingClause[T] {
	return &LockingClause[T]{
		self,
		"",
		"",
		sql.EmptyDirectListExp(),
	}
}

// ForUpdate sets the lock for update clause of the statement:
//   - ForUpdate(table any)
//   - ForUpdate(table any, option string)
func (l *LockingClause[T]) ForUpdate(table any, args ...string) T {
	return l.ForLock("UPDATE", table, args...)
}

// ForShare sets the lock for share clause of the statement:
//   - ForShare(table any)
//   - ForShare(table any, option string)
func (l *LockingClause[T]) ForShare(table any, args ...string) T {
	return l.ForLock("SHARE", table, args...)
}

// ForLock sets the lock clause of the statement:
//   - ForLock(strength string, table any)
//   - ForLock(strength string, table any, option string)
func (l *LockingClause[T]) ForLock(strength string, table any, args ...string) T {
	var option string
	if len(args) > 0 {
		option = args[0]
	}
	if strength != l.lockStrength {
		l.lockOf.Clean()
	}
	l.lockStrength = strength
	l.lockOption = option
	if table != nil {
		l.lockOf.Append(table)
	}
	l.self.Dirty()
	return l.self
}

func (l *LockingClause[T]) CleanLock() T {
	l.lockStrength = ""
	l.lockOption = ""
	l.lockOf.Clean()
	l.self.Dirty()
	return l.self
}

func (l *LockingClause[T]) CopyLock() *LockingClause[T] {
	return &LockingClause[T]{
		l.self,
		l.lockStrength,
		l.lockOption,
		l.lockOf.Copy(),
	}
}

func (l *LockingClause[T]) BuildLock() T {
	if l.lockStrength == "" {
		return l.self
	}
	l.self.AddSql(" FRO ")
	l.self.AddSql(l.lockStrength)
	if l.lockOf.IsNotEmpty() {
		l.self.AddSql(" OF ")
		l.self.AddSql(l.lockOf.String())
		l.self.AddParams(l.lockOf.Params())
	}
	if l.lockOption != "" {
		l.self.AddSql(" ")
		l.self.AddSql(l.lockOption)
	}
	return l.self
}
