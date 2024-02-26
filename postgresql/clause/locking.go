package postgresql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/clause"
)

type LockingClause[T sqb.Statement[T]] struct {
	*sql.LockingClause[T]
}

func NewLockingClause[T sqb.Statement[T]](self T) *LockingClause[T] {
	return &LockingClause[T]{sql.NewLockingClause[T](self)}
}

// ForNoKeyUpdate sets the lock for no key update clause of the statement:
//   - ForNoKeyUpdate(table any)
//   - ForNoKeyUpdate(table any, option string)
func (l *LockingClause[T]) ForNoKeyUpdate(args ...any) T {
	return l.ForLock("NO KEY UPDATE", args...)
}

// ForKeyShare sets the lock for key share clause of the statement:
//   - ForKeyShare(table any)
//   - ForKeyShare(table any, option string)
func (l *LockingClause[T]) ForKeyShare(args ...any) T {
	return l.ForLock("KEY SHARE", args...)
}

func (l *LockingClause[T]) CopyLock() *LockingClause[T] {
	return &LockingClause[T]{l.LockingClause.CopyLock()}
}
