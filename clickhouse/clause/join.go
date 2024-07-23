package clickhouse

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/clause"
)

type JoinClause[T sqb.Statement[T]] struct {
	*sql.JoinClause[T]
}

func NewJoinClause[T sqb.Statement[T]](self T) *JoinClause[T] {
	return &JoinClause[T]{sql.NewJoinClause[T](self)}
}
func (j *JoinClause[T]) LeftSemiJoin(table any, args ...any) T {
	return j.Join("LEFT SEMI JOIN", table, args...)
}
func (j *JoinClause[T]) RightSemiJoin(table any, args ...any) T {
	return j.Join("RIGHT SEMI JOIN", table, args...)
}

func (j *JoinClause[T]) ArrayJoin(array any, args ...any) T {
	return j.Join("ARRAY JOIN", array, args...)
}
func (j *JoinClause[T]) LeftAntiJoin(array any, args ...any) T {
	return j.Join("LEFT ANTI JOIN", array, args...)
}

func (j *JoinClause[T]) RightAntiJoin(array any, args ...any) T {
	return j.Join("RIGHT ANTI JOIN", array, args...)
}
func (j *JoinClause[T]) LeftAnyJoin(array any, args ...any) T {
	return j.Join("LEFT ANY JOIN", array, args...)
}
func (j *JoinClause[T]) RightAnyJoin(array any, args ...any) T {
	return j.Join("RIGHT ANY JOIN", array, args...)
}
func (j *JoinClause[T]) InnerAnyJoin(array any, args ...any) T {
	return j.Join("INNER ANY JOIN", array, args...)
}

func (j *JoinClause[T]) AsofJoin(array any, args ...any) T {
	return j.Join("ASOF JOIN", array, args...)
}

func (j *JoinClause[T]) LeftAsofJoin(array any, args ...any) T {
	return j.Join("LEFT ASOF JOIN", array, args...)
}
func (j *JoinClause[T]) PasteJoin(array any, args ...any) T {
	return j.Join("PASTE JOIN", array, args...)
}
func (j *JoinClause[T]) CopyJoin(self T) *JoinClause[T] {
	return &JoinClause[T]{j.JoinClause.CopyJoin(self)}
}
