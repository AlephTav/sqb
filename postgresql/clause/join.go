package postgresql

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

// FullJoin adds full join on new table with alias and condition:
//   - FullJoin(table any, condition any)
//   - FullJoin(table any, alias any, condition any)
func (j *JoinClause[T]) FullJoin(table any, args ...any) T {
	return j.Join("FULL JOIN", table, args...)
}

// FullOuterJoin adds full outer join on new table with alias and condition:
//   - FullOuterJoin(table any, condition any)
//   - FullOuterJoin(table any, alias any, condition any)
func (j *JoinClause[T]) FullOuterJoin(table any, args ...any) T {
	return j.Join("FULL OUTER JOIN", table, args...)
}

// NaturalFullJoin adds natural full join on new table with alias and condition:
//   - NaturalFullJoin(table any, condition any)
//   - NaturalFullJoin(table any, alias any, condition any)
func (j *JoinClause[T]) NaturalFullJoin(table any, args ...any) T {
	return j.Join("NATURAL FULL JOIN", table, args...)
}

// NaturalFullOuterJoin adds natural full outer join on new table with alias and condition:
//   - NaturalFullOuterJoin(table any, condition any)
//   - NaturalFullOuterJoin(table any, alias any, condition any)
func (j *JoinClause[T]) NaturalFullOuterJoin(table any, args ...any) T {
	return j.Join("NATURAL FULL OUTER JOIN", table, args...)
}

func (j *JoinClause[T]) CopyJoin() *JoinClause[T] {
	return &JoinClause[T]{j.JoinClause.CopyJoin()}
}
