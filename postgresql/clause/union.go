package postgresql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/clause"
)

type UnionClause[T sqb.QueryStmt[T]] struct {
	*sql.UnionClause[T]
}

func NewUnionClause[T sqb.QueryStmt[T]](self T) *UnionClause[T] {
	return &UnionClause[T]{sql.NewUnionClause(self)}
}

func (u *UnionClause[T]) UnionIntersect(query sqb.Query) T {
	return u.UnionType("INTERSECT", query)
}

func (u *UnionClause[T]) UnionIntersectAll(query sqb.Query) T {
	return u.UnionType("INTERSECT ALL", query)
}

func (u *UnionClause[T]) UnionExcept(query sqb.Query) T {
	return u.UnionType("EXCEPT", query)
}

func (u *UnionClause[T]) UnionExceptAll(query sqb.Query) T {
	return u.UnionType("EXCEPT ALL", query)
}
