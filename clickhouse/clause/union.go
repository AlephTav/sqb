package clickhouse

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

func (u *UnionClause[T]) UnionAll(query sqb.Query) T {
	return u.UnionType("UNION ALL", query)
}

func (u *UnionClause[T]) Union(query sqb.Query) T {
	return u.UnionType("UNION", query)
}
