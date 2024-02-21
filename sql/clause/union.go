package sql

import "github.com/AlephTav/sqb"

type UnionQuery[T any] struct {
	UnionType string
	Query     sqb.QueryStmt[T]
}

type UnionClause[T sqb.QueryStmt[T]] struct {
	self    T
	queries []UnionQuery[T]
}

func NewUnionClause[T sqb.QueryStmt[T]](self T) *UnionClause[T] {
	return &UnionClause[T]{self, nil}
}

func (u *UnionClause[T]) IsUnion() bool {
	return len(u.queries) > 0
}

func (u *UnionClause[T]) Union(query sqb.QueryStmt[T]) T {
	return u.UnionType("UNION", query)
}

func (u *UnionClause[T]) UnionAll(query sqb.QueryStmt[T]) T {
	return u.UnionType("UNION ALL", query)
}

func (u *UnionClause[T]) UnionType(unionType string, query sqb.QueryStmt[T]) T {
	if u.IsUnion() {
		u.queries = append(u.queries, UnionQuery[T]{unionType, u.self.Copy()})
		u.queries = append(u.queries, UnionQuery[T]{unionType, query})
		u.self.Clean()
	} else {
		u.queries = append(u.queries, UnionQuery[T]{unionType, query})
	}
	return u.self
}

func (u *UnionClause[T]) BuildUnion() T {
	var notFirst bool
	for _, item := range u.queries {
		if notFirst {
			u.self.AddSql(" ")
			u.self.AddSql(item.UnionType)
			u.self.AddSql(" ")
		}
		u.self.AddSql("(")
		u.self.AddSql(item.Query.String())
		u.self.AddSql(")")
		u.self.AddParams(item.Query.Params())
		notFirst = true
	}
	return u.self
}
