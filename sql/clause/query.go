package sql

import "github.com/AlephTav/sqb"

type QueryClause[T sqb.QueryStmt[T]] struct {
	self  T
	query *T
}

func NewQueryClause[T sqb.QueryStmt[T]](self T) *QueryClause[T] {
	return &QueryClause[T]{self, nil}
}

func (q *QueryClause[T]) Select(query T) T {
	q.query = &query
	return q.self
}

func (q *QueryClause[T]) CleanQuery() T {
	q.query = nil
	return q.self
}

func (q *QueryClause[T]) CopyQuery() *QueryClause[T] {
	if q.query == nil {
		return NewQueryClause[T](q.self)
	}
	query := (*q.query).Copy()
	return &QueryClause[T]{q.self, &query}
}

func (q *QueryClause[T]) BuildQuery() T {
	if q.query != nil {
		q.self.AddParams((*q.query).Params())
		q.self.AddSql(" ")
		q.self.AddSql((*q.query).String())
	}
	return q.self
}
