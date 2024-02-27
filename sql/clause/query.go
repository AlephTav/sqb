package sql

import "github.com/AlephTav/sqb"

type QueryClause[T sqb.Statement[T], Q sqb.QueryStmt[Q]] struct {
	self  T
	query *Q
}

func NewQueryClause[T sqb.Statement[T], Q sqb.QueryStmt[Q]](self T) *QueryClause[T, Q] {
	return &QueryClause[T, Q]{self, nil}
}

func (q *QueryClause[T, Q]) Select(query Q) T {
	q.query = &query
	q.self.Dirty()
	return q.self
}

func (q *QueryClause[T, Q]) CleanQuery() T {
	q.query = nil
	q.self.Dirty()
	return q.self
}

func (q *QueryClause[T, Q]) CopyQuery(self T) *QueryClause[T, Q] {
	if q.query == nil {
		return NewQueryClause[T, Q](self)
	}
	query := (*q.query).Copy()
	return &QueryClause[T, Q]{self, &query}
}

func (q *QueryClause[T, Q]) BuildQuery() T {
	if q.query != nil {
		q.self.AddParams((*q.query).Params())
		q.self.AddSql(" ")
		q.self.AddSql((*q.query).String())
	}
	return q.self
}
