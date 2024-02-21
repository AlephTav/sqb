package sql

import (
	"github.com/AlephTav/sqb"
	"strconv"
)

type LimitClause[T sqb.Statement[T]] struct {
	self  T
	limit int
}

func NewLimitClause[T sqb.Statement[T]](self T) *LimitClause[T] {
	return &LimitClause[T]{self, -1}
}

func (l *LimitClause[T]) Limit(limit int) T {
	l.limit = limit
	return l.self
}

func (l *LimitClause[T]) CleanLimit() T {
	l.limit = -1
	return l.self
}

func (l *LimitClause[T]) CopyLimit() *LimitClause[T] {
	return &LimitClause[T]{l.self, l.limit}
}

func (l *LimitClause[T]) BuildLimit() T {
	if l.limit >= 0 {
		l.self.AddSql(" LIMIT " + strconv.Itoa(l.limit))
	}
	return l.self
}
