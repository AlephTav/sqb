package sql

import (
	"strconv"

	"github.com/AlephTav/sqb"
)

type LimitClause[T sqb.Statement[T]] struct {
	self     T
	limit    int
	withties bool
	by       string
}

func NewLimitClause[T sqb.Statement[T]](self T) *LimitClause[T] {
	return &LimitClause[T]{self, -1, false, ""}
}

func (l *LimitClause[T]) Limit(limit int) T {
	l.limit = limit
	l.self.Dirty()
	return l.self
}

func (l *LimitClause[T]) CleanLimit() T {
	l.limit = -1
	l.self.Dirty()
	return l.self
}

func (l *LimitClause[T]) CopyLimit(self T) *LimitClause[T] {
	return &LimitClause[T]{self, l.limit, l.withties, l.by}
}

func (l *LimitClause[T]) WithTies() T {
	l.withties = true
	l.self.Dirty()
	return l.self
}

func (l *LimitClause[T]) By(column string) T {
	l.by = column
	l.self.Dirty()
	return l.self
}

func (l *LimitClause[T]) BuildLimit() T {
	if l.limit >= 0 {
		l.self.AddSql(" LIMIT " + strconv.Itoa(l.limit))
		if l.withties {
			l.self.AddSql(" WITH TIES")
		}
		if l.by != "" {
			l.self.AddSql(" BY " + l.by)
		}
	}

	return l.self
}
