package sql

import (
	"github.com/AlephTav/sqb"
	"strconv"
)

type OffsetClause[T sqb.Statement[T]] struct {
	self   T
	offset int
}

func NewOffsetClause[T sqb.Statement[T]](self T) *OffsetClause[T] {
	return &OffsetClause[T]{self, -1}
}

func (o *OffsetClause[T]) Offset(offset int) T {
	o.offset = offset
	o.self.Dirty()
	return o.self
}

func (o *OffsetClause[T]) CleanOffset() T {
	o.offset = -1
	o.self.Dirty()
	return o.self
}

func (o *OffsetClause[T]) CopyOffset() *OffsetClause[T] {
	return &OffsetClause[T]{o.self, o.offset}
}

func (o *OffsetClause[T]) BuildOffset() T {
	if o.offset >= 0 {
		o.self.AddSql(" OFFSET " + strconv.Itoa(o.offset))
	}
	return o.self
}
