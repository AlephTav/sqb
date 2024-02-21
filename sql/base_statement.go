package sql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type BaseStatement[T sqb.Statement[T]] struct {
	sql.Expression
	db    sqb.StatementExecutor
	self  T
	built bool
}

func NewBaseStatement[T sqb.Statement[T]](self T, db sqb.StatementExecutor) *BaseStatement[T] {
	return &BaseStatement[T]{
		sql.EmptyExp(),
		db,
		self,
		false,
	}
}

func (s *BaseStatement[T]) Executor() sqb.StatementExecutor {
	return s.db
}

func (s *BaseStatement[T]) String() string {
	s.self.Build()
	return s.Expression.String()
}

func (s *BaseStatement[T]) Params() map[string]any {
	s.self.Build()
	return s.Expression.Params()
}

func (s *BaseStatement[T]) IsBuilt() bool {
	return s.built
}

func (s *BaseStatement[T]) Built() T {
	s.built = true
	return s.self
}

func (s *BaseStatement[T]) Dirty() T {
	s.built = false
	return s.self
}
