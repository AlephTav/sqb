package execution

import "github.com/AlephTav/sqb"

type StatementExecution[T sqb.Statement[T]] struct {
	self T
}

func NewStatementExecution[T sqb.Statement[T]](self T) *StatementExecution[T] {
	return &StatementExecution[T]{self}
}

func (s *StatementExecution[T]) Exec() (int, error) {
	return s.self.Executor().Exec(s.self.String(), s.self.Params())
}
