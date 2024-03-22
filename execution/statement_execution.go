package execution

import "github.com/AlephTav/sqb"

type StatementExecution[T sqb.Statement[T]] struct {
	self T
}

func NewStatementExecution[T sqb.Statement[T]](self T) *StatementExecution[T] {
	return &StatementExecution[T]{self}
}

func (s *StatementExecution[T]) MustExec() int64 {
	return s.self.Executor().MustExec(s.self.String(), s.self.Params())
}

func (s *StatementExecution[T]) Exec() (int64, error) {
	return s.self.Executor().Exec(s.self.String(), s.self.Params())
}
