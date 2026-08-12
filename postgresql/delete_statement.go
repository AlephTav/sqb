package postgresql

import (
	"github.com/AlephTav/sqb"
	"github.com/AlephTav/sqb/execution"
	postgresql "github.com/AlephTav/sqb/postgresql/clause"
	"github.com/AlephTav/sqb/sql"
	cls "github.com/AlephTav/sqb/sql/clause"
)

type DeleteStmt struct {
	*execution.DataFetching[*DeleteStmt]
	*execution.StatementExecution[*DeleteStmt]
	*sql.BaseStatement[*DeleteStmt]
	*cls.WithClause[*DeleteStmt]
	*postgresql.DeleteClause[*DeleteStmt]
	*cls.UsingClause[*DeleteStmt]
	*cls.WhereClause[*DeleteStmt]
	*cls.ReturningClause[*DeleteStmt]
}

func NewDeleteStmt(db sqb.StatementExecutor) *DeleteStmt {
	st := &DeleteStmt{}
	st.DataFetching = execution.NewDataFetching(st)
	st.StatementExecution = execution.NewStatementExecution(st)
	st.BaseStatement = sql.NewBaseStatement(st, db)
	st.WithClause = cls.NewWithClause(st)
	st.DeleteClause = postgresql.NewDeleteClause(st)
	st.UsingClause = cls.NewUsingClause(st)
	st.WhereClause = cls.NewWhereClause(st)
	st.ReturningClause = cls.NewReturningClause(st)
	return st
}

func (s *DeleteStmt) ItIsCommand() {}

func (s *DeleteStmt) Clean() *DeleteStmt {
	s.CleanWith()
	s.CleanDelete()
	s.CleanUsing()
	s.CleanWhere()
	s.CleanReturning()
	return s
}

func (s *DeleteStmt) Copy() *DeleteStmt {
	st := &DeleteStmt{}
	st.WithClause = s.CopyWith(st)
	st.DeleteClause = s.CopyDelete(st)
	st.UsingClause = s.CopyUsing(st)
	st.WhereClause = s.CopyWhere(st)
	st.ReturningClause = s.CopyReturning(st)
	st.DataFetching = execution.NewDataFetching(st)
	st.StatementExecution = execution.NewStatementExecution(st)
	st.BaseStatement = sql.NewBaseStatement(st, s.Executor())
	return st
}

func (s *DeleteStmt) Build() *DeleteStmt {
	if s.IsBuilt() {
		return s
	}
	s.BaseStatement.Clean()
	s.BuildWith()
	s.BuildDelete()
	s.BuildUsing()
	s.BuildWhere()
	s.BuildReturning()
	s.Built()
	return s
}
