package postgresql

import (
	"github.com/AlephTav/sqb"
	"github.com/AlephTav/sqb/execution"
	postgresql "github.com/AlephTav/sqb/postgresql/clause"
	"github.com/AlephTav/sqb/sql"
	cls "github.com/AlephTav/sqb/sql/clause"
)

type UpdateStmt struct {
	*execution.DataFetching[*UpdateStmt]
	*execution.StatementExecution[*UpdateStmt]
	*sql.BaseStatement[*UpdateStmt]
	*cls.WithClause[*UpdateStmt]
	*postgresql.UpdateClause[*UpdateStmt]
	*cls.AssignmentClause[*UpdateStmt]
	*cls.FromClause[*UpdateStmt]
	*cls.WhereClause[*UpdateStmt]
	*cls.ReturningClause[*UpdateStmt]
}

func NewUpdateStmt(db sqb.StatementExecutor) *UpdateStmt {
	st := &UpdateStmt{}
	st.DataFetching = execution.NewDataFetching(st)
	st.StatementExecution = execution.NewStatementExecution(st)
	st.BaseStatement = sql.NewBaseStatement(st, db)
	st.WithClause = cls.NewWithClause(st)
	st.UpdateClause = postgresql.NewUpdateClause(st)
	st.AssignmentClause = cls.NewAssignmentClause(st)
	st.FromClause = cls.NewFromClause(st)
	st.WhereClause = cls.NewWhereClause(st)
	st.ReturningClause = cls.NewReturningClause(st)
	return st
}

func (s *UpdateStmt) ItIsCommand() {}

func (s *UpdateStmt) Clean() *UpdateStmt {
	s.CleanWith()
	s.CleanUpdate()
	s.CleanAssignment()
	s.CleanFrom()
	s.CleanWhere()
	s.CleanReturning()
	return s
}

func (s *UpdateStmt) Copy() *UpdateStmt {
	st := &UpdateStmt{}
	st.WithClause = s.CopyWith(st)
	st.UpdateClause = s.CopyUpdate(st)
	st.AssignmentClause = s.CopyAssignment(st)
	st.FromClause = s.CopyFrom(st)
	st.WhereClause = s.CopyWhere(st)
	st.ReturningClause = s.CopyReturning(st)
	st.DataFetching = execution.NewDataFetching(st)
	st.StatementExecution = execution.NewStatementExecution(st)
	st.BaseStatement = sql.NewBaseStatement(st, s.Executor())
	return st
}

func (s *UpdateStmt) Build() *UpdateStmt {
	if s.IsBuilt() {
		return s
	}
	s.BaseStatement.Clean()
	s.BuildWith()
	s.BuildUpdate()
	s.BuildAssignment()
	s.BuildFrom()
	s.BuildWhere()
	s.BuildReturning()
	s.Built()
	return s
}
