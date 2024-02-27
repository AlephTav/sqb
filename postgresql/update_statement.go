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
	st.DataFetching = execution.NewDataFetching[*UpdateStmt](st)
	st.StatementExecution = execution.NewStatementExecution[*UpdateStmt](st)
	st.BaseStatement = sql.NewBaseStatement[*UpdateStmt](st, db)
	st.WithClause = cls.NewWithClause[*UpdateStmt](st)
	st.UpdateClause = postgresql.NewUpdateClause[*UpdateStmt](st)
	st.AssignmentClause = cls.NewAssignmentClause[*UpdateStmt](st)
	st.FromClause = cls.NewFromClause[*UpdateStmt](st)
	st.WhereClause = cls.NewWhereClause[*UpdateStmt](st)
	st.ReturningClause = cls.NewReturningClause[*UpdateStmt](st)
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
	st.DataFetching = execution.NewDataFetching[*UpdateStmt](st)
	st.StatementExecution = execution.NewStatementExecution[*UpdateStmt](st)
	st.BaseStatement = sql.NewBaseStatement[*UpdateStmt](st, s.Executor())
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
