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
	*sql.BaseStatement[*DeleteStmt]
	*cls.WithClause[*DeleteStmt]
	*postgresql.DeleteClause[*DeleteStmt]
	*cls.UsingClause[*DeleteStmt]
	*cls.WhereClause[*DeleteStmt]
	*cls.ReturningClause[*DeleteStmt]
}

func NewDeleteStmt(db sqb.StatementExecutor) *DeleteStmt {
	st := &DeleteStmt{}
	st.DataFetching = execution.NewDataFetching[*DeleteStmt](st)
	st.BaseStatement = sql.NewBaseStatement[*DeleteStmt](st, db)
	st.WithClause = cls.NewWithClause[*DeleteStmt](st)
	st.DeleteClause = postgresql.NewDeleteClause[*DeleteStmt](st)
	st.UsingClause = cls.NewUsingClause[*DeleteStmt](st)
	st.WhereClause = cls.NewWhereClause[*DeleteStmt](st)
	st.ReturningClause = cls.NewReturningClause[*DeleteStmt](st)
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
	st.DataFetching = execution.NewDataFetching[*DeleteStmt](st)
	st.BaseStatement = sql.NewBaseStatement[*DeleteStmt](st, s.Executor())
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
