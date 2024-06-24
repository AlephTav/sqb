package postgresql

import (
	"github.com/AlephTav/sqb"
	"github.com/AlephTav/sqb/execution"
	postgresql "github.com/AlephTav/sqb/postgresql/clause"
	"github.com/AlephTav/sqb/sql"
	cls "github.com/AlephTav/sqb/sql/clause"
)

type MergeStmt struct {
	*execution.DataFetching[*MergeStmt]
	*execution.StatementExecution[*MergeStmt]
	*sql.BaseStatement[*MergeStmt]
	*cls.WithClause[*MergeStmt]
	*postgresql.MergeClause[*MergeStmt]
	*cls.UsingClause[*MergeStmt]
	*postgresql.OnClause[*MergeStmt]
	*postgresql.MatchClause[*MergeStmt, *InsertStmt, *UpdateStmt]
}

func NewMergeStmt(db sqb.StatementExecutor) *MergeStmt {
	st := &MergeStmt{}

	st.DataFetching = execution.NewDataFetching[*MergeStmt](st)
	st.StatementExecution = execution.NewStatementExecution[*MergeStmt](st)
	st.BaseStatement = sql.NewBaseStatement[*MergeStmt](st, db)
	st.WithClause = cls.NewWithClause[*MergeStmt](st)
	st.MergeClause = postgresql.NewMergeClause[*MergeStmt](st)
	st.UsingClause = cls.NewUsingClause[*MergeStmt](st)
	st.OnClause = postgresql.NewOnClause[*MergeStmt](st)
	st.MatchClause = postgresql.NewMatchClause[*MergeStmt, *InsertStmt, *UpdateStmt](st)

	return st
}

func (m *MergeStmt) Build() *MergeStmt {
	if m.IsBuilt() {
		return m
	}
	m.BaseStatement.Clean()
	m.BuildWith()
	m.BuildMerge()
	m.BuildUsing()
	m.BuildOn()
	m.BuildMatch()
	m.Built()
	return m
}

func (m *MergeStmt) Clean() *MergeStmt {
	m.CleanWith()
	m.CleanMerge()
	m.CleanUsing()
	m.CleanOn()
	m.CleanMatch()
	return m
}

func (m *MergeStmt) Copy() *MergeStmt {
	st := &MergeStmt{}
	st.WithClause = m.CopyWith(st)
	st.MergeClause = m.CopyMerge(st)
	st.UsingClause = m.CopyUsing(st)
	st.OnClause = m.CopyOn(st)
	st.MatchClause = m.CopyMatch(st)
	st.BaseStatement = sql.NewBaseStatement[*MergeStmt](st, m.Executor())
	return st
}
