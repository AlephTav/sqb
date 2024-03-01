package postgresql

import (
	"github.com/AlephTav/sqb"
	"github.com/AlephTav/sqb/execution"
	postgresql "github.com/AlephTav/sqb/postgresql/clause"
	"github.com/AlephTav/sqb/sql"
	cls "github.com/AlephTav/sqb/sql/clause"
)

type InsertStmt struct {
	*execution.DataFetching[*InsertStmt]
	*sql.BaseStatement[*InsertStmt]
	*cls.WithClause[*InsertStmt]
	*postgresql.InsertClause[*InsertStmt]
	*cls.ColumnsClause[*InsertStmt]
	*postgresql.ValueListClause[*InsertStmt, *SelectStmt]
	*postgresql.ConflictClause[*InsertStmt]
	*cls.ReturningClause[*InsertStmt]
}

func NewInsertStmt(db sqb.StatementExecutor) *InsertStmt {
	st := &InsertStmt{}
	st.DataFetching = execution.NewDataFetching[*InsertStmt](st)
	st.BaseStatement = sql.NewBaseStatement[*InsertStmt](st, db)
	st.WithClause = cls.NewWithClause[*InsertStmt](st)
	st.InsertClause = postgresql.NewInsertClause[*InsertStmt](st)
	st.ColumnsClause = cls.NewColumnsClause[*InsertStmt](st)
	st.ValueListClause = postgresql.NewValueListClause[*InsertStmt, *SelectStmt](st)
	st.ConflictClause = postgresql.NewConflictClause[*InsertStmt](st)
	st.ReturningClause = cls.NewReturningClause[*InsertStmt](st)
	return st
}

func (s *InsertStmt) ItIsCommand() {}

func (s *InsertStmt) Clean() *InsertStmt {
	s.CleanWith()
	s.CleanInsert()
	s.CleanColumns()
	s.CleanValueList()
	s.CleanConflict()
	s.CleanReturning()
	return s
}

func (s *InsertStmt) Copy() *InsertStmt {
	st := &InsertStmt{}
	st.WithClause = s.CopyWith(st)
	st.InsertClause = s.CopyInsert(st)
	st.ColumnsClause = s.CopyColumns(st)
	st.ValueListClause = s.CopyValueList(st)
	st.ConflictClause = s.CopyConflict(st)
	st.ReturningClause = s.CopyReturning(st)
	st.DataFetching = execution.NewDataFetching[*InsertStmt](st)
	st.BaseStatement = sql.NewBaseStatement[*InsertStmt](st, s.Executor())
	return st
}

func (s *InsertStmt) Build() *InsertStmt {
	if s.IsBuilt() {
		return s
	}
	s.BaseStatement.Clean()
	s.BuildWith()
	s.BuildInsert()
	s.BuildColumns()
	s.BuildValueList()
	s.BuildConflict()
	s.BuildReturning()
	s.Built()
	return s
}

func (s *InsertStmt) Exec(sequence string) (any, error) {
	return s.Executor().Insert(s.String(), s.Params(), sequence)
}
