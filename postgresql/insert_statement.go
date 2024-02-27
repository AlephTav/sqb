package postgresql

import (
	"github.com/AlephTav/sqb"
	postgresql "github.com/AlephTav/sqb/postgresql/clause"
	"github.com/AlephTav/sqb/sql"
	cls "github.com/AlephTav/sqb/sql/clause"
)

type InsertStmt struct {
	*sql.BaseStatement[*InsertStmt]
	*cls.WithClause[*InsertStmt]
	*postgresql.InsertClause[*InsertStmt]
	*cls.ColumnsClause[*InsertStmt]
	*postgresql.ValueListClause[*InsertStmt]
	*cls.QueryClause[*InsertStmt, *SelectStmt]
	*postgresql.ConflictClause[*InsertStmt]
	*cls.ReturningClause[*InsertStmt]
}

func NewInsertStmt(db sqb.StatementExecutor) *InsertStmt {
	st := &InsertStmt{}
	st.BaseStatement = sql.NewBaseStatement[*InsertStmt](st, db)
	st.WithClause = cls.NewWithClause[*InsertStmt](st)
	st.InsertClause = postgresql.NewInsertClause[*InsertStmt](st)
	st.ColumnsClause = cls.NewColumnsClause[*InsertStmt](st)
	st.ValueListClause = postgresql.NewValueListClause[*InsertStmt](st)
	st.QueryClause = cls.NewQueryClause[*InsertStmt, *SelectStmt](st)
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
	s.CleanQuery()
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
	st.QueryClause = s.CopyQuery(st)
	st.ConflictClause = s.CopyConflict(st)
	st.ReturningClause = s.CopyReturning(st)
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
	s.BuildQuery()
	s.BuildConflict()
	s.BuildReturning()
	s.Built()
	return s
}
