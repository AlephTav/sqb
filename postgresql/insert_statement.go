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
	st := &InsertStmt{
		nil,
		s.WithClause.CopyWith(),
		s.InsertClause.CopyInsert(),
		s.ColumnsClause.CopyColumns(),
		s.ValueListClause.CopyValueList(),
		s.QueryClause.CopyQuery(),
		s.ConflictClause.CopyConflict(),
		s.ReturningClause.CopyReturning(),
	}
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
