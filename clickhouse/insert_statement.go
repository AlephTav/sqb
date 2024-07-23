package clickhouse

import (
	"github.com/AlephTav/sqb"

	clickhouse "github.com/AlephTav/sqb/clickhouse/clause"
	"github.com/AlephTav/sqb/execution"
	"github.com/AlephTav/sqb/sql"
	cls "github.com/AlephTav/sqb/sql/clause"
)

type InsertStmt struct {
	*execution.DataFetching[*InsertStmt]
	*sql.BaseStatement[*InsertStmt]
	*clickhouse.InsertClause[*InsertStmt]
	*clickhouse.ValueListClause[*InsertStmt, *SelectStmt]

	*cls.ColumnsClause[*InsertStmt]
	*clickhouse.SettingsClause[*InsertStmt]
	*clickhouse.FromClause[*InsertStmt]
	*clickhouse.FormatClause[*InsertStmt]
}

func NewInsertStmt(db sqb.StatementExecutor) *InsertStmt {
	st := &InsertStmt{}
	st.DataFetching = execution.NewDataFetching[*InsertStmt](st)
	st.BaseStatement = sql.NewBaseStatement[*InsertStmt](st, db)
	st.InsertClause = clickhouse.NewInsertClause[*InsertStmt](st)
	st.ValueListClause = clickhouse.NewValueListClause[*InsertStmt, *SelectStmt](st)
	st.ColumnsClause = cls.NewColumnsClause[*InsertStmt](st)
	st.SettingsClause = clickhouse.NewSettingsClause[*InsertStmt](st)
	st.FromClause = clickhouse.NewFromClause[*InsertStmt](st)
	st.FormatClause = clickhouse.NewFormatClause[*InsertStmt](st)

	return st
}

func (s *InsertStmt) ItIsCommand() {}

func (s *InsertStmt) Clean() *InsertStmt {
	s.CleanColumns()
	s.CleanInsert()
	s.CleanValueList()
	s.CleanSettings()
	s.CleanFrom()
	s.CleanFormat()

	return s
}

func (s *InsertStmt) Copy() *InsertStmt {
	st := &InsertStmt{}

	st.DataFetching = execution.NewDataFetching[*InsertStmt](st)
	st.BaseStatement = sql.NewBaseStatement[*InsertStmt](st, s.Executor())
	st.InsertClause = s.CopyInsert(st)
	st.ColumnsClause = s.CopyColumns(st)
	st.ValueListClause = s.CopyValueList(st)
	st.SettingsClause = s.CopySettings(st)
	st.FromClause = s.CopyFrom(st)
	st.FormatClause = s.CopyFormat(st)

	return st
}

func (s *InsertStmt) Build() *InsertStmt {
	if s.IsBuilt() {
		return s
	}
	s.BaseStatement.Clean()
	s.BuildInsert()
	s.BuildColumns()
	s.BuildSettings()
	s.BuildValueList()
	s.BuildFrom()
	s.BuildFormat()
	s.Built()
	return s
}

func (s *InsertStmt) MustExec(sequence string) any {
	return s.Executor().MustInsert(s.String(), s.Params(), sequence)
}

func (s *InsertStmt) Exec(sequence string) (any, error) {
	return s.Executor().Insert(s.String(), s.Params(), sequence)
}
