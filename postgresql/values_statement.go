package postgresql

import (
	"github.com/AlephTav/sqb"
	"github.com/AlephTav/sqb/execution"
	postgresql "github.com/AlephTav/sqb/postgresql/clause"
	"github.com/AlephTav/sqb/sql"
	cls "github.com/AlephTav/sqb/sql/clause"
)

type ValuesStmt struct {
	*execution.DataFetching[*ValuesStmt]
	*sql.BaseStatement[*ValuesStmt]
	*postgresql.UnionClause[*ValuesStmt]
	*cls.ValuesClause[*ValuesStmt]
	*cls.OrderClause[*ValuesStmt]
	*cls.LimitClause[*ValuesStmt]
	*cls.OffsetClause[*ValuesStmt]
}

func NewValuesStmt(db sqb.StatementExecutor) *ValuesStmt {
	st := &ValuesStmt{}
	st.DataFetching = execution.NewDataFetching[*ValuesStmt](st)
	st.BaseStatement = sql.NewBaseStatement[*ValuesStmt](st, db)
	st.UnionClause = postgresql.NewUnionClause[*ValuesStmt](st)
	st.ValuesClause = cls.NewValuesClause[*ValuesStmt](st)
	st.OrderClause = cls.NewOrderClause[*ValuesStmt](st)
	st.LimitClause = cls.NewLimitClause[*ValuesStmt](st)
	st.OffsetClause = cls.NewOffsetClause[*ValuesStmt](st)
	return st
}

func (s *ValuesStmt) ItIsQuery() {}

func (s *ValuesStmt) Clean() *ValuesStmt {
	s.CleanValues()
	s.CleanOrder()
	s.CleanLimit()
	s.CleanOffset()
	return s
}

func (s *ValuesStmt) Copy() *ValuesStmt {
	st := &ValuesStmt{
		nil,
		nil,
		nil,
		s.ValuesClause.CopyValues(),
		s.OrderClause.CopyOrder(),
		s.LimitClause.CopyLimit(),
		s.OffsetClause.CopyOffset(),
	}
	st.DataFetching = execution.NewDataFetching[*ValuesStmt](st)
	st.BaseStatement = sql.NewBaseStatement[*ValuesStmt](st, s.Executor())
	st.UnionClause = postgresql.NewUnionClause[*ValuesStmt](st)
	return st
}

func (s *ValuesStmt) Build() *ValuesStmt {
	if s.IsBuilt() {
		return s
	}
	s.BaseStatement.Clean()
	if s.IsUnion() {
		s.BuildUnion()
	} else {
		s.BuildValues()
	}
	s.BuildOrder()
	s.BuildLimit()
	s.BuildOffset()
	s.Built()
	return s
}
