package postgresql

import (
	"github.com/AlephTav/sqb"
	"github.com/AlephTav/sqb/execution"
	"github.com/AlephTav/sqb/postgresql/clause"
	"github.com/AlephTav/sqb/sql"
	cls "github.com/AlephTav/sqb/sql/clause"
)

type SelectStmt struct {
	*execution.DataFetching[*SelectStmt]
	*sql.BaseStatement[*SelectStmt]
	*postgresql.UnionClause[*SelectStmt]
	*cls.WithClause[*SelectStmt]
	*cls.FromClause[*SelectStmt]
	*cls.SelectClause[*SelectStmt]
	*postgresql.JoinClause[*SelectStmt]
	*cls.WhereClause[*SelectStmt]
	*cls.GroupClause[*SelectStmt]
	*cls.HavingClause[*SelectStmt]
	*cls.OrderClause[*SelectStmt]
	*cls.LimitClause[*SelectStmt]
	*cls.OffsetClause[*SelectStmt]
	*postgresql.LockingClause[*SelectStmt]
}

func NewSelectStmt(db sqb.StatementExecutor) *SelectStmt {
	st := &SelectStmt{}
	st.DataFetching = execution.NewDataFetching[*SelectStmt](st)
	st.BaseStatement = sql.NewBaseStatement[*SelectStmt](st, db)
	st.UnionClause = postgresql.NewUnionClause[*SelectStmt](st)
	st.WithClause = cls.NewWithClause[*SelectStmt](st)
	st.FromClause = cls.NewFromClause[*SelectStmt](st)
	st.SelectClause = cls.NewSelectClause[*SelectStmt](st)
	st.JoinClause = postgresql.NewJoinClause[*SelectStmt](st)
	st.WhereClause = cls.NewWhereClause[*SelectStmt](st)
	st.GroupClause = cls.NewGroupClause[*SelectStmt](st)
	st.HavingClause = cls.NewHavingClause[*SelectStmt](st)
	st.OrderClause = cls.NewOrderClause[*SelectStmt](st)
	st.LimitClause = cls.NewLimitClause[*SelectStmt](st)
	st.OffsetClause = cls.NewOffsetClause[*SelectStmt](st)
	st.LockingClause = postgresql.NewLockingClause[*SelectStmt](st)
	return st
}

func (s *SelectStmt) ItIsQuery() {}

func (s *SelectStmt) Clean() *SelectStmt {
	s.CleanWith()
	s.CleanFrom()
	s.CleanSelect()
	s.CleanJoin()
	s.CleanWhere()
	s.CleanGroup()
	s.CleanHaving()
	s.CleanOrder()
	s.CleanLimit()
	s.CleanOffset()
	s.CleanLock()
	return s
}

func (s *SelectStmt) Copy() *SelectStmt {
	st := &SelectStmt{
		nil,
		nil,
		nil,
		s.WithClause.CopyWith(),
		s.FromClause.CopyFrom(),
		s.SelectClause.CopySelect(),
		s.JoinClause.CopyJoin(),
		s.WhereClause.CopyWhere(),
		s.GroupClause.CopyGroup(),
		s.HavingClause.CopyHaving(),
		s.OrderClause.CopyOrder(),
		s.LimitClause.CopyLimit(),
		s.OffsetClause.CopyOffset(),
		s.LockingClause.CopyLock(),
	}
	st.DataFetching = execution.NewDataFetching[*SelectStmt](st)
	st.BaseStatement = sql.NewBaseStatement[*SelectStmt](st, s.Executor())
	st.UnionClause = postgresql.NewUnionClause[*SelectStmt](st)
	return st
}

func (s *SelectStmt) Build() *SelectStmt {
	if s.IsBuilt() {
		return s
	}
	s.BaseStatement.Clean()
	if s.IsUnion() {
		s.BuildUnion()
		s.BuildOrder()
		s.BuildLimit()
		s.BuildOffset()
	} else {
		s.BuildWith()
		s.BuildSelect()
		s.BuildFrom()
		s.BuildJoin()
		s.BuildWhere()
		s.BuildGroup()
		s.BuildHaving()
		s.BuildOrder()
		s.BuildLimit()
		s.BuildOffset()
		s.BuildLock()
	}
	s.Built()
	return s
}
