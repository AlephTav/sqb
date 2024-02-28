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

func (s *SelectStmt) Paginate(page, size int) *SelectStmt {
	s.Offset(size * page)
	s.Limit(size)
	return s
}

func (s *SelectStmt) Column(args ...string) ([]any, error) {
	if len(args) == 0 || args[0] == "" {
		return s.DataFetching.Column()
	}
	built := s.IsBuilt()
	prevSelect := s.SelectClause
	s.SelectClause = cls.NewSelectClause[*SelectStmt](s)
	s.Select(args[0])
	result, err := s.DataFetching.Column()
	s.SelectClause = prevSelect
	if !built {
		s.Dirty()
	}
	return result, err
}

func (s *SelectStmt) One(args ...string) (any, error) {
	if len(args) == 0 || args[0] == "" {
		return s.DataFetching.One()
	}
	built := s.IsBuilt()
	prevSelect := s.SelectClause
	s.SelectClause = cls.NewSelectClause[*SelectStmt](s)
	s.Select(args[0])
	result, err := s.DataFetching.One()
	s.SelectClause = prevSelect
	if !built {
		s.Dirty()
	}
	return result, err
}

func (s *SelectStmt) Count(column string) (int, error) {
	prevLimit := s.LimitClause
	prevOffset := s.OffsetClause
	prevOrder := s.OrderClause
	prevGroup := s.GroupClause
	s.LimitClause = cls.NewLimitClause[*SelectStmt](s)
	s.OffsetClause = cls.NewOffsetClause[*SelectStmt](s)
	s.OrderClause = cls.NewOrderClause[*SelectStmt](s)
	s.GroupClause = cls.NewGroupClause[*SelectStmt](s)
	result, err := s.CountWithNonConditionalClauses(column)
	s.LimitClause = prevLimit
	s.OffsetClause = prevOffset
	s.OrderClause = prevOrder
	s.GroupClause = prevGroup
	return result, err
}

func (s *SelectStmt) CountWithNonConditionalClauses(column string) (int, error) {
	result, err := s.One("COUNT(" + column + ")")
	if err != nil {
		return result.(int), err
	}
	return 0, err
}

func (s *SelectStmt) Pages(size, page int) func() (map[string]any, error) {
	var err error
	var rows []map[string]any
	i, count := -1, 0
	return func() (map[string]any, error) {
		for {
			if i < 0 {
				if rows, err = s.Paginate(page, size).Rows(); err != nil {
					return nil, err
				}
				count = len(rows)
			}
			if i < count-1 {
				i++
				return rows[i], nil
			}
			if count < size {
				return nil, nil
			}
			i = -1
			page++
		}
	}
}

func (s *SelectStmt) Batches(size, page int) func() ([]map[string]any, error) {
	var err error
	var count = size
	var rows []map[string]any
	return func() ([]map[string]any, error) {
		for {
			if count < size {
				return nil, nil
			}
			if rows, err = s.Paginate(page, size).Rows(); err != nil {
				return nil, err
			}
			count = len(rows)
			if count > 0 {
				page++
				return rows, nil
			}
		}
	}
}

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
	st := &SelectStmt{}
	st.WithClause = s.CopyWith(st)
	st.FromClause = s.CopyFrom(st)
	st.SelectClause = s.CopySelect(st)
	st.JoinClause = s.CopyJoin(st)
	st.WhereClause = s.CopyWhere(st)
	st.GroupClause = s.CopyGroup(st)
	st.HavingClause = s.CopyHaving(st)
	st.OrderClause = s.CopyOrder(st)
	st.LimitClause = s.CopyLimit(st)
	st.OffsetClause = s.CopyOffset(st)
	st.LockingClause = s.CopyLock(st)
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
