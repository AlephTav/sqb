package clickhouse

import (
	"github.com/AlephTav/sqb"
	clickhouse "github.com/AlephTav/sqb/clickhouse/clause"
	"github.com/AlephTav/sqb/execution"
	"github.com/AlephTav/sqb/sql"
	cls "github.com/AlephTav/sqb/sql/clause"
)

type SelectStmt struct {
	*execution.DataFetching[*SelectStmt]
	*sql.BaseStatement[*SelectStmt]
	*clickhouse.UnionClause[*SelectStmt]
	*cls.WithClause[*SelectStmt]
	*cls.FromClause[*SelectStmt]
	*cls.SelectClause[*SelectStmt]
	*clickhouse.JoinClause[*SelectStmt]
	*cls.WhereClause[*SelectStmt]
	*cls.GroupClause[*SelectStmt]
	*cls.HavingClause[*SelectStmt]
	*cls.OrderClause[*SelectStmt]
	*cls.LimitClause[*SelectStmt]
	*cls.OffsetClause[*SelectStmt]
	*clickhouse.ApplyClause[*SelectStmt]
	*clickhouse.ExceptClause[*SelectStmt]
	*clickhouse.SettingsClause[*SelectStmt]
	*clickhouse.PrewhereClause[*SelectStmt]
	*clickhouse.IntersectClause[*SelectStmt]
	*clickhouse.SampleClause[*SelectStmt]
	*clickhouse.ReplaceClause[*SelectStmt]
	*clickhouse.QualifyClause[*SelectStmt]
	*clickhouse.IntoOutfileClause[*SelectStmt]
	*clickhouse.FormatClause[*SelectStmt]
}

func NewSelectStmt(db sqb.StatementExecutor) *SelectStmt {
	st := &SelectStmt{}
	st.DataFetching = execution.NewDataFetching[*SelectStmt](st)
	st.BaseStatement = sql.NewBaseStatement[*SelectStmt](st, db)
	st.UnionClause = clickhouse.NewUnionClause[*SelectStmt](st)
	st.WithClause = cls.NewWithClause[*SelectStmt](st)
	st.FromClause = cls.NewFromClause[*SelectStmt](st)
	st.SelectClause = cls.NewSelectClause[*SelectStmt](st)
	st.JoinClause = clickhouse.NewJoinClause[*SelectStmt](st)
	st.WhereClause = cls.NewWhereClause[*SelectStmt](st)
	st.GroupClause = cls.NewGroupClause[*SelectStmt](st)
	st.HavingClause = cls.NewHavingClause[*SelectStmt](st)
	st.OrderClause = cls.NewOrderClause[*SelectStmt](st)
	st.LimitClause = cls.NewLimitClause[*SelectStmt](st)
	st.OffsetClause = cls.NewOffsetClause[*SelectStmt](st)

	st.ApplyClause = clickhouse.NewApplyClause[*SelectStmt](st)
	st.ExceptClause = clickhouse.NewExceptClause[*SelectStmt](st)
	st.SettingsClause = clickhouse.NewSettingsClause[*SelectStmt](st)
	st.PrewhereClause = clickhouse.NewPrewhereClause[*SelectStmt](st)
	st.IntersectClause = clickhouse.NewIntersectClause[*SelectStmt](st)
	st.SampleClause = clickhouse.NewSampleClause[*SelectStmt](st)
	st.ReplaceClause = clickhouse.NewReplaceClause[*SelectStmt](st)
	st.QualifyClause = clickhouse.NewQualifyClause[*SelectStmt](st)
	st.IntoOutfileClause = clickhouse.NewIntoOutfileClause[*SelectStmt](st)
	st.FormatClause = clickhouse.NewFormatClause[*SelectStmt](st)
	return st
}

func (s *SelectStmt) ItIsQuery() {}

func (s *SelectStmt) Paginate(page, size int) *SelectStmt {
	s.Offset(size * page)
	s.Limit(size)
	return s
}

func (s *SelectStmt) MustColumn(args ...string) []any {
	r, err := s.Column(args...)
	if err != nil {
		panic(err)
	}
	return r
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

func (s *SelectStmt) MustOne(args ...string) any {
	r, err := s.One(args...)
	if err != nil {
		panic(err)
	}
	return r
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

func (s *SelectStmt) MustCount(column string) int64 {
	r, err := s.Count(column)
	if err != nil {
		panic(err)
	}
	return r
}

func (s *SelectStmt) Count(column string) (int64, error) {
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

func (s *SelectStmt) MustCountWithNonConditionalClauses(column string) int64 {
	r, err := s.CountWithNonConditionalClauses(column)
	if err != nil {
		panic(err)
	}
	return r
}

func (s *SelectStmt) CountWithNonConditionalClauses(column string) (int64, error) {
	cnt, err := s.One("COUNT(" + column + ")")
	if err != nil {
		return 0, err
	}
	return sqb.ToInt64(cnt)
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

	s.CleanApply()
	s.CleanExcept()
	s.CleanSettings()
	s.CleanPrewhere()
	s.CleanIntersect()
	return s
}

func (s *SelectStmt) Copy() *SelectStmt {
	st := &SelectStmt{}
	st.WithClause = s.CopyWith(st)
	st.ApplyClause = clickhouse.NewApplyClause[*SelectStmt](st)
	st.ExceptClause = clickhouse.NewExceptClause[*SelectStmt](st)
	st.IntersectClause = clickhouse.NewIntersectClause[*SelectStmt](st)
	st.PrewhereClause = clickhouse.NewPrewhereClause[*SelectStmt](st)
	st.SettingsClause = clickhouse.NewSettingsClause[*SelectStmt](st)
	st.JoinClause = clickhouse.NewJoinClause[*SelectStmt](st)
	st.FromClause = s.CopyFrom(st)
	st.SelectClause = s.CopySelect(st)

	st.WhereClause = s.CopyWhere(st)
	st.GroupClause = s.CopyGroup(st)
	st.HavingClause = s.CopyHaving(st)
	st.OrderClause = s.CopyOrder(st)
	st.LimitClause = s.CopyLimit(st)
	st.OffsetClause = s.CopyOffset(st)

	st.DataFetching = execution.NewDataFetching[*SelectStmt](st)
	st.BaseStatement = sql.NewBaseStatement[*SelectStmt](st, s.Executor())
	st.UnionClause = clickhouse.NewUnionClause[*SelectStmt](st)

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
		s.BuildReplace()
		s.BuildApply()
		s.BuildFrom()
		s.BuildExcept()
		s.BuildJoin()
		s.BuildPrewhere()
		s.BuildWhere()
		s.BuildSettings()
		s.BuildGroup()
		s.BuildHaving()
		s.BuildQualify()
		s.BuildOrder()
		s.BuildLimit()
		s.BuildSample()
		s.BuildOffset()
		s.BuildIntersect()
		s.BuildIntoOutfile()
		s.BuildFormat()

	}
	s.Built()
	return s
}
