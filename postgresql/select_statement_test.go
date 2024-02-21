package postgresql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
	"testing"
)

func TestSelectStmt_EmptySelect(t *testing.T) {
	st := NewSelectStmt(nil)

	sqb.CheckSql(t, "SELECT *", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

//region From

func TestSelectStmt_FromTable(t *testing.T) {
	st := NewSelectStmt(nil).
		From("tb")

	sqb.CheckSql(t, "SELECT * FROM tb", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_FromTableWithAlias(t *testing.T) {
	st := NewSelectStmt(nil).
		From("tb", "t1")

	sqb.CheckSql(t, "SELECT * FROM tb t1", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_FromSliceOfTables(t *testing.T) {
	st := NewSelectStmt(nil).
		From([]any{"t1", "t2", "t3"})

	sqb.CheckSql(t, "SELECT * FROM t1, t2, t3", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_FromMapOfTablesWithAliases(t *testing.T) {
	st := NewSelectStmt(nil).
		From(sqb.Map("a1", "t1", "a2", "t2", "a3", "t3"))

	sqb.CheckSql(t, "SELECT * FROM t1 a1, t2 a2, t3 a3", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_AppendTables(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		From("t2").
		From("t3")

	sqb.CheckSql(t, "SELECT * FROM t1, t2, t3", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_AppendTablesWithAliases(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1", "a1").
		From("t2", "a2").
		From("t3", "a3")

	sqb.CheckSql(t, "SELECT * FROM t1 a1, t2 a2, t3 a3", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_FromRawExpression(t *testing.T) {
	st := NewSelectStmt(nil).
		From(sql.NewExp("tb AS a"))

	sqb.CheckSql(t, "SELECT * FROM tb AS a", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_FromAnotherQuery(t *testing.T) {
	st := NewSelectStmt(nil).
		From(NewSelectStmt(nil).From("tb"))

	sqb.CheckSql(t, "SELECT * FROM (SELECT * FROM tb)", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_FromAnotherQueryWithAlias(t *testing.T) {
	st := NewSelectStmt(nil).
		From(NewSelectStmt(nil).From("tb"), "a1")

	sqb.CheckSql(t, "SELECT * FROM (SELECT * FROM tb) a1", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_FromListOfQueries(t *testing.T) {
	st := NewSelectStmt(nil).
		From([]any{
			NewSelectStmt(nil).From("t1"),
			NewSelectStmt(nil).From("t2"),
			NewSelectStmt(nil).From("t3"),
		})

	sqb.CheckSql(
		t,
		"SELECT * FROM (SELECT * FROM t1), (SELECT * FROM t2), (SELECT * FROM t3)",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_FromMapOfQueriesWithAliases(t *testing.T) {
	st := NewSelectStmt(nil).
		From(sqb.Map(
			"a1", NewSelectStmt(nil).From("t1"),
			"a2", NewSelectStmt(nil).From("t2"),
			"a3", NewSelectStmt(nil).From("t3"),
		))

	sqb.CheckSql(
		t,
		"SELECT * FROM (SELECT * FROM t1) a1, (SELECT * FROM t2) a2, (SELECT * FROM t3) a3",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_AppendMixedFrom(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1", "a1").
		From(NewSelectStmt(nil).From("t2"), "a2").
		From([]any{"t3"})

	sqb.CheckSql(t, "SELECT * FROM t1 a1, (SELECT * FROM t2) a2, t3", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

//endregion

//region SELECT

func TestSelectStmt_SelectColumn(t *testing.T) {
	st := NewSelectStmt(nil).
		Select("column").
		From("tb")

	sqb.CheckSql(t, "SELECT column FROM tb", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_SelectColumnWithAlias(t *testing.T) {
	st := NewSelectStmt(nil).
		Select("column", "a1").
		From("tb")

	sqb.CheckSql(t, "SELECT column a1 FROM tb", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_AppendColumnsToSelect(t *testing.T) {
	st := NewSelectStmt(nil).
		Select("c1").
		Select("c2", "a2").
		From("tb")

	sqb.CheckSql(t, "SELECT c1, c2 a2 FROM tb", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_SelectColumnList(t *testing.T) {
	st := NewSelectStmt(nil).
		Select([]any{"c1", "c2", "c3"}).
		From("tb")

	sqb.CheckSql(t, "SELECT c1, c2, c3 FROM tb", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_SelectColumnMapWithAliases(t *testing.T) {
	st := NewSelectStmt(nil).
		Select(sqb.Map(
			"a1", "c1",
			"a2", "c2",
			"a3", "c3",
		)).
		From("tb")

	sqb.CheckSql(t, "SELECT c1 a1, c2 a2, c3 a3 FROM tb", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_SelectFromRawExpression(t *testing.T) {
	st := NewSelectStmt(nil).
		Select(sql.NewExp("c1, c2, c3")).
		From("tb")

	sqb.CheckSql(t, "SELECT c1, c2, c3 FROM tb", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_SelectFromQuery(t *testing.T) {
	st := NewSelectStmt(nil).
		Select(NewSelectStmt(nil).From("t2")).
		From("t1")

	sqb.CheckSql(t, "SELECT (SELECT * FROM t2) FROM t1", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_SelectFromQueryWithAlias(t *testing.T) {
	st := NewSelectStmt(nil).
		Select(NewSelectStmt(nil).From("t2"), "a1").
		From("t1")

	sqb.CheckSql(t, "SELECT (SELECT * FROM t2) a1 FROM t1", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_SelectFromMixedSources(t *testing.T) {
	st := NewSelectStmt(nil).
		Select(sqb.Map(
			"a1", NewSelectStmt(nil).From("t2"),
			"a2", "c2",
			"", nil,
		)).
		Select("c3").
		Select(sql.NewValueListExp([]any{1, 2, 3}), sql.NewExp("a4")).
		From("t1")

	sqb.CheckSql(
		t,
		"SELECT (SELECT * FROM t2) a1, c2 a2, NULL, c3, (VALUES (:p1, :p2, :p3)) a4 FROM t1",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{"p1": 1, "p2": 2, "p3": 3}, st.Params())
}

//endregion

//region JOIN

func TestSelectStmt_JoinTable(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		Join("JOIN", "t2", "t2.id = t1.id")

	sqb.CheckSql(t, "SELECT * FROM t1 JOIN t2 ON t2.id = t1.id", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_JoinListOfTables(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		InnerJoin([]any{"t2", "t3"}, "t2.id = t1.id AND t3.id = t1.id")

	sqb.CheckSql(t, "SELECT * FROM t1 INNER JOIN (t2, t3) ON t2.id = t1.id AND t3.id = t1.id", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_JoinMapOfTablesWithAliases(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		InnerJoin(sqb.Map("a2", "t2", "a3", "t3"), "t2.id = t1.id AND t3.id = t1.id")

	sqb.CheckSql(
		t,
		"SELECT * FROM t1 INNER JOIN (t2 a2, t3 a3) ON t2.id = t1.id AND t3.id = t1.id",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_JoinTableWithListOfUsingColumns(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		InnerJoin("t2", []any{"t2.c1", "t2.c2"})

	sqb.CheckSql(t, "SELECT * FROM t1 INNER JOIN t2 USING (t2.c1, t2.c2)", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_JoinSubQuery(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		InnerJoin(NewSelectStmt(nil).From("t2"), "t1.id = t2.id")

	sqb.CheckSql(t, "SELECT * FROM t1 INNER JOIN (SELECT * FROM t2) ON t1.id = t2.id", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_JoinSubQueryWithAlias(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		InnerJoin(NewSelectStmt(nil).From("t2"), "a2", "t1.id = t2.id")

	sqb.CheckSql(t, "SELECT * FROM t1 INNER JOIN (SELECT * FROM t2) a2 ON t1.id = t2.id", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_JoinSubQueriesWithAliases(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		InnerJoin(
			sqb.Map(
				"a2", NewSelectStmt(nil).From("t2"),
				"a3", NewSelectStmt(nil).From("t3"),
			),
			"t1.id = a2.id AND t1.id = a3.id",
		)

	sqb.CheckSql(
		t,
		"SELECT * FROM t1 INNER JOIN ((SELECT * FROM t2) a2, (SELECT * FROM t3) a3) ON t1.id = a2.id AND t1.id = a3.id",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_JoinTableWithNestedConditions(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		InnerJoin("t2", sql.EmptyCondExp().
			Where("t2.id", "=", sql.NewExp("t1.id")).
			AndWhere("t1.f1", ">", sql.NewExp("t2.f2")).
			OrWhere("t2.f3", "<>", sql.NewExp("t1.f3")).
			OrWhere(nil),
		)

	sqb.CheckSql(
		t,
		"SELECT * FROM t1 INNER JOIN t2 ON (t2.id = t1.id AND t1.f1 > t2.f2 OR t2.f3 <> t1.f3 OR NULL)",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_JoinOfDifferentTypes(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		InnerJoin("t2").
		NaturalInnerJoin("t3").
		LeftJoin("t4").
		LeftOuterJoin("t5").
		NaturalLeftJoin("t6").
		NaturalLeftOuterJoin("t7").
		RightJoin("t8").
		RightOuterJoin("t9").
		NaturalRightJoin("t10").
		NaturalRightOuterJoin("t11").
		FullJoin("t12").
		FullOuterJoin("t13").
		NaturalFullJoin("t14").
		NaturalFullOuterJoin("t15").
		CrossJoin("t16")

	sqb.CheckSql(
		t,
		"SELECT * FROM t1 "+
			"INNER JOIN t2 "+
			"NATURAL INNER JOIN t3 "+
			"LEFT JOIN t4 "+
			"LEFT OUTER JOIN t5 "+
			"NATURAL LEFT JOIN t6 "+
			"NATURAL LEFT OUTER JOIN t7 "+
			"RIGHT JOIN t8 "+
			"RIGHT OUTER JOIN t9 "+
			"NATURAL RIGHT JOIN t10 "+
			"NATURAL RIGHT OUTER JOIN t11 "+
			"FULL JOIN t12 "+
			"FULL OUTER JOIN t13 "+
			"NATURAL FULL JOIN t14 "+
			"NATURAL FULL OUTER JOIN t15 "+
			"CROSS JOIN t16",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

//endregion
