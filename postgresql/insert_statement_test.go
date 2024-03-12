package postgresql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
	"testing"
)

func TestInsertStmt_EmptyInsert(t *testing.T) {
	st := NewInsertStmt(nil).
		Into("tb")

	sqb.CheckSql(t, "INSERT INTO tb DEFAULT VALUES", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

//region TABLE

func TestInsertStmt_IntoTable(t *testing.T) {
	st := NewInsertStmt(nil).
		Into("tb").
		Values(sqb.Map("col", "val"))

	sqb.CheckSql(t, "INSERT INTO tb (col) VALUES (:p1)", st.String())
	sqb.CheckParams(t, map[string]any{"p1": "val"}, st.Params())
}

func TestInsertStmt_IntoTableWithAlias(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewInsertStmt(nil).
		Into("tb", "t").
		Values(sqb.Map("col", "val"))

	sqb.CheckSql(t, "INSERT INTO tb t (col) VALUES (:p1)", st.String())
	sqb.CheckParams(t, map[string]any{"p1": "val"}, st.Params())
}

func TestInsertStmt_IntoRawExpression(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewInsertStmt(nil).
		Into(sql.NewExp("tb AS t")).
		Values(sqb.Map("col", "val"))

	sqb.CheckSql(t, "INSERT INTO tb AS t (col) VALUES (:p1)", st.String())
	sqb.CheckParams(t, map[string]any{"p1": "val"}, st.Params())
}

//endregion

//region COLUMNS & VALUES

func TestInsertStmt_ColumnsWithDefaultValues(t *testing.T) {
	st := NewInsertStmt(nil).
		Into("tb").
		Columns([]any{"c1", "c2", "c3"})

	sqb.CheckSql(t, "INSERT INTO tb (c1, c2, c3) DEFAULT VALUES", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestInsertStmt_ColumnsAndValuesAsStrings(t *testing.T) {
	st := NewInsertStmt(nil).
		Into("tb").
		Columns("c1, c2, c3").
		Values("(1, 2, 3)")

	sqb.CheckSql(t, "INSERT INTO tb (c1, c2, c3) VALUES (1, 2, 3)", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestInsertStmt_ColumnsSeparatelyFromValues(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewInsertStmt(nil).
		Into("tb").
		Columns([]any{"c1", "c2", "c3"}).
		Values([]any{"v1", "v2", "v3"})

	sqb.CheckSql(t, "INSERT INTO tb (c1, c2, c3) VALUES (:p1, :p2, :p3)", st.String())
	sqb.CheckParams(t, map[string]any{"p1": "v1", "p2": "v2", "p3": "v3"}, st.Params())
}

func TestInsertStmt_ColumnsSeparatelyFromSetOfValues(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewInsertStmt(nil).
		Into("tb").
		Columns([]any{"c1", "c2", "c3"}).
		Values([]any{
			[]any{"v1", "v2", "v3"},
			[]any{"v4", "v5", "v6"},
			[]any{"v7", "v8", "v9"},
		})

	sqb.CheckSql(
		t,
		"INSERT INTO tb (c1, c2, c3) VALUES (:p1, :p2, :p3), (:p4, :p5, :p6), (:p7, :p8, :p9)",
		st.String(),
	)
	sqb.CheckParams(
		t,
		map[string]any{
			"p1": "v1",
			"p2": "v2",
			"p3": "v3",
			"p4": "v4",
			"p5": "v5",
			"p6": "v6",
			"p7": "v7",
			"p8": "v8",
			"p9": "v9",
		},
		st.Params(),
	)
}

func TestInsertStmt_ColumnsWithValues(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewInsertStmt(nil).
		Into("tb").
		Values([]any{"v1", "v2", "v3"}, []any{"c1", "c2", "c3"})

	sqb.CheckSql(t, "INSERT INTO tb (c1, c2, c3) VALUES (:p1, :p2, :p3)", st.String())
	sqb.CheckParams(t, map[string]any{"p1": "v1", "p2": "v2", "p3": "v3"}, st.Params())
}

func TestInsertStmt_ColumnsWithSetOfValues(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewInsertStmt(nil).
		Into("tb").
		Values(
			[]any{
				[]any{"v1", "v2", "v3"},
				[]any{"v4", "v5", "v6"},
				[]any{"v7", "v8", "v9"},
			},
			[]any{"c1", "c2", "c3"},
		)

	sqb.CheckSql(
		t,
		"INSERT INTO tb (c1, c2, c3) VALUES (:p1, :p2, :p3), (:p4, :p5, :p6), (:p7, :p8, :p9)",
		st.String(),
	)
	sqb.CheckParams(
		t,
		map[string]any{
			"p1": "v1",
			"p2": "v2",
			"p3": "v3",
			"p4": "v4",
			"p5": "v5",
			"p6": "v6",
			"p7": "v7",
			"p8": "v8",
			"p9": "v9",
		},
		st.Params(),
	)
}

func TestInsertStmt_ValuesWithSliceMap(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewInsertStmt(nil).
		Into("tb").
		Values(sqb.Map("c1", "v1", "c2", "v2", "c3", "v3"))

	sqb.CheckSql(t, "INSERT INTO tb (c1, c2, c3) VALUES (:p1, :p2, :p3)", st.String())
	sqb.CheckParams(t, map[string]any{"p1": "v1", "p2": "v2", "p3": "v3"}, st.Params())
}

func TestInsertStmt_ValuesWithMap(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewInsertStmt(nil).
		Into("tb").
		Values(map[string]any{
			"c1": "v1",
			"c2": "v2",
		})

	sqb.CheckSql(
		t,
		[]string{
			"INSERT INTO tb (c1, c2) VALUES (:p1, :p2)",
			"INSERT INTO tb (c2, c1) VALUES (:p1, :p2)",
		},
		st.String(),
	)
	sqb.CheckParams(
		t,
		[]map[string]any{
			{"p1": "v1", "p2": "v2"},
			{"p1": "v2", "p2": "v1"},
		},
		st.Params(),
	)
}

func TestInsertStmt_ValuesWithSliceOfSliceMaps(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewInsertStmt(nil).
		Into("tb").
		Values(
			[]any{
				sqb.Map("c1", "v1", "c2", "v2", "c3", "v3"),
				sqb.Map("c1", "v4", "c2", "v5", "c3", "v6"),
				sqb.Map("c1", "v7", "c2", nil, "c3", sql.NewExp("DEFAULT")),
			},
		)

	sqb.CheckSql(
		t,
		"INSERT INTO tb (c1, c2, c3) VALUES (:p1, :p2, :p3), (:p4, :p5, :p6), (:p7, NULL, DEFAULT)",
		st.String(),
	)
	sqb.CheckParams(
		t,
		map[string]any{"p1": "v1", "p2": "v2", "p3": "v3", "p4": "v4", "p5": "v5", "p6": "v6", "p7": "v7"},
		st.Params(),
	)
}

func TestInsertStmt_ValuesWithSliceOfMaps(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewInsertStmt(nil).
		Into("tb").
		Values(
			[]map[string]any{
				{"c1": "v1", "c2": nil},
				{"c1": "v2", "c2": sql.NewExp("DEFAULT")},
			},
		)

	sqb.CheckSql(
		t,
		[]string{
			"INSERT INTO tb (c1, c2) VALUES (:p1, NULL), (:p2, DEFAULT)",
			"INSERT INTO tb (c2, c1) VALUES (NULL, :p1), (DEFAULT, :p2)",
		},
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{"p1": "v1", "p2": "v2"}, st.Params())
}

func TestInsertStmt_ValuesWithoutColumns(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewInsertStmt(nil).
		Into("tb").
		Values([]any{"v1", "v2", "v3"})

	sqb.CheckSql(t, "INSERT INTO tb VALUES (:p1, :p2, :p3)", st.String())
	sqb.CheckParams(t, map[string]any{"p1": "v1", "p2": "v2", "p3": "v3"}, st.Params())
}

//endregion

//region SELECT

func TestInsertStmt_InsertFromQuery(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewInsertStmt(nil).
		Into("t1").
		Columns([]any{"t1.c1", "t1.c2", "t1.c3"}).
		Select(
			NewSelectStmt(nil).
				From("t2").
				Select([]any{"t2.c1", "t2.c2", "t3.c3"}).
				Where("t2.c1", "=", 123),
		)

	sqb.CheckSql(t, "INSERT INTO t1 (t1.c1, t1.c2, t1.c3) SELECT t2.c1, t2.c2, t3.c3 FROM t2 WHERE t2.c1 = :p1", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 123}, st.Params())
}

//endregion

//region ON CONFLICT

func TestInsertStmt_OnConflictDoNothing(t *testing.T) {
	st := NewInsertStmt(nil).
		Into("tb").
		OnConflict().
		DoNothing()

	sqb.CheckSql(t, "INSERT INTO tb DEFAULT VALUES ON CONFLICT DO NOTHING", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestInsertStmt_OnConflictDoNothingAsOneMethodCall(t *testing.T) {
	st := NewInsertStmt(nil).
		Into("tb").
		OnConflictDoNothing()

	sqb.CheckSql(t, "INSERT INTO tb DEFAULT VALUES ON CONFLICT DO NOTHING", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestInsertStmt_OnSingleIndexConflictDoNothing(t *testing.T) {
	st := NewInsertStmt(nil).
		Into("tb").
		OnConflict("col").
		DoNothing()

	sqb.CheckSql(t, "INSERT INTO tb DEFAULT VALUES ON CONFLICT (col) DO NOTHING", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestInsertStmt_OnSingleIndexConflictDoNothingAsOneMethodCall(t *testing.T) {
	st := NewInsertStmt(nil).
		Into("tb").
		OnConflictDoNothing("col")

	sqb.CheckSql(t, "INSERT INTO tb DEFAULT VALUES ON CONFLICT (col) DO NOTHING", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestInsertStmt_OnSingleIndexConflictDoUpdateSingleColumn(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewInsertStmt(nil).
		Into("tb").
		OnConflict("c1").
		DoUpdate("c2", 123)

	sqb.CheckSql(t, "INSERT INTO tb DEFAULT VALUES ON CONFLICT (c1) DO UPDATE SET c2 = :p1", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 123}, st.Params())
}

func TestInsertStmt_OnSingleIndexConflictDoUpdateSingleColumnAsOneMethodCall(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewInsertStmt(nil).
		Into("tb").
		OnConflictDoUpdate("c1", "c2", 123)

	sqb.CheckSql(t, "INSERT INTO tb DEFAULT VALUES ON CONFLICT (c1) DO UPDATE SET c2 = :p1", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 123}, st.Params())
}

func TestInsertStmt_OnMultipleIndexConflictDoUpdateMultipleColumns(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewInsertStmt(nil).
		Into("tb").
		OnConflict([]any{"c1", "c2", "c3"}).
		DoUpdate(sqb.Map("c4", 123, "c5", "abc"))

	sqb.CheckSql(t, "INSERT INTO tb DEFAULT VALUES ON CONFLICT (c1, c2, c3) DO UPDATE SET c4 = :p1, c5 = :p2", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 123, "p2": "abc"}, st.Params())
}

func TestInsertStmt_OnConflictDoUpdateWithAppendedColumns(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewInsertStmt(nil).
		Into("tb").
		OnConflict("c1").
		OnConflict("c2").
		DoUpdate("c3", 123).
		DoUpdate("c4", "abc").
		DoUpdate("c5", sql.NewExp("NULL"))

	sqb.CheckSql(t, "INSERT INTO tb DEFAULT VALUES ON CONFLICT (c1, c2) DO UPDATE SET c3 = :p1, c4 = :p2, c5 = NULL", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 123, "p2": "abc"}, st.Params())
}

func TestInsertStmt_OnConflictWithConstraint(t *testing.T) {
	st := NewInsertStmt(nil).
		Into("tb").
		OnConflict("c1").
		OnConstraint("const_name")

	sqb.CheckSql(t, "INSERT INTO tb DEFAULT VALUES ON CONFLICT (c1) ON CONSTRAINT const_name DO NOTHING", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestInsertStmt_OnConflictWithConditionAsString(t *testing.T) {
	st := NewInsertStmt(nil).
		Into("tb").
		OnConflict("c1", "c1 IS NULL").
		OnConflict("c2", "c2 IS NULL")

	sqb.CheckSql(t, "INSERT INTO tb DEFAULT VALUES ON CONFLICT (c1, c2) WHERE c1 IS NULL AND c2 IS NULL DO NOTHING", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestInsertStmt_OnConflictWithConditionAsExpression(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewInsertStmt(nil).
		Into("tb").
		OnConflict(
			"c1",
			sql.NewCondExp().
				Where("c2", "=", true).
				OrWhere("c3", ">", 5),
		)

	sqb.CheckSql(t, "INSERT INTO tb DEFAULT VALUES ON CONFLICT (c1) WHERE c2 = :p1 OR c3 > :p2 DO NOTHING", st.String())
	sqb.CheckParams(t, map[string]any{"p1": true, "p2": 5}, st.Params())
}

func TestInsertStmt_OnConflictDoUpdateWithCondition(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewInsertStmt(nil).
		Into("tb").
		OnConflict("c1").
		DoUpdateWithCondition("c1 = NULL", "c1 > 5").
		DoUpdateWithCondition(
			"c2",
			123,
			(sql.NewCondExp()).
				Where("c2", "<", 300).
				AndWhere("c3", ">", 5),
		)

	sqb.CheckSql(t, "INSERT INTO tb DEFAULT VALUES ON CONFLICT (c1) DO UPDATE SET c1 = NULL, c2 = :p3 WHERE c1 > 5 AND (c2 < :p1 AND c3 > :p2)", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 300, "p2": 5, "p3": 123}, st.Params())
}

func TestInsertStmt_OnConflictDoUpdateWithConstraintAndMultipleConditions(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewInsertStmt(nil).
		Into("tb").
		OnConstraint("const").
		OnConflict("c1").
		Where("c2", "=", true).
		Where("c3", "<", 3).
		DoUpdate("c3", 1).
		AndWhere("c5", ">", 10).
		OrWhere("c6", "<", 0)

	sqb.CheckSql(
		t,
		"INSERT INTO tb DEFAULT VALUES ON CONFLICT (c1) WHERE c2 = :p1 AND c3 < :p2 ON CONSTRAINT const "+
			"DO UPDATE SET c3 = :p3 WHERE c5 > :p4 OR c6 < :p5",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{"p1": true, "p2": 3, "p3": 1, "p4": 10, "p5": 0}, st.Params())
}

//endregion

//region RETURNING

func TestInsertStmt_ReturningAllColumns(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewInsertStmt(nil).
		Into("tb").
		Values(sqb.Map("c1", "v1")).
		Returning("*")

	sqb.CheckSql(t, "INSERT INTO tb (c1) VALUES (:p1) RETURNING *", st.String())
	sqb.CheckParams(t, map[string]any{"p1": "v1"}, st.Params())
}

func TestInsertStmt_ReturningSpecificColumns(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewInsertStmt(nil).
		Into("tb").
		Values(sqb.Map("c1", "v1")).
		Returning("c1").
		Returning("c2", nil).
		Returning("col", "c3")

	sqb.CheckSql(t, "INSERT INTO tb (c1) VALUES (:p1) RETURNING c1, c2, col c3", st.String())
	sqb.CheckParams(t, map[string]any{"p1": "v1"}, st.Params())
}

func TestInsertStmt_ReturningColumnList(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewInsertStmt(nil).
		Into("tb").
		Values(sqb.Map("c1", "v1")).
		Returning([]any{"c1", "c2", "c3"})

	sqb.CheckSql(t, "INSERT INTO tb (c1) VALUES (:p1) RETURNING c1, c2, c3", st.String())
	sqb.CheckParams(t, map[string]any{"p1": "v1"}, st.Params())
}

func TestInsertStmt_ReturningColumnListWithAliases(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewInsertStmt(nil).
		Into("tb").
		Values(sqb.Map("c1", "v1")).
		Returning(sqb.Map("a", "c1", "b", "c2", "c", "c3"))

	sqb.CheckSql(t, "INSERT INTO tb (c1) VALUES (:p1) RETURNING c1 a, c2 b, c3 c", st.String())
	sqb.CheckParams(t, map[string]any{"p1": "v1"}, st.Params())
}

func TestInsertStmt_ReturningQueryWithAlias(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewInsertStmt(nil).
		Into("tb1").
		Values(sqb.Map("c1", "v1")).
		Returning(sqb.Map(
			"a",
			sql.NewCondExp(
				"NOT EXISTS",
				NewSelectStmt(nil).From("tb2").Where("c2", "=", 0),
			),
		))

	sqb.CheckSql(t, "INSERT INTO tb1 (c1) VALUES (:p1) RETURNING (NOT EXISTS (SELECT * FROM tb2 WHERE c2 = :p2)) a", st.String())
	sqb.CheckParams(t, map[string]any{"p1": "v1", "p2": 0}, st.Params())
}

//endregion

//region Copy & CLean

func TestInsertStmt_Copy(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewInsertStmt(nil).
		Into("tb", "t").
		With("(SELECT * FROM t1)", "tb").
		OnConflict("c1").
		DoNothing().
		Values(sqb.Map("c1", "v1")).
		Returning([]any{"c1", "c2", "c3"})

	double := st.Copy()

	sqb.CheckSql(
		t,
		"WITH tb AS (SELECT * FROM t1) "+
			"INSERT INTO tb t (c1) VALUES (:p1) ON CONFLICT (c1) DO NOTHING RETURNING c1, c2, c3",
		double.String(),
	)
	sqb.CheckParams(t, map[string]any{"p1": "v1"}, double.Params())
}

func TestInsertStmt_Clean(t *testing.T) {
	st := NewInsertStmt(nil).
		Into("tb", "t").
		With("(SELECT * FROM t1)", "tb").
		OnConflict("c1").
		DoNothing().
		Values(sqb.Map("c1", "v1")).
		Returning([]any{"c1", "c2", "c3"}).
		Clean()

	sqb.CheckSql(t, "INSERT INTO DEFAULT VALUES", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

//endregion
