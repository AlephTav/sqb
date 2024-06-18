package clickhouse

import (
	"fmt"
	"testing"

	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

//region SELECT

func TestSelectStmt_SelectWithColumns(t *testing.T) {
	st := NewSelectStmt(nil).
		Select("COLUMNS('a')").
		From("col_names")
	sqb.CheckSql(t, "SELECT COLUMNS('a') FROM col_names", st.String())
	sqb.CheckParams(t, map[string]interface{}{}, st.Params())
}

func TestSelectStmt_SelectApply(t *testing.T) {
	st := NewSelectStmt(nil).
		Apply("sum").
		From("columns_transformers")

	sqb.CheckSql(t, "SELECT * APPLY(sum) FROM columns_transformers", st.String())
	sqb.CheckParams(t, map[string]interface{}{}, st.Params())
}

func TestSelectStmt_SelectReplace(t *testing.T) {
	st := NewSelectStmt(nil).
		Replace("i + 1 AS i").
		From("columns_transformers")
	sqb.CheckSql(t, "SELECT * REPLACE(i + 1 AS i) FROM columns_transformers", st.String())
	sqb.CheckParams(t, map[string]interface{}{}, st.Params())
}

func TestSelectStmt_SelectApplyReplceMixed(t *testing.T) {
	st := NewSelectStmt(nil).
		Replace("i + 1 AS i").
		Apply("sum").
		From("columns_transformers")
	sqb.CheckSql(t, "SELECT * REPLACE(i + 1 AS i) APPLY(sum) FROM columns_transformers", st.String())
	sqb.CheckParams(t, map[string]interface{}{}, st.Params())
}
func TestSelectStmt_SelectMultiplyApply(t *testing.T) {
	st := NewSelectStmt(nil).
		Select("COLUMNS('[jk]')").
		Apply("toString").
		Apply("length").
		Apply("max").
		From("columns_transformers")
	fmt.Println(st.String())
	sqb.CheckSql(t, "SELECT COLUMNS('[jk]') APPLY(toString) APPLY(length) APPLY(max) FROM columns_transformers", st.String())
	sqb.CheckParams(t, map[string]interface{}{}, st.Params())
}

/*
func TestSelectStmt_SelectMultiply(t *testing.T) {
	st := NewSelectStmt(nil).
		Replace("i + 1 AS i").
		Except("j").
		Apply("sum").
		From("columns_transformers")
	fmt.Println(st.String())
	sqb.CheckSql(t, "SELECT * REPLACE(i + 1 AS i) EXCEPT (j) APPLY(sum) from columns_transformers", st.String())
	sqb.CheckParams(t, map[string]interface{}{}, st.Params())
}
*/
// region ALL
func TestSelectStmt_SelectALL(t *testing.T) {
	st := NewSelectStmt(nil).
		All().
		From("columns_transformers")

	sqb.CheckSql(t, "SELECT ALL FROM columns_transformers", st.String())
	sqb.CheckParams(t, map[string]interface{}{}, st.Params())
}

// region ARRAY JOIN
func TestSelectStmt_SimpleArrayJoin(t *testing.T) {
	st := NewSelectStmt(nil).
		Select("s").
		From("array_test").
		ArrayJoin("arr")

	sqb.CheckSql(
		t,
		"SELECT s FROM array_test ARRAY JOIN arr",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_ArrayJoinMultipleColumns(t *testing.T) {
	st := NewSelectStmt(nil).
		Select("s, a, b").
		From("array_test").
		ArrayJoin("arr, arr2")

	sqb.CheckSql(
		t,
		"SELECT s, a, b FROM array_test ARRAY JOIN arr, arr2",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_ArrayJoinWithAlias(t *testing.T) {
	st := NewSelectStmt(nil).
		Select("s").
		From("array_test").
		ArrayJoin("arr as a")

	sqb.CheckSql(
		t,
		"SELECT s FROM array_test ARRAY JOIN arr as a",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_ArrayJoinWithComplexExpressions(t *testing.T) {
	st := NewSelectStmt(nil).
		Select("s, sum(arr) as total").
		From("array_test").
		ArrayJoin("arr")

	sqb.CheckSql(
		t,
		"SELECT s, sum(arr) as total FROM array_test ARRAY JOIN arr",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

//region DISTINCT

func TestSelectStmt_SelectDistinct(t *testing.T) {

	st := NewSelectStmt(nil).
		Select("number").
		Distinct().
		From("numbers(1,10)").
		Intersect(NewSelectStmt(nil).Select("number").From("numbers(3,6)"))

	sqb.CheckSql(t, "SELECT DISTINCT number FROM numbers(1,10) INTERSECT (SELECT number FROM numbers(3,6))", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_SelectDistinctEmpty(t *testing.T) {

	st := NewSelectStmt(nil).
		Distinct().
		From("numbers(1,10)").
		Intersect(NewSelectStmt(nil).Select("number").From("numbers(3,6)"))

	sqb.CheckSql(t, "SELECT DISTINCT * FROM numbers(1,10) INTERSECT (SELECT number FROM numbers(3,6))", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

// region EXCEPT
func TestSelectStmt_SelectExceptWithAlias(t *testing.T) {
	st := NewSelectStmt(nil).
		From("numbers(1,10) as n1").
		Except(NewSelectStmt(nil).Select("number").From("numbers(3,6) as n2"))
	sqb.CheckSql(t, "SELECT * FROM numbers(1,10) as n1 EXCEPT (SELECT number FROM numbers(3,6) as n2)", st.String())
	sqb.CheckParams(t, map[string]interface{}{}, st.Params())
}

func TestSelectStmt_SelectExceptWithMultipleColumns(t *testing.T) {
	st := NewSelectStmt(nil).
		Select("number, name").
		From("numbers(1,10)").
		Except(NewSelectStmt(nil).Select("number, name").From("numbers(3,6)"))
	sqb.CheckSql(t, "SELECT number, name FROM numbers(1,10) EXCEPT (SELECT number, name FROM numbers(3,6))", st.String())
	sqb.CheckParams(t, map[string]interface{}{}, st.Params())
}

//region FROM

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

func TestSelectStmt_SelectFinal(t *testing.T) {
	st := NewSelectStmt(nil).
		Select("x, y").
		From("mytable").
		Final().
		Where("x > 1")
	sqb.CheckSql(t, "SELECT x, y FROM mytable FINAL WHERE x > 1", st.String())
	sqb.CheckParams(t, map[string]interface{}{}, st.Params())
}

func TestSelectStmt_SelectSettings(t *testing.T) {
	st := NewSelectStmt(nil).
		Select("x, y").
		From("mytable").
		Where("x > 1").
		Settings("final = 1")
	sqb.CheckSql(t, "SELECT x, y FROM mytable WHERE x > 1 SETTINGS final = 1", st.String())
	sqb.CheckParams(t, map[string]interface{}{}, st.Params())
}

func TestSelectStmt_SelectFromWithGroupBy(t *testing.T) {
	st := NewSelectStmt(nil).
		Select("x, COUNT(y)").
		From("mytable").
		GroupBy("x")
	sqb.CheckSql(t, "SELECT x, COUNT(y) FROM mytable GROUP BY x", st.String())
	sqb.CheckParams(t, map[string]interface{}{}, st.Params())
}
func TestSelectStmt_SelectFromWithOrderBy(t *testing.T) {
	st := NewSelectStmt(nil).
		Select("x, y").
		From("mytable").
		OrderBy("x DESC")
	sqb.CheckSql(t, "SELECT x, y FROM mytable ORDER BY x DESC", st.String())
	sqb.CheckParams(t, map[string]interface{}{}, st.Params())
}

// region GROUP BY

func TestSelectStmt_GroupByColumn(t *testing.T) {
	st := NewSelectStmt(nil).
		From("tb").
		GroupBy("col")

	sqb.CheckSql(t, "SELECT * FROM tb GROUP BY col", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_GroupByColumnWithDirection(t *testing.T) {
	st := NewSelectStmt(nil).
		From("tb").
		GroupBy("col", "DESC")

	sqb.CheckSql(t, "SELECT * FROM tb GROUP BY col DESC", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_GroupByColumnList(t *testing.T) {
	st := NewSelectStmt(nil).
		From("tb").
		GroupBy([]any{"c1", "c2", "c3"})

	sqb.CheckSql(t, "SELECT * FROM tb GROUP BY c1, c2, c3", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_GroupByColumnsWithDirections(t *testing.T) {
	st := NewSelectStmt(nil).
		From("tb").
		GroupBy(sqb.Map("c1", "ASC", "c2", "DESC", "c3", ""))

	sqb.CheckSql(t, "SELECT * FROM tb GROUP BY c1 ASC, c2 DESC, c3", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_GroupByAppendColumns(t *testing.T) {
	st := NewSelectStmt(nil).
		From("tb").
		GroupBy("c1").
		GroupBy("c2", "ASC").
		GroupBy("c3", "DESC")

	sqb.CheckSql(t, "SELECT * FROM tb GROUP BY c1, c2 ASC, c3 DESC", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_GroupByQuery(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		GroupBy(NewSelectStmt(nil).From("t2").Select("t2.id"), "DESC")

	sqb.CheckSql(t, "SELECT * FROM t1 GROUP BY (SELECT t2.id FROM t2) DESC", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_GroupByMixedSources(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		GroupBy("c1 ASC").
		GroupBy(sql.NewExp("c2 DESC")).
		GroupBy([]any{"c3", "c4"}).
		GroupBy(sqb.Map("c5", "DESC")).
		GroupBy(NewSelectStmt(nil).From("t2").Select("t2.id"))

	sqb.CheckSql(
		t,
		"SELECT * FROM t1 GROUP BY c1 ASC, c2 DESC, c3, c4, c5 DESC, (SELECT t2.id FROM t2)",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{}, st.Params())
}
func TestSelectStmt_GroupByRollup(t *testing.T) {
	st := NewSelectStmt(nil).
		Select("year, month, day, COUNT(*)").
		From("t").
		GroupBy("ROLLUP(year, month, day)")

	sqb.CheckSql(t, "SELECT year, month, day, COUNT(*) FROM t GROUP BY ROLLUP(year, month, day)", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}
func TestSelectStmt_GroupByCube(t *testing.T) {
	st := NewSelectStmt(nil).
		Select("year, month, day, COUNT(*)").
		From("t").
		GroupBy("CUBE(year, month, day)")

	sqb.CheckSql(t, "SELECT year, month, day, COUNT(*) FROM t GROUP BY CUBE(year, month, day)", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_GroupByAll(t *testing.T) {
	st := NewSelectStmt(nil).
		From("tb").
		GroupBy("ALL")

	sqb.CheckSql(t, "SELECT * FROM tb GROUP BY ALL", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

// region HAVING
func TestSelectStmt_HavingAsString(t *testing.T) {
	st := NewSelectStmt(nil).
		From("tb").
		Having("c1 = c2")

	sqb.CheckSql(t, "SELECT * FROM tb HAVING c1 = c2", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_HavingAsRawExpression(t *testing.T) {
	st := NewSelectStmt(nil).
		From("tb").
		Having(sql.NewExp("c1 = c2"))

	sqb.CheckSql(t, "SELECT * FROM tb HAVING c1 = c2", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_HavingBinaryOpWithScalar(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewSelectStmt(nil).
		From("tb").
		Having("col", "=", 1)

	sqb.CheckSql(t, "SELECT * FROM tb HAVING col = :p1", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 1}, st.Params())
}

func TestSelectStmt_HavingBinaryOpWithNull(t *testing.T) {
	st := NewSelectStmt(nil).
		From("tb").
		Having("col", "=", nil)

	sqb.CheckSql(t, "SELECT * FROM tb HAVING col = NULL", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_HavingBinaryOpWithQuery(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		Having("t1.col", "=", NewSelectStmt(nil).From("t2").Select("COUNT(*)"))

	sqb.CheckSql(t, "SELECT * FROM t1 HAVING t1.col = (SELECT COUNT(*) FROM t2)", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_HavingBinaryOpWithValueList(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewSelectStmt(nil).
		From("tb").
		Having("col", "IN", []any{1, 2, 3})

	sqb.CheckSql(t, "SELECT * FROM tb HAVING col IN (:p1, :p2, :p3)", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 1, "p2": 2, "p3": 3}, st.Params())
}

func TestSelectStmt_HavingBinaryOpBetween(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewSelectStmt(nil).
		From("tb").
		Having("col", "BETWEEN", []any{1, 2})

	sqb.CheckSql(t, "SELECT * FROM tb HAVING col BETWEEN :p1 AND :p2", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 1, "p2": 2}, st.Params())
}

func TestSelectStmt_HavingBinaryOpWithRawValue(t *testing.T) {
	st := NewSelectStmt(nil).
		From("tb").
		Having("c1", "=", sql.NewExp("c2"))

	sqb.CheckSql(t, "SELECT * FROM tb HAVING c1 = c2", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_HavingUnaryOpWithRawExpression(t *testing.T) {
	st := NewSelectStmt(nil).
		From("tb").
		Having("NOT", sql.NewExp("col"))

	sqb.CheckSql(t, "SELECT * FROM tb HAVING NOT col", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_HavingUnaryOpWithQuery(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		Having("NOT", NewSelectStmt(nil).From("t2").Select("COUNT(*)"))

	sqb.CheckSql(t, "SELECT * FROM t1 HAVING NOT (SELECT COUNT(*) FROM t2)", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_HavingWithQueryAsOperand(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewSelectStmt(nil).
		From("t1").
		Having(NewSelectStmt(nil).From("t2").Select("COUNT(*)"), ">", 5)

	sqb.CheckSql(t, "SELECT * FROM t1 HAVING (SELECT COUNT(*) FROM t2) > :p1", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 5}, st.Params())
}

func TestSelectStmt_HavingWithQueriesAsOperandAndValue(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		Having(
			NewSelectStmt(nil).From("t2").Select("COUNT(*)"),
			"<>",
			NewSelectStmt(nil).From("t3").Select("COUNT(*)"),
		)

	sqb.CheckSql(t, "SELECT * FROM t1 HAVING (SELECT COUNT(*) FROM t2) <> (SELECT COUNT(*) FROM t3)", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_HavingAsConditionList(t *testing.T) {
	st := NewSelectStmt(nil).
		From("tb").
		Having([]any{"c1 = c2", "c3 <> c4"})

	sqb.CheckSql(t, "SELECT * FROM tb HAVING c1 = c2 AND c3 <> c4", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_HavingAsConditionMap(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewSelectStmt(nil).
		From("tb").
		Having(sqb.Map("c1", 1, "c2", 2))

	sqb.CheckSql(t, "SELECT * FROM tb HAVING c1 = :p1 AND c2 = :p2", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 1, "p2": 2}, st.Params())
}

func TestSelectStmt_HavingWithNestedConditions(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewSelectStmt(nil).
		From("tb").
		Having("c1 IS NULL").
		AndHaving(sql.EmptyCondExp().
			OrWhere("c2", "=", 1).
			OrWhere("c3", "<", 2),
		)

	sqb.CheckSql(t, "SELECT * FROM tb HAVING c1 IS NULL AND (c2 = :p1 OR c3 < :p2)", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 1, "p2": 2}, st.Params())
}

// region INTERSECT
func TestSelectStmt_SelectIntersect(t *testing.T) {
	st := NewSelectStmt(nil).
		Select("number").
		From("numbers(1,10)").
		Intersect(NewSelectStmt(nil).Select("number").From("numbers(3,6)"))

	sqb.CheckSql(t, "SELECT number FROM numbers(1,10) INTERSECT (SELECT number FROM numbers(3,6))", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_SelectIntersectWithAlias(t *testing.T) {
	st := NewSelectStmt(nil).
		Select("n1.number").
		From("numbers(1,10) AS n1").
		Intersect(NewSelectStmt(nil).Select("n2.number").From("numbers(3,6) AS n2"))

	sqb.CheckSql(t, "SELECT n1.number FROM numbers(1,10) AS n1 INTERSECT (SELECT n2.number FROM numbers(3,6) AS n2)", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_SelectIntersectWithMultipleColumns(t *testing.T) {
	st := NewSelectStmt(nil).
		Select("number, name").
		From("numbers(1,10)").
		Intersect(NewSelectStmt(nil).Select("number, name").From("numbers(3,6)"))

	sqb.CheckSql(t, "SELECT number, name FROM numbers(1,10) INTERSECT (SELECT number, name FROM numbers(3,6))", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}
func TestSelectStmt_SelectIntersectWithLimit(t *testing.T) {
	st := NewSelectStmt(nil).
		Select("number").
		From("numbers(1,10)").
		Limit(10).
		Intersect(NewSelectStmt(nil).Select("number").From("numbers(3,6)"))

	sqb.CheckSql(t, "SELECT number FROM numbers(1,10) LIMIT 10 INTERSECT (SELECT number FROM numbers(3,6))", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}
func TestSelectStmt_SelectIntersectWithOffset(t *testing.T) {
	st := NewSelectStmt(nil).
		Select("number").
		From("numbers(1,10)").
		Limit(10).
		Offset(5).
		Intersect(NewSelectStmt(nil).Select("number").From("numbers(3,6)"))

	sqb.CheckSql(t, "SELECT number FROM numbers(1,10) LIMIT 10 OFFSET 5 INTERSECT (SELECT number FROM numbers(3,6))", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

//region JOIN

func TestSelectStmt_LeftSemiJoin(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		LeftSemiJoin(
			sqb.Map(
				"a2", NewSelectStmt(nil).From("t2"),
				"a3", NewSelectStmt(nil).From("t3"),
			),
			"t1.id = a2.id AND t1.id = a3.id",
		)

	sqb.CheckSql(
		t,
		"SELECT * FROM t1 LEFT SEMI JOIN ((SELECT * FROM t2) a2, (SELECT * FROM t3) a3) ON t1.id = a2.id AND t1.id = a3.id",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_RightSemiJoin(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		RightSemiJoin(
			sqb.Map(
				"a2", NewSelectStmt(nil).From("t2"),
				"a3", NewSelectStmt(nil).From("t3"),
			),
			"t1.id = a2.id AND t1.id = a3.id",
		)

	sqb.CheckSql(
		t,
		"SELECT * FROM t1 RIGHT SEMI JOIN ((SELECT * FROM t2) a2, (SELECT * FROM t3) a3) ON t1.id = a2.id AND t1.id = a3.id",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_LeftAntiJoin(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		LeftAntiJoin(
			sqb.Map(
				"a2", NewSelectStmt(nil).From("t2"),
				"a3", NewSelectStmt(nil).From("t3"),
			),
			"t1.id = a2.id AND t1.id = a3.id",
		)

	sqb.CheckSql(
		t,
		"SELECT * FROM t1 LEFT ANTI JOIN ((SELECT * FROM t2) a2, (SELECT * FROM t3) a3) ON t1.id = a2.id AND t1.id = a3.id",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{}, st.Params())
}
func TestSelectStmt_RightAntiJoin(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		RightAntiJoin(
			sqb.Map(
				"a2", NewSelectStmt(nil).From("t2"),
				"a3", NewSelectStmt(nil).From("t3"),
			),
			"t1.id = a2.id AND t1.id = a3.id",
		)

	sqb.CheckSql(
		t,
		"SELECT * FROM t1 RIGHT ANTI JOIN ((SELECT * FROM t2) a2, (SELECT * FROM t3) a3) ON t1.id = a2.id AND t1.id = a3.id",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_LeftAnyJoin(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		LeftAnyJoin(
			sqb.Map(
				"a2", NewSelectStmt(nil).From("t2"),
				"a3", NewSelectStmt(nil).From("t3"),
			),
			"t1.id = a2.id AND t1.id = a3.id",
		)

	sqb.CheckSql(
		t,
		"SELECT * FROM t1 LEFT ANY JOIN ((SELECT * FROM t2) a2, (SELECT * FROM t3) a3) ON t1.id = a2.id AND t1.id = a3.id",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_RightAnyJoin(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		RightAnyJoin(
			sqb.Map(
				"a2", NewSelectStmt(nil).From("t2"),
				"a3", NewSelectStmt(nil).From("t3"),
			),
			"t1.id = a2.id AND t1.id = a3.id",
		)

	sqb.CheckSql(
		t,
		"SELECT * FROM t1 RIGHT ANY JOIN ((SELECT * FROM t2) a2, (SELECT * FROM t3) a3) ON t1.id = a2.id AND t1.id = a3.id",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{}, st.Params())
}
func TestSelectStmt_InnerAnyJoin(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		InnerAnyJoin(
			sqb.Map(
				"a2", NewSelectStmt(nil).From("t2"),
				"a3", NewSelectStmt(nil).From("t3"),
			),
			"t1.id = a2.id AND t1.id = a3.id",
		)

	sqb.CheckSql(
		t,
		"SELECT * FROM t1 INNER ANY JOIN ((SELECT * FROM t2) a2, (SELECT * FROM t3) a3) ON t1.id = a2.id AND t1.id = a3.id",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_AsofJoin(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		AsofJoin(
			sqb.Map(
				"a2", NewSelectStmt(nil).From("t2"),
				"a3", NewSelectStmt(nil).From("t3"),
			),
			"t1.id = a2.id AND t1.id = a3.id",
		)

	sqb.CheckSql(
		t,
		"SELECT * FROM t1 ASOF JOIN ((SELECT * FROM t2) a2, (SELECT * FROM t3) a3) ON t1.id = a2.id AND t1.id = a3.id",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_LeftAsofJoin(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		LeftAsofJoin(
			sqb.Map(
				"a2", NewSelectStmt(nil).From("t2"),
				"a3", NewSelectStmt(nil).From("t3"),
			),
			"t1.id = a2.id AND t1.id = a3.id",
		)

	sqb.CheckSql(
		t,
		"SELECT * FROM t1 LEFT ASOF JOIN ((SELECT * FROM t2) a2, (SELECT * FROM t3) a3) ON t1.id = a2.id AND t1.id = a3.id",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_PasteJoin(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		PasteJoin(
			sqb.Map(
				"a2", NewSelectStmt(nil).From("t2"),
				"a3", NewSelectStmt(nil).From("t3"),
			),
			"t1.id = a2.id AND t1.id = a3.id",
		)

	sqb.CheckSql(
		t,
		"SELECT * FROM t1 PASTE JOIN ((SELECT * FROM t2) a2, (SELECT * FROM t3) a3) ON t1.id = a2.id AND t1.id = a3.id",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

//region LIMIT & OFFSET

func TestSelectStmt_SelectLimit(t *testing.T) {
	st := NewSelectStmt(nil).
		From("numbers").
		Limit(10)
	sqb.CheckSql(t, "SELECT * FROM numbers LIMIT 10", st.String())
	sqb.CheckParams(t, map[string]interface{}{}, st.Params())
}

func TestSelectStmt_Limit(t *testing.T) {
	st := NewSelectStmt(nil).
		From("tb").
		Limit(10)

	sqb.CheckSql(t, "SELECT * FROM tb LIMIT 10", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_Offset(t *testing.T) {
	st := NewSelectStmt(nil).
		From("tb").
		Offset(12)

	sqb.CheckSql(t, "SELECT * FROM tb OFFSET 12", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_LimitAndOffset(t *testing.T) {
	st := NewSelectStmt(nil).
		From("tb").
		Offset(10).
		Limit(5)

	sqb.CheckSql(t, "SELECT * FROM tb LIMIT 5 OFFSET 10", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}
func TestSelectStmt_SelectOffset(t *testing.T) {
	st := NewSelectStmt(nil).
		From("test_fetch").
		OrderBy("a").
		Limit(3).
		Offset(1)
	sqb.CheckSql(t, "SELECT * FROM test_fetch ORDER BY a LIMIT 3 OFFSET 1", st.String())
	sqb.CheckParams(t, map[string]interface{}{}, st.Params())
}

func TestSelectStmt_SelectWithTies(t *testing.T) {
	st := NewSelectStmt(nil).
		From("test_fetch").
		OrderBy("a").
		Limit(3).
		WithTies()
	sqb.CheckSql(t, "SELECT * FROM test_fetch ORDER BY a LIMIT 3 WITH TIES", st.String())
	sqb.CheckParams(t, map[string]interface{}{}, st.Params())
}

// region ORDER BY
func TestSelectStmt_OrderByColumn(t *testing.T) {
	st := NewSelectStmt(nil).
		From("tb").
		OrderBy("col")

	sqb.CheckSql(t, "SELECT * FROM tb ORDER BY col", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_OrderByColumnWithDirection(t *testing.T) {
	st := NewSelectStmt(nil).
		From("tb").
		OrderBy("col", "DESC")

	sqb.CheckSql(t, "SELECT * FROM tb ORDER BY col DESC", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_OrderByColumnList(t *testing.T) {
	st := NewSelectStmt(nil).
		From("tb").
		OrderBy([]any{"c1", "c2", "c3"})

	sqb.CheckSql(t, "SELECT * FROM tb ORDER BY c1, c2, c3", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_OrderByColumnsWithDirections(t *testing.T) {
	st := NewSelectStmt(nil).
		From("tb").
		OrderBy(sqb.Map("c1", "ASC", "c2", "DESC", "c3", ""))

	sqb.CheckSql(t, "SELECT * FROM tb ORDER BY c1 ASC, c2 DESC, c3", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_OrderByAppendColumns(t *testing.T) {
	st := NewSelectStmt(nil).
		From("tb").
		OrderBy("c1").
		OrderBy("c2", "ASC").
		OrderBy("c3", "DESC")

	sqb.CheckSql(t, "SELECT * FROM tb ORDER BY c1, c2 ASC, c3 DESC", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_OrderByQuery(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		OrderBy(NewSelectStmt(nil).From("t2").Select("t2.id"), "DESC")

	sqb.CheckSql(t, "SELECT * FROM t1 ORDER BY (SELECT t2.id FROM t2) DESC", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_OrderByMixedSources(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		OrderBy("c1 ASC").
		OrderBy(sql.NewExp("c2 DESC")).
		OrderBy([]any{"c3", "c4"}).
		OrderBy(sqb.Map("c5", "DESC")).
		OrderBy(NewSelectStmt(nil).From("t2").Select("t2.id"))

	sqb.CheckSql(
		t,
		"SELECT * FROM t1 ORDER BY c1 ASC, c2 DESC, c3, c4, c5 DESC, (SELECT t2.id FROM t2)",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_OrderByCollate(t *testing.T) {
	st := NewSelectStmt(nil).
		From("collate_test").
		OrderBy("s ASC COLLATE 'en'")

	sqb.CheckSql(
		t,
		"SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en'",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_OrderByWithFill(t *testing.T) {
	st := NewSelectStmt(nil).
		From("collate_test").
		OrderBy("s WITH FILL")

	sqb.CheckSql(
		t,
		"SELECT * FROM collate_test ORDER BY s WITH FILL",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

// region LIMIT BY

func TestSelectStmt_LimitBy(t *testing.T) {
	st := NewSelectStmt(nil).
		From("limit_by").
		OrderBy("id, val").
		Limit(1).
		By("id")

	sqb.CheckSql(
		t,
		"SELECT * FROM limit_by ORDER BY id, val LIMIT 1 BY id",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

//region PREWHERE

func TestSelectStmt_SelectPrewhere(t *testing.T) {

	st := NewSelectStmt(nil).
		From("mydata").
		Prewhere("C = 1").
		Where("B = 0")

	sqb.CheckSql(t, "SELECT * FROM mydata PREWHERE C = 1 WHERE B = 0", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

//region SAMPLE

func TestSelectStmt_SelectSample(t *testing.T) {
	st := NewSelectStmt(nil).
		Select("Title, COUNT(pages) as Views").
		From("hits_distributed").
		Sample(0.1)
	sqb.CheckSql(t, "SELECT Title, COUNT(pages) as Views FROM hits_distributed SAMPLE 0.1", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
	fmt.Println(st.String())

}

func TestSelectStmt_SelectSampleWithOffset(t *testing.T) {
	st := NewSelectStmt(nil).
		Select("Title, COUNT(pages) as Views").
		From("hits_distributed").
		Sample(0.1).
		Offset(5)
	sqb.CheckSql(t, "SELECT Title, COUNT(pages) as Views FROM hits_distributed SAMPLE 0.1 OFFSET 5", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
	fmt.Println(st.String())

}

//region WHERE

func TestSelectStmt_WhereAsString(t *testing.T) {
	st := NewSelectStmt(nil).
		From("tb").
		Where("c1 = c2")

	sqb.CheckSql(t, "SELECT * FROM tb WHERE c1 = c2", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_WhereAsRawExpression(t *testing.T) {
	st := NewSelectStmt(nil).
		From("tb").
		Where(sql.NewExp("c1 = c2"))

	sqb.CheckSql(t, "SELECT * FROM tb WHERE c1 = c2", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_WhereBinaryOpWithScalar(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewSelectStmt(nil).
		From("tb").
		Where("col", "=", 1)

	sqb.CheckSql(t, "SELECT * FROM tb WHERE col = :p1", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 1}, st.Params())
}

func TestSelectStmt_WhereBinaryOpWithNull(t *testing.T) {
	st := NewSelectStmt(nil).
		From("tb").
		Where("col", "=", nil)

	sqb.CheckSql(t, "SELECT * FROM tb WHERE col = NULL", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_WhereBinaryOpWithQuery(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		Where("t1.col", "=", NewSelectStmt(nil).From("t2").Select("COUNT(*)"))

	sqb.CheckSql(t, "SELECT * FROM t1 WHERE t1.col = (SELECT COUNT(*) FROM t2)", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_WhereBinaryOpWithValueList(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewSelectStmt(nil).
		From("tb").
		Where("col", "IN", []any{1, 2, 3})

	sqb.CheckSql(t, "SELECT * FROM tb WHERE col IN (:p1, :p2, :p3)", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 1, "p2": 2, "p3": 3}, st.Params())
}

func TestSelectStmt_WhereBinaryOpBetween(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewSelectStmt(nil).
		From("tb").
		Where("col", "BETWEEN", []any{1, 2})

	sqb.CheckSql(t, "SELECT * FROM tb WHERE col BETWEEN :p1 AND :p2", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 1, "p2": 2}, st.Params())
}

func TestSelectStmt_WhereBinaryOpWithRawValue(t *testing.T) {
	st := NewSelectStmt(nil).
		From("tb").
		Where("c1", "=", sql.NewExp("c2"))

	sqb.CheckSql(t, "SELECT * FROM tb WHERE c1 = c2", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_WhereUnaryOpWithRawExpression(t *testing.T) {
	st := NewSelectStmt(nil).
		From("tb").
		Where("NOT", sql.NewExp("col"))

	sqb.CheckSql(t, "SELECT * FROM tb WHERE NOT col", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_WhereUnaryOpWithQuery(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		Where("NOT", NewSelectStmt(nil).From("t2").Select("COUNT(*)"))

	sqb.CheckSql(t, "SELECT * FROM t1 WHERE NOT (SELECT COUNT(*) FROM t2)", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_WhereWithQueryAsOperand(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewSelectStmt(nil).
		From("t1").
		Where(NewSelectStmt(nil).From("t2").Select("COUNT(*)"), ">", 5)

	sqb.CheckSql(t, "SELECT * FROM t1 WHERE (SELECT COUNT(*) FROM t2) > :p1", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 5}, st.Params())
}

func TestSelectStmt_WhereWithQueriesAsOperandAndValue(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		Where(
			NewSelectStmt(nil).From("t2").Select("COUNT(*)"),
			"<>",
			NewSelectStmt(nil).From("t3").Select("COUNT(*)"),
		)

	sqb.CheckSql(t, "SELECT * FROM t1 WHERE (SELECT COUNT(*) FROM t2) <> (SELECT COUNT(*) FROM t3)", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_WhereAsConditionList(t *testing.T) {
	st := NewSelectStmt(nil).
		From("tb").
		Where([]any{"c1 = c2", "c3 <> c4"})

	sqb.CheckSql(t, "SELECT * FROM tb WHERE c1 = c2 AND c3 <> c4", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_WhereAsConditionMap(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewSelectStmt(nil).
		From("tb").
		Where(sqb.Map("c1", 1, "c2", 2))

	sqb.CheckSql(t, "SELECT * FROM tb WHERE c1 = :p1 AND c2 = :p2", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 1, "p2": 2}, st.Params())
}

func TestSelectStmt_WhereWithNestedConditions(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewSelectStmt(nil).
		From("tb").
		Where("c1 IS NULL").
		AndWhere(sql.EmptyCondExp().
			OrWhere("c2", "=", 1).
			OrWhere("c3", "<", 2),
		)

	sqb.CheckSql(t, "SELECT * FROM tb WHERE c1 IS NULL AND (c2 = :p1 OR c3 < :p2)", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 1, "p2": 2}, st.Params())
}

//region QUALIFY

func TestSelectStmt_Qualify(t *testing.T) {
	st := NewSelectStmt(nil).
		Select("number AS partition").
		From("numbers(10)").
		Qualify("partition = 10").
		OrderBy("number")

	sqb.CheckSql(t, "SELECT number AS partition FROM numbers(10) QUALIFY partition = 10 ORDER BY number", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

// region WITH
func TestSelectStmt_WithSimpleQuery(t *testing.T) {
	st := NewSelectStmt(nil).
		With(NewSelectStmt(nil).From("t1"), "tb").
		From("tb")

	sqb.CheckSql(t, "WITH tb AS (SELECT * FROM t1) SELECT * FROM tb", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_WithRawExpression(t *testing.T) {
	st := NewSelectStmt(nil).
		With("(SELECT * FROM t1)", "tb").
		With(sql.NewExp("n1 AS NULL")).
		With(nil, "n2").
		From("tb")

	sqb.CheckSql(t, "WITH tb AS (SELECT * FROM t1), n1 AS NULL, n2 AS NULL SELECT * FROM tb", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_WithSeveralQueries(t *testing.T) {
	st := NewSelectStmt(nil).
		With(
			NewSelectStmt(nil).
				From("orders").
				Select(sqb.Map("", "region", "total_sales", "SUM(amount)")).
				GroupBy("region"),
			"regional_sales",
		).
		With(
			NewSelectStmt(nil).
				From("regional_sales").
				Select("region").
				Select("SUM(total_sales) / 10").
				Where("total_sales", ">", NewSelectStmt(nil).From("regional_sales")),
			"top_regions",
		).
		From("orders").
		Select("region").
		Select("product").
		Select(sqb.Map(
			"product_units", "SUM(quantity)",
			"product_sales", "SUM(amount)",
		)).
		Where("region", "IN", NewSelectStmt(nil).From("top_regions").Select("region")).
		OrderBy([]any{"region", "product"})

	sqb.CheckSql(
		t,
		"WITH regional_sales AS (SELECT region, SUM(amount) total_sales FROM orders GROUP BY region), "+
			"top_regions AS (SELECT region, SUM(total_sales) / 10 FROM regional_sales WHERE total_sales > "+
			"(SELECT * FROM regional_sales)) SELECT region, product, SUM(quantity) product_units, "+
			"SUM(amount) product_sales FROM orders WHERE region IN (SELECT region FROM top_regions) "+
			"ORDER BY region, product",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

// region FORMAT & INTO OUTFILE

func TestSelectStmt_FormatIntoOutfile(t *testing.T) {
	st := NewSelectStmt(nil).
		Select("1, 'abc'").
		IntoOutfile("'select.gz'").
		Format("CSV")
	sqb.CheckSql(t, "SELECT 1, 'abc' INTO OUTFILE 'select.gz' FORMAT CSV", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}
