package postgresql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
	"reflect"
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
	sqb.ResetParameterIndex()
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

func TestSelectStmt_JoinValues(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewSelectStmt(nil).
		From("t1").
		RightJoin(NewValuesStmt(nil).
			Values([]any{
				[]any{"a1", 1},
				[]any{"a2", 2},
				[]any{"a3", 3},
			}),
			"t2 (name, id)",
			"t1.id = t2.id",
		)

	sqb.CheckSql(
		t,
		"SELECT * FROM t1 RIGHT JOIN (VALUES (:p1, :p2), (:p3, :p4), (:p5, :p6)) t2 (name, id) ON t1.id = t2.id",
		st.String(),
	)
	sqb.CheckParams(
		t,
		map[string]any{
			"p1": "a1",
			"p2": 1,
			"p3": "a2",
			"p4": 2,
			"p5": "a3",
			"p6": 3,
		},
		st.Params(),
	)
}

//endregion

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

//endregion

//region HAVING

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

//endregion

//region GROUP BY

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

//endregion

//region ORDER BY

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

//endregion

//region Limit & Offset

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

//endregion

//region LOCK

func TestSelectStmt_SelectForLock(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		ForUpdate()

	sqb.CheckSql(t, "SELECT * FROM t1 FOR UPDATE", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_SelectForLockWithTables(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		Limit(1).
		Offset(0).
		ForShare([]any{"t1", "t2"})

	sqb.CheckSql(t, "SELECT * FROM t1 LIMIT 1 OFFSET 0 FOR SHARE OF t1, t2", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_SelectForLockWithOption(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		ForNoKeyUpdate(nil, "NOWAIT")

	sqb.CheckSql(t, "SELECT * FROM t1 FOR NO KEY UPDATE NOWAIT", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_SelectForLockWithTablesAndOption(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		ForKeyShare("t1", "SKIP LOCKED")

	sqb.CheckSql(t, "SELECT * FROM t1 FOR KEY SHARE OF t1 SKIP LOCKED", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

//endregion

//region UNION

func TestSelectStmt_UnionOfSimpleQueries(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		Union(NewSelectStmt(nil).From("t2"))

	sqb.CheckSql(t, "(SELECT * FROM t1) UNION (SELECT * FROM t2)", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_UnionOfQueriesWithSorting(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		Union(NewSelectStmt(nil).From("t2").OrderBy("t2.id", "ASC")).
		Union(NewSelectStmt(nil).From("t3").OrderBy("t3.id", "DESC")).
		OrderBy("id", "DESC")

	sqb.CheckSql(
		t,
		"(SELECT * FROM t1) UNION (SELECT * FROM t2 ORDER BY t2.id ASC) UNION "+
			"(SELECT * FROM t3 ORDER BY t3.id DESC) ORDER BY id DESC",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestSelectStmt_UnionOfDifferentTypes(t *testing.T) {
	st := NewSelectStmt(nil).
		From("t1").
		UnionAll(NewSelectStmt(nil).From("t2")).
		UnionIntersect(NewSelectStmt(nil).From("t3")).
		UnionIntersectAll(NewSelectStmt(nil).From("t4")).
		UnionExcept(NewSelectStmt(nil).From("t5")).
		UnionExceptAll(NewSelectStmt(nil).From("t6")).
		Paginate(10, 5)

	sqb.CheckSql(
		t,
		"(SELECT * FROM t1) "+
			"UNION ALL (SELECT * FROM t2) "+
			"INTERSECT (SELECT * FROM t3) "+
			"INTERSECT ALL (SELECT * FROM t4) "+
			"EXCEPT (SELECT * FROM t5) "+
			"EXCEPT ALL (SELECT * FROM t6) "+
			"LIMIT 5 OFFSET 50",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

//endregion

//region WITH

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

func TestSelectStmt_WithRecursiveQuery(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewSelectStmt(nil).
		WithRecursive(
			NewValuesStmt(nil).
				Values([]any{1}).
				UnionAll(
					NewSelectStmt(nil).
						From("t").
						Select(sql.NewExp("n + 1")).Where("n", "<", 100),
				),
			"t(n)",
		).
		From("t").
		Select("SUM(n)")

	sqb.CheckSql(
		t,
		"WITH RECURSIVE t(n) AS ((VALUES (:p1)) UNION ALL (SELECT n + 1 FROM t WHERE n < :p2)) "+
			"SELECT SUM(n) FROM t",
		st.String(),
	)
	sqb.CheckParams(t, map[string]any{"p1": 1, "p2": 100}, st.Params())
}

//endregion

//region Copy & Clean

func TestSelectStmt_Copy(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewSelectStmt(nil).
		With("(SELECT * FROM t1)", "tb").
		Select("c1, c2").
		From("tb1", "t").
		InnerJoin("tb", "tb.id = t.id").
		Where("c1", ">", 0).
		Having("c2", "<", 1).
		OrderBy("c3").
		GroupBy("c4", "DESC").
		Limit(10).
		Offset(5).
		ForUpdate()

	double := st.Copy()

	sqb.CheckSql(
		t,
		"WITH tb AS (SELECT * FROM t1) "+
			"SELECT c1, c2 FROM tb1 t INNER JOIN tb ON tb.id = t.id WHERE c1 > :p1 "+
			"GROUP BY c4 DESC HAVING c2 < :p2 ORDER BY c3 LIMIT 10 OFFSET 5 FOR UPDATE",
		double.String(),
	)
	sqb.CheckParams(t, map[string]any{"p1": 0, "p2": 1}, double.Params())
}

func TestSelectStmt_Clean(t *testing.T) {
	st := NewSelectStmt(nil).
		With("(SELECT * FROM t1)", "tb").
		Select("c1, c2").
		From("tb1", "t").
		InnerJoin("tb", "tb.id = t.id").
		Where("c1", ">", 0).
		Having("c2", "<", 1).
		OrderBy("c3").
		GroupBy("c4", "DESC").
		Limit(10).
		Offset(5).
		ForUpdate().
		Clean()

	sqb.CheckSql(t, "SELECT *", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

//endregion

//region Statement Execution

func TestSelectStmt_PairsWithNonExistentKeyOfKey(t *testing.T) {
	st := NewSelectStmt(sqb.NewStatementExecutorMock())
	_, err := st.Pairs("c4", "c1")

	actualError := "key \"c4\" is not found in the row set"
	if err == nil || err.Error() != actualError {
		t.Errorf("Expected error is %q, %q given", actualError, err)
	}
}

func TestSelectStmt_PairsWithNonExistentKeyOfValue(t *testing.T) {
	st := NewSelectStmt(sqb.NewStatementExecutorMock())
	_, err := st.Pairs("c1", "c4")

	actualError := "key \"c4\" is not found in the row set"
	if err == nil || err.Error() != actualError {
		t.Errorf("Pairs() must throw error %q, but %q given", actualError, err)
	}
}

func TestSelectStmt_Pairs(t *testing.T) {
	st := NewSelectStmt(sqb.NewStatementExecutorMock())
	items := []struct {
		key   string
		value string
		pairs map[any]any
	}{
		{
			"c1", "c2", map[any]any{"v1": "v2", "v3": "v4", "v5": "v6"},
		},
		{
			"c2", "c1", map[any]any{"v2": "v1", "v4": "v3", "v6": "v5"},
		},
		{
			"c3", "c1", map[any]any{"a": "v1", "b": "v5"},
		},
	}
	for _, item := range items {
		pairs, err := st.Pairs(item.key, item.value)
		if err != nil {
			t.Errorf("Pairs() is failed: %s", err)
		}
		if !reflect.DeepEqual(item.pairs, pairs) {
			t.Errorf("Pairs() must return %#v, %#v received", item.pairs, pairs)
		}
	}
}

func TestSelectStmt_RowsByKeyWithNonExistentKey(t *testing.T) {
	st := NewSelectStmt(sqb.NewStatementExecutorMock())
	_, err := st.RowsByKey("c4", false)

	actualError := "key \"c4\" is not found in the row set"
	if err == nil || err.Error() != actualError {
		t.Errorf("RowsByKey() must throw error %q, but %q given", actualError, err)
	}
}

func TestSelectStmt_RowsByKey(t *testing.T) {
	st := NewSelectStmt(sqb.NewStatementExecutorMock())
	items := []struct {
		key    string
		remove bool
		rows   map[any]map[string]any
	}{
		{
			"c2",
			false,
			map[any]map[string]any{
				"v2": {"c1": "v1", "c2": "v2", "c3": "a"},
				"v4": {"c1": "v3", "c2": "v4", "c3": "b"},
				"v6": {"c1": "v5", "c2": "v6", "c3": "b"},
			},
		},
		{
			"c3",
			false,
			map[any]map[string]any{
				"a": {"c1": "v1", "c2": "v2", "c3": "a"},
				"b": {"c1": "v5", "c2": "v6", "c3": "b"},
			},
		},
		{
			"c1",
			true,
			map[any]map[string]any{
				"v1": {"c2": "v2", "c3": "a"},
				"v3": {"c2": "v4", "c3": "b"},
				"v5": {"c2": "v6", "c3": "b"},
			},
		},
		{
			"c3",
			true,
			map[any]map[string]any{
				"a": {"c1": "v1", "c2": "v2"},
				"b": {"c1": "v5", "c2": "v6"},
			},
		},
	}
	for _, item := range items {
		rows, err := st.RowsByKey(item.key, item.remove)
		if err != nil {
			t.Errorf("RowsByKey() is failed: %s", err)
		}
		if !reflect.DeepEqual(item.rows, rows) {
			t.Errorf("RowsByKey() must return %#v, %#v received", item.rows, rows)
		}
	}
}

func TestSelectStmt_RowsByGroupWithNonExistentKey(t *testing.T) {
	st := NewSelectStmt(sqb.NewStatementExecutorMock())
	_, err := st.RowsByGroup("c4", false)

	actualError := "key \"c4\" is not found in the row set"
	if err == nil || err.Error() != actualError {
		t.Errorf("RowsByGroup() must throw error %q, but %q given", actualError, err)
	}
}

func TestSelectStmt_RowsByGroup(t *testing.T) {
	st := NewSelectStmt(sqb.NewStatementExecutorMock())
	items := []struct {
		key    string
		remove bool
		rows   map[any][]map[string]any
	}{
		{
			"c2",
			false,
			map[any][]map[string]any{
				"v2": {
					{"c1": "v1", "c2": "v2", "c3": "a"},
				},
				"v4": {
					{"c1": "v3", "c2": "v4", "c3": "b"},
				},
				"v6": {
					{"c1": "v5", "c2": "v6", "c3": "b"},
				},
			},
		},
		{
			"c3",
			false,
			map[any][]map[string]any{
				"a": {
					{"c1": "v1", "c2": "v2", "c3": "a"},
				},
				"b": {
					{"c1": "v3", "c2": "v4", "c3": "b"},
					{"c1": "v5", "c2": "v6", "c3": "b"},
				},
			},
		},
		{
			"c2",
			true,
			map[any][]map[string]any{
				"v2": {
					{"c1": "v1", "c3": "a"},
				},
				"v4": {
					{"c1": "v3", "c3": "b"},
				},
				"v6": {
					{"c1": "v5", "c3": "b"},
				},
			},
		},
		{
			"c3",
			true,
			map[any][]map[string]any{
				"a": {
					{"c1": "v1", "c2": "v2"},
				},
				"b": {
					{"c1": "v3", "c2": "v4"},
					{"c1": "v5", "c2": "v6"},
				},
			},
		},
	}
	for _, item := range items {
		rows, err := st.RowsByGroup(item.key, item.remove)
		if err != nil {
			t.Errorf("RowsByGroup() is failed: %s", err)
		}
		if !reflect.DeepEqual(item.rows, rows) {
			t.Errorf("RowsByGroup() must return %#v, %#v received", item.rows, rows)
		}
	}
}

func TestSelectStmt_Pages(t *testing.T) {
	st := NewSelectStmt(sqb.NewStatementExecutorMock())
	expected := []map[string]any{
		{"c1": "v1", "c2": "v2", "c3": "a"},
		{"c1": "v3", "c2": "v4", "c3": "b"},
		{"c1": "v5", "c2": "v6", "c3": "b"},
	}
	for page := 0; page < 2; page++ {
		nextRow := st.Pages(2, page)
		i := 2 * page
		for {
			row, err := nextRow()
			if err != nil {
				t.Errorf("Pages(2, %d) is failed on iteration #%d: %s", page, i+1, err)
			}
			if row == nil {
				break
			}
			if !reflect.DeepEqual(row, expected[i]) {
				t.Errorf(
					"Pages(2, %d) on iteration #%d must return %#v, but %#v received",
					page,
					i+1,
					expected[i],
					row,
				)
			}
			i++
		}
	}
}

func TestSelectStmt_Batches(t *testing.T) {
	st := NewSelectStmt(sqb.NewStatementExecutorMock())
	expected := [][]map[string]any{
		{
			{"c1": "v1", "c2": "v2", "c3": "a"},
			{"c1": "v3", "c2": "v4", "c3": "b"},
		},
		{
			{"c1": "v5", "c2": "v6", "c3": "b"},
		},
	}
	for page := 0; page < 2; page++ {
		nextRow := st.Batches(2, page)
		i := page
		for {
			rows, err := nextRow()
			if err != nil {
				t.Errorf("Batches(2, %d) is failed on iteration #%d: %s", page, i+1, err)
			}
			if rows == nil {
				break
			}
			if !reflect.DeepEqual(rows, expected[i]) {
				t.Errorf(
					"Batches(2, %d) on iteration #%d must return %#v, but %#v received",
					page,
					i+1,
					expected[i],
					rows,
				)
			}
			i++
		}
	}
}

//endregion
