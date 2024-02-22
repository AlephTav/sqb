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
