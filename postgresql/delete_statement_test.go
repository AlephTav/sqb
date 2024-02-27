package postgresql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
	"testing"
)

func TestDeleteStmt_EmptyDelete(t *testing.T) {
	st := NewDeleteStmt(nil)

	sqb.CheckSql(t, "DELETE FROM", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

//region FROM

func TestDeleteStmt_FromTable(t *testing.T) {
	st := NewDeleteStmt(nil).
		From("tb")

	sqb.CheckSql(t, "DELETE FROM tb", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestDeleteStmt_FromTableWithAlias(t *testing.T) {
	st := NewDeleteStmt(nil).
		From("tb", "t")

	sqb.CheckSql(t, "DELETE FROM tb t", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestDeleteStmt_FromTables(t *testing.T) {
	st := NewDeleteStmt(nil).
		From([]any{"t1", "t2"})

	sqb.CheckSql(t, "DELETE FROM t1, t2", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestDeleteStmt_FromTablesWithAliases(t *testing.T) {
	st := NewDeleteStmt(nil).
		From(sqb.Map("a", "t1", "b", "t2"))

	sqb.CheckSql(t, "DELETE FROM t1 a, t2 b", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestDeleteStmt_FromRawExpression(t *testing.T) {
	st := NewDeleteStmt(nil).
		From(sql.NewExp("tb AS t"))

	sqb.CheckSql(t, "DELETE FROM tb AS t", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestDeleteStmt_FromOnly(t *testing.T) {
	st := NewDeleteStmt(nil).
		FromOnly("tb")

	sqb.CheckSql(t, "DELETE FROM ONLY tb", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestDeleteStmt_FromOnlyWithAlias(t *testing.T) {
	st := NewDeleteStmt(nil).
		FromOnly("tb", "t")

	sqb.CheckSql(t, "DELETE FROM ONLY tb t", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

//endregion

//region USING

func TestDeleteStmt_UsingTable(t *testing.T) {
	st := NewDeleteStmt(nil).
		From("tb").
		Using("t")

	sqb.CheckSql(t, "DELETE FROM tb USING t", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestDeleteStmt_UsingTableWithAlias(t *testing.T) {
	st := NewDeleteStmt(nil).
		From("tb").
		Using("t1", "t")

	sqb.CheckSql(t, "DELETE FROM tb USING t1 t", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestDeleteStmt_UsingTables(t *testing.T) {
	st := NewDeleteStmt(nil).
		From("tb").
		Using([]any{"t1", "t2"})

	sqb.CheckSql(t, "DELETE FROM tb USING t1, t2", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestDeleteStmt_UsingTablesWithAliases(t *testing.T) {
	st := NewDeleteStmt(nil).
		From("tb").
		Using(sqb.Map("a", "t1", "b", "t2"))

	sqb.CheckSql(t, "DELETE FROM tb USING t1 a, t2 b", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestDeleteStmt_UsingRawExpression(t *testing.T) {
	st := NewDeleteStmt(nil).
		From("tb").
		Using(sql.NewExp("t1 AS t"))

	sqb.CheckSql(t, "DELETE FROM tb USING t1 AS t", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

//endregion

// region WHERE

func TestDeleteStmt_WhereAsString(t *testing.T) {
	st := NewDeleteStmt(nil).
		From("tb").
		Where("c1 = c2")

	sqb.CheckSql(t, "DELETE FROM tb WHERE c1 = c2", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestDeleteStmt_WhereAsRawValue(t *testing.T) {
	st := NewDeleteStmt(nil).
		From("tb").
		Where(sql.NewExp("c1 = c2"))

	sqb.CheckSql(t, "DELETE FROM tb WHERE c1 = c2", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestDeleteStmt_WhereBinaryOpWithScalar(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewDeleteStmt(nil).
		From("tb").
		Where("col", "=", 1)

	sqb.CheckSql(t, "DELETE FROM tb WHERE col = :p1", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 1}, st.Params())
}

func TestDeleteStmt_WhereBinaryOpWithNull(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewDeleteStmt(nil).
		From("tb").
		Where("col", "=", nil)

	sqb.CheckSql(t, "DELETE FROM tb WHERE col = NULL", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestDeleteStmt_WhereBinaryOpWithQuery(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewDeleteStmt(nil).
		From("tb").
		Where("col", "IN", []any{1, 2, 3})

	sqb.CheckSql(t, "DELETE FROM tb WHERE col IN (:p1, :p2, :p3)", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 1, "p2": 2, "p3": 3}, st.Params())
}

func TestDeleteStmt_whereBinaryOpBetween(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewDeleteStmt(nil).
		From("tb").
		Where("col", "BETWEEN", []any{1, 2})

	sqb.CheckSql(t, "DELETE FROM tb WHERE col BETWEEN :p1 AND :p2", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 1, "p2": 2}, st.Params())
}

func TestDeleteStmt_WhereBinaryOpWithRawValue(t *testing.T) {
	st := NewDeleteStmt(nil).
		From("tb").
		Where("c1", "=", sql.NewExp("c2"))

	sqb.CheckSql(t, "DELETE FROM tb WHERE c1 = c2", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestDeleteStmt_WhereUnaryOpWithScalar(t *testing.T) {
	st := NewDeleteStmt(nil).
		From("tb").
		Where("NOT", sql.NewExp("col"))

	sqb.CheckSql(t, "DELETE FROM tb WHERE NOT col", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestDeleteStmt_WhereUnaryOpWithQuery(t *testing.T) {
	st := NewDeleteStmt(nil).
		From("t1").
		Where("NOT", NewSelectStmt(nil).From("t2").Select("COUNT(*)"))

	sqb.CheckSql(t, "DELETE FROM t1 WHERE NOT (SELECT COUNT(*) FROM t2)", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestDeleteStmt_WhereWithQueryAsOperand(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewDeleteStmt(nil).
		From("t1").
		Where(NewSelectStmt(nil).From("t2").Select("COUNT(*)"), ">", 5)

	sqb.CheckSql(t, "DELETE FROM t1 WHERE (SELECT COUNT(*) FROM t2) > :p1", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 5}, st.Params())
}

func TestDeleteStmt_WhereWithQueriesAsOperandAndValue(t *testing.T) {
	st := NewDeleteStmt(nil).
		From("t1").
		Where(
			NewSelectStmt(nil).From("t2").Select("COUNT(*)"),
			"<>",
			NewSelectStmt(nil).From("t3").Select("COUNT(*)"),
		)

	sqb.CheckSql(t, "DELETE FROM t1 WHERE (SELECT COUNT(*) FROM t2) <> (SELECT COUNT(*) FROM t3)", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestDeleteStmt_WhereAsConditionList(t *testing.T) {
	st := NewDeleteStmt(nil).
		From("tb").
		Where([]any{"c1 = c2", "c3 <> c4"})

	sqb.CheckSql(t, "DELETE FROM tb WHERE c1 = c2 AND c3 <> c4", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestDeleteStmt_WhereAsConditionMap(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewDeleteStmt(nil).
		From("tb").
		Where(sqb.Map("c1", 1, "c2", 2))

	sqb.CheckSql(t, "DELETE FROM tb WHERE c1 = :p1 AND c2 = :p2", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 1, "p2": 2}, st.Params())
}

func TestDeleteStmt_WhereWithNestedConditionsAsConditionalExpression(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewDeleteStmt(nil).
		From("tb").
		Where("c1 IS NULL").
		Where(sql.NewCondExp().
			OrWhere("c2", "=", 1).
			OrWhere("c3", "<", 2),
		)

	sqb.CheckSql(t, "DELETE FROM tb WHERE c1 IS NULL AND (c2 = :p1 OR c3 < :p2)", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 1, "p2": 2}, st.Params())
}

//endregion

//region RETURNING

func TestDeleteStmt_ReturningAllColumns(t *testing.T) {
	st := NewDeleteStmt(nil).
		From("tb").
		ReturningAll()

	sqb.CheckSql(t, "DELETE FROM tb RETURNING *", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestDeleteStmt_ReturningSpecificColumns(t *testing.T) {
	st := NewDeleteStmt(nil).
		From("tb").
		Returning("c1").
		Returning("c2", nil).
		Returning("col", "c3")

	sqb.CheckSql(t, "DELETE FROM tb RETURNING c1, c2, col c3", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestDeleteStmt_ReturningColumnList(t *testing.T) {
	st := NewDeleteStmt(nil).
		From("tb").
		Returning([]any{"c1", "c2", "c3"})

	sqb.CheckSql(t, "DELETE FROM tb RETURNING c1, c2, c3", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestDeleteStmt_ReturningColumnListWithAliases(t *testing.T) {
	st := NewDeleteStmt(nil).
		From("tb").
		Returning(sqb.Map("a", "c1", "b", "c2", "c", "c3"))

	sqb.CheckSql(t, "DELETE FROM tb RETURNING c1 a, c2 b, c3 c", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

//endregion

//region WITH

func TestDeleteStmt_WithSimpleQuery(t *testing.T) {
	st := NewDeleteStmt(nil).
		With(NewSelectStmt(nil).From("t1"), "tb").
		From("tb")

	sqb.CheckSql(t, "WITH tb AS (SELECT * FROM t1) DELETE FROM tb", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestDeleteStmt_WithRawExpression(t *testing.T) {
	st := NewDeleteStmt(nil).
		With("(SELECT * FROM t1)", "tb").
		With([]any{sql.NewExp("n1 AS NULL")}).
		With(nil, sql.NewExp("n2")).
		From("tb")

	sqb.CheckSql(t, "WITH tb AS (SELECT * FROM t1), n1 AS NULL, n2 AS NULL DELETE FROM tb", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

//endregion

//region Copy & Clean

func TestDeleteStmt_Copy(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewDeleteStmt(nil).
		With("(SELECT * FROM t1)", "tb").
		FromOnly("tb", "t").
		Using("t1 AS t").
		Where("c1", ">", 0).
		OrWhere("c2", "IN", []any{1, 2, 3}).
		Returning("c3")

	double := st.Copy()

	sqb.CheckSql(
		t,
		"WITH tb AS (SELECT * FROM t1) "+
			"DELETE FROM ONLY tb t USING t1 AS t WHERE c1 > :p1 OR c2 IN (:p2, :p3, :p4) RETURNING c3",
		double.String(),
	)
	sqb.CheckParams(t, map[string]any{"p1": 0, "p2": 1, "p3": 2, "p4": 3}, double.Params())
}

func TestDeleteStmt_Clean(t *testing.T) {
	st := NewDeleteStmt(nil).
		With("(SELECT * FROM t1)", "tb").
		FromOnly("tb", "t").
		Using("t1 AS t").
		Where("c1", ">", 0).
		OrWhere("c2", "IN", []any{1, 2, 3}).
		Returning("c3").
		Clean()

	sqb.CheckSql(t, "DELETE FROM", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

//endregion
