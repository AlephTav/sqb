package postgresql

import (
	"testing"

	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

// assertEquals=checkSql
// $st->toSql()=st.String()
// new RawExpression = newExp
func TestUpdateStmt_EmptyUpdate(t *testing.T) {
	st := NewUpdateStmt(nil)
	sqb.CheckSql(t, "UPDATE", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

//region TABLE

func TestUpdateStmt_UpdateTable(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table("tb")
	sqb.CheckSql(t, "UPDATE tb", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())

}

func TestUpdateStmt_UpdateTableWithAlias(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table("tb", "t")
	sqb.CheckSql(t, "UPDATE tb t", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestUpdateStmt_UpdateTables(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table([]any{"t1", "t2"})
	sqb.CheckSql(t, "UPDATE t1, t2", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestUpdateStmt_UpdateTablesWithAliases(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table(sqb.Map(
			"a", "t1",
			"b", "t2",
		))
	sqb.CheckSql(t, "UPDATE t1 a, t2 b", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestUpdateStmt_UpdateRawExpression(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table(sql.NewExp("tb AS t"))

	sqb.CheckSql(t, "UPDATE tb AS t", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestUpdateStmt_UpdateOnlyTable(t *testing.T) {
	st := NewUpdateStmt(nil).
		OnlyTable("tb")
	sqb.CheckSql(t, "UPDATE ONLY tb", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

// region SET

func TestUpdateStmt_AssignSingleValue(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table("tb").
		Assign("c1", "v1")
	sqb.CheckSql(t, "UPDATE tb SET c1 = :p1", st.String())
	sqb.CheckParams(t, map[string]any{"p1": "v1"}, st.Params())
}

func TestUpdateStmt_AssignMultipleValues(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table("t1").
		Assign("c1", "v1").
		Assign("c2", sql.NewExp("DEFAULT")).
		Assign(sqb.Map("c3", nil)).
		Assign(
			sql.NewExp("(c4, c5)"),
			NewSelectStmt(nil).From("t2").Select("t2.c1, t2.c2"),
		).
		Assign(NewSelectStmt(nil).From("t3")).
		Assign([]any{"c6 = 5"}).
		Assign(nil)

	sqb.CheckSql(t, " 'UPDATE t1 SET c1 = :p1, c2 = DEFAULT, c3 = NULL, ' .'(c4, c5) = (SELECT t2.c1, t2.c2 FROM t2), (SELECT * FROM t3), c6 = 5, NULL'", st.String())
	sqb.CheckParams(t, map[string]any{"p1": "v1"}, st.Params())
}

// region FROM
// Поправить
func TestUpdateStmt_FromTable(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table("t1").
		Assign("c1", "v1").
		From("t2")
	sqb.CheckSql(t, "UPDATE t1 SET c1 = :p1 FROM t2", st.String())
	sqb.CheckParams(t, map[string]any{"p1": "v1"}, st.Params())
}

func TestUpdateStmt_FromTablewithAlias(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table("t1").
		Assign("c1", "v1").
		From("t2", "t")
	sqb.CheckSql(t, "UPDATE t1 SET c1 = :p1 FROM t2 t", st.String())
	sqb.CheckParams(t, map[string]any{"p1": "v1"}, st.Params())
}

func TestUpdateStmt_FromTables(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table("tb").
		Assign("c1", "v1").
		From([]any{"t1", "t2"})

	sqb.CheckSql(t, "UPDATE tb SET c1 = :p1 FROM t1, t2", st.String())
	sqb.CheckParams(t, map[string]any{"p1": "v1"}, st.Params())
}

func TestUpdateStmt_FromTablesWithAliases(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table("tb").
		Assign("c1", "v1").
		From([]any{sqb.Map("a", "t1", "b", "t2")})

	sqb.CheckSql(t, "UPDATE tb SET c1 = :p1 FROM t1 a, t2 b", st.String())
	sqb.CheckParams(t, map[string]any{"p1": "v1"}, st.Params())
}

func TestUpdateStmt_FromRawExpression(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table("t1").
		Assign("c1", "v1").
		From(sql.NewExp("t2 As t"))
	sqb.CheckSql(t, "UPDATE t1 SET c1 = :p1 FROM t2 AS t", st.String())
	sqb.CheckParams(t, map[string]any{"p1": "v1"}, st.Params())
}

//region WHERE

func TestUpdateStmt_WhereAsString(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table("tb").
		Where("c1 = c2")
	sqb.CheckSql(t, "UPDATE tb WHERE c1 = c2", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestUpdateStmt_WhereAsRawValue(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table("tb").
		Where(sql.NewExp("c1 = c2"))

	sqb.CheckSql(t, "UPDATE tb WHERE c1 = c2", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestUpdateStmt_WhereBinaryOpWithScalar(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table("tb").
		Where("col", "=", "1")
	sqb.CheckSql(t, "UPDATE tb WHERE col = :p1", st.String())
	sqb.CheckParams(t, map[string]any{"p1": "1"}, st.Params())

}

func TestUpdateStmt_WhereBinaryOpWithNull(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table("tb").
		Where("col", "=", nil)

	sqb.CheckSql(t, "UPDATE tb WHERE col = NULL", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestUpdateStmt_WhereBinaryOpWithQuery(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table("t1").
		Where("t1.col", "=", (NewSelectStmt(nil).From("t2").Select("COUNT(*)")))

	sqb.CheckSql(t, "UPDATE t1 WHERE t1.col = (SELECT COUNT(*) FROM t2)", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestUpdateStmt_WhereBinaryOpWithTuple(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table("tb").
		Where("col", "IN", []any{1, 2, 3})

	sqb.CheckSql(t, "UPDATE tb WHERE col IN (:p1, :p2, :p3)", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 1, "p2": 2, "p3": 3}, st.Params())
}

func TestUpdateStmt_WhereBinaryOpBetween(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table("tb").
		Where("col", "BETWEEN", []any{1, 2})
	sqb.CheckSql(t, "UPDATE tb WHERE col BETWEEN :p1 AND :p2", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 1, "p2": 2}, st.Params())

}

func TestUpdateStmt_WhereBinaryOpWithRawValue(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table("tb").
		Where("c1", "=", sql.NewExp("c2"))

	sqb.CheckSql(t, "UPDATE tb WHERE c1 = c2", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())

}

func TestUpdateStmt_WhereUnaryOpWithScalar(t *testing.T) {

	st := NewUpdateStmt(nil).
		Table("tb").
		Where("NOT", sql.NewExp("col"))

	sqb.CheckSql(t, "UPDATE tb WHERE NOT col", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestUpdateStmt_WhereUnaryOpWithQuery(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table("t1").
		Where("NOT", (NewSelectStmt(nil).From("t2").Select("COUNT(*)")))

	sqb.CheckSql(t, "UPDATE t1 WHERE NOT (SELECT COUNT(*) FROM t2)", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())

}

func TestUpdateStmt_WhereWithQueryAsOperand(t *testing.T) {

	st := NewUpdateStmt(nil).
		Table("t1").
		Where(NewSelectStmt(nil).From("t2").Select("COUNT(*)"), ">", 5)

	sqb.CheckSql(t, "UPDATE t1 WHERE (SELECT COUNT(*) FROM t2) > :p1", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 5}, st.Params())
}

func TestUpdateStmt_WhereWithQueriesAsOperandAndValue(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table("t1").
		Where(
			NewSelectStmt(nil).From("t2").Select("COUNT(*)"),
			"<>",
			NewSelectStmt(nil).From("t3").Select("COUNT(*)"),
		)

	sqb.CheckSql(t, "UPDATE t1 WHERE (SELECT COUNT(*) FROM t2) <> (SELECT COUNT(*) FROM t3)", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestUpdateStmt_WhereAsConditionList(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table("tb").
		Where([]any{"c1 = c2", "c3 <> c4"})
	sqb.CheckSql(t, "UPDATE tb WHERE c1 = c2 AND c3 <> c4", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())

}

func TestUpdateStmt_WhereAsConditionMap(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table("tb").
		Where([]any{sqb.Map("c1", 1, "c2", 2)})
	sqb.CheckSql(t, "UPDATE tb WHERE c1 = :p1 AND c2 = :p2", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 1, "p2": 2}, st.Params())
}

func TestUpdateStmt_WhereWithNestedConditionsAsConditionalExpression(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table("tb").
		Where("c1 IS NULL").
		Where(
			sql.NewCondExp().
				OrWhere("c2", "=", 1).
				OrWhere("c3", "<", 2),
		)
	sqb.CheckSql(t, "UPDATE tb WHERE c1 IS NULL AND (c2 = :p1 OR c3 < :p2)", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 1, "p2": 2}, st.Params())
}

func TestUpdateStmt_WhereWithNestedConditionsAsClosure(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table("tb").
		Where("c1 IS NULL").
		Where(func(conditon sql.ConditionalExpression) {
			conditon.OrWhere("c2", "=", 1).
				OrWhere("c3", "<", 2)
		})

	sqb.CheckSql(t, "UPDATE tb WHERE c1 IS NULL AND (c2 = :p1 OR c3 < :p2)", st.String())
	sqb.CheckParams(t, map[string]any{"p1": 1, "p2": 2}, st.Params())
}

//region RETURNING

func TestUpdateStmt_ReturningAllColumns(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table("tb").
		Returning("*")

	sqb.CheckSql(t, "UPDATE tb RETURNING *", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestUpdateStmt_ReturningSpecificColumns(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table("tb").
		Returning("c1").
		Returning("c2", nil).
		Returning("col", "c3")
	sqb.CheckSql(t, "UPDATE tb RETURNING c1, c2, col c3", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestUpdateStmt_ReturningColumnList(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table("tb").
		Returning([]any{"c1", "c2", "c3"})

	sqb.CheckSql(t, "UPDATE tb RETURNING c1, c2, c3", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestUpdateStmt_ReturningColumnListWithAliases(t *testing.T) {
	st := NewUpdateStmt(nil).
		Table("tb").
		Returning(sqb.Map("a", "c1", "b", "c2", "c", "c3"))
	sqb.CheckSql(t, "UPDATE tb RETURNING c1 a, c2 b, c3 c", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

//region WITH

func TestUpdateStmt_WithSimpleQuery(t *testing.T) {
	st := NewUpdateStmt(nil).
		With(NewSelectStmt(nil).From("t1"), "tb").
		Table("tb")

	sqb.CheckSql(t, "WITH tb AS (SELECT * FROM t1) UPDATE tb", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestUpdateStmt_WithRawExpression(t *testing.T) {
	st := NewUpdateStmt(nil).
		With("(SELECT * FROM t1)", "tb").
		With(sql.NewExp("n1 AS NULL")).
		With(nil, "n2").
		Table("tb")

	sqb.CheckSql(t, "WITH tb AS (SELECT * FROM t1), n1 AS NULL, n2 AS NULL UPDATE tb", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}
