package postgresql

import (
	"testing"

	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

func TestValuesStmt_EmptyValues(t *testing.T) {
	st := NewValuesStmt(nil)
	sqb.CheckSql(t, "VALUES", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

//region VALUE

func TestValuesStmt_ValuesAsString(t *testing.T) {
	st := NewValuesStmt(nil).
		Values(sql.NewExp("(1), (2), (3)"))

	sqb.CheckSql(t, "VALUES (1), (2), (3)", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestValuesStmt_ValuesAsRawExpression(t *testing.T) {

	st := NewValuesStmt(nil).
		Values(sql.NewExp("(1), (2), (3)"))
	sqb.CheckSql(t, "VALUES (1), (2), (3)", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestValuesStmt_ValuesAsCollectionOfScalars(t *testing.T) {
	st := NewValuesStmt(nil).
		Values([]any{1, 2, 3})

	sqb.CheckSql(t, "VALUES (:p1, :p2, :p3)", st.String())
	sqb.CheckParams(t, map[string]any{
		"p1": 1,
		"p2": 2,
		"p3": 3,
	}, st.Params())
}

func TestValuesStmt_ValuesAsMapOfScalars(t *testing.T) {
	st := NewValuesStmt(nil).
		Values(sqb.Map(
			"k1", 1,
			"k2", 2,
			"k3", 3,
		))
	sqb.CheckSql(t, "VALUES (:p1, :p2, :p3)", st.String())
	sqb.CheckParams(t, map[string]any{
		"p1": 1,
		"p2": 2,
		"p3": 3,
	}, st.Params())
}

func TestValuesStmt_ValuesAsCollectionOfCollections(t *testing.T) {
	st := NewValuesStmt(nil).
		Values([]any{[]any{1, 2}, []any{"a", []any{"b", "c"}}})
	sqb.CheckSql(t, "VALUES (:p1, :p2), (:p3, :p4, :p5)", st.String())
	sqb.CheckParams(t, map[string]any{
		"p1": 1,
		"p2": 2,
		"p3": "a",
		"p4": "b",
		"p5": "c",
	}, st.Params())
}

func TestValuesStmt_ValuesAsMapOfCollections(t *testing.T) {
	st := NewValuesStmt(nil).
		Values(sqb.Map(
			"k1", []any{1, 2},
			"k2", []any{"a", "b"},
		))

	sqb.CheckSql(t, "VALUES (:p1, :p2), (:p3, :p4)", st.String())
	sqb.CheckParams(t, map[string]any{
		"p1": 1,
		"p2": 2,
		"p3": "a",
		"p4": "b",
	}, st.Params())
}

func TestValuesStmt_ValuesAsCollectionOfMaps(t *testing.T) {
	st := NewValuesStmt(nil).
		Values([]any{
			[]any{sqb.Map("k1", 1, "k2", 2)},
			[]any(sqb.Map("k1", "a", "k2", "b")),
		})
	sqb.CheckSql(t, "VALUES (:p1, :p2), (:p3, :p4)", st.String())
	sqb.CheckParams(t, map[string]any{
		"p1": 1,
		"p2": 2,
		"p3": "a",
		"p4": "b",
	}, st.Params())
}

func TestValuesStmt_ValuesAsMapOfMaps(t *testing.T) {
	st := NewValuesStmt(nil).
		Values(
			[]any{
				sqb.Map("k1", []any{sqb.Map("k1", 1, "k2", 2)}),
				sqb.Map("k2", []any{sqb.Map("k1", "a", "k2", "b")}),
			},
		)

	sqb.CheckSql(t, "VALUES (:p1, :p2), (:p3, :p4)", st.String())
	sqb.CheckParams(t, map[string]any{
		"p1": 1,
		"p2": 2,
		"p3": "a",
		"p4": "b",
	}, st.Params())
}

func TestValuesStmt_AppendValues(t *testing.T) {
	st := NewValuesStmt(nil).
		Values([]any{1}).
		Values([]any{"a"}).
		Values([]any{
			[]any{1, 2},
			[]any{true},
		})

	sqb.CheckSql(t, "VALUES (:p1), (:p2), (:p3, :p4), (:p5)", st.String())
	sqb.CheckParams(t, map[string]any{
		"p1": 1,
		"p2": "a",
		"p3": 1,
		"p4": 2,
		"p5": true,
	}, st.Params())
}

func TestValuesStmt_ValuesAsQuery(t *testing.T) {
	st := NewValuesStmt(nil).
		Values("(1), (2)").
		Values(
			NewValuesStmt(nil).
				Values("(3)"),
		)

	sqb.CheckSql(t, "VALUES (1), (2), (VALUES (3))", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

//region ORDER BY

func TestValuesStmt_OrderByColums(t *testing.T) {
	st := NewValuesStmt(nil).
		Values("(1), (2), (3)").
		OrderBy("column1")

	sqb.CheckSql(t, "VALUES (1), (2), (3) ORDER BY column1", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestValuesStmt_OrderByColumnWithOrder(t *testing.T) {
	st := NewValuesStmt(nil).
		Values("(1), (2), (3)").
		OrderBy("column1", "DESC")

	sqb.CheckSql(t, "VALUES (1), (2), (3) ORDER BY column1 DESC", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

//region LIMIT & OFFSET

func TestValuesStmt_LimitAndOffset(t *testing.T) {
	st := NewValuesStmt(nil).
		Values("(1), (2), (3)").
		Limit(2).
		Offset(1)

	sqb.CheckSql(t, "VALUES (1), (2), (3) LIMIT 2 OFFSET 1", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())

}

// region UNION
/*
func TestValuesStmt_UnionWithSelectStatement(t *testing.T) {
	st := NewValuesStmt(nil).
		Values("(1), (2), (3)").
		Union(NewSelectStmt(nil).From("tb"))

	sqb.CheckSql(t, "(VALUES (1), (2), (3)) UNION (SELECT * FROM tb)", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestValuesStmt_UnionOfValuesAndQueriesWithSorting(t *testing.T) {
	st := NewValuesStmt(nil).
		Values("(1), (2), (3)").
		Union(NewSelectStmt(nil).From("tb").OrderBy("id", "ASC")).
		Union(NewValuesStmt(nil).Values("('a'), ('b'), ('b')")).
		OrderBy("column1", "DESC")

	sqb.CheckSql(t, "(VALUES (1), (2), (3)) UNION (SELECT * FROM tb ORDER BY id ASC) UNION; (VALUES ('a'), ('b'), ('b') ORDER BY column1 DESC) ORDER BY column1 ASC", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestValuesStmt_UnionOfDifferentTypes(t *testing.T) {
	st := NewValuesStmt(nil).
		Values("(1), (2), (3)").
		UnionAll(NewSelectStmt(nil).From("t2")).
		UnionIntersect(NewSelectStmt(nil).From("t3")).
		UnionIntersectAll(NewSelectStmt(nil).From("t4")).
		UnionExcept(NewValuesStmt(nil).Values("('a')")).
		UnionExceptAll(NewSelectStmt(nil).From("t6"))

	sqb.CheckSql(t, "'(VALUES (1), (2), (3)) ' .	'UNION ALL (SELECT * FROM t2) ' .	'INTERSECT (SELECT * FROM t3) ' .	'INTERSECT ALL (SELECT * FROM t4) ' .	'EXCEPT (VALUES ('a')) '' .'EXCEPT ALL (SELECT * FROM t6) ' .	'LIMIT 5 OFFSET 50'", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}


*/
