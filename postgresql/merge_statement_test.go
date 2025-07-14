package postgresql

import (
	"github.com/AlephTav/sqb"
	"github.com/AlephTav/sqb/sql/expression"
	"testing"
)

func TestMergeStmt_MergeTwoTablesThenDelete(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewMergeStmt(nil).
		Into("target t").
		Using("source s").
		On("t.name", "=", "s.name").
		WhenMatchedThenDelete()

	sqb.CheckSql(t,
		"MERGE INTO target t "+
			"USING source s "+
			"ON t.name = :p1 WHEN MATCHED THEN DELETE", st.String())
	sqb.CheckParams(t, map[string]any{
		"p1": "s.name",
	}, st.Params())
}

func TestMergeStmt_MergeTwoTableThenDeleteWithCondition(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewMergeStmt(nil).
		Into("target t").
		Using("source s").
		On("t.name", "=", "s.name").
		WhenMatchedAnd("t.age <> s.name").
		ThenDelete()

	sqb.CheckSql(t,
		"MERGE INTO target t "+
			"USING source s "+
			"ON t.name = :p1 "+
			"WHEN MATCHED AND t.age <> s.name THEN DELETE", st.String())
	sqb.CheckParams(t, map[string]any{
		"p1": "s.name",
	}, st.Params())
}

//region Copy & Clean

func TestMergeStmt_Copy(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewMergeStmt(nil).
		With("(SELECT * FROM t1)", "source").
		Into("target", "t").
		Using("source", "s").
		On(sql.NewExp("t.c1 = s.c1")).
		AndOn("t.c2 = s.c2").
		WhenNotMatched().
		ThenInsert(
			NewInsertStmt(nil).
				Columns([]any{"c1", "c2", "c3"}).
				Values([]any{"v1", "v2", "v3"}),
		).
		WhenMatchedAnd("s.c2 < 18").
		ThenUpdate(
			NewUpdateStmt(nil).
				Assign("c1", sql.NewColumnListExp("s.c1")).
				Assign("c2", sql.NewColumnListExp("s.c2")).
				Assign("c3", sql.NewColumnListExp("s.c3")),
		).
		WhenMatchedAnd("s.c2 > 18").
		ThenUpdate(
			NewUpdateStmt(nil).
				Assign("c1", 1).
				Assign("c2", "v").
				Assign("c3", "v1"),
		)

	clone := st.Copy()

	sqb.CheckSql(
		t,
		"WITH source AS (SELECT * FROM t1) "+
			"MERGE INTO target t "+
			"USING source s "+
			"ON t.c1 = s.c1 AND t.c2 = s.c2 "+
			"WHEN NOT MATCHED THEN INSERT (c1, c2, c3) VALUES (:p1, :p2, :p3) "+
			"WHEN MATCHED AND s.c2 < 18 THEN UPDATE SET c1 = :p4, c2 = :p5, c3 = :p6 "+
			"WHEN MATCHED AND s.c2 > 18 THEN UPDATE SET c1 = :p7, c2 = :p8, c3 = :p9",
		clone.String(),
	)
	sqb.CheckParams(t, map[string]any{
		"p1": "v1",
		"p2": "v2",
		"p3": "v3",
		"p4": sql.NewColumnListExp("s.c1"),
		"p5": sql.NewColumnListExp("s.c2"),
		"p6": sql.NewColumnListExp("s.c3"),
		"p7": 1,
		"p8": "v",
		"p9": "v1",
	}, clone.Params())
}

func TestMergeStmt_Clean(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewMergeStmt(nil).
		With("(SELECT * FROM t1)", "source").
		Into("target", "t").
		Using("source", "s").
		On(sql.NewExp("t.c1 = s.c1")).
		AndOn("t.c2 = s.c2").
		WhenNotMatched().
		ThenDoNothing().
		WhenMatchedAnd("s.c2 < 18").
		ThenDelete()

	st.Clean()

	sqb.CheckSql(t, "MERGE ", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}
func TestMergeStmt_Returning(t *testing.T) {
	sqb.ResetParameterIndex()
	st := NewMergeStmt(nil).
		Into("target t").
		Using("source s").
		On("t.name", "=", "s.name").
		WhenMatchedThenDelete().
		Returning("t.*")

	sqb.CheckSql(t,
		"MERGE INTO target t "+
			"USING source s "+
			"ON t.name = :p1 WHEN MATCHED THEN DELETE RETURNING t.*", st.String())
	sqb.CheckParams(t, map[string]any{
		"p1": "s.name",
	}, st.Params())
}

//endregion
