package clickhouse

import (
	"fmt"
	"testing"

	"github.com/AlephTav/sqb"
)

func TestInsertDefault(t *testing.T) {
	st := NewInsertStmt(nil).
		Into("insert_select_testtable (*)").
		Values("1, 'a', 1")

	sqb.CheckSql(t, "INSERT INTO insert_select_testtable (*) VALUES (1, 'a', 1)", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestInsertExcept(t *testing.T) {
	st := NewInsertStmt(nil).
		Into("insert_select_testtable (* EXCEPT(b))").
		Values("2, 2")

	sqb.CheckSql(t, "INSERT INTO insert_select_testtable (* EXCEPT(b)) VALUES (2, 2)", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestInsertFormat(t *testing.T) {
	st := NewInsertStmt(nil).
		Into("insert_select_testtable (* EXCEPT(b))").
		FormatValueList().Values("v11, v12, v13")

	sqb.CheckSql(t, "INSERT INTO insert_select_testtable (* EXCEPT(b)) FORMAT VALUES (v11, v12, v13)", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestInsertSettingsFormat(t *testing.T) {
	st := NewInsertStmt(nil).
		Into("insert_select_testtable (* EXCEPT(b))").
		Settings("some settings").
		FormatValueList().Values("v11, v12, v13")

	sqb.CheckSql(t, "INSERT INTO insert_select_testtable (* EXCEPT(b)) SETTINGS some settings FORMAT VALUES (v11, v12, v13)", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
}

func TestInsertSettingsFromInfile(t *testing.T) {
	st := NewInsertStmt(nil).
		Into("table").
		FromInfile("'data.csv'").
		Format("CSV")
	sqb.CheckSql(t, "INSERT INTO table FROM INFILE 'data.csv' FORMAT CSV", st.String())
	sqb.CheckParams(t, map[string]any{}, st.Params())
	fmt.Println(st.String())
}
