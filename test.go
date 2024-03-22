package sqb

import (
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"testing"
)

func CheckSql(t *testing.T, expected any, actual string) {
	switch expected.(type) {
	case string:
		if expected != actual {
			t.Errorf("Expected SQL is %q, actual is %q", expected, actual)
		}
	default:
		if !slices.Contains[[]string, string](expected.([]string), actual) {
			t.Errorf("Expected SQL is %q, actual is %q", expected.([]string)[0], actual)
		}
	}
}

func CheckParams(t *testing.T, expected any, actual map[string]any) {
	switch expected.(type) {
	case map[string]any:
		if !reflect.DeepEqual(expected, actual) {
			t.Errorf("Expected SQL params are %#v, actual is %#v", expected, actual)
		}
	default:
		for _, exp := range expected.([]map[string]any) {
			if reflect.DeepEqual(exp, actual) {
				return
			}
		}
		t.Errorf("Expected SQL params are %#v, actual is %#v", expected.([]map[string]any)[0], actual)
	}
}

type StatementExecutorMock struct{}

func NewStatementExecutorMock() *StatementExecutorMock {
	return &StatementExecutorMock{}
}

func (m *StatementExecutorMock) rows(sql string) []map[string]any {
	limit, offset := 3, 0
	r, _ := regexp.Compile("(LIMIT|OFFSET) (\\d+)")
	matches := r.FindAllStringSubmatch(sql, -1)
	for _, match := range matches {
		switch match[1] {
		case "LIMIT":
			limit, _ = strconv.Atoi(match[2])
		case "OFFSET":
			offset, _ = strconv.Atoi(match[2])
		}
	}
	if offset > 2 {
		return []map[string]any{}
	}
	top := offset + limit
	if top > 3 {
		top = 3
	}
	return []map[string]any{
		{"c1": "v1", "c2": "v2", "c3": "a"},
		{"c1": "v3", "c2": "v4", "c3": "b"},
		{"c1": "v5", "c2": "v6", "c3": "b"},
	}[offset:top]
}

func (m *StatementExecutorMock) MustExec(sql string, params map[string]any) int64 {
	return int64(len(m.rows(sql)))
}

func (m *StatementExecutorMock) Exec(sql string, params map[string]any) (int64, error) {
	return m.MustExec(sql, params), nil
}

func (m *StatementExecutorMock) MustInsert(sql string, params map[string]any, sequence string) any {
	return 1
}

func (m *StatementExecutorMock) Insert(sql string, params map[string]any, sequence string) (any, error) {
	return 1, nil
}

func (m *StatementExecutorMock) MustRows(sql string, params map[string]any) []map[string]any {
	return m.rows(sql)
}

func (m *StatementExecutorMock) Rows(sql string, params map[string]any) ([]map[string]any, error) {
	return m.MustRows(sql, params), nil
}

func (m *StatementExecutorMock) MustRow(sql string, params map[string]any) map[string]any {
	return m.rows(sql)[0]
}

func (m *StatementExecutorMock) Row(sql string, params map[string]any) (map[string]any, error) {
	return m.MustRow(sql, params), nil
}

func (m *StatementExecutorMock) MustColumn(sql string, params map[string]any) []any {
	values := make([]any, 0, 3)
	for _, row := range m.rows(sql) {
		values = append(values, row["c1"])
	}
	return values
}

func (m *StatementExecutorMock) Column(sql string, params map[string]any) ([]any, error) {
	return m.MustColumn(sql, params), nil
}

func (m *StatementExecutorMock) MustOne(sql string, params map[string]any) any {
	return m.rows(sql)[0]["c1"]
}

func (m *StatementExecutorMock) One(sql string, params map[string]any) (any, error) {
	return m.MustOne(sql, params), nil
}
