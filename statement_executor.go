package sqb

type StatementExecutor interface {
	Exec(sql string, params map[string]any) (int64, error)
	MustExec(sql string, params map[string]any) int64

	Insert(sql string, params map[string]any, sequence string) (any, error)
	MustInsert(sql string, params map[string]any, sequence string) any

	Rows(sql string, params map[string]any) ([]map[string]any, error)
	MustRows(sql string, params map[string]any) []map[string]any

	Row(sql string, params map[string]any) (map[string]any, error)
	MustRow(sql string, params map[string]any) map[string]any

	Column(sql string, params map[string]any) ([]any, error)
	MustColumn(sql string, params map[string]any) []any

	One(sql string, params map[string]any) (any, error)
	MustOne(sql string, params map[string]any) any
}
