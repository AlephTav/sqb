package sqb

type StatementExecutor interface {
	Exec(sql string, params map[string]any) (int, error)
	Insert(sql string, params map[string]any, sequence string) (any, error)
	Rows(sql string, params map[string]any) ([]map[string]any, error)
	Row(sql string, params map[string]any) (map[string]any, error)
	Column(sql string, params map[string]any) ([]any, error)
	One(sql string, params map[string]any) (any, error)
}
