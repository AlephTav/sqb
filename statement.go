package sqb

type Statement[T any] interface {
	IsEmpty() bool
	IsNotEmpty() bool
	String() string
	Params() map[string]any
	Executor() StatementExecutor
	AddParams(params map[string]any)
	AddSql(sql string)
	IsBuilt() bool
	Built() T
	Dirty() T
	Build() T
	Clean() T
	Copy() T
}

type ColumnsAwareStmt[T any] interface {
	Statement[T]
	Columns(columns any) T
}
