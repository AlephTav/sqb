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

type InsertStatement[T any] interface {
	Statement[T]
	Columns(columns any) T
	Values(values any, args ...any) T
}

type UpdateStatement[T any] interface {
	Statement[T]
	Assign(column any, args ...any) T
}
