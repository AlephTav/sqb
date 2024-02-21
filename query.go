package sqb

type Query interface {
	ItIsQuery()
	String() string
	Params() map[string]any
}

type QueryStmt[T any] interface {
	Statement[T]
	ItIsQuery()
}
