package sqb

type Command interface {
	ItIsCommand()
	String() string
	Params() map[string]any
}

type CommandStmt[T any] interface {
	Statement[T]
	ItIsCommand()
}
