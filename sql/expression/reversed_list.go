package sql

type ReversedListExpression struct {
	ListExpression
}

func EmptyReversedListExp() ReversedListExpression {
	return ReversedListExpression{EmptyListExp(true)}
}

func (e ReversedListExpression) Copy() ReversedListExpression {
	return ReversedListExpression{e.ListExpression.Copy()}
}

func (e ReversedListExpression) Append(alias any, args ...any) {
	var name any
	if len(args) > 0 {
		name = args[0]
	}
	e.AppendName(name, alias)
}
