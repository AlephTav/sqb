package sql

type DirectListExpression struct {
	ListExpression
}

func EmptyDirectListExp() DirectListExpression {
	return DirectListExpression{EmptyListExp(false)}
}

func (e DirectListExpression) Copy() DirectListExpression {
	return DirectListExpression{e.ListExpression.Copy()}
}

func (e DirectListExpression) Append(name any, args ...any) {
	var alias any
	if len(args) > 0 {
		alias = args[0]
	}
	e.AppendName(name, alias)
}
