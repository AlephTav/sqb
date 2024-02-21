package sql

type ColumnListExpression struct {
	ListExpression
}

func NewColumnListExp(column any, args ...any) ColumnListExpression {
	exp := EmptyColumnListExp()
	exp.Append(column, args...)
	return exp
}

func EmptyColumnListExp() ColumnListExpression {
	return ColumnListExpression{EmptyListExp(false)}
}

func (e ColumnListExpression) Copy() ColumnListExpression {
	return ColumnListExpression{e.ListExpression.Copy()}
}

// Append adds column with its alias to the expression:
//   - Append(column any)
//   - Append(column any, alias any)
func (e ColumnListExpression) Append(column any, args ...any) {
	var alias any
	if len(args) > 0 {
		alias = args[0]
	}
	e.AppendName(column, alias)
}
