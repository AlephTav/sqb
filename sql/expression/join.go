package sql

import "github.com/AlephTav/sqb"

type JoinExpression struct {
	Expression
}

func EmptyJoinExp() JoinExpression {
	return JoinExpression{EmptyExp()}
}

func (e JoinExpression) Copy() JoinExpression {
	return JoinExpression{e.Expression.Copy()}
}

func (e JoinExpression) Append(joinType string, table any, args ...any) {
	if e.IsNotEmpty() {
		e.AddSql(" ")
	}
	e.AddSql(joinType)
	e.AddSql(" ")
	argNum := len(args)
	var alias, condition any
	if argNum > 1 {
		condition = args[1]
		alias = args[0]
	} else if argNum > 0 {
		condition = args[0]
	}
	e.AddSql(e.tableToString(table, alias))
	if condition != nil {
		e.addCondition(condition)
	}
}

func (e JoinExpression) tableToString(table any, alias any) string {
	var tb = NewColumnListExp(table, alias)
	e.AddParams(tb.Params())
	switch table.(type) {
	case []any:
		tables := table.([]any)
		if len(tables) > 1 {
			return "(" + tb.String() + ")"
		}
	case sqb.SliceMap:
		tables := table.(sqb.SliceMap)
		if len(tables) > 2 {
			return "(" + tb.String() + ")"
		}
	}
	return tb.String()
}

func (e JoinExpression) addCondition(condition any) {
	if e.isConditionalExpression(condition) {
		cond := NewCondExp(condition)
		e.AddSql(" ON ")
		e.AddSql(cond.String())
		e.AddParams(cond.Params())
	} else {
		cond := NewColumnListExp(condition)
		e.AddSql(" USING (")
		e.AddSql(cond.String())
		e.AddSql(")")
		e.AddParams(cond.Params())
	}
}

func (e JoinExpression) isConditionalExpression(exp any) bool {
	switch exp.(type) {
	case string, Expression, ConditionalExpression:
		return true
	default:
		return false
	}
}
