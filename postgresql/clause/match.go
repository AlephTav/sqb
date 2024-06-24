package postgresql

import (
	"github.com/AlephTav/sqb"
	exp "github.com/AlephTav/sqb/sql/expression"
)

type matchItem[I sqb.InsertStatement[I], U sqb.UpdateStatement[U]] struct {
	matched    bool
	condition  *exp.ConditionalExpression
	insertStmt sqb.InsertStatement[I]
	updateStmt sqb.UpdateStatement[U]
	expression *exp.Expression
}

func (m matchItem[I, U]) Copy() matchItem[I, U] {
	var condition exp.ConditionalExpression
	var expression exp.Expression
	var insertStmt sqb.InsertStatement[I]
	var updateStmt sqb.UpdateStatement[U]

	if m.condition != nil {
		condition = m.condition.Copy()
	}
	if m.expression != nil {
		expression = m.expression.Copy()
	}
	if m.insertStmt != nil {
		insertStmt = m.insertStmt.Copy()
	}
	if m.updateStmt != nil {
		updateStmt = m.updateStmt.Copy()
	}
	return matchItem[I, U]{
		matched:    m.matched,
		condition:  &condition,
		insertStmt: insertStmt,
		updateStmt: updateStmt,
		expression: &expression,
	}
}

type MatchClause[T sqb.Statement[T], I sqb.InsertStatement[I], U sqb.UpdateStatement[U]] struct {
	self    T
	matches []matchItem[I, U]
}

func NewMatchClause[T sqb.Statement[T], I sqb.InsertStatement[I], U sqb.UpdateStatement[U]](self T) *MatchClause[T, I, U] {
	var matches []matchItem[I, U]
	return &MatchClause[T, I, U]{self, matches}
}

func (m *MatchClause[T, I, U]) when(matched bool, condition *exp.ConditionalExpression, insertStmt sqb.InsertStatement[I], updateStmt sqb.UpdateStatement[U], expression *exp.Expression) *MatchClause[T, I, U] {
	m.matches = append(m.matches, matchItem[I, U]{matched, condition, insertStmt, updateStmt, expression})
	return m
}

func (m *MatchClause[T, I, U]) WhenMatched() T {
	return m.when(true, nil, nil, nil, nil).self
}

func (m *MatchClause[T, I, U]) WhenMatchedThenDelete() T {
	return m.when(true, nil, nil, nil, nil).ThenDelete()
}

func (m *MatchClause[T, I, U]) WhenMatchedThenDoNothing() T {
	return m.when(true, nil, nil, nil, nil).ThenDoNothing()
}

func (m *MatchClause[T, I, U]) WhenMatchedAnd(args ...any) T {
	cond := exp.EmptyCondExp().Where(args)
	return m.when(true, &cond, nil, nil, nil).self
}

func (m *MatchClause[T, I, U]) WhenNotMatched() T {
	return m.when(false, nil, nil, nil, nil).self
}

func (m *MatchClause[T, I, U]) WhenNotMatchedThenDoNothing() T {
	return m.when(false, nil, nil, nil, nil).ThenDoNothing()
}

func (m *MatchClause[T, I, U]) WhenNotMatchedAnd(args ...any) T {
	cond := exp.EmptyCondExp().Where(args)
	return m.when(false, &cond, nil, nil, nil).self
}

func (m *MatchClause[T, I, U]) ThenDoNothing() T {
	match := m.lastMatch()
	expression := exp.NewExp("DO NOTHING")
	if match != nil {
		match.expression = &expression
	}
	return m.self
}

func (m *MatchClause[T, I, U]) ThenDelete() T {
	match := m.lastMatch()
	expression := exp.NewExp("DELETE")
	if match != nil {
		match.expression = &expression
	}
	return m.self
}

func (m *MatchClause[T, I, U]) ThenInsert(insertStmt I) T {
	match := m.lastMatch()
	if match != nil {
		match.insertStmt = insertStmt
	}
	return m.self
}

func (m *MatchClause[T, I, U]) ThenUpdate(updateStmt U) T {
	match := m.lastMatch()
	if match != nil {
		match.updateStmt = updateStmt
	}
	return m.self
}

func (m *MatchClause[T, I, U]) lastMatch() *matchItem[I, U] {
	if len(m.matches) == 0 {
		return nil
	}
	return &m.matches[len(m.matches)-1]
}

func (m *MatchClause[T, I, U]) CleanMatch() T {
	m.matches = nil
	m.self.Dirty()
	return m.self
}

func (m *MatchClause[T, I, U]) CopyMatch(self T) *MatchClause[T, I, U] {
	matches := make([]matchItem[I, U], len(m.matches))
	for i, match := range m.matches {
		matches[i] = match.Copy()
	}
	copy(matches, m.matches)
	return &MatchClause[T, I, U]{self, matches}
}

func (m *MatchClause[T, I, U]) BuildMatch() T {
	for _, match := range m.matches {
		m.self.AddSql(" WHEN")
		if !match.matched {
			m.self.AddSql(" NOT")
		}
		m.self.AddSql(" MATCHED")
		if match.condition != nil && match.condition.IsNotEmpty() {
			m.self.AddSql(" AND")
			m.self.AddSql(" ")
			m.self.AddSql(match.condition.String())
			m.self.AddParams(match.condition.Params())
		}
		m.self.AddSql(" THEN")
		if match.insertStmt != nil {
			m.self.AddSql(" ")
			m.self.AddSql(match.insertStmt.String())
			m.self.AddParams(match.insertStmt.Params())
		} else if match.updateStmt != nil {
			m.self.AddSql(" ")
			m.self.AddSql(match.updateStmt.String())
			m.self.AddParams(match.updateStmt.Params())
		} else if match.expression != nil && match.expression.IsNotEmpty() {
			m.self.AddSql(" ")
			m.self.AddSql(match.expression.String())
		}
	}

	return m.self
}
