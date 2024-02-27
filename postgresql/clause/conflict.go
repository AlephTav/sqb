package postgresql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type ConflictClause[T sqb.Statement[T]] struct {
	self                   T
	indexColumn            sql.ColumnListExpression
	indexPredicate         sql.ConditionalExpression
	assignment             sql.AssignmentExpression
	assignmentPredicate    sql.ConditionalExpression
	indexConstraint        string
	whereBelongsToConflict bool
}

func NewConflictClause[T sqb.Statement[T]](self T) *ConflictClause[T] {
	return &ConflictClause[T]{
		self,
		sql.EmptyColumnListExp(),
		sql.EmptyCondExp(),
		sql.EmptyAssignmentExp(),
		sql.EmptyCondExp(),
		"",
		false,
	}
}

func (c *ConflictClause[T]) OnConflictDoNothing(args ...any) T {
	var indexColumn any
	if len(args) > 0 {
		indexColumn = args[0]
	} else {
		indexColumn = ""
	}
	c.OnConflict(indexColumn)
	c.DoNothing()
	return c.self
}

// OnConflictDoUpdate adds index column and column assignment to the "on conflict" clause:
//   - OnConflictDoUpdate(indexColumn any, column any)
//   - OnConflictDoUpdate(indexColumn any, column any, value any)
func (c *ConflictClause[T]) OnConflictDoUpdate(indexColumn any, column any, args ...any) T {
	var value any
	if len(args) > 0 {
		value = args[0]
	}
	c.OnConflict(indexColumn)
	c.DoUpdate(column, value)
	return c.self
}

// OnConflict adds index column and predicate to the "on conflict" clause:
//   - OnConflict()
//   - OnConflict(indexColumn any)
//   - OnConflict(indexColumn any, indexPredicate any)
func (c *ConflictClause[T]) OnConflict(args ...any) T {
	var indexColumn, indexPredicate any
	if len(args) > 1 {
		indexColumn = args[0]
		indexPredicate = args[1]
	} else if (len(args)) > 0 {
		indexColumn = args[0]
	} else {
		indexColumn = ""
	}
	c.indexColumn.Append(indexColumn)
	if indexPredicate != nil {
		if c.indexPredicate.IsEmpty() {
			if cond, ok := indexPredicate.(sql.ConditionalExpression); ok {
				c.indexPredicate = cond
			} else {
				c.indexPredicate.Where(indexPredicate)
			}
		} else {
			c.indexPredicate.Where(indexPredicate)
		}
	}
	c.self.Dirty()
	return c.self
}

func (c *ConflictClause[T]) OnConstraint(indexConstraint string) T {
	c.indexConstraint = indexConstraint
	c.self.Dirty()
	return c.self
}

func (c *ConflictClause[T]) DoNothing() T {
	c.assignment.Clean()
	c.self.Dirty()
	return c.self
}

// DoUpdate adds value assignment to a column of the "on conflict do update" clause:
//   - DoUpdate(column any)
//   - DoUpdate(column any, value any)
func (c *ConflictClause[T]) DoUpdate(column any, args ...any) T {
	c.assignment.Append(column, args...)
	c.self.Dirty()
	return c.self
}

// DoUpdateWithCondition adds value assignment to a column with condition of the "on conflict do update" clause:
//   - DoUpdateWithCondition(column any, assignmentPredicate any)
//   - DoUpdateWithCondition(column any, value any, assignmentPredicate any)
func (c *ConflictClause[T]) DoUpdateWithCondition(column any, valueOrAssignmentPredicate any, args ...any) T {
	var value, predicate any
	if len(args) > 0 {
		value = valueOrAssignmentPredicate
		predicate = args[0]
	} else {
		predicate = valueOrAssignmentPredicate
	}
	c.assignment.Append(column, value)
	if predicate != nil {
		if c.assignmentPredicate.IsEmpty() {
			if cond, ok := predicate.(sql.ConditionalExpression); ok {
				c.assignmentPredicate = cond
			} else {
				c.assignmentPredicate.Where(predicate)
			}
		} else {
			c.assignmentPredicate.Where(predicate)
		}
	}
	c.whereBelongsToConflict = false
	c.self.Dirty()
	return c.self
}

// AndWhere adds "AND" condition to the conflict clause:
//   - AndWhere(condition string)
//   - AndWhere(condition ConditionalExpression)
//   - AndWhere(column string, operator string, value any)
//   - AndWhere(operand any, operator string, value any)
//   - AndWhere(operator string, operand any)
func (c *ConflictClause[T]) AndWhere(args ...any) T {
	if c.whereBelongsToConflict {
		c.indexPredicate.AndWhere(args...)
	} else {
		c.assignmentPredicate.AndWhere(args...)
	}
	c.self.Dirty()
	return c.self
}

// OrWhere adds "OR" condition to the conflict clause:
//   - OrWhere(condition string)
//   - OrWhere(condition ConditionalExpression)
//   - OrWhere(column string, operator string, value any)
//   - OrWhere(operand any, operator string, value any)
//   - OrWhere(operator string, operand any)
func (c *ConflictClause[T]) OrWhere(args ...any) T {
	if c.whereBelongsToConflict {
		c.indexPredicate.OrWhere(args...)
	} else {
		c.assignmentPredicate.OrWhere(args...)
	}
	c.self.Dirty()
	return c.self
}

// Where adds "AND" or "OR" condition to the conflict clause:
//   - Where(condition string)
//   - Where(condition ConditionalExpression)
//   - Where(column string, operator string, value any)
//   - Where(operand any, operator string, value any)
//   - Where(operator string, operand any)
func (c *ConflictClause[T]) Where(args ...any) T {
	if c.whereBelongsToConflict {
		c.indexPredicate.Where(args...)
	} else {
		c.assignmentPredicate.Where(args...)
	}
	c.self.Dirty()
	return c.self
}

func (c *ConflictClause[T]) CleanConflict() T {
	c.indexColumn.Clean()
	c.assignmentPredicate.Clean()
	c.assignment.Clean()
	c.assignmentPredicate.Clean()
	c.indexConstraint = ""
	c.whereBelongsToConflict = false
	c.self.Dirty()
	return c.self
}

func (c *ConflictClause[T]) CopyConflict(self T) *ConflictClause[T] {
	return &ConflictClause[T]{
		self,
		c.indexColumn.Copy(),
		c.indexPredicate.Copy(),
		c.assignment.Copy(),
		c.assignmentPredicate.Copy(),
		c.indexConstraint,
		c.whereBelongsToConflict,
	}
}

func (c *ConflictClause[T]) BuildConflict() T {
	if c.indexColumn.IsEmpty() && c.indexPredicate.IsEmpty() && c.indexConstraint == "" && c.assignment.IsEmpty() {
		return c.self
	}
	c.self.AddSql(" ON CONFLICT")
	if c.indexColumn.IsNotEmpty() {
		c.self.AddSql(" (")
		c.self.AddSql(c.indexColumn.String())
		c.self.AddSql(")")
		c.self.AddParams(c.indexColumn.Params())
	}
	if c.indexPredicate.IsNotEmpty() {
		c.self.AddSql(" WHERE ")
		c.self.AddSql(c.indexPredicate.String())
		c.self.AddParams(c.indexPredicate.Params())
	}
	if c.indexConstraint != "" {
		c.self.AddSql(" ON CONSTRAINT ")
		c.self.AddSql(c.indexConstraint)
	}
	c.self.AddSql(" DO ")
	if c.assignment.IsNotEmpty() {
		c.self.AddSql("UPDATE SET ")
		c.self.AddSql(c.assignment.String())
		c.self.AddParams(c.assignment.Params())
		if c.assignmentPredicate.IsNotEmpty() {
			c.self.AddSql(" WHERE ")
			c.self.AddSql(c.assignmentPredicate.String())
			c.self.AddParams(c.assignmentPredicate.Params())
		}
	} else {
		c.self.AddSql("NOTHING")
	}
	return c.self
}
