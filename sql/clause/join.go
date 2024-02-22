package sql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type JoinClause[T sqb.Statement[T]] struct {
	self T
	exp  sql.JoinExpression
}

func NewJoinClause[T sqb.Statement[T]](self T) *JoinClause[T] {
	return &JoinClause[T]{self, sql.EmptyJoinExp()}
}

// CrossJoin adds cross join on new table with alias and condition:
//   - CrossJoin(table any, condition any)
//   - CrossJoin(table any, alias any, condition any)
func (j *JoinClause[T]) CrossJoin(table any, args ...any) T {
	return j.Join("CROSS JOIN", table, args...)
}

// InnerJoin adds inner join on new table with alias and condition:
//   - InnerJoin(table any, condition any)
//   - InnerJoin(table any, alias any, condition any)
func (j *JoinClause[T]) InnerJoin(table any, args ...any) T {
	return j.Join("INNER JOIN", table, args...)
}

// LeftJoin adds left join on new table with alias and condition:
//   - LeftJoin(table any, condition any)
//   - LeftJoin(table any, alias any, condition any)
func (j *JoinClause[T]) LeftJoin(table any, args ...any) T {
	return j.Join("LEFT JOIN", table, args...)
}

// RightJoin adds right join on new table with alias and condition:
//   - RightJoin(table any, condition any)
//   - rightJoin(table any, alias any, condition any)
func (j *JoinClause[T]) RightJoin(table any, args ...any) T {
	return j.Join("RIGHT JOIN", table, args...)
}

// NaturalInnerJoin adds natural inner join on new table with alias and condition:
//   - NaturalInnerJoin(table any, condition any)
//   - NaturalInnerJoin(table any, alias any, condition any)
func (j *JoinClause[T]) NaturalInnerJoin(table any, args ...any) T {
	return j.Join("NATURAL INNER JOIN", table, args...)
}

// NaturalLeftJoin adds natural left join on new table with alias and condition:
//   - NaturalLeftJoin(table any, condition any)
//   - NaturalLeftJoin(table any, alias any, condition any)
func (j *JoinClause[T]) NaturalLeftJoin(table any, args ...any) T {
	return j.Join("NATURAL LEFT JOIN", table, args...)
}

// NaturalRightJoin adds natural right join on new table with alias and condition:
//   - NaturalRightJoin(table any, condition any)
//   - NaturalRightJoin(table any, alias any, condition any)
func (j *JoinClause[T]) NaturalRightJoin(table any, args ...any) T {
	return j.Join("NATURAL RIGHT JOIN", table, args...)
}

// LeftOuterJoin adds left outer join on new table with alias and condition:
//   - LeftOuterJoin(table any, condition any)
//   - LeftOuterJoin(table any, alias any, condition any)
func (j *JoinClause[T]) LeftOuterJoin(table any, args ...any) T {
	return j.Join("LEFT OUTER JOIN", table, args...)
}

// RightOuterJoin adds right outer join on new table with alias and condition:
//   - RightOuterJoin(table any, condition any)
//   - RightOuterJoin(table any, alias any, condition any)
func (j *JoinClause[T]) RightOuterJoin(table any, args ...any) T {
	return j.Join("RIGHT OUTER JOIN", table, args...)
}

// NaturalLeftOuterJoin adds natural left outer join on new table with alias and condition:
//   - NaturalLeftOuterJoin(table any, condition any)
//   - NaturalLeftOuterJoin(table any, alias any, condition any)
func (j *JoinClause[T]) NaturalLeftOuterJoin(table any, args ...any) T {
	return j.Join("NATURAL LEFT OUTER JOIN", table, args...)
}

// NaturalRightOuterJoin adds natural right outer join on new table with alias and condition:
//   - NaturalRightOuterJoin(table any, condition any)
//   - NaturalRightOuterJoin(table any, alias any, condition any)
func (j *JoinClause[T]) NaturalRightOuterJoin(table any, args ...any) T {
	return j.Join("NATURAL RIGHT OUTER JOIN", table, args...)
}

// Join joins new table with alias and condition:
//   - Join(joinType string, table any, condition any)
//   - Join(joinType string, table any, alias any, condition any)
func (j *JoinClause[T]) Join(joinType string, table any, args ...any) T {
	j.exp.Append(joinType, table, args...)
	j.self.Dirty()
	return j.self
}

func (j *JoinClause[T]) CleanJoin() T {
	j.exp.Clean()
	j.self.Dirty()
	return j.self
}

func (j *JoinClause[T]) CopyJoin() *JoinClause[T] {
	return &JoinClause[T]{j.self, j.exp.Copy()}
}

func (j *JoinClause[T]) BuildJoin() T {
	if j.exp.IsNotEmpty() {
		j.self.AddParams(j.exp.Params())
		j.self.AddSql(" ")
		j.self.AddSql(j.exp.String())
	}
	return j.self
}
