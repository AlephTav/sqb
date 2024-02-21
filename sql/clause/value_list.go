package sql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type ValueListClause[T sqb.ColumnsAwareStmt[T]] struct {
	self T
	exp  sql.ValueListExpression
}

func NewValueListClause[T sqb.ColumnsAwareStmt[T]](self T) *ValueListClause[T] {
	return &ValueListClause[T]{self, sql.EmptyValueListExp()}
}

// Values add values and columns to the value list clause:
// Values(values any)
// Values(values any, columns any)
func (v *ValueListClause[T]) Values(values any, args ...any) T {
	if len(args) > 0 {
		v.self.Columns(args[0])
	} else {
		switch values.(type) {
		case sqb.SliceMap:
			v.self.Columns(sqb.Keys(values.(sqb.SliceMap)))
		case []any:
			for _, item := range values.([]any) {
				if m, ok := item.(sqb.SliceMap); ok {
					v.self.Columns(sqb.Keys(m))
				}
				break
			}
		}
	}
	v.exp.Append(values)
	return v.self
}

func (v *ValueListClause[T]) CleanValueList() T {
	v.exp.Clean()
	return v.self
}
