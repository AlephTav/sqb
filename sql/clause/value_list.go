package sql

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/expression"
)

type ValueListClause[T sqb.ColumnsAwareStmt[T], Q sqb.QueryStmt[Q]] struct {
	self  T
	query *Q
	exp   sql.ValueListExpression
}

func NewValueListClause[T sqb.ColumnsAwareStmt[T], Q sqb.QueryStmt[Q]](self T) *ValueListClause[T, Q] {
	return &ValueListClause[T, Q]{self, nil, sql.EmptyValueListExp()}
}

func (v *ValueListClause[T, Q]) Select(query Q) T {
	v.query = &query
	v.self.Dirty()
	return v.self
}

// Values add values and columns to the value list clause:
// Values(values any)
// Values(values any, columns any)
func (v *ValueListClause[T, Q]) Values(values any, args ...any) T {
	if len(args) > 0 {
		v.self.Columns(args[0])
	} else {
	separate:
		switch values.(type) {
		case map[string]any:
			values = sqb.ToSliceMap[string, any](values.(map[string]any))
			break separate
		case []map[string]any:
			values = sqb.ToSliceMapSlice[string, any](values.([]map[string]any))
			break separate
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
	v.self.Dirty()
	return v.self
}

func (v *ValueListClause[T, Q]) CleanValueList() T {
	v.query = nil
	v.exp.Clean()
	v.self.Dirty()
	return v.self
}

func (v *ValueListClause[T, Q]) CopyValueList(self T) *ValueListClause[T, Q] {
	if v.query == nil {
		return &ValueListClause[T, Q]{self, nil, v.exp.Copy()}
	}
	query := (*v.query).Copy()
	return &ValueListClause[T, Q]{self, &query, v.exp.Copy()}
}

func (v *ValueListClause[T, Q]) BuildValueList() (T, *Q, sql.ValueListExpression) {
	return v.self, v.query, v.exp
}
