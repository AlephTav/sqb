package execution

import (
	"fmt"
	"github.com/AlephTav/sqb"
)

type DataFetching[T sqb.Statement[T]] struct {
	self T
}

func NewDataFetching[T sqb.Statement[T]](self T) *DataFetching[T] {
	return &DataFetching[T]{self}
}

func (d *DataFetching[T]) MustPairs(keyKey, valueKey string) map[any]any {
	r, err := d.pairs(d.MustRows(), keyKey, valueKey)
	if err != nil {
		panic(err)
	}
	return r
}

func (d *DataFetching[T]) Pairs(keyKey, valueKey string) (map[any]any, error) {
	rows, err := d.Rows()
	if err != nil {
		return nil, err
	}
	return d.pairs(rows, keyKey, valueKey)
}

func (d *DataFetching[T]) pairs(rows []map[string]any, keyKey, valueKey string) (map[any]any, error) {
	pairs := make(map[any]any)
	if len(rows) == 0 {
		return pairs, nil
	}
	if _, exists := rows[0][keyKey]; !exists {
		return pairs, fmt.Errorf("key %q is not found in the row set", keyKey)
	}
	if _, exists := rows[0][valueKey]; !exists {
		return pairs, fmt.Errorf("key %q is not found in the row set", valueKey)
	}
	for _, row := range rows {
		pairs[row[keyKey]] = row[valueKey]
	}
	return pairs, nil
}

func (d *DataFetching[T]) MustRowsByKey(key string, removeKeyFromRow bool) map[any]map[string]any {
	r, err := d.rowsByKey(d.MustRows(), key, removeKeyFromRow)
	if err != nil {
		panic(err)
	}
	return r
}

func (d *DataFetching[T]) RowsByKey(key string, removeKeyFromRow bool) (map[any]map[string]any, error) {
	rows, err := d.Rows()
	if err != nil {
		return nil, err
	}
	return d.rowsByKey(rows, key, removeKeyFromRow)
}

func (d *DataFetching[T]) rowsByKey(
	rows []map[string]any,
	key string,
	removeKeyFromRow bool,
) (map[any]map[string]any, error) {
	result := make(map[any]map[string]any)
	if len(rows) == 0 {
		return result, nil
	}
	if _, exists := rows[0][key]; !exists {
		return result, fmt.Errorf("key %q is not found in the row set", key)
	}
	for _, row := range rows {
		k := row[key]
		if removeKeyFromRow {
			delete(row, key)
		}
		result[k] = row
	}
	return result, nil
}

func (d *DataFetching[T]) MustRowsByGroup(key string, removeKeyFromRow bool) map[any][]map[string]any {
	r, err := d.rowsByGroup(d.MustRows(), key, removeKeyFromRow)
	if err != nil {
		panic(err)
	}
	return r
}

func (d *DataFetching[T]) RowsByGroup(key string, removeKeyFromRow bool) (map[any][]map[string]any, error) {
	rows, err := d.Rows()
	if err != nil {
		return nil, err
	}
	return d.rowsByGroup(rows, key, removeKeyFromRow)
}

func (d *DataFetching[T]) rowsByGroup(
	rows []map[string]any,
	key string,
	removeKeyFromRow bool,
) (map[any][]map[string]any, error) {
	result := make(map[any][]map[string]any)
	if len(rows) == 0 {
		return result, nil
	}
	if _, exists := rows[0][key]; !exists {
		return result, fmt.Errorf("key %q is not found in the row set", key)
	}
	for _, row := range rows {
		k := row[key]
		if removeKeyFromRow {
			delete(row, key)
		}
		result[k] = append(result[k], row)
	}
	return result, nil
}

func (d *DataFetching[T]) MustRows() []map[string]any {
	return d.self.Executor().MustRows(d.self.String(), d.self.Params())
}

func (d *DataFetching[T]) Rows() ([]map[string]any, error) {
	return d.self.Executor().Rows(d.self.String(), d.self.Params())
}

func (d *DataFetching[T]) MustRow() map[string]any {
	return d.self.Executor().MustRow(d.self.String(), d.self.Params())
}

func (d *DataFetching[T]) Row() (map[string]any, error) {
	return d.self.Executor().Row(d.self.String(), d.self.Params())
}

func (d *DataFetching[T]) MustColumn() []any {
	return d.self.Executor().MustColumn(d.self.String(), d.self.Params())
}

func (d *DataFetching[T]) Column() ([]any, error) {
	return d.self.Executor().Column(d.self.String(), d.self.Params())
}

func (d *DataFetching[T]) MustOne() any {
	return d.self.Executor().MustOne(d.self.String(), d.self.Params())
}

func (d *DataFetching[T]) One() (any, error) {
	return d.self.Executor().One(d.self.String(), d.self.Params())
}
