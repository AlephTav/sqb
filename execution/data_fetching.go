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

func (d *DataFetching[T]) Pairs(keyKey, valueKey string) (map[any]any, error) {
	rows, err := d.Rows()
	if err != nil {
		return nil, err
	}
	pairs := make(map[any]any)
	if len(rows) == 0 {
		return pairs, nil
	}
	if _, exists := rows[0][keyKey]; !exists {
		return pairs, fmt.Errorf("key %q is not found the result set", keyKey)
	}
	if _, exists := rows[0][valueKey]; !exists {
		return pairs, fmt.Errorf("key %q is not found the result set", valueKey)
	}
	for _, row := range rows {
		pairs[row[keyKey]] = row[valueKey]
	}
	return pairs, nil
}

func (d *DataFetching[T]) RowsByKey(key string, removeKeyFromRow bool) (map[any]any, error) {
	rows, err := d.Rows()
	if err != nil {
		return nil, err
	}
	result := make(map[any]any)
	if len(rows) == 0 {
		return result, nil
	}
	if _, exists := rows[0][key]; !exists {
		return result, fmt.Errorf("key %q is not found the result set", key)
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

func (d *DataFetching[T]) RowsByGroup(key string, removeKeyFromRow bool) (map[any][]map[string]any, error) {
	rows, err := d.Rows()
	if err != nil {
		return nil, err
	}
	result := make(map[any][]map[string]any)
	if len(rows) == 0 {
		return result, nil
	}
	if _, exists := rows[0][key]; !exists {
		return result, fmt.Errorf("key %q is not found the result set", key)
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

func (d *DataFetching[T]) Rows() ([]map[string]any, error) {
	return d.self.Executor().Rows(d.self.String(), d.self.Params())
}

func (d *DataFetching[T]) Row() (map[string]any, error) {
	return d.self.Executor().Row(d.self.String(), d.self.Params())
}

func (d *DataFetching[T]) Column() ([]any, error) {
	return d.self.Executor().Column(d.self.String(), d.self.Params())
}

func (d *DataFetching[T]) One() (any, error) {
	return d.self.Executor().One(d.self.String(), d.self.Params())
}
