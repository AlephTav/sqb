package execution

import "github.com/AlephTav/sqb"

type DataFetching[T sqb.Statement[T]] struct {
	self T
}

func NewDataFetching[T sqb.Statement[T]](self T) *DataFetching[T] {
	return &DataFetching[T]{self}
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
