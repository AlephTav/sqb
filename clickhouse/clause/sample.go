package clickhouse

import (
	"strconv"

	"github.com/AlephTav/sqb"
)

type SampleClause[T sqb.Statement[T]] struct {
	self   T
	sample float64
}

func NewSampleClause[T sqb.Statement[T]](self T) *SampleClause[T] {
	return &SampleClause[T]{self, -1}
}

func (o *SampleClause[T]) Sample(Sample float64) T {
	o.sample = Sample
	o.self.Dirty()
	return o.self
}

func (o *SampleClause[T]) CleanSample() T {
	o.sample = -1
	o.self.Dirty()
	return o.self
}

func (o *SampleClause[T]) CopySample(self T) *SampleClause[T] {
	return &SampleClause[T]{self, o.sample}
}

func (o *SampleClause[T]) BuildSample() T {
	if o.sample >= 0 {
		o.self.AddSql(" SAMPLE " + strconv.FormatFloat(o.sample, 'f', -1, 64))
	}
	return o.self
}
