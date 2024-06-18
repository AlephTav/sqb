package clickhouse

import (
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/sql/clause"
)

type FromClause[T sqb.Statement[T]] struct {
	*sql.FromClause[T]
}

func NewFromClause[T sqb.Statement[T]](self T) *FromClause[T] {
	return &FromClause[T]{sql.NewFromClause[T](self)}
}
func (j *FromClause[T]) FromInfile(table any) T {
	return j.From("INFILE", table)
}
func (j *FromClause[T]) CopyFrom(self T) *FromClause[T] {
	return &FromClause[T]{j.FromClause.CopyFrom(self)}
}
