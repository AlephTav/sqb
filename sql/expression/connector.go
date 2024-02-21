package sql

type Connector string

func (c Connector) String() string {
	return string(c)
}

const (
	AND Connector = "AND"
	OR  Connector = "OR"
)
