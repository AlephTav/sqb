# SQL Builder

The library bases on two ideas:
- an SQL statement expressed with this library should be similar to the statement itself;
- flexible combination of statement components.

Thus, using this library you get simple but, at the same time, powerful tool to build SQL statements in flexible and
intuitive way.

```go
package main

import (
	"fmt"
	"github.com/AlephTav/sqb"
	sql "github.com/AlephTav/sqb/postgresql"
)

func main() {
	st := sql.NewSelectStmt(NewStatementExecutor()).
		Select(sqb.Map(
			"", "u.id",
			"", "u.name",
			"company", "c.name",
			"unreadMessageCount", sql.NewSelectStmt(nil).
				Select("COUNT(*)").
				From("user_messages", "m").
				Where("m.user_id = u.id").
				AndWhere("m.read_at IS NULL"),
		)).
		From("users u").
		InnerJoin("companies c", "c.id = u.company_id").
		Where("u.deleted_at IS NULL").
		AndWhere(sql.NewCondExp().
			Where("u.roles", "IN", []any{"ADMIN", "RESELLER"}).
			OrWhere(
				sql.NewSelectStmt(nil).
					Select("COUNT(*)").
					From("user_contacts uc").
					Where("uc.user_id = u.id"),
				">",
				5,
			))

	// Outputs: 
	// SELECT
	//     u.id, u.name, c.name company,
	//     (SELECT COUNT(*) FROM user_messages m WHERE m.user_id = u.id AND m.read_at IS NULL) unreadMessageCount
	// FROM users u
	// INNER JOIN companies c ON c.id = u.company_id
	// WHERE
	//     u.deleted_at IS NULL AND (
	//         u.roles IN (:p1, :p2) OR 
	//         (SELECT COUNT(*) FROM user_contacts uc WHERE uc.user_id = u.id) > :p3
	//     )
	fmt.Println(st.String())

	// Outputs:
	// map[string]any{"p1": "ADMIN", "p2": "RESELLER", "p3": 5}
	fmt.Printf("%#v", st.Params())

	// Executes statement if StatementExecutor is defined, otherwise it panics
	rows, err := st.Rows()
}

```

## Installation

```
go install github.com/AlephTav/sqb
```