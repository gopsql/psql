# psql

[![Go Reference](https://pkg.go.dev/badge/github.com/gopsql/psql.svg)](https://pkg.go.dev/github.com/gopsql/psql)

Package psql provides a PostgreSQL ORM-like query builder for Go.

## Features

- **Model-based CRUD** - Map Go structs to PostgreSQL tables with automatic column inference
- **JSONB support** - Store multiple fields in a single JSONB column
- **Mass assignment protection** - Safely filter user input with `Permit` and `Filter`
- **Schema generation** - Generate CREATE TABLE statements from struct definitions
- **Multiple drivers** - Works with pq, pgx, and go-pg at runtime
- **Query builder** - Fluent API for SELECT, INSERT, UPDATE, DELETE with JOIN, CTE, and more

## Installation

```bash
go get github.com/gopsql/psql
```

## Quick Start

```go
package main

import (
    "github.com/gopsql/psql"
    "github.com/gopsql/pq"
)

type User struct {
    Id        int
    Name      string
    Email     string
    CreatedAt time.Time
}

func main() {
    // Connect to database
    conn := pq.MustOpen("postgres://localhost:5432/mydb?sslmode=disable")
    defer conn.Close()

    // Create model
    users := psql.NewModel(User{}, conn)

    // Insert
    var id int
    users.Insert("Name", "Alice", "Email", "alice@example.com").
        Returning("id").MustQueryRow(&id)

    // Select
    var user User
    users.Find().Where("id = $1", id).MustQuery(&user)

    // Update
    users.Update("Name", "Bob").Where("id = $1", id).MustExecute()

    // Delete
    users.Delete().Where("id = $1", id).MustExecute()
}
```

## Database Drivers

Choose one of three PostgreSQL drivers:

```go
import (
    "github.com/gopsql/pq"    // github.com/lib/pq
    "github.com/gopsql/pgx"   // github.com/jackc/pgx
    "github.com/gopsql/gopg"  // github.com/go-pg/pg
)

// Using pgx (recommended for new projects)
conn := pgx.MustOpen("postgres://localhost:5432/mydb?sslmode=disable")

// Using go-pg
conn := gopg.MustOpen("postgres://localhost:5432/mydb?sslmode=disable")

// Using lib/pq
conn := pq.MustOpen("postgres://localhost:5432/mydb?sslmode=disable")
```

For minimal dependencies with lib/pq:

```go
import (
    "database/sql"
    "github.com/gopsql/standard"
    _ "github.com/lib/pq"
)

db, _ := sql.Open("postgres", "postgres://localhost:5432/mydb?sslmode=disable")
conn := standard.NewDB("postgres", db)
```

## Usage Examples

### Schema Generation

```go
type Post struct {
    Id        int
    Title     string
    Views     int    `dataType:"bigint DEFAULT 0"`
    CreatedAt time.Time
}

posts := psql.NewModel(Post{}, conn)
fmt.Println(posts.Schema())
// CREATE TABLE posts (
//     id SERIAL PRIMARY KEY,
//     title text DEFAULT ''::text NOT NULL,
//     views bigint DEFAULT 0,
//     created_at timestamptz DEFAULT NOW() NOT NULL
// );
```

### JSONB Fields

```go
type Product struct {
    Id       int
    Name     string
    Price    int    `jsonb:"metadata"`
    Currency string `jsonb:"metadata"`
}

// Price and Currency are stored in a single "metadata" JSONB column
```

### Mass Assignment Protection

```go
// Only allow Name and Email from user input
changes := users.Permit("Name", "Email").Filter(requestBody)
users.Insert(changes).MustExecute()

// Or with echo/gin style binding
changes, _ := users.Permit("Name", "Email").Bind(c, &user)
```

### Query Results

```go
// Into a slice
var userList []User
users.Find().MustQuery(&userList)

// Into a map
var id2name map[int]string
users.Select("id", "name").MustQuery(&id2name)

// Grouped results
var byDept map[int][]User
users.Select("department_id", "id", "name", "email").MustQuery(&byDept)
```

### Transactions

```go
users.MustTransaction(func(ctx context.Context, tx db.Tx) error {
    users.Insert("Name", "Alice").MustExecuteCtxTx(ctx, tx)
    users.Insert("Name", "Bob").MustExecuteCtxTx(ctx, tx)
    return nil // commit; return error to rollback
})
```

### Raw SQL Expressions

```go
// Increment a counter
users.Update("Views", psql.String("Views + 1")).Where("id = $1", 1).MustExecute()

// With parameters
users.Update("Views", psql.StringWithArg("Views + $?", 10)).Where("id = $1", 1).MustExecute()
```

## Benchmarks

<img width="400" src="./tests/benchmark.svg">

Benchmark results for Insert, Update, and Select operations (100 rows each) compared to native driver usage. Run benchmarks with:

```bash
cd tests && GENERATE=1 go test -v ./benchmark_test.go
```

## Documentation

Full documentation is available at [pkg.go.dev](https://pkg.go.dev/github.com/gopsql/psql).

For more examples, see [examples_test.go](tests/examples_test.go).

## License

MIT
