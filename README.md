# gopsql

**gopsql** is a simple tool to make PostgreSQL database queries, especially for
developing RESTful API with JSON. Some features are learned from Rails.

From [furk](https://github.com/caiguanhao/furk).

## Features

- ✅ Model is a database table and it is created from struct. Column names are
  inferred from struct field names. You can write less SQL statements for CRUD
  operations.
- ✅ Support JSONB data type, you can store many fields in one jsonb column,
  like Rails'
  [store_accessor](https://api.rubyonrails.org/v6.1.3/classes/ActiveRecord/Store.html).
- ✅ Safely insert and update record with Filter() to filter unwanted data,
  like Rails'
  [permit](https://api.rubyonrails.org/v6.1.2.1/classes/ActionController/Parameters.html).
- ✅ Migrate your database like Rails database migrations.
- ✅ Support pq, pgx and go-pg, you can switch driver at runtime.

## Model

For more use cases, see [Examples](tests/examples_test.go) or [Tests](tests/sql_test.go).

### Database Connection

You can choose one of three PostgreSQL drivers (pq, pgx, gopq) to use at runtime:
1. [github.com/lib/pq](https://github.com/lib/pq) v1.9.0
2. [github.com/jackc/pgx](https://github.com/jackc/pgx) v4.10.1
3. [github.com/go-pg/pg](https://github.com/go-pg/pg) v10.9.0

```go
// import "github.com/gopsql/db"
// import "github.com/gopsql/gopg"
// import "github.com/gopsql/pgx"
// import "github.com/gopsql/pq"
connStr := "postgres://localhost:5432/gopsql?sslmode=disable"
connDrv := "gopg"
var conn db.DB
if connDrv == "pgx" {
	conn = pgx.MustOpen(connStr)
} else if connDrv == "gopg" {
	conn = gopg.MustOpen(connStr)
} else {
	conn = pq.MustOpen(connStr)
}
defer conn.Close()
var name string
conn.QueryRow("SELECT current_database()").Scan(&name)
fmt.Println(name) // gopsql
```

If you don't want too much entries being inserted into your go.sum file, you
can use lib/pq:

```go
// import "database/sql"
// import "github.com/gopsql/standard"
// import _ "github.com/lib/pq"
c, err := sql.Open("postgres", "postgres://localhost:5432/gopsql?sslmode=disable")
if err != nil {
	panic(err)
}
conn := &standard.DB{c}
defer conn.Close()
var name string
conn.QueryRow("SELECT current_database()").Scan(&name)
fmt.Println(name) // gopsql
```

### Performance

<img width="400" src="./tests/benchmark.svg">

Benchmark results for Insert, Update, and Select operations (100 rows each)
using different drivers, compared to their native usages. Benchmarked on Apple
M1 Pro MacBook Pro. You can run `cd tests && GENERATE=1 go test -v ./benchmark_test.go` to
make this benchmark chart. For more information, see
[Benchmark](tests/benchmark_test.go).

### New Model

```go
// type (
// 	Post struct {
// 		Id         int
// 		CategoryId int
// 		Title      string
// 		Picture    string `jsonb:"Meta"`
// 		Views      int    `dataType:"bigint DEFAULT 100"`
// 	}
// )
Posts := psql.NewModel(Post{}, conn, logger.StandardLogger)
```

Table name is inferred from the name of the struct, the tag of `__TABLE_NAME__`
field or its `TableName() string` receiver. Column names are inferred from
struct field names or theirs "column" tags. Both table names and field names
are in snake case by default.

### Create Table

```go
// CREATE TABLE Posts (
//	Id SERIAL PRIMARY KEY,
//	CategoryId bigint DEFAULT 0 NOT NULL,
//	Title text DEFAULT ''::text NOT NULL,
//	Views bigint DEFAULT 100,
//	Meta jsonb DEFAULT '{}'::jsonb NOT NULL
// )
Posts.NewSQL(Posts.Schema()).MustExecute()
```

### Insert Record

```go
var newPostId int
Posts.Insert(
	Posts.Permit("Title", "Picture").Filter(`{ "Title": "hello", "Picture": "world!" }`),
	"CategoryId", 2,
).Returning("Id").MustQueryRow(&newPostId)
// or:
Posts.Insert(
	"Title", "hello",
	"Picture", "world!",
	"CategoryId", 2,
).Returning("Id").MustQueryRow(&newPostId)
// INSERT INTO Posts (Title, CategoryId, Meta) VALUES ($1, $2, $3) RETURNING Id
```

### Find Record

```go
var firstPost Post
Posts.Find().Where("Id = $?", newPostId).MustQuery(&firstPost)
// or: Posts.WHERE("Id", "=", newPostId).Find().MustQuery(&firstPost)
// or: Posts.Where("Id = $1", newPostId).Find().MustQuery(&firstPost)
// SELECT Id, CategoryId, Title, Views, Meta FROM Posts WHERE Id = $1 [1] 1.505779ms
// {1 2 hello world! 100}

var ids []int
Posts.Select("Id").OrderBy("Id ASC").MustQuery(&ids)
// [1]

// group results by key
var id2title map[int]string
Posts.Select("Id", "Title").MustQuery(&id2title)
// map[1:hello]

// map's key and value can be int, string, bool, array or struct
// if it is one-to-many, use slice as map's value
var postsByCategoryId map[struct{ categoryId int }][]struct{ title string }
Posts.Select("CategoryId", "Title").MustQuery(&postsByCategoryId)
// map[{2}:[{hello}]]

var posts []Post
Posts.Find().MustQuery(&posts)
// [{1 2 hello world! 100}]
```

### Update Record

```go
var rowsAffected int
Posts.Update(
	Posts.Permit("Picture").Filter(`{ "Picture": "WORLD!" }`),
).Where("Id = $1", newPostId).MustExecute(&rowsAffected)
// or: Posts.Where(...).Update(...).MustExecute(...)
// UPDATE Posts SET Meta = jsonb_set(COALESCE(Meta, '{}'::jsonb), '{Picture}', $2) WHERE Id = $1

Posts.Update("Views", psql.String("Views * 2")).Where("Id = $?", 1).MustExecute()
// UPDATE Posts SET Views = Views * 2 WHERE Id = $1

Posts.Update("Views", psql.StringWithArg("Views + $?", 99)).Where("Id = $?", 1).MustExecute()
// UPDATE Posts SET Views = Views + $2 WHERE Id = $1
```

### Delete Record

```go
var rowsDeleted int
Posts.Delete().Where("Id = $?", newPostId).MustExecute(&rowsDeleted)
// or: Posts.Where(...).Delete().MustExecute(...)
// DELETE FROM Posts WHERE Id = $1
```

### Other

```go
Posts.Where("Id = $?", newPostId).MustExists() // true or false
Posts.MustCount() // integer
```
