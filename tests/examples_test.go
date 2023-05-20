package psql_test

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/gopsql/db"
	"github.com/gopsql/gopg"
	"github.com/gopsql/logger"
	"github.com/gopsql/pgx"
	"github.com/gopsql/pq"
	"github.com/gopsql/psql"
	"github.com/gopsql/standard"

	_ "github.com/lib/pq"
)

type (
	Post struct {
		Id         int
		CategoryId int
		Title      string
		Picture    string `jsonb:"meta"`
	}
)

func ExamplePQ() {
	c, err := sql.Open("postgres", "postgres://localhost:5432/gopsqltests?sslmode=disable")
	if err != nil {
		panic(err)
	}
	var conn db.DB = standard.NewDB("postgres", c)
	defer conn.Close()
	if err := c.Ping(); err != nil {
		panic(err)
	}
	var name string
	psql.NewModelTable("", conn).NewSQL("SELECT current_database()").MustQueryRow(&name)
	fmt.Println(name)
	// Output:
	// gopsqltests
}

func ExampleGOPG() {
	conn := gopg.MustOpen("postgres://localhost:5432/gopsqltests?sslmode=disable")
	defer conn.Close()

	var level string
	var output0 string
	var output1 map[int]string
	var output2 []int

	m := psql.NewModelTable("", conn, logger.StandardLogger)
	m.MustTransaction(func(ctx context.Context, tx db.Tx) error {
		m.NewSQL("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE").MustExecuteCtxTx(ctx, tx)
		m.NewSQL("SHOW TRANSACTION ISOLATION LEVEL").MustQueryRowCtxTx(ctx, tx, &level)
		m.NewSQL("SELECT s.a, chr(s.a) FROM generate_series(65,70) AS s(a)").MustQueryCtxTx(ctx, tx, &output1)
		m.NewSQL("SELECT current_database()").MustQueryRowCtxTx(ctx, tx, &output0)
		m.NewSQL("SELECT * FROM generate_series(11, 15)").MustQueryCtxTx(ctx, tx, &output2)
		return nil
	})
	fmt.Println("level:", level)
	fmt.Println("output0:", output0)
	fmt.Println("output1:", output1)
	fmt.Println("output2:", output2)

	var outpu3 map[int]string
	m.NewSQL("SELECT s.a, chr(s.a) FROM generate_series(71,75) AS s(a)").MustQuery(&outpu3)
	fmt.Println("output3:", outpu3)

	// Output:
	// level: serializable
	// output0: gopsqltests
	// output1: map[65:A 66:B 67:C 68:D 69:E 70:F]
	// output2: [11 12 13 14 15]
	// output3: map[71:G 72:H 73:I 74:J 75:K]
}

func ExamplePost() {
	connStr := "postgres://localhost:5432/gopsqltests?sslmode=disable"
	usePgxPool := true
	var conn db.DB
	if usePgxPool {
		conn = pgx.MustOpen(connStr)
	} else {
		conn = pq.MustOpen(connStr)
	}
	defer conn.Close()

	m := psql.NewModel(Post{}, conn, logger.StandardLogger)

	fmt.Println(m.Schema())
	m.NewSQL(m.Schema()).MustExecute()

	defer func() {
		fmt.Println(m.DropSchema())
		m.NewSQL(m.DropSchema()).MustExecute()
	}()

	var newPostId int
	i := m.Insert(
		m.Permit("Title", "Picture").Filter(`{ "Title": "hello", "Picture": "world!" }`),
		"CategoryId", 2,
	).Returning("id")
	fmt.Println(i)
	i.MustQueryRow(&newPostId)
	fmt.Println("id:", newPostId)

	var firstPost Post
	m.Find().Where("id = $1", newPostId).MustQuery(&firstPost)
	fmt.Println("post:", firstPost)

	var ids []int
	m.Select("id").OrderBy("id ASC").MustQuery(&ids)
	fmt.Println("ids:", ids)

	var id2title map[int]string
	m.Select("id, title").MustQuery(&id2title)
	fmt.Println("map:", id2title)

	var postsByCategoryId map[struct{ categoryId int }][]struct{ title string }
	m.Select("category_id, title").MustQuery(&postsByCategoryId)
	fmt.Println("map:", postsByCategoryId)

	var rowsAffected int
	u := m.Update(
		m.Permit("Picture").Filter(`{ "Picture": "WORLD!" }`),
	).Where("id = $1", newPostId)
	fmt.Println(u)
	u.MustExecute(&rowsAffected)
	fmt.Println("updated:", rowsAffected)

	var posts []Post
	m.Find().MustQuery(&posts)
	fmt.Println("posts:", posts)

	n := m.Where("id > $1", 0).MustCount()
	fmt.Println("count:", n)

	e := m.WHERE("id", "=", newPostId).MustExists()
	fmt.Println("exists:", e)

	c := m.MustCount()
	fmt.Println("count:", c)

	var rowsDeleted int
	d := m.Delete().Where("id = $1", newPostId)
	fmt.Println(d)
	d.MustExecute(&rowsDeleted)
	fmt.Println("deleted:", rowsDeleted)

	// Output:
	// CREATE TABLE posts (
	// 	id SERIAL PRIMARY KEY,
	// 	category_id bigint DEFAULT 0 NOT NULL,
	// 	title text DEFAULT ''::text NOT NULL,
	// 	meta jsonb DEFAULT '{}'::jsonb NOT NULL
	// );
	//
	// INSERT INTO posts (title, category_id, meta) VALUES ($1, $2, $3) RETURNING id
	// id: 1
	// post: {1 2 hello world!}
	// ids: [1]
	// map: map[1:hello]
	// map: map[{2}:[{hello}]]
	// UPDATE posts SET meta = jsonb_set(COALESCE(meta, '{}'::jsonb), '{picture}', $2) WHERE id = $1
	// updated: 1
	// posts: [{1 2 hello WORLD!}]
	// count: 1
	// exists: true
	// count: 1
	// DELETE FROM posts WHERE id = $1
	// deleted: 1
	// DROP TABLE IF EXISTS posts;
}
