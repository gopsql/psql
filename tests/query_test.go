package psql_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/gopsql/db"
	"github.com/gopsql/gopg"
	"github.com/gopsql/pgx"
	"github.com/gopsql/psql"
	"github.com/shopspring/decimal"
)

func testQuery(_t *testing.T, conn db.DB) {
	t := test{_t}
	model := psql.NewModelTable("", conn)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := model.NewSQL("SELECT pg_sleep(1)").ExecuteCtx(ctx)
	if _, ok := conn.(*gopg.DB); ok {
		t.Bool("expect error to be timeout", os.IsTimeout(err))
	} else if _, ok := conn.(*pgx.DB); ok {
		t.Bool("expect error to context.DeadlineExceeded", errors.Is(err, context.DeadlineExceeded))
	} else {
		t.String("expect error: canceling statement due to user request", conn.ErrGetCode(err), "57014")
	}
	check := func(sql string, dest interface{}, expected string) {
		rv := reflect.ValueOf(dest)
		model.NewSQL(sql).MustQuery(&rv)
		t.Helper()
		t.String("test query: "+sql, fmt.Sprintf("%+v", rv.Elem().Interface()), expected)
	}
	check("SELECT 'a' UNION SELECT 'b'", &[]string{}, "[a b]")
	check("SELECT 1 UNION SELECT 2", &[]int{}, "[1 2]")
	check("SELECT 1, 1.23 UNION SELECT 2, 3.45", &map[int]decimal.Decimal{}, "map[1:1.23 2:3.45]")
	check("SELECT 1, 'a' UNION SELECT 2, 'b'", &map[int]string{}, "map[1:a 2:b]")
	check("SELECT 'a', 2 UNION SELECT 'b', 1", &map[string]int{}, "map[a:2 b:1]")
	check("SELECT 1, 2, 'a', 'b' UNION SELECT 2, 3, 'c', 'd'", &map[int]struct {
		id     int
		status string
		Name   string
	}{}, "map[1:{id:2 status:a Name:b} 2:{id:3 status:c Name:d}]")
	check("SELECT 1, 2, 'a' UNION SELECT 1, 3, 'd'", &map[int][]struct {
		UserId int
		status string
	}{}, "map[1:[{UserId:2 status:a} {UserId:3 status:d}]]")
	check("SELECT 1, 's', true, 0 UNION SELECT 1, 's', false, 1",
		&map[int]map[string]map[bool]int{},
		"map[1:map[s:map[false:1 true:0]]]")
	check("SELECT 1, false, 0, 'hello' UNION SELECT 1, true, 1, 'world'", &map[int]map[bool]struct {
		foo int
		bar string
	}{}, "map[1:map[false:{foo:0 bar:hello} true:{foo:1 bar:world}]]")
	check("SELECT 1, 'a', 'b', 'c' UNION SELECT 2, 'd', 'e', 'f'", &map[int][3]string{}, "map[1:[a b c] 2:[d e f]]")
	check("SELECT 1, 2, 'a', 'b' UNION SELECT 3, 4, 'c', 'd'", &map[[2]int][2]string{}, "map[[1 2]:[a b] [3 4]:[c d]]")
	check("SELECT 1, 2, 'a', 'b' UNION SELECT 3, 4, 'c', 'd'", &map[struct {
		uid int
		id  int
	}]struct {
		status string
		Name   string
	}{}, "map[{uid:1 id:2}:{status:a Name:b} {uid:3 id:4}:{status:c Name:d}]")
}
