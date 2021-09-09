package psql_test

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gopsql/db"
	"github.com/gopsql/gopg"
	"github.com/gopsql/logger"
	"github.com/gopsql/pgx"
	"github.com/gopsql/pq"
	"github.com/gopsql/psql"
	"github.com/shopspring/decimal"
)

type (
	test struct {
		*testing.T
	}

	order struct {
		__TABLE_NAME__ string `orders`

		Id          int
		Status      string
		TradeNumber string
		UserId      int `json:"foobar_user_id"`
		TotalAmount decimal.Decimal
		CreatedAt   time.Time
		UpdatedAt   time.Time
		name        string `column:"name"`
		title       string `column:"title,options"`
		Ignored     string `column:"-"`
		ignored     string
		Password    password

		FieldInJsonb string `jsonb:"meta"`
		OtherJsonb   string `json:"otherjsonb" jsonb:"meta"`
		jsonbTest    int    `json:"testjsonb" column:"JSONBTEST" jsonb:"meta"`
		BadType      int    `jsonb:"meta"`
		Sources      []struct {
			Name string
		} `jsonb:"meta"`
		Sources2 map[string]int `jsonb:"meta2"`
		Sources3 struct {
			Word string
		} `jsonb:"meta3"`
	}

	password struct {
		hashed string
		clear  string
	}
)

func (p password) String() string {
	return p.hashed
}

func (p *password) Update(password string) {
	p.hashed = fmt.Sprintf("%x", md5.Sum([]byte(password)))
	p.clear = password
}

func (p password) Equal(password string) bool {
	return fmt.Sprintf("%x", md5.Sum([]byte(password))) == p.hashed
}

// used in pq or pgx
func (p *password) Scan(src interface{}) error {
	if value, ok := src.(string); ok {
		*p = password{
			hashed: value,
		}
	}
	return nil
}

// used in gopg
func (p *password) ScanValue(rd gopg.TypesReader, n int) error {
	value, err := gopg.TypesScanString(rd, n)
	if err == nil {
		*p = password{
			hashed: value,
		}
	}
	return err
}

func (p password) Value() (driver.Value, error) {
	return p.hashed, nil
}

func (p password) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.clear)
}

func (p *password) UnmarshalJSON(t []byte) error {
	var value string
	if err := json.Unmarshal(t, &value); err != nil {
		return err
	}
	*p = password{}
	if value != "" {
		p.Update(value)
	}
	return nil
}

var connStr string

func init() {
	connStr = os.Getenv("DBCONNSTR")
	if connStr == "" {
		connStr = "postgres://localhost:5432/gopsqltests?sslmode=disable"
	}
}

func TestCRUDInPQ(t *testing.T) {
	conn, err := pq.Open(connStr)
	if err != nil {
		t.Fatal(err)
	}
	testCRUD(t, conn)
}

func TestCRUDInPGX(t *testing.T) {
	conn, err := pgx.Open(connStr)
	if err != nil {
		t.Fatal(err)
	}
	testCRUD(t, conn)
}

func TestCRUDInGOPG(t *testing.T) {
	conn, err := gopg.Open(connStr)
	if err != nil {
		t.Fatal(err)
	}
	testCRUD(t, conn)
}

func testCRUD(_t *testing.T, conn db.DB) {
	t := test{_t}

	o := psql.NewModel(order{})
	o.SetConnection(conn)
	o.SetLogger(logger.StandardLogger)

	// drop table
	err := o.NewSQLWithValues(o.DropSchema()).Execute()
	if err != nil {
		t.Fatal(err)
	}

	// create table
	err = o.NewSQLWithValues(o.Schema()).Execute()
	if err != nil {
		t.Fatal(err)
	}

	// test Columns()
	rows, err := o.Connection().Query("SELECT id, status FROM orders")
	if err != nil {
		t.Fatal(err)
	}
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	t.Int("columns size", len(columns), 2)
	t.String("columns #1", columns[0], "id")
	t.String("columns #2", columns[1], "status")

	randomBytes := make([]byte, 10)
	if _, err := rand.Read(randomBytes); err != nil {
		t.Fatal(err)
	}
	tradeNo := hex.EncodeToString(randomBytes)
	totalAmount, _ := decimal.NewFromString("12.34")
	createInput := strings.NewReader(`{
		"Status": "changed",
		"TradeNumber": "` + tradeNo + `",
		"TotalAmount": "` + totalAmount.String() + `",
		"foobar_user_id": 1,
		"NotAllowed": "foo",
		"Password": "123123",
		"FieldInJsonb": "yes",
		"otherjsonb": "no",
		"testjsonb": 123,
		"BadType": "string",
		"Sources": [{
			"Name": "yes",
			"baddata": "foobar"
		}],
		"Sources2": {
			"cash": 100
		},
		"Sources3": {
			"Word": "finish"
		}
	}`)
	var createData map[string]interface{}
	if err := json.NewDecoder(createInput).Decode(&createData); err != nil {
		t.Fatal(err)
	}
	model := psql.NewModel(order{}, conn, logger.StandardLogger)

	var id int
	err = model.Insert(
		model.Permit(
			"Status", "TradeNumber", "UserId", "Password", "FieldInJsonb", "OtherJsonb",
			"jsonbTest", "TotalAmount", "BadType", "Sources", "Sources2", "Sources3",
		).Filter(createData),
		model.Changes(psql.RawChanges{
			"name":   "foobar",
			"title":  "hello",
			"Status": "new",
		}),
		model.CreatedAt(),
		model.UpdatedAt(),
	).Returning("id").QueryRow(&id)
	if err != nil {
		t.Fatal(err)
	}
	t.Int("first order id", id, 1)

	var badType, sources, sources2, sources3 string
	model.Select(
		"COALESCE(meta->>'bad_type', 'empty'), meta->>'sources', meta2::text, meta3::text",
	).MustQueryRow(&badType, &sources, &sources2, &sources3)
	// field with wrong type is skipped, so empty is returned
	t.String("first order bad type", badType, "empty")
	// unwanted content "baddata" is filtered
	t.String("first order sources", sources, `[{"Name": "yes"}]`)
	t.String("first order sources 2", sources2, `{"sources2": {"cash": 100}}`)      // map
	t.String("first order sources 3", sources3, `{"sources3": {"Word": "finish"}}`) // struct

	exists := model.MustExists("WHERE id = $1", id)
	t.Bool("first order exists", exists)

	exists2 := model.MustExists("WHERE id = $1", id+1)
	t.Bool("first order exists #2", exists2 == false)

	err = model.Insert(
		model.Changes(psql.RawChanges{
			"Status": "new2",
		}),
	).Returning("id").QueryRow(&id)
	if err != nil {
		t.Fatal(err)
	}
	t.Int("second order id", id, 2)

	var statuses []string
	model.Select("status").MustQuery(&statuses)
	t.Int("statuses length", len(statuses), 2)
	t.String("status 0", statuses[0], "new")
	t.String("status 1", statuses[1], "new2")

	var ids []int
	model.Select("id").MustQuery(&ids)
	t.Int("ids length", len(ids), 2)
	t.Int("id 0", ids[0], 1)
	t.Int("id 1", ids[1], 2)

	id2status := map[int]string{}
	model.Select("id, status").MustQuery(&id2status)
	t.Int("map length", len(id2status), 2)
	t.String("map 0", id2status[1], "new")
	t.String("map 1", id2status[2], "new2")

	var status2id map[string]int
	model.Select("status, id").MustQuery(&status2id)
	t.Int("map length", len(status2id), 2)
	t.Int("map 0", status2id["new"], 1)
	t.Int("map 1", status2id["new2"], 2)

	var id2struct map[int]struct {
		id     int
		status string
		Name   string
	}
	model.Select("user_id, id, status, name").MustQuery(&id2struct)
	t.Int("map length", len(id2struct), 2)
	t.String("struct string", fmt.Sprintf("%+v", id2struct[1]), "{id:1 status:new Name:foobar}")
	// map[0:{id:2 status:new2 Name:} 1:{id:1 status:new Name:foobar}]

	var id2structs map[int][]struct {
		UserId int
		status string
	}
	model.Select("1, user_id, status").MustQuery(&id2structs)
	t.Int("map length", len(id2structs), 1)
	t.String("struct string", fmt.Sprintf("%+v", id2structs[1]), "[{UserId:1 status:new} {UserId:0 status:new2}]")
	// map[1:[{UserId:1 status:new} {UserId:0 status:new2}]]

	// map[int][]interface{}: gopg returns error pg: Scan(nil)
	// https://github.com/go-pg/pg/blob/v10.9.0/types/scan.go#L55
	var id2strs map[int][3]string
	model.Select("user_id, id::text, status, name").MustQuery(&id2strs)
	t.Int("map length", len(id2strs), 2)
	t.String("strs string", fmt.Sprintf("%+v", id2strs[1]), "[1 new foobar]")
	// map[0:[2 new2 ] 1:[1 new foobar]]

	var array2strs map[[2]int][2]string
	model.Select("user_id, id, status, name").MustQuery(&array2strs)
	t.Int("map length", len(array2strs), 2)
	t.String("array string", fmt.Sprintf("%+v", array2strs[[2]int{1, 1}]), "[new foobar]")
	// map[[0 2]:[new2 ] [1 1]:[new foobar]]

	var struct2struct map[struct {
		uid int
		id  int
	}]struct {
		status string
		Name   string
	}
	model.Select("user_id, id, status, name").MustQuery(&struct2struct)
	t.Int("map length", len(struct2struct), 2)
	k := struct {
		uid int
		id  int
	}{1, 1}
	t.String("struct string", fmt.Sprintf("%+v", struct2struct[k]), "{status:new Name:foobar}")
	// map[{uid:0 id:2}:{status:new2 Name:} {uid:1 id:1}:{status:new Name:foobar}]

	var createdAts []time.Time
	model.Select("created_at").MustQuery(&createdAts)
	t.Int("created_at length", len(createdAts), 2)
	d1 := time.Since(createdAts[0])
	d2 := time.Since(createdAts[1])
	t.Bool("created_at 0", d1 > 0 && d1 < 200*time.Millisecond)
	t.Bool("created_at 1", d2 > 0 && d2 < 200*time.Millisecond)

	var customOrders []struct {
		status string
		id     int
	}
	psql.NewModelTable("orders", conn, logger.StandardLogger).
		Select("status, id", "ORDER BY id ASC").MustQuery(&customOrders)
	t.String("custom order struct", fmt.Sprintf("%+v", customOrders), "[{status:new id:1} {status:new2 id:2}]")

	var firstOrder order
	err = model.Find("ORDER BY created_at ASC LIMIT 1").Query(&firstOrder) // "LIMIT 1" only necessary for gopg
	if err != nil {
		t.Fatal(err)
	}
	t.Int("order id", firstOrder.Id, 1)
	t.String("order status", firstOrder.Status, "new")
	t.String("order trade number", firstOrder.TradeNumber, tradeNo)
	t.Decimal("order total amount", firstOrder.TotalAmount, totalAmount)
	t.Int("order user", firstOrder.UserId, 1)
	t.String("order name", firstOrder.name, "foobar")
	t.String("order title", firstOrder.title, "hello")
	ca := time.Since(firstOrder.CreatedAt)
	ua := time.Since(firstOrder.UpdatedAt)
	t.Bool("order created at", ca > 0 && ca < 200*time.Millisecond)
	t.Bool("order updated at", ua > 0 && ua < 200*time.Millisecond)
	t.String("order ignored", firstOrder.Ignored, "")
	t.String("order ignored #2", firstOrder.ignored, "")
	t.String("order password", firstOrder.Password.String(), "4297f44b13955235245b2497399d7a93")
	t.Bool("order password 2", firstOrder.Password.Equal("123123"))
	t.String("order FieldInJsonb", firstOrder.FieldInJsonb, "yes")
	t.String("order OtherJsonb", firstOrder.OtherJsonb, "no")
	t.Int("order jsonbTest", firstOrder.jsonbTest, 123)

	var c echoContext
	changes, err := model.Permit().Bind(c, &firstOrder)
	if err != nil {
		t.Fatal(err)
	}
	t.Int("bind changes size", len(changes), 0)
	t.Int("bind order id", firstOrder.Id, 1)
	t.String("bind order status", firstOrder.Status, "new")
	t.String("bind order trade number", firstOrder.TradeNumber, tradeNo)
	changes, err = model.Permit("Id", "TradeNumber").Bind(c, &firstOrder)
	if err != nil {
		t.Fatal(err)
	}
	t.Int("bind changes size", len(changes), 2)
	t.Int("bind order id", firstOrder.Id, 2)
	t.String("bind order status", firstOrder.Status, "new")
	t.String("bind order trade number", firstOrder.TradeNumber, "")

	var orders []order
	err = model.Find("ORDER BY created_at DESC").Query(&orders)
	if err != nil {
		t.Fatal(err)
	}
	t.Int("orders size", len(orders), 2)
	t.Int("first order id", orders[0].Id, 2)
	t.Int("first order jsonbTest", orders[0].jsonbTest, 0)
	t.Int("second order id", orders[1].Id, 1)
	t.Int("second order jsonbTest", orders[1].jsonbTest, 123)

	time.Sleep(200 * time.Millisecond)
	updateInput := strings.NewReader(`{
		"Status": "modified",
		"NotAllowed": "foo",
		"FieldInJsonb": "red",
		"otherjsonb": "blue"
	}`)
	var updateData map[string]interface{}
	err = json.NewDecoder(updateInput).Decode(&updateData)
	if err != nil {
		t.Fatal(err)
	}
	var ao order
	achanges, err := model.Assign(
		&ao,
		model.Permit("Status", "FieldInJsonb", "OtherJsonb").Filter(updateData),
		model.Permit("Status").Filter(psql.RawChanges{
			"x":            "1",
			"Status":       "gopsql",
			"FieldInJsonb": "black",
		}),
		model.UpdatedAt(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.String("order status", ao.Status, "gopsql")
	t.String("order FieldInJsonb", ao.FieldInJsonb, "red")
	t.String("order OtherJsonb", ao.OtherJsonb, "blue")
	var rowsAffected int
	err = model.Update(achanges...)().ExecuteInTransaction(&psql.TxOptions{
		IsolationLevel: db.LevelSerializable,
		Before: func(ctx context.Context, tx db.Tx) (err error) {
			err = model.NewSQLWithValues(
				"UPDATE "+model.TableName()+" SET user_id = user_id - $1",
				23,
			).ExecTx(tx, ctx)
			return
		},
		After: func(ctx context.Context, tx db.Tx) (err error) {
			err = model.NewSQLWithValues(
				"UPDATE "+model.TableName()+" SET user_id = user_id + $1",
				99,
			).ExecTx(tx, ctx)
			return
		},
	}, &rowsAffected)
	if err != nil {
		t.Fatal(err)
	}
	t.Int("rows affected", rowsAffected, 2)

	var secondOrder order
	err = model.Find("WHERE id = $1", 2).Query(&secondOrder)
	if err != nil {
		t.Fatal(err)
	}
	t.Int("order id", secondOrder.Id, 2)
	t.String("order status", secondOrder.Status, "gopsql")
	ca = time.Since(secondOrder.CreatedAt)
	ua = time.Since(secondOrder.UpdatedAt)
	t.Bool("order created at", ca > 200*time.Millisecond) // because of time.Sleep
	t.Bool("order updated at", ua > 0 && ua < 200*time.Millisecond)
	t.String("order FieldInJsonb", secondOrder.FieldInJsonb, "red")
	t.String("order OtherJsonb", secondOrder.OtherJsonb, "blue")
	var u int
	t.Int("order user", secondOrder.UserId, u-23+99)

	count, err := model.Count()
	if err != nil {
		t.Fatal(err)
	}
	t.Int("rows count", count, 2)

	var rowsDeleted int
	err = model.Delete().Execute(&rowsDeleted)
	if err != nil {
		t.Fatal(err)
	}
	t.Int("rows deleted", rowsDeleted, 2)

	count, err = model.Count()
	if err != nil {
		t.Fatal(err)
	}
	t.Int("rows count", count, 0)
}

func (t *test) Bool(name string, b bool) {
	t.Helper()
	if b {
		t.Logf("%s test passed", name)
	} else {
		t.Errorf("%s test failed, got %t", name, b)
	}
}

func (t *test) String(name, got, expected string) {
	t.Helper()
	if got == expected {
		t.Logf("%s test passed", name)
	} else {
		t.Errorf("%s test failed, got %s", name, got)
	}
}

func (t *test) Int(name string, got, expected int) {
	t.Helper()
	if got == expected {
		t.Logf("%s test passed", name)
	} else {
		t.Errorf("%s test failed, got %d", name, got)
	}
}

func (t *test) Decimal(name string, got, expected decimal.Decimal) {
	t.Helper()
	if got.Equal(expected) {
		t.Logf("%s test passed", name)
	} else {
		t.Errorf("%s test failed, got %d", name, got)
	}
}

type (
	echoContext struct{}
)

func (c echoContext) Bind(i interface{}) error {
	if o, ok := i.(*order); ok {
		o.Id = 2
		o.Status = "foo"
	}
	return nil
}
