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
	"reflect"
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

func init() {
	psql.DefaultColumnNamer = psql.ToUnderscore
	psql.DefaultTableNamer = psql.ToPluralUnderscore
}

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

		NoError  int `column:"NoError" jsonb:"error"`
		HasError int `column:"HasError" jsonb:"error,strict"`
	}

	password struct {
		hashed string
		clear  string
	}

	datatypes struct {
		__TABLE_NAME__ string `datatypes`

		Aa int8
		Ba int16
		Ca int32
		Da int64
		Ea int
		Fa uint8
		Ga uint16
		Ha uint32
		Ia uint64
		Ja uint
		Ka time.Time
		La float32
		Ma float64
		Na decimal.Decimal
		Oa bool
		Pa string

		Ab *int8
		Bb *int16
		Cb *int32
		Db *int64
		Eb *int
		Fb *uint8
		Gb *uint16
		Hb *uint32
		Ib *uint64
		Jb *uint
		Kb *time.Time
		Lb *float32
		Mb *float64
		// Nb *decimal.Decimal // bug
		Ob *bool
		Pb *string

		Ac []int8
		Bc []int16
		Cc []int32
		Dc []int64
		Ec []int
		Fc []uint8
		Gc []uint16
		Hc []uint32
		Ic []uint64
		Jc []uint
		Kc []time.Time
		Lc []float32
		Mc []float64
		Nc []decimal.Decimal
		Oc []bool
		Pc []string

		Ad *[]int8
		Bd *[]int16
		Cd *[]int32
		Dd *[]int64
		Ed *[]int
		Fd *[]uint8
		Gd *[]uint16
		Hd *[]uint32
		// Id *[]uint64 // bug
		Jd *[]uint
		Kd *[]time.Time
		Ld *[]float32
		Md *[]float64
		Nd *[]decimal.Decimal
		Od *[]bool
		Pd *[]string
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

func TestDataTypes(t *testing.T) {
	var x datatypes
	conn := pgx.MustOpen(connStr)
	m := psql.NewModel(x, conn, logger.StandardLogger)
	m.NewSQL(m.DropSchema()).MustExecute()
	m.NewSQL(m.Schema()).MustExecute()
	m.NewSQL("INSERT INTO datatypes DEFAULT VALUES").MustExecute()
	var y datatypes
	m.Find().MustQuery(&y)
	m.NewSQL(m.DropSchema()).MustExecute()
	conn.Close()
}

func TestNewSQL(_t *testing.T) {
	m := psql.NewModel(order{})
	sql := "INSERT INTO orders (status, trade_number) VALUES ($?, $?)"
	s1 := m.NewSQL(sql, "new", "1234567890")
	t := test{_t}
	t.String("sql #1", s1.String(), "INSERT INTO orders (status, trade_number) VALUES ($1, $2)")
	s2 := s1.AsInsert()
	t.String("sql #2", s2.OnConflict().DoNothing().String(),
		"INSERT INTO orders (status, trade_number) VALUES ($1, $2) ON CONFLICT DO NOTHING")

	sql = "UPDATE orders SET status = $?"
	s3 := m.NewSQL(sql, "new")
	t.String("sql #3", s3.String(), "UPDATE orders SET status = $1")
	s4 := s3.AsUpdate()
	sql, values := s4.Where("status = $?", "old").Returning("id").StringValues()
	t.String("sql #4", sql, "UPDATE orders SET status = $2 WHERE status = $1 RETURNING id")
	t.String("sql #4", fmt.Sprintf("%v", values), "[old new]")

	sql = "DELETE FROM orders WHERE status = $?"
	s5 := m.NewSQL(sql, "new")
	t.String("sql #5", s5.String(), "DELETE FROM orders WHERE status = $1")
	s6 := s5.AsDelete()
	t.String("sql #6", s6.Returning("id").String(), "DELETE FROM orders WHERE status = $1 RETURNING id")

	sql = "SELECT id, status FROM orders WHERE status = $?"
	s7 := m.NewSQL(sql, "new")
	t.String("sql #7", s7.String(), "SELECT id, status FROM orders WHERE status = $1")
	s8 := s7.AsSelect()
	t.String("sql #8", s8.Offset(10).String(), "SELECT id, status FROM orders WHERE status = $1 OFFSET 10")
}

func TestCRUDInPQ(t *testing.T) {
	conn, err := pq.Open(connStr)
	if err != nil {
		t.Fatal(err)
	}
	testCRUD(t, conn)
	testQuery(t, conn)
}

func TestCRUDInPGX(t *testing.T) {
	conn, err := pgx.Open(connStr)
	if err != nil {
		t.Fatal(err)
	}
	testCRUD(t, conn)
	testQuery(t, conn)
}

func TestCRUDInGOPG(t *testing.T) {
	conn, err := gopg.Open(connStr)
	if err != nil {
		t.Fatal(err)
	}
	testCRUD(t, conn)
	testQuery(t, conn)
}

func testCRUD(_t *testing.T, conn db.DB) {
	t := test{_t}

	o := psql.NewModel(order{})
	o.SetConnection(conn)
	o.SetLogger(logger.StandardLogger)

	// drop table
	err := o.NewSQL(o.DropSchema()).Execute()
	if err != nil {
		t.Fatal(err)
	}

	// create table
	err = o.NewSQL(o.Schema()).Execute()
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
	if model.Insert().Execute() != psql.ErrNoSQL {
		t.Fatal("should have no sql to execute error")
	}
	if model.Update().Execute() != psql.ErrNoSQL {
		t.Fatal("should have no sql to execute error")
	}

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

	exists := model.Where("id = $1", id).MustExists()
	t.Bool("first order exists", exists)

	exists2 := model.Where("id = $1", id+1).MustExists()
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
	var rvStatuses = reflect.ValueOf(&[]string{})
	model.Select("status").Tap(func(q *psql.SelectSQL) *psql.SelectSQL {
		q.MustQuery(&statuses)
		q.MustQuery(rvStatuses)
		return q
	})
	t.Int("statuses length", len(statuses), 2)
	t.String("status 0", statuses[0], "new")
	t.String("status 1", statuses[1], "new2")
	t.Int("statuses length", rvStatuses.Elem().Len(), 2)
	t.String("status 0", rvStatuses.Elem().Index(0).String(), "new")
	t.String("status 1", rvStatuses.Elem().Index(1).String(), "new2")

	var createdAts []time.Time
	model.Select("created_at").MustQuery(&createdAts)
	t.Int("created_at length", len(createdAts), 2)
	d1 := time.Since(createdAts[0])
	d2 := time.Since(createdAts[1])
	t.Bool("created_at 0", d1 > 0 && d1 < 200*time.Millisecond)
	t.Bool("created_at 1", d2 > 0 && d2 < 200*time.Millisecond)

	customModel := psql.NewModelTable("orders", conn, logger.StandardLogger)
	var customOrders []struct {
		Status       string
		id           int    `column:"id"`
		FieldInJsonb string `jsonb:"meta"`
	}
	customModel.Select("status, id, meta").OrderBy("id ASC").MustQuery(&customOrders)
	t.String("custom order struct", fmt.Sprintf("%+v", customOrders),
		"[{Status:new id:1 FieldInJsonb:yes} {Status:new2 id:2 FieldInJsonb:}]")

	var customOrder struct {
		Status       string
		Id           int
		FieldInJsonb string `jsonb:"meta"`
	}
	customModel.Select("status, id, meta").OrderBy("id ASC").Limit(1).MustQuery(&customOrder)
	t.String("custom order struct", fmt.Sprintf("%+v", customOrder), "{Status:new Id:1 FieldInJsonb:yes}")

	var firstOrder order
	model.Find().OrderBy("created_at ASC").Limit(1).MustQuery(&firstOrder) // "LIMIT 1" only necessary for gopg

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

	type nested struct {
		number int `column:"number"`
	}

	type deep struct {
		nested *nested `column:",anonymous"`
	}

	anonymous := struct {
		id   int `column:"id"`
		deep struct {
			nested struct {
				order order `column:",anonymous"`
			} `column:",anonymous"`
		} `column:",anonymous"`
		deep2 *deep `column:",anonymous"`
	}{
		deep2: &deep{nested: &nested{}}, // pointer field should be initialized first
	}
	model.Select("1").
		Select(model.AddTableName(model.Fields()...)...).
		Select("2").
		Select(model.AddTableName(model.JSONBFields()...)...). // jsonb fields should be placed at the end
		Where("id = $1", 1).MustQuery(&anonymous)
	t.Int("anonymous id", anonymous.id, 1)
	t.Int("anonymous order id", anonymous.deep.nested.order.Id, 1)
	t.Int("anonymous nested number", anonymous.deep2.nested.number, 2)

	var customOrder2 struct {
		order
		CustomField int
	}
	model.Find().Select("123").OrderBy("created_at ASC").Limit(1).MustQuery(&customOrder2) // "LIMIT 1" only necessary for gopg
	t.Int("order id", customOrder2.Id, 1)
	t.Int("order custom field", customOrder2.CustomField, 123)

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
	err = model.Find(psql.AddTableName).OrderBy("created_at DESC").Query(&orders)
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
	model.MustTransaction(func(ctx context.Context, tx db.Tx) error {
		model.Update("UserId", psql.StringWithArg("user_id - $?", 23)).MustExecuteCtxTx(ctx, tx)
		model.Update(achanges...).MustExecuteCtxTx(ctx, tx, &rowsAffected)
		model.Update(
			"UserId", psql.StringWithArg("user_id * $?", 2),
			"UserId", psql.StringWithArg("user_id + $?", 99), // this will override the previous one
		).MustExecuteCtxTx(ctx, tx)
		model.Update("UserId", psql.String("user_id * 3")).MustExecuteCtxTx(ctx, tx)
		model.Update("FieldInJsonb", psql.StringWithArg(
			`to_jsonb(concat_ws(E'\n', NULLIF(meta->>'field_in_jsonb', ''), $?::text))`,
			"foo",
		)).MustExecuteCtxTx(ctx, tx)
		return nil
	})
	t.Int("rows affected", rowsAffected, 2)

	var secondOrder order
	err = model.Find().Where("id = $1", 2).Query(&secondOrder)
	if err != nil {
		t.Fatal(err)
	}
	t.Int("order id", secondOrder.Id, 2)
	t.String("order status", secondOrder.Status, "gopsql")
	ca = time.Since(secondOrder.CreatedAt)
	ua = time.Since(secondOrder.UpdatedAt)
	t.Bool("order created at", ca > 200*time.Millisecond) // because of time.Sleep
	t.Bool("order updated at", ua > 0 && ua < 200*time.Millisecond)
	t.String("order FieldInJsonb", secondOrder.FieldInJsonb, "red\nfoo")
	t.String("order OtherJsonb", secondOrder.OtherJsonb, "blue")
	var u int
	t.Int("order user", secondOrder.UserId, (u-23+99)*3)

	var testError order
	model.Find().ReplaceSelect("error", `'{"NoError":"foo"}' AS error`).Where("id = $1", 2).MustQuery(&testError)
	err = model.Find().ReplaceSelect("error", `'{"HasError":"foo"}' AS error`).Where("id = $1", 2).Query(&testError)
	if !strings.Contains(err.Error(), "error unmarshaling field HasError of error") {
		t.Fatal("expect error: error unmarshaling field", err)
	}

	count, err := model.Count()
	if err != nil {
		t.Fatal(err)
	}
	t.Int("rows count", count, 2)

	count = model.MustCount("COUNT(*) * 11")
	t.Int("rows count", count, 2*11)

	var total string
	model.Where("id > $1", 0).GroupBy("id").Select("sum(total_amount)::text").
		Having("sum(total_amount) > $2", 1).MustQueryRow(&total)
	t.String("total", total, "12.34")

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
