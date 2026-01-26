package psql_test

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
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

func init() {
	psql.DefaultColumnNamer = psql.ToUnderscore
	psql.DefaultTableNamer = psql.ToPluralUnderscore
}

var connStr string

func init() {
	connStr = os.Getenv("DBCONNSTR")
	if connStr == "" {
		connStr = "postgres://localhost:5432/gopsqltests?sslmode=disable"
	}
}

// Test types
type (
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

func (p *password) Scan(src interface{}) error {
	if value, ok := src.(string); ok {
		*p = password{hashed: value}
	}
	return nil
}

func (p *password) ScanValue(rd gopg.TypesReader, n int) error {
	value, err := gopg.TypesScanString(rd, n)
	if err == nil {
		*p = password{hashed: value}
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

// echoContext simulates echo.Context for Bind testing
type echoContext struct{}

func (c echoContext) Bind(i interface{}) error {
	if o, ok := i.(*order); ok {
		o.Id = 2
		o.Status = "foo"
	}
	return nil
}

func getConnections(t *testing.T) []db.DB {
	t.Helper()

	var connections []db.DB

	if conn, err := pq.Open(connStr); err == nil {
		connections = append(connections, conn)
	} else {
		t.Logf("pq connection failed: %v", err)
	}

	if conn, err := pgx.Open(connStr); err == nil {
		connections = append(connections, conn)
	} else {
		t.Logf("pgx connection failed: %v", err)
	}

	if conn, err := gopg.Open(connStr); err == nil {
		connections = append(connections, conn)
	} else {
		t.Logf("gopg connection failed: %v", err)
	}

	if len(connections) == 0 {
		t.Skip("No database connections available")
	}

	return connections
}

func TestCRUD(t *testing.T) {
	connections := getConnections(t)

	for _, conn := range connections {
		connName := fmt.Sprintf("%T", conn)
		t.Run(connName, func(t *testing.T) {
			testCRUDOperations(t, conn)
		})
	}
}

func testCRUDOperations(t *testing.T, conn db.DB) {
	defer conn.Close()

	model := psql.NewModel(order{}, conn, logger.StandardLogger)

	// Ensure cleanup happens even on test failure
	t.Cleanup(func() {
		model.NewSQL(model.DropSchema()).Execute()
	})

	// Setup: drop and create table
	if err := model.NewSQL(model.DropSchema()).Execute(); err != nil {
		t.Fatalf("DropSchema failed: %v", err)
	}
	if err := model.NewSQL(model.Schema()).Execute(); err != nil {
		t.Fatalf("Schema failed: %v", err)
	}

	// Test Insert
	t.Run("Insert", func(t *testing.T) {
		randomBytes := make([]byte, 10)
		rand.Read(randomBytes)
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
			"Sources": [{"Name": "yes", "baddata": "foobar"}],
			"Sources2": {"cash": 100},
			"Sources3": {"Word": "finish"}
		}`)
		var createData map[string]interface{}
		if err := json.NewDecoder(createInput).Decode(&createData); err != nil {
			t.Fatalf("Decode failed: %v", err)
		}

		var id int
		err := model.Insert(
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
			t.Fatalf("Insert failed: %v", err)
		}
		if id != 1 {
			t.Errorf("id = %d, want 1", id)
		}

		// Insert second row
		err = model.Insert(
			model.Changes(psql.RawChanges{"Status": "new2"}),
		).Returning("id").QueryRow(&id)
		if err != nil {
			t.Fatalf("Insert second row failed: %v", err)
		}
		if id != 2 {
			t.Errorf("id = %d, want 2", id)
		}
	})

	// Test Exists
	t.Run("Exists", func(t *testing.T) {
		exists := model.Where("id = $1", 1).MustExists()
		if !exists {
			t.Error("Expected record to exist")
		}

		exists = model.Where("id = $1", 999).MustExists()
		if exists {
			t.Error("Expected record to not exist")
		}
	})

	// Test Count
	t.Run("Count", func(t *testing.T) {
		count, err := model.Count()
		if err != nil {
			t.Fatalf("Count failed: %v", err)
		}
		if count != 2 {
			t.Errorf("count = %d, want 2", count)
		}

		count = model.MustCount("COUNT(*) * 11")
		if count != 22 {
			t.Errorf("count = %d, want 22", count)
		}
	})

	// Test Find
	t.Run("Find", func(t *testing.T) {
		var orders []order
		err := model.Find(psql.AddTableName).OrderBy("created_at DESC").Query(&orders)
		if err != nil {
			t.Fatalf("Find failed: %v", err)
		}
		if len(orders) != 2 {
			t.Errorf("len(orders) = %d, want 2", len(orders))
		}
		if orders[0].Id != 2 {
			t.Errorf("orders[0].Id = %d, want 2", orders[0].Id)
		}
	})

	// Test Find single row
	t.Run("FindSingleRow", func(t *testing.T) {
		var firstOrder order
		model.Find().OrderBy("created_at ASC").Limit(1).MustQuery(&firstOrder)
		if firstOrder.Id != 1 {
			t.Errorf("firstOrder.Id = %d, want 1", firstOrder.Id)
		}
		if firstOrder.Status != "new" {
			t.Errorf("firstOrder.Status = %q, want 'new'", firstOrder.Status)
		}
		if firstOrder.FieldInJsonb != "yes" {
			t.Errorf("firstOrder.FieldInJsonb = %q, want 'yes'", firstOrder.FieldInJsonb)
		}
		if !firstOrder.Password.Equal("123123") {
			t.Errorf("firstOrder.Password.Equal('123123') = false, want true")
		}
	})

	// Test Update
	t.Run("Update", func(t *testing.T) {
		var rowsAffected int
		model.MustTransaction(func(ctx context.Context, tx db.Tx) error {
			model.Update("Status", "updated").MustExecuteCtxTx(ctx, tx, &rowsAffected)
			return nil
		})
		if rowsAffected != 2 {
			t.Errorf("rowsAffected = %d, want 2", rowsAffected)
		}

		var status string
		model.Select("status").Where("id = $1", 1).MustQueryRow(&status)
		if status != "updated" {
			t.Errorf("status = %q, want 'updated'", status)
		}
	})

	// Test Delete
	t.Run("Delete", func(t *testing.T) {
		var rowsDeleted int
		err := model.Delete().Execute(&rowsDeleted)
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
		if rowsDeleted != 2 {
			t.Errorf("rowsDeleted = %d, want 2", rowsDeleted)
		}

		count, _ := model.Count()
		if count != 0 {
			t.Errorf("count = %d, want 0", count)
		}
	})
}

func TestTransaction(t *testing.T) {
	connections := getConnections(t)

	for _, conn := range connections {
		connName := fmt.Sprintf("%T", conn)
		t.Run(connName, func(t *testing.T) {
			testTransactionOperations(t, conn)
		})
	}
}

func testTransactionOperations(t *testing.T, conn db.DB) {
	defer conn.Close()

	model := psql.NewModel(order{}, conn, logger.StandardLogger)

	// Ensure cleanup happens even on test failure
	t.Cleanup(func() {
		model.NewSQL(model.DropSchema()).Execute()
	})

	model.NewSQL(model.DropSchema()).MustExecute()
	model.NewSQL(model.Schema()).MustExecute()

	t.Run("CommitTransaction", func(t *testing.T) {
		model.MustTransaction(func(ctx context.Context, tx db.Tx) error {
			model.Insert(model.Changes(psql.RawChanges{"Status": "tx1"})).MustExecuteCtxTx(ctx, tx)
			return nil
		})

		count := model.MustCount()
		if count != 1 {
			t.Errorf("count = %d, want 1", count)
		}
	})

	t.Run("RollbackTransaction", func(t *testing.T) {
		err := model.Transaction(func(ctx context.Context, tx db.Tx) error {
			model.Insert(model.Changes(psql.RawChanges{"Status": "tx2"})).MustExecuteCtxTx(ctx, tx)
			return errors.New("rollback")
		})
		if err == nil || err.Error() != "rollback" {
			t.Errorf("err = %v, want 'rollback'", err)
		}

		count := model.MustCount()
		if count != 1 {
			t.Errorf("count = %d, want 1 (should not have increased)", count)
		}
	})

	t.Run("PanicRollbackTransaction", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Should not panic with MustTransaction: %v", r)
			}
		}()

		err := model.Transaction(func(ctx context.Context, tx db.Tx) error {
			model.Insert(model.Changes(psql.RawChanges{"Status": "tx3"})).MustExecuteCtxTx(ctx, tx)
			panic("test panic")
		})
		if err == nil || err.Error() != "test panic" {
			t.Errorf("err = %v, want 'test panic'", err)
		}

		count := model.MustCount()
		if count != 1 {
			t.Errorf("count = %d, want 1 (should not have increased)", count)
		}
	})
}

func TestQueryTimeout(t *testing.T) {
	connections := getConnections(t)

	for _, conn := range connections {
		connName := fmt.Sprintf("%T", conn)
		t.Run(connName, func(t *testing.T) {
			defer conn.Close()

			model := psql.NewModelTable("", conn)
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			err := model.NewSQL("SELECT pg_sleep(1)").ExecuteCtx(ctx)
			if err == nil {
				t.Error("Expected timeout error")
			}

			// Different drivers report timeout differently
			if _, ok := conn.(*gopg.DB); ok {
				if !os.IsTimeout(err) {
					t.Errorf("Expected timeout error for gopg, got: %v", err)
				}
			} else if _, ok := conn.(*pgx.DB); ok {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("Expected DeadlineExceeded for pgx, got: %v", err)
				}
			} else {
				if conn.ErrGetCode(err) != "57014" {
					t.Errorf("Expected error code 57014 for pq, got: %v", err)
				}
			}
		})
	}
}

func TestExplain(t *testing.T) {
	conn, err := pgx.Open(connStr)
	if err != nil {
		t.Skip("Database connection not available")
	}
	defer conn.Close()

	model := psql.NewModelTable("pg_class", conn)

	t.Run("ExplainWithStringPointer", func(t *testing.T) {
		var explain string
		var count int
		if err := model.Select("COUNT(*)").Explain(&explain).QueryRow(&count); err != nil {
			t.Fatalf("QueryRow failed: %v", err)
		}
		if !strings.Contains(explain, "Aggregate") {
			t.Errorf("explain = %q, want to contain 'Aggregate'", explain)
		}
	})

	t.Run("ExplainAnalyze", func(t *testing.T) {
		var explainAnalyze string
		var count int
		if err := model.Select("COUNT(*)").ExplainAnalyze(&explainAnalyze).QueryRow(&count); err != nil {
			t.Fatalf("QueryRow failed: %v", err)
		}
		if !strings.Contains(explainAnalyze, "actual time") {
			t.Errorf("explainAnalyze = %q, want to contain 'actual time'", explainAnalyze)
		}
	})

	t.Run("ExplainWithFunc", func(t *testing.T) {
		var captured string
		var count int
		if err := model.Select("COUNT(*)").Explain(func(v ...interface{}) {
			captured = v[0].(string)
		}).QueryRow(&count); err != nil {
			t.Fatalf("QueryRow failed: %v", err)
		}
		if !strings.Contains(captured, "Aggregate") {
			t.Errorf("captured = %q, want to contain 'Aggregate'", captured)
		}
	})

	t.Run("ExplainUnsupportedTarget", func(t *testing.T) {
		var count int
		err := model.Select("COUNT(*)").Explain(123).QueryRow(&count)
		if err != psql.ErrUnsupportedExplainTarget {
			t.Errorf("err = %v, want ErrUnsupportedExplainTarget", err)
		}
	})

	t.Run("ExplainNilTarget", func(t *testing.T) {
		var count int
		if err := model.Select("COUNT(*)").Explain(nil).QueryRow(&count); err != nil {
			t.Fatalf("QueryRow failed: %v", err)
		}
	})
}

func TestNewSQL(t *testing.T) {
	m := psql.NewModel(order{})

	t.Run("SelectSQL", func(t *testing.T) {
		sql := "INSERT INTO orders (status, trade_number) VALUES ($?, $?)"
		s := m.NewSQL(sql, "new", "1234567890")
		got := s.String()
		want := "INSERT INTO orders (status, trade_number) VALUES ($1, $2)"
		if got != want {
			t.Errorf("String() = %q, want %q", got, want)
		}
	})

	t.Run("AsInsert", func(t *testing.T) {
		sql := "INSERT INTO orders (status, trade_number) VALUES ($?, $?)"
		s := m.NewSQL(sql, "new", "1234567890").AsInsert()
		got := s.OnConflict().DoNothing().String()
		want := "INSERT INTO orders (status, trade_number) VALUES ($1, $2) ON CONFLICT DO NOTHING"
		if got != want {
			t.Errorf("String() = %q, want %q", got, want)
		}
	})

	t.Run("AsUpdate", func(t *testing.T) {
		sql := "UPDATE orders SET status = $?"
		s := m.NewSQL(sql, "new").AsUpdate()
		gotSQL, gotArgs := s.Where("status = $?", "old").Returning("id").StringValues()
		wantSQL := "UPDATE orders SET status = $2 WHERE status = $1 RETURNING id"
		if gotSQL != wantSQL {
			t.Errorf("SQL = %q, want %q", gotSQL, wantSQL)
		}
		if len(gotArgs) != 2 || gotArgs[0] != "old" || gotArgs[1] != "new" {
			t.Errorf("Args = %v, want [old new]", gotArgs)
		}
	})

	t.Run("AsDelete", func(t *testing.T) {
		sql := "DELETE FROM orders WHERE status = $?"
		s := m.NewSQL(sql, "new").AsDelete()
		got := s.Returning("id").String()
		want := "DELETE FROM orders WHERE status = $1 RETURNING id"
		if got != want {
			t.Errorf("String() = %q, want %q", got, want)
		}
	})

	t.Run("AsSelect", func(t *testing.T) {
		sql := "SELECT id, status FROM orders WHERE status = $?"
		s := m.NewSQL(sql, "new").AsSelect()
		got := s.Offset(10).String()
		want := "SELECT id, status FROM orders WHERE status = $1 OFFSET 10"
		if got != want {
			t.Errorf("String() = %q, want %q", got, want)
		}
	})
}

func TestBind(t *testing.T) {
	conn, err := pgx.Open(connStr)
	if err != nil {
		t.Skip("Database connection not available")
	}
	defer conn.Close()

	model := psql.NewModel(order{}, conn, logger.StandardLogger)

	// Ensure cleanup happens even on test failure
	t.Cleanup(func() {
		model.NewSQL(model.DropSchema()).Execute()
	})

	model.NewSQL(model.DropSchema()).MustExecute()
	model.NewSQL(model.Schema()).MustExecute()

	// Insert a test row
	model.Insert(model.Changes(psql.RawChanges{
		"Status":      "new",
		"TradeNumber": "test123",
	})).MustExecute()

	var firstOrder order
	model.Find().OrderBy("id ASC").Limit(1).MustQuery(&firstOrder)

	t.Run("BindWithNoPermit", func(t *testing.T) {
		var c echoContext
		changes, err := model.Permit().Bind(c, &firstOrder)
		if err != nil {
			t.Fatalf("Bind failed: %v", err)
		}
		if len(changes) != 0 {
			t.Errorf("len(changes) = %d, want 0", len(changes))
		}
		if firstOrder.Id != 1 {
			t.Errorf("firstOrder.Id = %d, want 1", firstOrder.Id)
		}
	})

	t.Run("BindWithPermit", func(t *testing.T) {
		var c echoContext
		changes, err := model.Permit("Id", "TradeNumber").Bind(c, &firstOrder)
		if err != nil {
			t.Fatalf("Bind failed: %v", err)
		}
		if len(changes) != 2 {
			t.Errorf("len(changes) = %d, want 2", len(changes))
		}
		if firstOrder.Id != 2 {
			t.Errorf("firstOrder.Id = %d, want 2 (set by Bind)", firstOrder.Id)
		}
	})
}

func TestJsonbStrictError(t *testing.T) {
	conn, err := pgx.Open(connStr)
	if err != nil {
		t.Skip("Database connection not available")
	}
	defer conn.Close()

	model := psql.NewModel(order{}, conn, logger.StandardLogger)

	// Ensure cleanup happens even on test failure
	t.Cleanup(func() {
		model.NewSQL(model.DropSchema()).Execute()
	})

	model.NewSQL(model.DropSchema()).MustExecute()
	model.NewSQL(model.Schema()).MustExecute()

	model.Insert(model.Changes(psql.RawChanges{"Status": "test"})).MustExecute()

	t.Run("NoErrorField", func(t *testing.T) {
		var testOrder order
		err := model.Find().ReplaceSelect("error", `'{"NoError":"foo"}' AS error`).Where("id = $1", 1).Query(&testOrder)
		if err != nil {
			t.Errorf("Query failed: %v", err)
		}
	})

	t.Run("StrictErrorField", func(t *testing.T) {
		var testOrder order
		err := model.Find().ReplaceSelect("error", `'{"HasError":"foo"}' AS error`).Where("id = $1", 1).Query(&testOrder)
		if err == nil {
			t.Error("Expected error for strict field")
		}
		if !strings.Contains(err.Error(), "error unmarshaling field HasError") {
			t.Errorf("err = %v, want to contain 'error unmarshaling field HasError'", err)
		}
	})
}
