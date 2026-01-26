package psql_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/gopsql/db"
	"github.com/gopsql/gopg"
	"github.com/gopsql/pgx"
	"github.com/gopsql/pq"
	"github.com/gopsql/psql"
	"github.com/shopspring/decimal"
)

func getQueryConnections(t *testing.T) []db.DB {
	t.Helper()

	connStr := os.Getenv("DBCONNSTR")
	if connStr == "" {
		connStr = "postgres://localhost:5432/gopsqltests?sslmode=disable"
	}

	var connections []db.DB

	if conn, err := pq.Open(connStr); err == nil {
		connections = append(connections, conn)
	}
	if conn, err := pgx.Open(connStr); err == nil {
		connections = append(connections, conn)
	}
	if conn, err := gopg.Open(connStr); err == nil {
		connections = append(connections, conn)
	}

	if len(connections) == 0 {
		t.Skip("No database connections available")
	}

	return connections
}

func TestQueryIntoSlice(t *testing.T) {
	connections := getQueryConnections(t)

	for _, conn := range connections {
		connName := fmt.Sprintf("%T", conn)
		t.Run(connName, func(t *testing.T) {
			defer conn.Close()
			model := psql.NewModelTable("", conn)

			t.Run("StringSlice", func(t *testing.T) {
				var result []string
				model.NewSQL("SELECT 'a' UNION SELECT 'b'").MustQuery(&result)
				got := fmt.Sprintf("%v", result)
				if got != "[a b]" {
					t.Errorf("result = %v, want [a b]", got)
				}
			})

			t.Run("IntSlice", func(t *testing.T) {
				var result []int
				model.NewSQL("SELECT 1 UNION SELECT 2").MustQuery(&result)
				got := fmt.Sprintf("%v", result)
				if got != "[1 2]" {
					t.Errorf("result = %v, want [1 2]", got)
				}
			})
		})
	}
}

func TestQueryIntoMap(t *testing.T) {
	connections := getQueryConnections(t)

	for _, conn := range connections {
		connName := fmt.Sprintf("%T", conn)
		t.Run(connName, func(t *testing.T) {
			defer conn.Close()
			model := psql.NewModelTable("", conn)

			t.Run("IntToDecimalMap", func(t *testing.T) {
				var result map[int]decimal.Decimal
				model.NewSQL("SELECT 1, 1.23 UNION SELECT 2, 3.45").MustQuery(&result)
				got := fmt.Sprintf("%v", result)
				if got != "map[1:1.23 2:3.45]" {
					t.Errorf("result = %v, want map[1:1.23 2:3.45]", got)
				}
			})

			t.Run("IntToStringMap", func(t *testing.T) {
				var result map[int]string
				model.NewSQL("SELECT 1, 'a' UNION SELECT 2, 'b'").MustQuery(&result)
				got := fmt.Sprintf("%v", result)
				if got != "map[1:a 2:b]" {
					t.Errorf("result = %v, want map[1:a 2:b]", got)
				}
			})

			t.Run("StringToIntMap", func(t *testing.T) {
				var result map[string]int
				model.NewSQL("SELECT 'a', 2 UNION SELECT 'b', 1").MustQuery(&result)
				got := fmt.Sprintf("%v", result)
				if got != "map[a:2 b:1]" {
					t.Errorf("result = %v, want map[a:2 b:1]", got)
				}
			})
		})
	}
}

func TestQueryIntoStruct(t *testing.T) {
	connections := getQueryConnections(t)

	for _, conn := range connections {
		connName := fmt.Sprintf("%T", conn)
		t.Run(connName, func(t *testing.T) {
			defer conn.Close()
			model := psql.NewModelTable("", conn)

			t.Run("MapToStruct", func(t *testing.T) {
				var result map[int]struct {
					id     int
					status string
					Name   string
				}
				model.NewSQL("SELECT 1, 2, 'a', 'b' UNION SELECT 2, 3, 'c', 'd'").MustQuery(&result)
				got := fmt.Sprintf("%+v", result)
				if got != "map[1:{id:2 status:a Name:b} 2:{id:3 status:c Name:d}]" {
					t.Errorf("result = %v", got)
				}
			})

			t.Run("MapToSliceOfStruct", func(t *testing.T) {
				var result map[int][]struct {
					UserId int
					status string
				}
				model.NewSQL("SELECT 1, 2, 'a' UNION SELECT 1, 3, 'd'").MustQuery(&result)
				got := fmt.Sprintf("%+v", result)
				if got != "map[1:[{UserId:2 status:a} {UserId:3 status:d}]]" {
					t.Errorf("result = %v", got)
				}
			})
		})
	}
}

func TestQueryIntoNestedMap(t *testing.T) {
	connections := getQueryConnections(t)

	for _, conn := range connections {
		connName := fmt.Sprintf("%T", conn)
		t.Run(connName, func(t *testing.T) {
			defer conn.Close()
			model := psql.NewModelTable("", conn)

			t.Run("NestedMapWithBoolKey", func(t *testing.T) {
				var result map[int]map[string]map[bool]int
				model.NewSQL("SELECT 1, 's', true, 0 UNION SELECT 1, 's', false, 1").MustQuery(&result)
				got := fmt.Sprintf("%v", result)
				if got != "map[1:map[s:map[false:1 true:0]]]" {
					t.Errorf("result = %v", got)
				}
			})

			t.Run("NestedMapToStruct", func(t *testing.T) {
				var result map[int]map[bool]struct {
					foo int
					bar string
				}
				model.NewSQL("SELECT 1, false, 0, 'hello' UNION SELECT 1, true, 1, 'world'").MustQuery(&result)
				got := fmt.Sprintf("%+v", result)
				if got != "map[1:map[false:{foo:0 bar:hello} true:{foo:1 bar:world}]]" {
					t.Errorf("result = %v", got)
				}
			})

			t.Run("MapWithArrayKey", func(t *testing.T) {
				var result map[int][3]string
				model.NewSQL("SELECT 1, 'a', 'b', 'c' UNION SELECT 2, 'd', 'e', 'f'").MustQuery(&result)
				got := fmt.Sprintf("%v", result)
				if got != "map[1:[a b c] 2:[d e f]]" {
					t.Errorf("result = %v", got)
				}
			})

			t.Run("MapWithArrayKeyToArrayValue", func(t *testing.T) {
				var result map[[2]int][2]string
				model.NewSQL("SELECT 1, 2, 'a', 'b' UNION SELECT 3, 4, 'c', 'd'").MustQuery(&result)
				got := fmt.Sprintf("%v", result)
				if got != "map[[1 2]:[a b] [3 4]:[c d]]" {
					t.Errorf("result = %v", got)
				}
			})

			t.Run("MapWithStructKeyToStructValue", func(t *testing.T) {
				var result map[struct {
					uid int
					id  int
				}]struct {
					status string
					Name   string
				}
				model.NewSQL("SELECT 1, 2, 'a', 'b' UNION SELECT 3, 4, 'c', 'd'").MustQuery(&result)
				got := fmt.Sprintf("%+v", result)
				if got != "map[{uid:1 id:2}:{status:a Name:b} {uid:3 id:4}:{status:c Name:d}]" {
					t.Errorf("result = %v", got)
				}
			})
		})
	}
}

func TestQueryIntoCustomStruct(t *testing.T) {
	connections := getQueryConnections(t)

	for _, conn := range connections {
		connName := fmt.Sprintf("%T", conn)
		t.Run(connName, func(t *testing.T) {
			defer conn.Close()

			type testOrder struct {
				__TABLE_NAME__ string `orders`

				Id     int
				Status string
			}

			model := psql.NewModel(testOrder{}, conn)

			// Ensure cleanup happens even on test failure
			t.Cleanup(func() {
				model.NewSQL(model.DropSchema()).Execute()
			})

			model.NewSQL(model.DropSchema()).MustExecute()
			model.NewSQL(model.Schema()).MustExecute()

			// Insert test data
			model.Insert("Status", "active").MustExecute()
			model.Insert("Status", "inactive").MustExecute()

			t.Run("QueryIntoSliceOfStruct", func(t *testing.T) {
				var orders []testOrder
				model.Find().OrderBy("id ASC").MustQuery(&orders)
				if len(orders) != 2 {
					t.Fatalf("len = %d, want 2", len(orders))
				}
				if orders[0].Id != 1 {
					t.Errorf("orders[0].Id = %d, want 1", orders[0].Id)
				}
				if orders[0].Status != "active" {
					t.Errorf("orders[0].Status = %q, want 'active'", orders[0].Status)
				}
			})

			t.Run("QueryIntoSingleStruct", func(t *testing.T) {
				var order testOrder
				model.Find().Where("id = $1", 1).MustQuery(&order)
				if order.Id != 1 {
					t.Errorf("order.Id = %d, want 1", order.Id)
				}
			})
		})
	}
}

func TestQueryWithAnonymousFields(t *testing.T) {
	connections := getQueryConnections(t)

	for _, conn := range connections {
		connName := fmt.Sprintf("%T", conn)
		t.Run(connName, func(t *testing.T) {
			defer conn.Close()

			type baseOrder struct {
				__TABLE_NAME__ string `orders`

				Id     int
				Status string
				Meta   string `jsonb:"meta"`
			}

			type orderWithMeta struct {
				baseOrder
				FieldInJsonb string `jsonb:"meta"`
			}

			model := psql.NewModel(orderWithMeta{}, conn)

			// Ensure cleanup happens even on test failure
			t.Cleanup(func() {
				model.NewSQL(model.DropSchema()).Execute()
			})

			model.NewSQL(model.DropSchema()).MustExecute()
			model.NewSQL(model.Schema()).MustExecute()

			// Insert with jsonb data
			model.Insert(
				model.Changes(psql.RawChanges{"Status": "test"}),
				model.Changes(psql.RawChanges{"FieldInJsonb": "jsonb_value"}),
			).MustExecute()

			var result orderWithMeta
			model.Find().Where("id = $1", 1).MustQuery(&result)

			if result.Id != 1 {
				t.Errorf("result.Id = %d, want 1", result.Id)
			}
			if result.Status != "test" {
				t.Errorf("result.Status = %q, want 'test'", result.Status)
			}
			if result.FieldInJsonb != "jsonb_value" {
				t.Errorf("result.FieldInJsonb = %q, want 'jsonb_value'", result.FieldInJsonb)
			}
		})
	}
}

func TestQueryErrors(t *testing.T) {
	connections := getQueryConnections(t)

	for _, conn := range connections {
		connName := fmt.Sprintf("%T", conn)
		t.Run(connName, func(t *testing.T) {
			defer conn.Close()
			model := psql.NewModelTable("", conn)

			t.Run("InvalidTarget", func(t *testing.T) {
				var result string // not a pointer
				err := model.NewSQL("SELECT 1").Query(result)
				if err != psql.ErrInvalidTarget {
					t.Errorf("err = %v, want ErrInvalidTarget", err)
				}
			})

			t.Run("NoConnection", func(t *testing.T) {
				noConnModel := psql.NewModelTable("test")
				err := noConnModel.NewSQL("SELECT 1").Query(&[]string{})
				if err != psql.ErrNoConnection {
					t.Errorf("err = %v, want ErrNoConnection", err)
				}
			})
		})
	}
}
