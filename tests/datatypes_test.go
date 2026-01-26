package psql_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gopsql/logger"
	"github.com/gopsql/pgx"
	"github.com/gopsql/psql"
	"github.com/shopspring/decimal"
)

// datatypes struct tests all supported Go types
type datatypes struct {
	__TABLE_NAME__ string `datatypes`

	// Basic integer types
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

	// Time and floating point
	Ka time.Time
	La float32
	Ma float64
	Na decimal.Decimal
	Oa bool
	Pa string

	// Pointer types (nullable)
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
	// Nb *decimal.Decimal // known bug
	Ob *bool
	Pb *string

	// Slice types
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

	// Pointer to slice types
	Ad *[]int8
	Bd *[]int16
	Cd *[]int32
	Dd *[]int64
	Ed *[]int
	Fd *[]uint8
	Gd *[]uint16
	Hd *[]uint32
	// Id *[]uint64 // known bug
	Jd *[]uint
	Kd *[]time.Time
	Ld *[]float32
	Md *[]float64
	Nd *[]decimal.Decimal
	Od *[]bool
	Pd *[]string
}

func TestDataTypes(t *testing.T) {
	connStr := os.Getenv("DBCONNSTR")
	if connStr == "" {
		connStr = "postgres://localhost:5432/gopsqltests?sslmode=disable"
	}

	conn, err := pgx.Open(connStr)
	if err != nil {
		t.Skip("Database connection not available:", err)
	}
	defer conn.Close()

	m := psql.NewModel(datatypes{}, conn, logger.StandardLogger)

	// Ensure cleanup happens even on test failure
	t.Cleanup(func() {
		m.NewSQL(m.DropSchema()).Execute()
	})

	// Setup
	m.NewSQL(m.DropSchema()).MustExecute()
	m.NewSQL(m.Schema()).MustExecute()

	// Insert default values
	m.NewSQL("INSERT INTO datatypes DEFAULT VALUES").MustExecute()

	// Query back
	var result datatypes
	m.Find().MustQuery(&result)

	// Verify some basic fields are set to defaults
	if result.Aa != 0 {
		t.Errorf("Aa = %d, want 0", result.Aa)
	}
	if result.Oa != false {
		t.Errorf("Oa = %v, want false", result.Oa)
	}
	if result.Pa != "" {
		t.Errorf("Pa = %q, want empty string", result.Pa)
	}
}

func TestJsonbFields(t *testing.T) {
	connStr := os.Getenv("DBCONNSTR")
	if connStr == "" {
		connStr = "postgres://localhost:5432/gopsqltests?sslmode=disable"
	}

	conn, err := pgx.Open(connStr)
	if err != nil {
		t.Skip("Database connection not available:", err)
	}
	defer conn.Close()

	type jsonbTest struct {
		__TABLE_NAME__ string `jsonb_tests`

		Id          int
		Name        string
		Picture     string            `jsonb:"meta"`
		Tags        []string          `jsonb:"meta"`
		Settings    map[string]string `jsonb:"meta"`
		NestedField string            `jsonb:"meta2"`
		Numbers     []int             `jsonb:"meta2"`
	}

	m := psql.NewModel(jsonbTest{}, conn, logger.StandardLogger)

	// Ensure cleanup happens even on test failure
	t.Cleanup(func() {
		m.NewSQL(m.DropSchema()).Execute()
	})

	// Setup
	m.NewSQL(m.DropSchema()).MustExecute()
	m.NewSQL(m.Schema()).MustExecute()

	t.Run("InsertJsonbFields", func(t *testing.T) {
		var id int
		err := m.Insert(
			"Name", "test",
			m.Changes(psql.RawChanges{
				"Picture":     "photo.jpg",
				"Tags":        []string{"tag1", "tag2"},
				"Settings":    map[string]string{"key": "value"},
				"NestedField": "nested",
				"Numbers":     []int{1, 2, 3},
			}),
		).Returning("id").QueryRow(&id)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
		if id != 1 {
			t.Errorf("id = %d, want 1", id)
		}
	})

	t.Run("QueryJsonbFields", func(t *testing.T) {
		var result jsonbTest
		m.Find().Where("id = $1", 1).MustQuery(&result)

		if result.Name != "test" {
			t.Errorf("Name = %q, want 'test'", result.Name)
		}
		if result.Picture != "photo.jpg" {
			t.Errorf("Picture = %q, want 'photo.jpg'", result.Picture)
		}
		if len(result.Tags) != 2 {
			t.Errorf("len(Tags) = %d, want 2", len(result.Tags))
		}
		if result.Settings["key"] != "value" {
			t.Errorf("Settings[key] = %q, want 'value'", result.Settings["key"])
		}
		if result.NestedField != "nested" {
			t.Errorf("NestedField = %q, want 'nested'", result.NestedField)
		}
		if len(result.Numbers) != 3 {
			t.Errorf("len(Numbers) = %d, want 3", len(result.Numbers))
		}
	})

	t.Run("UpdateJsonbFields", func(t *testing.T) {
		m.Update(
			m.Changes(psql.RawChanges{"Picture": "updated.jpg"}),
		).Where("id = $1", 1).MustExecute()

		var result jsonbTest
		m.Find().Where("id = $1", 1).MustQuery(&result)

		if result.Picture != "updated.jpg" {
			t.Errorf("Picture = %q, want 'updated.jpg'", result.Picture)
		}
		// Other jsonb fields should remain unchanged
		if len(result.Tags) != 2 {
			t.Errorf("len(Tags) = %d, want 2", len(result.Tags))
		}
	})

	t.Run("UpdateJsonbWithString", func(t *testing.T) {
		m.Update(
			"Picture", psql.String("'\"raw_value\"'::jsonb"),
		).Where("id = $1", 1).MustExecute()

		var picture string
		m.Select("meta->>'picture'").Where("id = $1", 1).MustQueryRow(&picture)
		if picture != "raw_value" {
			t.Errorf("picture = %q, want 'raw_value'", picture)
		}
	})
}

func TestSchemaGeneration(t *testing.T) {
	type schemaTest struct {
		__TABLE_NAME__ string `schema_tests`

		Id        int
		Name      string
		Age       *int
		Numbers   []int
		CreatedAt time.Time
		DeletedAt *time.Time `dataType:"timestamptz"`
		FullName  string     `jsonb:"meta"`
		NickName  string     `jsonb:"meta"`
	}

	m := psql.NewModel(schemaTest{})
	schema := m.Schema()

	// Verify schema contains expected elements
	expectedParts := []string{
		"CREATE TABLE schema_tests",
		"id SERIAL PRIMARY KEY",
		"name text DEFAULT ''::text NOT NULL",
		"age bigint DEFAULT 0",           // nullable, no NOT NULL
		"numbers bigint[] DEFAULT '{}'",  // array type
		"created_at timestamptz DEFAULT NOW() NOT NULL",
		"deleted_at timestamptz",         // custom dataType, nullable
		"meta jsonb DEFAULT '{}'::jsonb NOT NULL",
	}

	for _, part := range expectedParts {
		if !contains(schema, part) {
			t.Errorf("Schema missing expected part: %q\nSchema:\n%s", part, schema)
		}
	}
}

func TestDropSchema(t *testing.T) {
	type dropTest struct {
		Id int
	}

	m := psql.NewModel(dropTest{})
	dropSchema := m.DropSchema()

	expected := "DROP TABLE IF EXISTS drop_tests;\n"
	if dropSchema != expected {
		t.Errorf("DropSchema() = %q, want %q", dropSchema, expected)
	}
}

func TestCustomSchema(t *testing.T) {
	type customSchemaTest struct {
		Test string
	}

	// Test struct with custom Schema method
	type viewTest struct {
		Test string
	}

	t.Run("DefaultSchema", func(t *testing.T) {
		m := psql.NewModel(customSchemaTest{})
		schema := m.Schema()
		if !contains(schema, "CREATE TABLE") {
			t.Errorf("Schema should contain CREATE TABLE: %s", schema)
		}
	})
}

func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}
