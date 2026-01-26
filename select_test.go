package psql

import (
	"reflect"
	"testing"
)

// Test struct for SELECT tests
type selectTestStruct struct {
	Id        int
	Name      string
	Status    string
	CreatedAt string
}

func TestSelect(t *testing.T) {
	t.Parallel()
	m := NewModel(selectTestStruct{})

	tests := []struct {
		name    string
		build   func() *SelectSQL
		wantSQL string
	}{
		{
			name:    "single field",
			build:   func() *SelectSQL { return m.Select("id") },
			wantSQL: "SELECT id FROM select_test_structs",
		},
		{
			name:    "multiple fields",
			build:   func() *SelectSQL { return m.Select("id", "name") },
			wantSQL: "SELECT id, name FROM select_test_structs",
		},
		{
			name:    "with expression",
			build:   func() *SelectSQL { return m.Select("COUNT(*)") },
			wantSQL: "SELECT COUNT(*) FROM select_test_structs",
		},
		{
			name:    "chained select",
			build:   func() *SelectSQL { return m.Select("id").Select("name") },
			wantSQL: "SELECT id, name FROM select_test_structs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.build().String()
			if got != tt.wantSQL {
				t.Errorf("String() = %q, want %q", got, tt.wantSQL)
			}
		})
	}
}

func TestSelectFind(t *testing.T) {
	t.Parallel()
	m := NewModel(selectTestStruct{})

	tests := []struct {
		name    string
		build   func() *SelectSQL
		wantSQL string
	}{
		{
			name:    "find all fields",
			build:   func() *SelectSQL { return m.Find() },
			wantSQL: "SELECT id, name, status, created_at FROM select_test_structs",
		},
		{
			name:    "find with table name prefix",
			build:   func() *SelectSQL { return m.Find(AddTableName) },
			wantSQL: "SELECT select_test_structs.id, select_test_structs.name, select_test_structs.status, select_test_structs.created_at FROM select_test_structs",
		},
		{
			name:    "select then find resets",
			build:   func() *SelectSQL { return m.Select("name").Find() },
			wantSQL: "SELECT id, name, status, created_at FROM select_test_structs",
		},
		{
			name:    "select then find with no-reset",
			build:   func() *SelectSQL { return m.Select("name").Find("--no-reset") },
			wantSQL: "SELECT name, id, name, status, created_at FROM select_test_structs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.build().String()
			if got != tt.wantSQL {
				t.Errorf("String() = %q, want %q", got, tt.wantSQL)
			}
		})
	}
}

func TestSelectWhere(t *testing.T) {
	t.Parallel()
	m := NewModel(selectTestStruct{})

	tests := []struct {
		name     string
		build    func() *SelectSQL
		wantSQL  string
		wantArgs []interface{}
	}{
		{
			name:     "single condition with positional param",
			build:    func() *SelectSQL { return m.Select("id").Where("id = $1", 1) },
			wantSQL:  "SELECT id FROM select_test_structs WHERE id = $1",
			wantArgs: []interface{}{1},
		},
		{
			name:     "single condition with auto param",
			build:    func() *SelectSQL { return m.Select("id").Where("id = $?", 1) },
			wantSQL:  "SELECT id FROM select_test_structs WHERE id = $1",
			wantArgs: []interface{}{1},
		},
		{
			name:     "multiple conditions",
			build:    func() *SelectSQL { return m.Select("id").Where("id = $?", 1).Where("name = $?", "test") },
			wantSQL:  "SELECT id FROM select_test_structs WHERE (id = $1) AND (name = $2)",
			wantArgs: []interface{}{1, "test"},
		},
		{
			name:     "where before select",
			build:    func() *SelectSQL { return m.Where("id = $1", 1).Select("id", "name") },
			wantSQL:  "SELECT id, name FROM select_test_structs WHERE id = $1",
			wantArgs: []interface{}{1},
		},
		{
			name:     "multiple auto params in single condition",
			build:    func() *SelectSQL { return m.Select("id").Where("id = $? AND id = $?", 1) },
			wantSQL:  "SELECT id FROM select_test_structs WHERE id = $1 AND id = $1",
			wantArgs: []interface{}{1},
		},
		{
			name:     "multiple values for multiple params",
			build:    func() *SelectSQL { return m.Select("id").Where("id = $? AND id = $?", 1, 2) },
			wantSQL:  "SELECT id FROM select_test_structs WHERE id = $? AND id = $?",
			wantArgs: []interface{}{1, 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := tt.build()
			gotSQL, gotArgs := sql.StringValues()
			if gotSQL != tt.wantSQL {
				t.Errorf("SQL = %q, want %q", gotSQL, tt.wantSQL)
			}
			if !reflect.DeepEqual(gotArgs, tt.wantArgs) {
				t.Errorf("Args = %v, want %v", gotArgs, tt.wantArgs)
			}
		})
	}
}

func TestSelectWHERE(t *testing.T) {
	t.Parallel()
	m := NewModel(selectTestStruct{})

	tests := []struct {
		name     string
		build    func() *SelectSQL
		wantSQL  string
		wantArgs []interface{}
	}{
		{
			name:     "single field condition",
			build:    func() *SelectSQL { return m.Select("id").WHERE("Id", "=", 1) },
			wantSQL:  "SELECT id FROM select_test_structs WHERE id = $1",
			wantArgs: []interface{}{1},
		},
		{
			name:     "multiple field conditions",
			build:    func() *SelectSQL { return m.Select("id").WHERE("Id", "=", 1, "Name", "=", "test") },
			wantSQL:  "SELECT id FROM select_test_structs WHERE (id = $1) AND (name = $2)",
			wantArgs: []interface{}{1, "test"},
		},
		{
			name:     "combined with Where",
			build:    func() *SelectSQL { return m.Select("id").Where("id = $?", 1).WHERE("Name", "=", "a") },
			wantSQL:  "SELECT id FROM select_test_structs WHERE (id = $1) AND (name = $2)",
			wantArgs: []interface{}{1, "a"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := tt.build()
			gotSQL, gotArgs := sql.StringValues()
			if gotSQL != tt.wantSQL {
				t.Errorf("SQL = %q, want %q", gotSQL, tt.wantSQL)
			}
			if !reflect.DeepEqual(gotArgs, tt.wantArgs) {
				t.Errorf("Args = %v, want %v", gotArgs, tt.wantArgs)
			}
		})
	}
}

func TestSelectOrderBy(t *testing.T) {
	t.Parallel()
	m := NewModel(selectTestStruct{})

	tests := []struct {
		name    string
		build   func() *SelectSQL
		wantSQL string
	}{
		{
			name:    "single column",
			build:   func() *SelectSQL { return m.Select("id").OrderBy("id DESC") },
			wantSQL: "SELECT id FROM select_test_structs ORDER BY id DESC",
		},
		{
			name:    "multiple columns",
			build:   func() *SelectSQL { return m.Select("id").OrderBy("name ASC", "id DESC") },
			wantSQL: "SELECT id FROM select_test_structs ORDER BY name ASC, id DESC",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.build().String()
			if got != tt.wantSQL {
				t.Errorf("String() = %q, want %q", got, tt.wantSQL)
			}
		})
	}
}

func TestSelectLimit(t *testing.T) {
	t.Parallel()
	m := NewModel(selectTestStruct{})

	tests := []struct {
		name    string
		build   func() *SelectSQL
		wantSQL string
	}{
		{
			name:    "with int",
			build:   func() *SelectSQL { return m.Select("id").Limit(10) },
			wantSQL: "SELECT id FROM select_test_structs LIMIT 10",
		},
		{
			name:    "with string",
			build:   func() *SelectSQL { return m.Select("id").Limit("5") },
			wantSQL: "SELECT id FROM select_test_structs LIMIT 5",
		},
		{
			name:    "with nil removes limit",
			build:   func() *SelectSQL { return m.Select("id").Limit(10).Limit(nil) },
			wantSQL: "SELECT id FROM select_test_structs",
		},
		{
			name:    "with order by",
			build:   func() *SelectSQL { return m.Select("id").OrderBy("id DESC").Limit(2) },
			wantSQL: "SELECT id FROM select_test_structs ORDER BY id DESC LIMIT 2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.build().String()
			if got != tt.wantSQL {
				t.Errorf("String() = %q, want %q", got, tt.wantSQL)
			}
		})
	}
}

func TestSelectOffset(t *testing.T) {
	t.Parallel()
	m := NewModel(selectTestStruct{})

	tests := []struct {
		name    string
		build   func() *SelectSQL
		wantSQL string
	}{
		{
			name:    "with int",
			build:   func() *SelectSQL { return m.Select("id").Offset(10) },
			wantSQL: "SELECT id FROM select_test_structs OFFSET 10",
		},
		{
			name:    "with string",
			build:   func() *SelectSQL { return m.Select("id").Offset("10") },
			wantSQL: "SELECT id FROM select_test_structs OFFSET 10",
		},
		{
			name:    "with limit",
			build:   func() *SelectSQL { return m.Select("id").Offset("10").Limit(2) },
			wantSQL: "SELECT id FROM select_test_structs LIMIT 2 OFFSET 10",
		},
		{
			name:    "with nil removes offset",
			build:   func() *SelectSQL { return m.Select("id").Offset(10).Offset(nil) },
			wantSQL: "SELECT id FROM select_test_structs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.build().String()
			if got != tt.wantSQL {
				t.Errorf("String() = %q, want %q", got, tt.wantSQL)
			}
		})
	}
}

func TestSelectGroupBy(t *testing.T) {
	t.Parallel()
	m := NewModel(selectTestStruct{})

	tests := []struct {
		name    string
		build   func() *SelectSQL
		wantSQL string
	}{
		{
			name:    "single column",
			build:   func() *SelectSQL { return m.Select("array_agg(id)").GroupBy("name") },
			wantSQL: "SELECT array_agg(id) FROM select_test_structs GROUP BY name",
		},
		{
			name:    "multiple columns",
			build:   func() *SelectSQL { return m.Select("array_agg(id)").GroupBy("name", "status") },
			wantSQL: "SELECT array_agg(id) FROM select_test_structs GROUP BY name, status",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.build().String()
			if got != tt.wantSQL {
				t.Errorf("String() = %q, want %q", got, tt.wantSQL)
			}
		})
	}
}

func TestSelectHaving(t *testing.T) {
	t.Parallel()
	m := NewModel(selectTestStruct{})

	tests := []struct {
		name     string
		build    func() *SelectSQL
		wantSQL  string
		wantArgs []interface{}
	}{
		{
			name: "with group by",
			build: func() *SelectSQL {
				return m.Select("sum(id)").Where("id > $1", 1).GroupBy("status").Having("sum(id) < $?", 100)
			},
			wantSQL:  "SELECT sum(id) FROM select_test_structs WHERE id > $1 GROUP BY status HAVING sum(id) < $2",
			wantArgs: []interface{}{1, 100},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := tt.build()
			gotSQL, gotArgs := sql.StringValues()
			if gotSQL != tt.wantSQL {
				t.Errorf("SQL = %q, want %q", gotSQL, tt.wantSQL)
			}
			if !reflect.DeepEqual(gotArgs, tt.wantArgs) {
				t.Errorf("Args = %v, want %v", gotArgs, tt.wantArgs)
			}
		})
	}
}

func TestSelectJoin(t *testing.T) {
	t.Parallel()
	m := NewModel(selectTestStruct{})

	tests := []struct {
		name    string
		build   func() *SelectSQL
		wantSQL string
	}{
		{
			name:    "single join",
			build:   func() *SelectSQL { return m.Select("id").Join("JOIN a ON a.id = select_test_structs.id") },
			wantSQL: "SELECT id FROM select_test_structs JOIN a ON a.id = select_test_structs.id",
		},
		{
			name: "multiple joins",
			build: func() *SelectSQL {
				return m.Select("id").
					Join("JOIN a ON a.id = select_test_structs.id").
					Join("JOIN b ON b.id = select_test_structs.id")
			},
			wantSQL: "SELECT id FROM select_test_structs JOIN a ON a.id = select_test_structs.id JOIN b ON b.id = select_test_structs.id",
		},
		{
			name: "reset join",
			build: func() *SelectSQL {
				return m.Select("id").
					Join("JOIN a ON a.id = select_test_structs.id").
					ResetJoin("JOIN b ON b.id = select_test_structs.id")
			},
			wantSQL: "SELECT id FROM select_test_structs JOIN b ON b.id = select_test_structs.id",
		},
		{
			name:    "join from model",
			build:   func() *SelectSQL { return m.Join("JOIN other ON other.id = select_test_structs.id").Select("id") },
			wantSQL: "SELECT id FROM select_test_structs JOIN other ON other.id = select_test_structs.id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.build().String()
			if got != tt.wantSQL {
				t.Errorf("String() = %q, want %q", got, tt.wantSQL)
			}
		})
	}
}

func TestSelectFrom(t *testing.T) {
	t.Parallel()
	m := NewModel(selectTestStruct{})

	tests := []struct {
		name    string
		build   func() *SelectSQL
		wantSQL string
	}{
		{
			name:    "additional from",
			build:   func() *SelectSQL { return m.Select("id").From("other_table") },
			wantSQL: "SELECT id FROM select_test_structs, other_table",
		},
		{
			name:    "reset from",
			build:   func() *SelectSQL { return m.Select("id").ResetFrom("other_table") },
			wantSQL: "SELECT id FROM other_table",
		},
		{
			name:    "from on model",
			build:   func() *SelectSQL { return m.From("other_table").Select("id") },
			wantSQL: "SELECT id FROM select_test_structs, other_table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.build().String()
			if got != tt.wantSQL {
				t.Errorf("String() = %q, want %q", got, tt.wantSQL)
			}
		})
	}
}

func TestSelectWith(t *testing.T) {
	t.Parallel()
	m := NewModel(selectTestStruct{})

	tests := []struct {
		name     string
		build    func() *SelectSQL
		wantSQL  string
		wantArgs []interface{}
	}{
		{
			name: "simple CTE",
			build: func() *SelectSQL {
				return m.With("RECURSIVE a(n) AS (VALUES (1) UNION ALL SELECT n+1 FROM a WHERE n < 3)").
					Select("a.n").
					ResetFrom("a")
			},
			wantSQL:  "WITH RECURSIVE a(n) AS (VALUES (1) UNION ALL SELECT n+1 FROM a WHERE n < 3) SELECT a.n FROM a",
			wantArgs: nil,
		},
		{
			name: "CTE from subquery",
			build: func() *SelectSQL {
				subQuery := m.Select("id").Where("id = $?", 1)
				return m.WITH("a2", subQuery).Where("name = $?", "new").Select("id")
			},
			wantSQL:  "WITH a2 AS (SELECT id FROM select_test_structs WHERE id = $1) SELECT id FROM select_test_structs WHERE name = $2",
			wantArgs: []interface{}{1, "new"},
		},
		{
			name: "CTE with MATERIALIZED",
			build: func() *SelectSQL {
				subQuery := m.Select("id").Where("id = $?", 1)
				return m.WITH("a2 as materialized", subQuery).Where("name = $?", "new").Select("id")
			},
			wantSQL:  "WITH a2 as materialized (SELECT id FROM select_test_structs WHERE id = $1) SELECT id FROM select_test_structs WHERE name = $2",
			wantArgs: []interface{}{1, "new"},
		},
		{
			name: "multiple CTEs",
			build: func() *SelectSQL {
				return m.With("RECURSIVE a(n) AS (VALUES (1) UNION ALL SELECT n+1 FROM a WHERE n < 3)").
					With("b(n) AS (VALUES (10) UNION ALL SELECT n+1 FROM b WHERE n < 12)").
					Select("a.n AS a_value, b.n AS b_value, a.n + b.n AS total").
					ResetFrom("a").From("b")
			},
			wantSQL:  "WITH RECURSIVE a(n) AS (VALUES (1) UNION ALL SELECT n+1 FROM a WHERE n < 3), b(n) AS (VALUES (10) UNION ALL SELECT n+1 FROM b WHERE n < 12) SELECT a.n AS a_value, b.n AS b_value, a.n + b.n AS total FROM a, b",
			wantArgs: nil,
		},
		{
			name: "from model WITH",
			build: func() *SelectSQL {
				subQuery := m.Select("id").Where("id = $?", 1)
				return m.WITH("cte", subQuery).Select("*")
			},
			wantSQL:  "WITH cte AS (SELECT id FROM select_test_structs WHERE id = $1) SELECT * FROM select_test_structs",
			wantArgs: []interface{}{1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := tt.build()
			gotSQL, gotArgs := sql.StringValues()
			if gotSQL != tt.wantSQL {
				t.Errorf("SQL = %q, want %q", gotSQL, tt.wantSQL)
			}
			if tt.wantArgs == nil && gotArgs != nil && len(gotArgs) > 0 {
				t.Errorf("Args = %v, want nil or empty", gotArgs)
			} else if tt.wantArgs != nil && !reflect.DeepEqual(gotArgs, tt.wantArgs) {
				t.Errorf("Args = %v, want %v", gotArgs, tt.wantArgs)
			}
		})
	}
}

func TestSelectResetSelect(t *testing.T) {
	t.Parallel()
	m := NewModel(selectTestStruct{})

	sql := m.Select("id", "name").ResetSelect("status")
	got := sql.String()
	want := "SELECT status FROM select_test_structs"
	if got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestSelectReplaceSelect(t *testing.T) {
	t.Parallel()
	m := NewModel(selectTestStruct{})

	sql := m.Select("id", "name").ReplaceSelect("name", "UPPER(name) AS name")
	got := sql.String()
	want := "SELECT id, UPPER(name) AS name FROM select_test_structs"
	if got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestSelectTap(t *testing.T) {
	t.Parallel()
	m := NewModel(selectTestStruct{})

	sql := m.Select("id").Tap(func(s *SelectSQL) *SelectSQL {
		return s.Where("id = $?", 1)
	})
	got := sql.String()
	want := "SELECT id FROM select_test_structs WHERE id = $1"
	if got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestSelectToDeleteAndUpdate(t *testing.T) {
	t.Parallel()
	m := NewModel(selectTestStruct{})

	t.Run("to delete", func(t *testing.T) {
		del := m.Where("id = $1", 1).Delete()
		got := del.String()
		want := "DELETE FROM select_test_structs WHERE id = $1"
		if got != want {
			t.Errorf("String() = %q, want %q", got, want)
		}
	})

	t.Run("to update", func(t *testing.T) {
		upd := m.Where("id = $1", 1).Update("Name", "test")
		got := upd.String()
		want := "UPDATE select_test_structs SET name = $2 WHERE id = $1"
		if got != want {
			t.Errorf("String() = %q, want %q", got, want)
		}
	})
}

func TestSelectAsSelect(t *testing.T) {
	t.Parallel()
	m := NewModel(selectTestStruct{})

	sql := m.NewSQL("SELECT id FROM custom_table WHERE status = $?", "active").AsSelect()
	got := sql.Offset(10).String()
	want := "SELECT id FROM custom_table WHERE status = $1 OFFSET 10"
	if got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}
