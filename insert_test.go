package psql

import (
	"reflect"
	"testing"
)

// Test struct for INSERT tests
type insertTestStruct struct {
	Id    int
	Name  string
	Email string
}

// Test struct with JSONB for INSERT tests
type insertJsonbStruct struct {
	Id      int
	Picture string `jsonb:"meta"`
	Tags    string `jsonb:"meta"`
}

func TestInsert(t *testing.T) {
	t.Parallel()
	m := NewModel(insertTestStruct{})

	tests := []struct {
		name     string
		build    func() *InsertSQL
		wantSQL  string
		wantArgs []interface{}
	}{
		{
			name: "single field using field name",
			build: func() *InsertSQL {
				return m.Insert("Name", "test")
			},
			wantSQL:  "INSERT INTO insert_test_structs (name) VALUES ($1)",
			wantArgs: []interface{}{"test"},
		},
		{
			name: "multiple fields using field names",
			build: func() *InsertSQL {
				return m.Insert("Name", "test", "Email", "test@example.com")
			},
			wantSQL:  "INSERT INTO insert_test_structs (name, email) VALUES ($1, $2)",
			wantArgs: []interface{}{"test", "test@example.com"},
		},
		{
			name: "using Changes",
			build: func() *InsertSQL {
				changes := m.Changes(RawChanges{"Name": "test"})
				return m.Insert(changes)
			},
			wantSQL:  "INSERT INTO insert_test_structs (name) VALUES ($1)",
			wantArgs: []interface{}{"test"},
		},
		{
			name: "using FieldChanges",
			build: func() *InsertSQL {
				changes := m.FieldChanges(RawChanges{"Name": "test"})
				return m.Insert(changes)
			},
			wantSQL:  "INSERT INTO insert_test_structs (name) VALUES ($1)",
			wantArgs: []interface{}{"test"},
		},
		{
			name: "mixed changes",
			build: func() *InsertSQL {
				changes := m.Changes(RawChanges{"Name": "test"})
				return m.Insert(changes, "Email", "test@example.com")
			},
			wantSQL:  "INSERT INTO insert_test_structs (name, email) VALUES ($1, $2)",
			wantArgs: []interface{}{"test", "test@example.com"},
		},
		{
			name: "duplicate field uses last value",
			build: func() *InsertSQL {
				return m.Insert("Name", "first", "Name", "second")
			},
			wantSQL:  "INSERT INTO insert_test_structs (name) VALUES ($1)",
			wantArgs: []interface{}{"second"},
		},
		{
			name: "invalid field is ignored",
			build: func() *InsertSQL {
				return m.Insert("InvalidField", "test", "Name", "valid")
			},
			wantSQL:  "INSERT INTO insert_test_structs (name) VALUES ($1)",
			wantArgs: []interface{}{"valid"},
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

func TestInsertReturning(t *testing.T) {
	t.Parallel()
	m := NewModel(insertTestStruct{})

	tests := []struct {
		name    string
		build   func() *InsertSQL
		wantSQL string
	}{
		{
			name: "single column",
			build: func() *InsertSQL {
				return m.Insert("Name", "test").Returning("id")
			},
			wantSQL: "INSERT INTO insert_test_structs (name) VALUES ($1) RETURNING id",
		},
		{
			name: "multiple columns",
			build: func() *InsertSQL {
				return m.Insert("Name", "test").Returning("id", "name")
			},
			wantSQL: "INSERT INTO insert_test_structs (name) VALUES ($1) RETURNING id, name",
		},
		{
			name: "with alias",
			build: func() *InsertSQL {
				return m.Insert("Name", "test").Returning("id AS foobar", "name")
			},
			wantSQL: "INSERT INTO insert_test_structs (name) VALUES ($1) RETURNING id AS foobar, name",
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

func TestInsertOnConflict(t *testing.T) {
	t.Parallel()
	m := NewModel(insertTestStruct{})

	tests := []struct {
		name    string
		build   func() *InsertSQL
		wantSQL string
	}{
		{
			name: "do nothing without target",
			build: func() *InsertSQL {
				return m.Insert("Name", "test").OnConflict().DoNothing()
			},
			wantSQL: "INSERT INTO insert_test_structs (name) VALUES ($1) ON CONFLICT DO NOTHING",
		},
		{
			name: "do nothing with single target",
			build: func() *InsertSQL {
				return m.Insert("Name", "test").OnConflict("name").DoNothing()
			},
			wantSQL: "INSERT INTO insert_test_structs (name) VALUES ($1) ON CONFLICT (name) DO NOTHING",
		},
		{
			name: "do nothing with multiple targets",
			build: func() *InsertSQL {
				return m.Insert("Name", "test").OnConflict("name", "email").DoNothing()
			},
			wantSQL: "INSERT INTO insert_test_structs (name) VALUES ($1) ON CONFLICT (name, email) DO NOTHING",
		},
		{
			name: "do nothing with expression target",
			build: func() *InsertSQL {
				return m.Insert("Name", "test").OnConflict("lower(name)").DoNothing()
			},
			wantSQL: "INSERT INTO insert_test_structs (name) VALUES ($1) ON CONFLICT (lower(name)) DO NOTHING",
		},
		{
			name: "do nothing with partial index",
			build: func() *InsertSQL {
				return m.Insert("Name", "test").OnConflict("(name) WHERE TRUE").DoNothing()
			},
			wantSQL: "INSERT INTO insert_test_structs (name) VALUES ($1) ON CONFLICT (name) WHERE TRUE DO NOTHING",
		},
		{
			name: "do nothing reversed order",
			build: func() *InsertSQL {
				return m.Insert("Name", "test").DoNothing().OnConflict()
			},
			wantSQL: "INSERT INTO insert_test_structs (name) VALUES ($1) ON CONFLICT DO NOTHING",
		},
		{
			name: "only OnConflict without action",
			build: func() *InsertSQL {
				return m.Insert("Name", "test").OnConflict()
			},
			wantSQL: "INSERT INTO insert_test_structs (name) VALUES ($1)",
		},
		{
			name: "only DoNothing without OnConflict",
			build: func() *InsertSQL {
				return m.Insert("Name", "test").DoNothing()
			},
			wantSQL: "INSERT INTO insert_test_structs (name) VALUES ($1)",
		},
		{
			name: "with returning",
			build: func() *InsertSQL {
				return m.Insert("Name", "test").Returning("id").OnConflict().DoNothing()
			},
			wantSQL: "INSERT INTO insert_test_structs (name) VALUES ($1) ON CONFLICT DO NOTHING RETURNING id",
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

func TestInsertDoUpdate(t *testing.T) {
	t.Parallel()
	m := NewModel(insertTestStruct{})

	tests := []struct {
		name    string
		build   func() *InsertSQL
		wantSQL string
	}{
		{
			name: "do update with expression",
			build: func() *InsertSQL {
				return m.Insert("Name", "test").OnConflict("name").DoUpdate("email = NULL")
			},
			wantSQL: "INSERT INTO insert_test_structs (name) VALUES ($1) ON CONFLICT (name) DO UPDATE SET email = NULL",
		},
		{
			name: "do update all",
			build: func() *InsertSQL {
				return m.Insert("Name", "test").OnConflict("name").DoUpdateAll()
			},
			wantSQL: "INSERT INTO insert_test_structs (name) VALUES ($1) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name",
		},
		{
			name: "do update all with additional expression",
			build: func() *InsertSQL {
				return m.Insert("Name", "test").OnConflict("name").DoUpdateAll().DoUpdate("email = NULL")
			},
			wantSQL: "INSERT INTO insert_test_structs (name) VALUES ($1) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name, email = NULL",
		},
		{
			name: "do update all except",
			build: func() *InsertSQL {
				return m.Insert("Name", "test", "Email", "test@example.com").OnConflict("name").DoUpdateAllExcept("name")
			},
			wantSQL: "INSERT INTO insert_test_structs (name, email) VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET email = EXCLUDED.email",
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

func TestInsertWithJsonb(t *testing.T) {
	t.Parallel()
	m := NewModel(insertJsonbStruct{})

	tests := []struct {
		name     string
		build    func() *InsertSQL
		wantSQL  string
		wantArgs []interface{}
	}{
		{
			name: "single jsonb field",
			build: func() *InsertSQL {
				changes := m.Changes(RawChanges{"Picture": "test.jpg"})
				return m.Insert(changes)
			},
			wantSQL:  "INSERT INTO insert_jsonb_structs (meta) VALUES ($1)",
			wantArgs: []interface{}{`{"picture":"test.jpg"}`},
		},
		{
			name: "multiple jsonb fields in same column",
			build: func() *InsertSQL {
				changes := m.Changes(RawChanges{"Picture": "test.jpg", "Tags": "a,b,c"})
				return m.Insert(changes)
			},
			wantSQL: "INSERT INTO insert_jsonb_structs (meta) VALUES ($1)",
			// Note: the order may vary, so we just check that SQL is correct
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := tt.build()
			gotSQL, gotArgs := sql.StringValues()
			if gotSQL != tt.wantSQL {
				t.Errorf("SQL = %q, want %q", gotSQL, tt.wantSQL)
			}
			if tt.wantArgs != nil && len(gotArgs) != len(tt.wantArgs) {
				t.Errorf("Args count = %d, want %d", len(gotArgs), len(tt.wantArgs))
			}
		})
	}
}

func TestInsertTap(t *testing.T) {
	t.Parallel()
	m := NewModel(insertTestStruct{})

	sql := m.Insert("Name", "test").Tap(func(i *InsertSQL) *InsertSQL {
		return i.Returning("id")
	})
	got := sql.String()
	want := "INSERT INTO insert_test_structs (name) VALUES ($1) RETURNING id"
	if got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestInsertAsInsert(t *testing.T) {
	t.Parallel()
	m := NewModel(insertTestStruct{})

	sql := m.NewSQL("INSERT INTO custom_table (name) VALUES ($?)", "test").AsInsert()
	got := sql.OnConflict().DoNothing().String()
	want := "INSERT INTO custom_table (name) VALUES ($1) ON CONFLICT DO NOTHING"
	if got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestInsertEmpty(t *testing.T) {
	t.Parallel()
	m := NewModel(insertTestStruct{})

	// Insert with no changes should return empty SQL string
	sql := m.Insert()
	sqlStr, args := sql.StringValues()
	if sqlStr != "" {
		t.Errorf("SQL = %q, want empty string", sqlStr)
	}
	if len(args) != 0 {
		t.Errorf("Args = %v, want empty", args)
	}
}
