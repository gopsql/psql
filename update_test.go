package psql

import (
	"reflect"
	"testing"
)

// Test struct for UPDATE tests
type updateTestStruct struct {
	Id    int
	Name  string
	Email string
}

// Test struct with JSONB for UPDATE tests
type updateJsonbStruct struct {
	Id      int
	Picture string `jsonb:"meta"`
	Tags    string `jsonb:"meta"`
}

func TestUpdate(t *testing.T) {
	t.Parallel()
	m := NewModel(updateTestStruct{})

	tests := []struct {
		name     string
		build    func() *UpdateSQL
		wantSQL  string
		wantArgs []interface{}
	}{
		{
			name: "single field using field name",
			build: func() *UpdateSQL {
				return m.Update("Name", "test")
			},
			wantSQL:  "UPDATE update_test_structs SET name = $1",
			wantArgs: []interface{}{"test"},
		},
		{
			name: "multiple fields using field names",
			build: func() *UpdateSQL {
				return m.Update("Name", "test", "Email", "test@example.com")
			},
			wantSQL:  "UPDATE update_test_structs SET name = $1, email = $2",
			wantArgs: []interface{}{"test", "test@example.com"},
		},
		{
			name: "using Changes",
			build: func() *UpdateSQL {
				changes := m.Changes(RawChanges{"Name": "test"})
				return m.Update(changes)
			},
			wantSQL:  "UPDATE update_test_structs SET name = $1",
			wantArgs: []interface{}{"test"},
		},
		{
			name: "using FieldChanges",
			build: func() *UpdateSQL {
				changes := m.FieldChanges(RawChanges{"Name": "test"})
				return m.Update(changes)
			},
			wantSQL:  "UPDATE update_test_structs SET name = $1",
			wantArgs: []interface{}{"test"},
		},
		{
			name: "duplicate field uses last value",
			build: func() *UpdateSQL {
				return m.Update("Name", "first", "Name", "second")
			},
			wantSQL:  "UPDATE update_test_structs SET name = $1",
			wantArgs: []interface{}{"second"},
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

func TestUpdateReturning(t *testing.T) {
	t.Parallel()
	m := NewModel(updateTestStruct{})

	tests := []struct {
		name    string
		build   func() *UpdateSQL
		wantSQL string
	}{
		{
			name: "single column",
			build: func() *UpdateSQL {
				return m.Update("Name", "test").Returning("id")
			},
			wantSQL: "UPDATE update_test_structs SET name = $1 RETURNING id",
		},
		{
			name: "multiple columns",
			build: func() *UpdateSQL {
				return m.Update("Name", "test").Returning("id", "name")
			},
			wantSQL: "UPDATE update_test_structs SET name = $1 RETURNING id, name",
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

func TestUpdateWhere(t *testing.T) {
	t.Parallel()
	m := NewModel(updateTestStruct{})

	tests := []struct {
		name     string
		build    func() *UpdateSQL
		wantSQL  string
		wantArgs []interface{}
	}{
		{
			name: "where before update",
			build: func() *UpdateSQL {
				return m.Where("id = $1", 1).Update("Name", "test")
			},
			wantSQL:  "UPDATE update_test_structs SET name = $2 WHERE id = $1",
			wantArgs: []interface{}{1, "test"},
		},
		{
			name: "where after update",
			build: func() *UpdateSQL {
				return m.Update("Name", "test").Where("id = $1", 1)
			},
			wantSQL:  "UPDATE update_test_structs SET name = $2 WHERE id = $1",
			wantArgs: []interface{}{1, "test"},
		},
		{
			name: "multiple where conditions",
			build: func() *UpdateSQL {
				return m.Update("Name", "test").Where("name = $?", "foo").Where("id = $?", 1)
			},
			wantSQL:  "UPDATE update_test_structs SET name = $3 WHERE (name = $1) AND (id = $2)",
			wantArgs: []interface{}{"foo", 1, "test"},
		},
		{
			name: "auto param",
			build: func() *UpdateSQL {
				return m.Update("Name", "test").Where("id = $?", 1)
			},
			wantSQL:  "UPDATE update_test_structs SET name = $2 WHERE id = $1",
			wantArgs: []interface{}{1, "test"},
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

func TestUpdateWHERE(t *testing.T) {
	t.Parallel()
	m := NewModel(updateTestStruct{})

	tests := []struct {
		name     string
		build    func() *UpdateSQL
		wantSQL  string
		wantArgs []interface{}
	}{
		{
			name: "single field condition",
			build: func() *UpdateSQL {
				return m.Update("Name", "test").WHERE("Id", "=", 1)
			},
			wantSQL:  "UPDATE update_test_structs SET name = $2 WHERE id = $1",
			wantArgs: []interface{}{1, "test"},
		},
		{
			name: "multiple field conditions",
			build: func() *UpdateSQL {
				return m.Update("Name", "test").WHERE("Id", "=", 1, "Email", "!=", "old@example.com")
			},
			wantSQL:  "UPDATE update_test_structs SET name = $3 WHERE (id = $1) AND (email != $2)",
			wantArgs: []interface{}{1, "old@example.com", "test"},
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

func TestUpdateWithString(t *testing.T) {
	t.Parallel()
	m := NewModel(updateTestStruct{})

	tests := []struct {
		name     string
		build    func() *UpdateSQL
		wantSQL  string
		wantArgs []interface{}
	}{
		{
			name: "raw SQL expression",
			build: func() *UpdateSQL {
				return m.Update("Name", String("CONCAT(name, '1')"))
			},
			wantSQL:  "UPDATE update_test_structs SET name = CONCAT(name, '1')",
			wantArgs: nil,
		},
		{
			name: "with arg",
			build: func() *UpdateSQL {
				return m.Update("Name", StringWithArg("CONCAT(name, $?)", "suffix"))
			},
			wantSQL:  "UPDATE update_test_structs SET name = CONCAT(name, $1)",
			wantArgs: []interface{}{"suffix"},
		},
		{
			name: "duplicate with StringWithArg",
			build: func() *UpdateSQL {
				return m.Update(
					"Name", StringWithArg("name || $?", "a"),
					"Name", StringWithArg("name || $?", "b"),
				)
			},
			wantSQL:  "UPDATE update_test_structs SET name = name || $1",
			wantArgs: []interface{}{"b"},
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

func TestUpdateWithJsonb(t *testing.T) {
	t.Parallel()
	m := NewModel(updateJsonbStruct{})

	tests := []struct {
		name     string
		build    func() *UpdateSQL
		wantSQL  string
		wantArgs []interface{}
	}{
		{
			name: "single jsonb field",
			build: func() *UpdateSQL {
				changes := m.Changes(RawChanges{"Picture": "test.jpg"})
				return m.Update(changes)
			},
			wantSQL:  "UPDATE update_jsonb_structs SET meta = jsonb_set(COALESCE(meta, '{}'::jsonb), '{picture}', $1)",
			wantArgs: []interface{}{`"test.jpg"`},
		},
		{
			name: "jsonb field with raw expression",
			build: func() *UpdateSQL {
				return m.Update("Picture", String("to_jsonb(UPPER(COALESCE(meta->>'picture', '')))"))
			},
			wantSQL:  "UPDATE update_jsonb_structs SET meta = jsonb_set(COALESCE(meta, '{}'::jsonb), '{picture}', to_jsonb(UPPER(COALESCE(meta->>'picture', ''))))",
			wantArgs: nil,
		},
		{
			name: "jsonb field with where",
			build: func() *UpdateSQL {
				changes := m.Changes(RawChanges{"Picture": "test.jpg"})
				return m.Update(changes).Where("id = $1", 1)
			},
			wantSQL:  "UPDATE update_jsonb_structs SET meta = jsonb_set(COALESCE(meta, '{}'::jsonb), '{picture}', $2) WHERE id = $1",
			wantArgs: []interface{}{1, `"test.jpg"`},
		},
		{
			name: "jsonb with StringWithArg",
			build: func() *UpdateSQL {
				return m.Update("Picture", StringWithArg(
					`to_jsonb(concat_ws(E'\n', NULLIF(meta->>'picture', ''), $?::text))`,
					"foo",
				))
			},
			wantSQL:  "UPDATE update_jsonb_structs SET meta = jsonb_set(COALESCE(meta, '{}'::jsonb), '{picture}', to_jsonb(concat_ws(E'\\n', NULLIF(meta->>'picture', ''), $1::text)))",
			wantArgs: []interface{}{"foo"},
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

func TestUpdateTap(t *testing.T) {
	t.Parallel()
	m := NewModel(updateTestStruct{})

	sql := m.Update("Name", "test").Tap(func(u *UpdateSQL) *UpdateSQL {
		return u.Where("id = $?", 1)
	})
	got := sql.String()
	want := "UPDATE update_test_structs SET name = $2 WHERE id = $1"
	if got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestUpdateAsUpdate(t *testing.T) {
	t.Parallel()
	m := NewModel(updateTestStruct{})

	sql := m.NewSQL("UPDATE custom_table SET status = $?", "new").AsUpdate()
	gotSQL, gotArgs := sql.Where("status = $?", "old").Returning("id").StringValues()
	wantSQL := "UPDATE custom_table SET status = $2 WHERE status = $1 RETURNING id"
	wantArgs := []interface{}{"old", "new"}

	if gotSQL != wantSQL {
		t.Errorf("SQL = %q, want %q", gotSQL, wantSQL)
	}
	if !reflect.DeepEqual(gotArgs, wantArgs) {
		t.Errorf("Args = %v, want %v", gotArgs, wantArgs)
	}
}

func TestUpdateEmpty(t *testing.T) {
	t.Parallel()
	m := NewModel(updateTestStruct{})

	// Update with no changes should return empty SQL string
	sql := m.Update()
	sqlStr, args := sql.StringValues()
	if sqlStr != "" {
		t.Errorf("SQL = %q, want empty string", sqlStr)
	}
	if len(args) != 0 {
		t.Errorf("Args = %v, want empty", args)
	}
}

func TestUpdateMixedJsonbAndRegularFields(t *testing.T) {
	t.Parallel()

	type mixedStruct struct {
		Id        int
		Name      string
		CreatedAt string
		Picture   string `jsonb:"meta"`
	}

	m := NewModel(mixedStruct{})

	sql := m.Update(
		"Id", String("id + 1"),
		"Picture", String("'null'::jsonb"),
		"CreatedAt", String("now()"),
	)
	got := sql.String()
	want := "UPDATE mixed_structs SET id = id + 1, created_at = now(), meta = jsonb_set(COALESCE(meta, '{}'::jsonb), '{picture}', 'null'::jsonb)"
	if got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}
