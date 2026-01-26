package psql

import (
	"reflect"
	"testing"
)

// Test struct for DELETE tests
type deleteTestStruct struct {
	Id     int
	Name   string
	Status string
}

func TestDelete(t *testing.T) {
	t.Parallel()
	m := NewModel(deleteTestStruct{})

	tests := []struct {
		name    string
		build   func() *DeleteSQL
		wantSQL string
	}{
		{
			name:    "basic delete",
			build:   func() *DeleteSQL { return m.Delete() },
			wantSQL: "DELETE FROM delete_test_structs",
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

func TestDeleteReturning(t *testing.T) {
	t.Parallel()
	m := NewModel(deleteTestStruct{})

	tests := []struct {
		name    string
		build   func() *DeleteSQL
		wantSQL string
	}{
		{
			name:    "single column",
			build:   func() *DeleteSQL { return m.Delete().Returning("id") },
			wantSQL: "DELETE FROM delete_test_structs RETURNING id",
		},
		{
			name:    "multiple columns",
			build:   func() *DeleteSQL { return m.Delete().Returning("id", "name") },
			wantSQL: "DELETE FROM delete_test_structs RETURNING id, name",
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

func TestDeleteWhere(t *testing.T) {
	t.Parallel()
	m := NewModel(deleteTestStruct{})

	tests := []struct {
		name     string
		build    func() *DeleteSQL
		wantSQL  string
		wantArgs []interface{}
	}{
		{
			name:     "where before delete",
			build:    func() *DeleteSQL { return m.Where("id = $1", 1).Delete() },
			wantSQL:  "DELETE FROM delete_test_structs WHERE id = $1",
			wantArgs: []interface{}{1},
		},
		{
			name:     "where after delete",
			build:    func() *DeleteSQL { return m.Delete().Where("id = $1", 1) },
			wantSQL:  "DELETE FROM delete_test_structs WHERE id = $1",
			wantArgs: []interface{}{1},
		},
		{
			name:     "auto param",
			build:    func() *DeleteSQL { return m.Delete().Where("id = $?", 1) },
			wantSQL:  "DELETE FROM delete_test_structs WHERE id = $1",
			wantArgs: []interface{}{1},
		},
		{
			name:     "multiple where conditions",
			build:    func() *DeleteSQL { return m.Delete().Where("id = $?", 1).Where("name = $?", "test") },
			wantSQL:  "DELETE FROM delete_test_structs WHERE (id = $1) AND (name = $2)",
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

func TestDeleteWHERE(t *testing.T) {
	t.Parallel()
	m := NewModel(deleteTestStruct{})

	tests := []struct {
		name     string
		build    func() *DeleteSQL
		wantSQL  string
		wantArgs []interface{}
	}{
		{
			name:     "single field condition",
			build:    func() *DeleteSQL { return m.Delete().WHERE("Id", "=", 1) },
			wantSQL:  "DELETE FROM delete_test_structs WHERE id = $1",
			wantArgs: []interface{}{1},
		},
		{
			name:     "multiple field conditions",
			build:    func() *DeleteSQL { return m.Delete().WHERE("Id", "=", 1, "Name", "=", "test") },
			wantSQL:  "DELETE FROM delete_test_structs WHERE (id = $1) AND (name = $2)",
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

func TestDeleteUsing(t *testing.T) {
	t.Parallel()
	m := NewModel(deleteTestStruct{})

	tests := []struct {
		name     string
		build    func() *DeleteSQL
		wantSQL  string
		wantArgs []interface{}
	}{
		{
			name: "single using",
			build: func() *DeleteSQL {
				return m.Delete().Using("users").
					Where("delete_test_structs.user_id = users.id").
					Where("users.status = $1", "inactive")
			},
			wantSQL:  "DELETE FROM delete_test_structs USING users WHERE (delete_test_structs.user_id = users.id) AND (users.status = $1)",
			wantArgs: []interface{}{"inactive"},
		},
		{
			name: "multiple using",
			build: func() *DeleteSQL {
				return m.Delete().Using("users", "orders").
					Where("delete_test_structs.user_id = users.id").
					Where("delete_test_structs.order_id = orders.id").
					Where("orders.name = $1", "foobar").
					Returning("delete_test_structs.id")
			},
			wantSQL:  "DELETE FROM delete_test_structs USING users, orders WHERE (delete_test_structs.user_id = users.id) AND (delete_test_structs.order_id = orders.id) AND (orders.name = $1) RETURNING delete_test_structs.id",
			wantArgs: []interface{}{"foobar"},
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

func TestDeleteTap(t *testing.T) {
	t.Parallel()
	m := NewModel(deleteTestStruct{})

	sql := m.Delete().Tap(func(d *DeleteSQL) *DeleteSQL {
		return d.Where("id = $?", 1)
	})
	got := sql.String()
	want := "DELETE FROM delete_test_structs WHERE id = $1"
	if got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestDeleteAsDelete(t *testing.T) {
	t.Parallel()
	m := NewModel(deleteTestStruct{})

	sql := m.NewSQL("DELETE FROM custom_table WHERE status = $?", "old").AsDelete()
	got := sql.Returning("id").String()
	want := "DELETE FROM custom_table WHERE status = $1 RETURNING id"
	if got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestDeleteFromSelect(t *testing.T) {
	t.Parallel()
	m := NewModel(deleteTestStruct{})

	// Test converting a select with where to delete
	del := m.Where("id = $1", 1).Delete()
	got := del.String()
	want := "DELETE FROM delete_test_structs WHERE id = $1"
	if got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}
