package psql

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func init() {
	DefaultColumnNamer = ToUnderscore
	DefaultTableNamer = ToPluralUnderscore
}

// Test structs for Model tests
type (
	basicUser struct {
		Id       int
		Name     string
		Password string
	}

	admin struct {
		basicUser
	}

	user struct {
		basicUser
		Phone string
	}

	category struct {
		Id        int
		Names     []map[string]string `jsonb:"meta"`
		Picture   string              `jsonb:"meta"`
		CreatedAt time.Time
		UpdatedAt time.Time
	}

	product struct {
		Id    int    `json:"id"`
		Name  string `json:"name"`
		Price int    `json:"PRICE"`
	}

	dataTypeTestStruct struct {
		Test0 string
		Test1 string
		Test2 string `dataType:"test"`
		Test3 string `dataType:"hello"`
	}

	schemaTestStruct0 struct {
		Test0 string
	}

	schemaTestStruct1 struct {
		Test0 string
	}

	schemaTestStruct2 struct {
		Test0 string
	}
)

func (dataTypeTestStruct) DataType(m Model, fieldName string) string {
	if fieldName == "Test1" {
		return "foo"
	}
	if fieldName == "Test3" {
		return "world"
	}
	return ""
}

func (schemaTestStruct1) Schema() string {
	return `CREATE VIEW schemaTestStruct1s AS SELECT 'yes' AS test0;`
}

func (schemaTestStruct2) BeforeCreateSchema() string {
	return `-- comment b`
}

func (schemaTestStruct2) AfterCreateSchema() string {
	return `-- comment a`
}

func TestNewModel(t *testing.T) {
	t.Parallel()

	t.Run("creates model from struct", func(t *testing.T) {
		m := NewModel(admin{})
		if m.tableName != "admins" {
			t.Errorf("tableName = %q, want %q", m.tableName, "admins")
		}
		if len(m.modelFields) != 3 {
			t.Errorf("len(modelFields) = %d, want %d", len(m.modelFields), 3)
		}
	})

	t.Run("inherits fields from embedded struct", func(t *testing.T) {
		m := NewModel(user{})
		if m.tableName != "users" {
			t.Errorf("tableName = %q, want %q", m.tableName, "users")
		}
		if len(m.modelFields) != 4 {
			t.Errorf("len(modelFields) = %d, want %d", len(m.modelFields), 4)
		}
	})

	t.Run("handles json tags", func(t *testing.T) {
		m := NewModel(product{})
		if m.tableName != "products" {
			t.Errorf("tableName = %q, want %q", m.tableName, "products")
		}
		if len(m.modelFields) != 3 {
			t.Errorf("len(modelFields) = %d, want %d", len(m.modelFields), 3)
		}
	})

	t.Run("handles jsonb fields", func(t *testing.T) {
		m := NewModel(category{})
		if len(m.jsonbColumns) != 1 {
			t.Errorf("len(jsonbColumns) = %d, want %d", len(m.jsonbColumns), 1)
		}
		if m.jsonbColumns[0] != "meta" {
			t.Errorf("jsonbColumns[0] = %q, want %q", m.jsonbColumns[0], "meta")
		}
	})
}

func TestNewModelTable(t *testing.T) {
	t.Parallel()

	m := NewModelTable("custom_table")
	if m.tableName != "custom_table" {
		t.Errorf("tableName = %q, want %q", m.tableName, "custom_table")
	}
	if len(m.modelFields) != 0 {
		t.Errorf("len(modelFields) = %d, want %d", len(m.modelFields), 0)
	}
}

func TestModelColumns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		model  *Model
		want   string
	}{
		{
			name:  "basic struct",
			model: NewModel(admin{}),
			want:  "id,name,password",
		},
		{
			name:  "struct with jsonb",
			model: NewModel(category{}),
			want:  "id,created_at,updated_at,meta",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := strings.Join(tt.model.Columns(), ",")
			if got != tt.want {
				t.Errorf("Columns() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestModelFields(t *testing.T) {
	t.Parallel()

	m := NewModel(category{})
	fields := m.Fields()
	want := []string{"id", "created_at", "updated_at"}

	if len(fields) != len(want) {
		t.Fatalf("len(Fields()) = %d, want %d", len(fields), len(want))
	}
	for i, f := range fields {
		if f != want[i] {
			t.Errorf("Fields()[%d] = %q, want %q", i, f, want[i])
		}
	}
}

func TestModelJSONBFields(t *testing.T) {
	t.Parallel()

	m := NewModel(category{})
	fields := m.JSONBFields()
	want := []string{"meta"}

	if len(fields) != len(want) {
		t.Fatalf("len(JSONBFields()) = %d, want %d", len(fields), len(want))
	}
	for i, f := range fields {
		if f != want[i] {
			t.Errorf("JSONBFields()[%d] = %q, want %q", i, f, want[i])
		}
	}
}

func TestModelFieldByName(t *testing.T) {
	t.Parallel()

	m := NewModel(admin{})

	t.Run("finds existing field", func(t *testing.T) {
		f := m.FieldByName("Name")
		if f == nil {
			t.Fatal("FieldByName(\"Name\") returned nil")
		}
		if f.ColumnName != "name" {
			t.Errorf("ColumnName = %q, want %q", f.ColumnName, "name")
		}
	})

	t.Run("returns nil for non-existent field", func(t *testing.T) {
		f := m.FieldByName("NonExistent")
		if f != nil {
			t.Errorf("FieldByName(\"NonExistent\") = %v, want nil", f)
		}
	})

	t.Run("returns nil for column name (not field name)", func(t *testing.T) {
		f := m.FieldByName("name")
		if f != nil {
			t.Errorf("FieldByName(\"name\") = %v, want nil", f)
		}
	})
}

func TestModelWithoutFields(t *testing.T) {
	t.Parallel()

	m := NewModel(admin{})

	tests := []struct {
		name       string
		exclude    []string
		wantCount  int
	}{
		{
			name:      "exclude one field",
			exclude:   []string{"Name"},
			wantCount: 2,
		},
		{
			name:      "exclude multiple fields",
			exclude:   []string{"Name", "Password"},
			wantCount: 1,
		},
		{
			name:      "exclude all fields",
			exclude:   []string{"Id", "Name", "Password"},
			wantCount: 0,
		},
		{
			name:      "exclude non-existent field",
			exclude:   []string{"NonExistent"},
			wantCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filtered := m.WithoutFields(tt.exclude...)
			if len(filtered.modelFields) != tt.wantCount {
				t.Errorf("len(modelFields) = %d, want %d", len(filtered.modelFields), tt.wantCount)
			}
			// Verify original model is unchanged
			if len(m.modelFields) != 3 {
				t.Errorf("original model modified: len(modelFields) = %d, want 3", len(m.modelFields))
			}
		})
	}
}

func TestModelTypeName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		model *Model
		want  string
	}{
		{
			name:  "struct model",
			model: NewModel(admin{}),
			want:  "admin",
		},
		{
			name:  "table model",
			model: NewModelTable("custom"),
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.model.TypeName()
			if got != tt.want {
				t.Errorf("TypeName() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestModelTableName(t *testing.T) {
	t.Parallel()

	m := NewModel(admin{})
	if got := m.TableName(); got != "admins" {
		t.Errorf("TableName() = %q, want %q", got, "admins")
	}
}

func TestModelAddTableName(t *testing.T) {
	t.Parallel()

	m := NewModel(admin{})
	fields := []string{"id", "name"}
	got := m.AddTableName(fields...)
	want := []string{"admins.id", "admins.name"}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("AddTableName() = %v, want %v", got, want)
	}
}

func TestColumnNamer(t *testing.T) {
	t.Parallel()

	m := NewModel(admin{})

	t.Run("with default namer", func(t *testing.T) {
		got := strings.Join(m.Columns(), ",")
		if got != "id,name,password" {
			t.Errorf("Columns() = %q, want %q", got, "id,name,password")
		}
		if got := m.ToColumnName("FooBar"); got != "foo_bar" {
			t.Errorf("ToColumnName(\"FooBar\") = %q, want %q", got, "foo_bar")
		}
	})

	t.Run("with nil namer", func(t *testing.T) {
		m2 := m.Clone()
		m2.SetColumnNamer(nil)
		got := strings.Join(m2.Columns(), ",")
		if got != "Id,Name,Password" {
			t.Errorf("Columns() = %q, want %q", got, "Id,Name,Password")
		}
		if got := m2.ToColumnName("FooBar"); got != "FooBar" {
			t.Errorf("ToColumnName(\"FooBar\") = %q, want %q", got, "FooBar")
		}
	})
}

func TestModelClone(t *testing.T) {
	t.Parallel()

	m := NewModel(admin{})
	clone := m.Clone()

	// Verify clone has same data
	if clone.tableName != m.tableName {
		t.Errorf("clone.tableName = %q, want %q", clone.tableName, m.tableName)
	}
	if len(clone.modelFields) != len(m.modelFields) {
		t.Errorf("clone.modelFields length = %d, want %d", len(clone.modelFields), len(m.modelFields))
	}

	// Verify clone is independent
	clone.tableName = "modified"
	if m.tableName == "modified" {
		t.Error("modifying clone affected original")
	}
}

func TestModelNew(t *testing.T) {
	t.Parallel()

	m := NewModel(admin{})
	v := m.New()

	if v.Kind() != reflect.Ptr {
		t.Errorf("New() kind = %v, want Ptr", v.Kind())
	}
	if v.Elem().Type() != m.structType {
		t.Errorf("New() type = %v, want %v", v.Elem().Type(), m.structType)
	}
}

func TestModelNewSlice(t *testing.T) {
	t.Parallel()

	m := NewModel(admin{})
	v := m.NewSlice()

	if v.Kind() != reflect.Ptr {
		t.Errorf("NewSlice() kind = %v, want Ptr", v.Kind())
	}
	if v.Elem().Kind() != reflect.Slice {
		t.Errorf("NewSlice() elem kind = %v, want Slice", v.Elem().Kind())
	}
}

func TestModelString(t *testing.T) {
	t.Parallel()

	m := NewModel(admin{})
	got := m.String()
	want := `model (table: "admins") has 3 modelFields`
	if got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestModelColumnDataTypes(t *testing.T) {
	t.Parallel()

	m := NewModel(dataTypeTestStruct{})
	dataTypes := m.ColumnDataTypes()

	tests := []struct {
		column string
		want   string
	}{
		{"test0", "text DEFAULT ''::text NOT NULL"},
		{"test1", "foo"},
		{"test2", "test"},
		{"test3", "world"},
	}

	for _, tt := range tests {
		t.Run(tt.column, func(t *testing.T) {
			got := dataTypes[tt.column]
			if got != tt.want {
				t.Errorf("ColumnDataTypes()[%q] = %q, want %q", tt.column, got, tt.want)
			}
		})
	}
}

func TestModelSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		model *Model
		want  string
	}{
		{
			name:  "basic schema",
			model: NewModel(schemaTestStruct0{}),
			want: `CREATE TABLE schema_test_struct0s (
	test0 text DEFAULT ''::text NOT NULL
);
`,
		},
		{
			name:  "custom schema method",
			model: NewModel(schemaTestStruct1{}),
			want: `CREATE VIEW schemaTestStruct1s AS SELECT 'yes' AS test0;
`,
		},
		{
			name:  "with before and after hooks",
			model: NewModel(schemaTestStruct2{}),
			want: `-- comment b

CREATE TABLE schema_test_struct2s (
	test0 text DEFAULT ''::text NOT NULL
);

-- comment a
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.model.Schema()
			if got != tt.want {
				t.Errorf("Schema() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestModelDropSchema(t *testing.T) {
	t.Parallel()

	m := NewModel(admin{})
	got := m.DropSchema()
	want := "DROP TABLE IF EXISTS admins;\n"
	if got != want {
		t.Errorf("DropSchema() = %q, want %q", got, want)
	}
}
