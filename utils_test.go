package psql

import (
	"testing"
)

type (
	testUser    struct{}
	testProduct struct{}
)

func (testProduct) TableName() string {
	return "different_products"
}

func TestToTableName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		object interface{}
		want   string
	}{
		{
			name:   "anonymous struct",
			object: struct{}{},
			want:   "error_no_table_name",
		},
		{
			name:   "named struct with default namer",
			object: testUser{},
			want:   "test_users",
		},
		{
			name:   "struct with TableName method",
			object: testProduct{},
			want:   "different_products",
		},
		{
			name: "struct with __TABLE_NAME__ tag",
			object: struct {
				__TABLE_NAME__ string `custom_table`
			}{},
			want: "custom_table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ToTableName(tt.object)
			if got != tt.want {
				t.Errorf("ToTableName() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestToPlural(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  string
	}{
		{"", ""},
		{"user", "users"},
		{"product", "products"},
		{"category", "categories"},
		{"status", "statuses"},
		{"hero", "heroes"},
		{"photo", "photoes"},  // Note: simple pluralization adds "es" for "o" endings
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := ToPlural(tt.input)
			if got != tt.want {
				t.Errorf("ToPlural(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestToUnderscore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  string
	}{
		{"column", "column"},
		{"Column", "column"},
		{"ColumnName", "column_name"},
		{"FullName", "full_name"},
		{"HTTPServer", "h_t_t_p_server"},
		{"userID", "user_i_d"},
		{"ID", "i_d"},
		{"User123", "user123"},
		{"Test123Name", "test123_name"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := ToUnderscore(tt.input)
			if got != tt.want {
				t.Errorf("ToUnderscore(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestToPluralUnderscore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  string
	}{
		{"User", "users"},
		{"PostComment", "post_comments"},
		{"Category", "categories"},
		{"ProductStatus", "product_statuses"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := ToPluralUnderscore(tt.input)
			if got != tt.want {
				t.Errorf("ToPluralUnderscore(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestFieldDataType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		fieldName string
		fieldType string
		want      string
	}{
		{
			name:      "id field with int",
			fieldName: "id",
			fieldType: "int",
			want:      "SERIAL PRIMARY KEY",
		},
		{
			name:      "Id field with int64",
			fieldName: "Id",
			fieldType: "int64",
			want:      "SERIAL PRIMARY KEY",
		},
		{
			name:      "int8",
			fieldName: "age",
			fieldType: "int8",
			want:      "integer DEFAULT 0 NOT NULL",
		},
		{
			name:      "int16",
			fieldName: "count",
			fieldType: "int16",
			want:      "integer DEFAULT 0 NOT NULL",
		},
		{
			name:      "int32",
			fieldName: "count",
			fieldType: "int32",
			want:      "integer DEFAULT 0 NOT NULL",
		},
		{
			name:      "int64",
			fieldName: "count",
			fieldType: "int64",
			want:      "bigint DEFAULT 0 NOT NULL",
		},
		{
			name:      "int",
			fieldName: "count",
			fieldType: "int",
			want:      "bigint DEFAULT 0 NOT NULL",
		},
		{
			name:      "uint8",
			fieldName: "count",
			fieldType: "uint8",
			want:      "integer DEFAULT 0 NOT NULL",
		},
		{
			name:      "uint16",
			fieldName: "count",
			fieldType: "uint16",
			want:      "integer DEFAULT 0 NOT NULL",
		},
		{
			name:      "uint32",
			fieldName: "count",
			fieldType: "uint32",
			want:      "integer DEFAULT 0 NOT NULL",
		},
		{
			name:      "uint64",
			fieldName: "count",
			fieldType: "uint64",
			want:      "bigint DEFAULT 0 NOT NULL",
		},
		{
			name:      "uint",
			fieldName: "count",
			fieldType: "uint",
			want:      "bigint DEFAULT 0 NOT NULL",
		},
		{
			name:      "time.Time",
			fieldName: "created_at",
			fieldType: "time.Time",
			want:      "timestamptz DEFAULT NOW() NOT NULL",
		},
		{
			name:      "float32",
			fieldName: "price",
			fieldType: "float32",
			want:      "numeric(10, 2) DEFAULT 0.0 NOT NULL",
		},
		{
			name:      "float64",
			fieldName: "price",
			fieldType: "float64",
			want:      "numeric(10, 2) DEFAULT 0.0 NOT NULL",
		},
		{
			name:      "decimal.Decimal",
			fieldName: "amount",
			fieldType: "decimal.Decimal",
			want:      "numeric(10, 2) DEFAULT 0.0 NOT NULL",
		},
		{
			name:      "bool",
			fieldName: "active",
			fieldType: "bool",
			want:      "boolean DEFAULT false NOT NULL",
		},
		{
			name:      "string",
			fieldName: "name",
			fieldType: "string",
			want:      "text DEFAULT ''::text NOT NULL",
		},
		{
			name:      "pointer to int",
			fieldName: "nullable_count",
			fieldType: "*int",
			want:      "bigint DEFAULT 0",
		},
		{
			name:      "pointer to string",
			fieldName: "nullable_name",
			fieldType: "*string",
			want:      "text DEFAULT ''::text",
		},
		{
			name:      "pointer to time.Time",
			fieldName: "deleted_at",
			fieldType: "*time.Time",
			want:      "timestamptz DEFAULT NOW()",
		},
		{
			name:      "slice of int",
			fieldName: "numbers",
			fieldType: "[]int",
			want:      "bigint[] DEFAULT '{}' NOT NULL",
		},
		{
			name:      "slice of string",
			fieldName: "tags",
			fieldType: "[]string",
			want:      "text[] DEFAULT '{}' NOT NULL",
		},
		{
			name:      "unknown type",
			fieldName: "custom",
			fieldType: "CustomType",
			want:      "text DEFAULT ''::text NOT NULL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FieldDataType(tt.fieldName, tt.fieldType)
			if got != tt.want {
				t.Errorf("FieldDataType(%q, %q) = %q, want %q", tt.fieldName, tt.fieldType, got, tt.want)
			}
		})
	}
}
