package psql

import (
	"reflect"
	"strings"
	"unicode"
)

var (
	// DefaultColumnNamer is the default function for transforming struct field
	// names to database column names. If nil (the default), field names are
	// used as-is. Set to ToUnderscore for snake_case column names.
	DefaultColumnNamer func(string) string = nil

	// DefaultTableNamer is the default function for transforming struct names
	// to database table names. Defaults to ToPlural, which converts "User" to
	// "Users". Set to ToPluralUnderscore for snake_case table names.
	DefaultTableNamer func(string) string = ToPlural
)

const (
	tableNameField = "__TABLE_NAME__"
)

// ToTableName extracts the database table name from a struct. The name is
// determined in order of priority:
//  1. Return value of TableName() method if implemented
//  2. Tag value of __TABLE_NAME__ field if present
//  3. Struct name transformed by DefaultTableNamer
//
// Returns "error_no_table_name" for anonymous structs without explicit naming.
func ToTableName(object interface{}) (name string) {
	if o, ok := object.(interface{ TableName() string }); ok {
		name = o.TableName()
		if name != "" {
			return
		}
	}
	rt := reflect.TypeOf(object)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	if rt.Kind() == reflect.Struct {
		if f, ok := rt.FieldByName(tableNameField); ok {
			name = string(f.Tag)
			if name != "" {
				return
			}
		}
		name = rt.Name()
		if DefaultTableNamer != nil {
			name = DefaultTableNamer(name)
		}
	}
	if name == "" { // anonymous struct has no name
		return "error_no_table_name"
	}
	return
}

// ToPlural converts a word to its plural form using simple English rules:
// words ending in "y" become "ies", words ending in "s" or "o" add "es",
// and other words add "s". For example, "Product" becomes "Products".
func ToPlural(in string) string {
	if in == "" {
		return ""
	}
	if strings.HasSuffix(in, "y") {
		return in[:len(in)-1] + "ies"
	}
	if strings.HasSuffix(in, "s") || strings.HasSuffix(in, "o") {
		return in + "es"
	}
	return in + "s"
}

// ToPluralUnderscore converts a CamelCase word to plural snake_case form.
// For example, "PostComment" becomes "post_comments".
func ToPluralUnderscore(in string) string {
	return ToPlural(ToUnderscore(in))
}

// ToUnderscore converts a CamelCase string to snake_case. For example,
// "FullName" becomes "full_name". Numbers are not treated as word separators.
func ToUnderscore(str string) string {
	var output []rune
	var segment []rune
	for _, r := range str {
		// not treat number as separate segment
		if !unicode.IsLower(r) && string(r) != "_" && !unicode.IsNumber(r) {
			output = addSegment(output, segment)
			segment = nil
		}
		segment = append(segment, unicode.ToLower(r))
	}
	output = addSegment(output, segment)
	return string(output)
}

func addSegment(inrune, segment []rune) []rune { // from govalidator
	if len(segment) == 0 {
		return inrune
	}
	if len(inrune) != 0 {
		inrune = append(inrune, '_')
	}
	inrune = append(inrune, segment...)
	return inrune
}

// FieldDataType generates a PostgreSQL data type definition from a Go field
// name and type. This is the default implementation used by Schema. Fields
// named "id" with integer types become SERIAL PRIMARY KEY. Pointer types are
// nullable; non-pointer types include NOT NULL. To customize type mapping,
// implement FieldDataType on your database connection type.
func FieldDataType(fieldName, fieldType string) (dataType string) {
	if strings.ToLower(fieldName) == "id" && strings.Contains(fieldType, "int") {
		dataType = "SERIAL PRIMARY KEY"
		return
	}
	var null bool
	if strings.HasPrefix(fieldType, "*") {
		fieldType = strings.TrimPrefix(fieldType, "*")
		null = true
	}
	var isArray bool
	if strings.HasPrefix(fieldType, "[]") {
		fieldType = strings.TrimPrefix(fieldType, "[]")
		isArray = true
	}
	var defValue string
	switch fieldType {
	case "int8", "int16", "int32", "uint8", "uint16", "uint32":
		dataType = "integer"
		defValue = "0"
	case "int64", "uint64", "int", "uint":
		dataType = "bigint"
		defValue = "0"
	case "time.Time":
		dataType = "timestamptz"
		defValue = "NOW()"
	case "float32", "float64":
		dataType = "numeric(10, 2)"
		defValue = "0.0"
	case "decimal.Decimal":
		dataType = "numeric(10, 2)"
		defValue = "0.0"
	case "bool":
		dataType = "boolean"
		defValue = "false"
	default:
		dataType = "text"
		defValue = "''::text"
	}
	if isArray {
		dataType += "[] DEFAULT '{}'"
	} else {
		dataType += " DEFAULT " + defValue
	}
	if !null {
		dataType += " NOT NULL"
	}
	return
}
