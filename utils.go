package psql

import (
	"reflect"
	"strings"
	"unicode"
)

var (
	// DefaultColumnNamer is default column naming function used when
	// calling NewModel. Default is null, which uses field name as column
	// name.
	DefaultColumnNamer func(string) string = nil

	// DefaultColumnNamer is default table naming function used when
	// calling NewModel. Default is ToPlural, which converts table name to
	// its plural form.
	DefaultTableNamer func(string) string = ToPlural
)

const (
	tableNameField = "__TABLE_NAME__"
)

// ToTableName returns table name of a struct. If struct has "TableName()
// string" receiver method, its return value is used. If name is empty and
// struct has a __TABLE_NAME__ field, its tag value is used. If it is still
// empty, struct's name is used. If name is still empty, "error_no_table_name"
// is returned.
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

// Convert a word to its plural form. Add "es" for "s" or "o" ending,
// "y" ending will be replaced with "ies", for other endings, add "s".
// For example, "product" will be converted to "products".
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

// Convert a "CamelCase" word to its plural "snake_case" (underscore) form.
// For example, "PostComment" will be converted to "post_comments".
func ToPluralUnderscore(in string) string {
	return ToPlural(ToUnderscore(in))
}

// Convert "CamelCase" word to its "snake_case" (underscore) form. For example,
// "FullName" will be converted to "full_name".
func ToUnderscore(str string) string { // from govalidator
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

// FieldDataType generates PostgreSQL data type based on struct's field name
// and type.  This is default function used when calling ColumnDataTypes() or
// Schema(). To use custom data type function, define "FieldDataType(fieldName,
// fieldType string) (dataType string)" function for your connection.
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
