package psql

import (
	"reflect"
	"strings"
	"unicode"
)

var (
	// Function to convert a name to table or column name used in database,
	// by default uses DefaultColumnizer which converts "CamelCase" to "snake_case".
	Columnizer func(string) string = DefaultColumnizer

	// Function to convert table name to its plural form.
	// By default, table name uses plural form.
	Pluralizer func(string) string = DefaultPluralizer
)

const (
	tableNameField = "__TABLE_NAME__"
)

// Get table name from a struct. If a struct has "TableName() string" function,
// then the return value of the function will be used. If a struct has a field
// named "__TABLE_NAME__", then value of the field tag will be used. Otherwise,
// the name of the struct will be used. If name is empty, "error_no_table_name"
// is returned.
// Examples:
//  - type Product struct{}; func (_ Product) TableName() string { return "foobar" }; ToTableName(Product{}) == "foobar"
//  - ToTableName(struct { __TABLE_NAME__ string `users` }{}) == "users"
//  - type Product struct{}; ToTableName(Product{}) == "products"
//  - ToTableName(struct{}{}) == "error_no_table_name"
func ToTableName(object interface{}) (name string) {
	if o, ok := object.(ModelWithTableName); ok {
		name = o.TableName()
		return
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
		name = ToColumnName(rt.Name())
	}
	if name == "" { // anonymous struct has no name
		return "error_no_table_name"
	}
	name = Pluralizer(name)
	return
}

// Function to convert struct name to name used in database, using the Columnizer function.
func ToColumnName(in string) string {
	return Columnizer(strings.TrimSpace(in))
}

// Default function to convert "CamelCase" struct name to "snake_case" column
// name used in database. For example, "FullName" will be converted to "full_name".
func DefaultColumnizer(in string) string {
	return camelCaseToUnderscore(in)
}

// Default function to convert a word to its plural form. Add "es" for "s" or "o" ending,
// "y" ending will be replaced with "ies", for other endings, add "s".
// For example, "product" will be converted to "products".
func DefaultPluralizer(in string) string {
	if strings.HasSuffix(in, "y") {
		return in[:len(in)-1] + "ies"
	}
	if strings.HasSuffix(in, "s") || strings.HasSuffix(in, "o") {
		return in + "es"
	}
	return in + "s"
}

func camelCaseToUnderscore(str string) string { // from govalidator
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
