package psql

import (
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"unsafe"

	"github.com/gopsql/db"
	"github.com/gopsql/logger"
)

type (
	// Model is a database table and it is created from struct. Table name
	// is inferred from struct's name, and converted to its the plural form
	// using psql.TransformTableName by default. To use a different table
	// name, define a __TABLE_NAME__ field in the struct and set the tag
	// value as the table name, or add a "TableName() string" receiver
	// method for the struct.
	//
	// Column names are inferred from struct field names. To use a
	// different column name, set the "column" tag for the field, or use
	// SetColumnNamer() to define column namer function to transform all
	// field names in this model.
	Model struct {
		connection         db.DB
		logger             logger.Logger
		structType         reflect.Type
		structDataTypeFunc func(Model, string) string
		*modelInfo
	}

	modelInfo struct {
		columnNamer  func(string) string // defaults to DefaultColumnNamer
		tableName    string
		modelFields  []Field
		jsonbColumns []string
	}

	Field struct {
		Name       string // struct field name
		ColumnName string // column name (or jsonb key name) in database
		ColumnType string // column type
		JsonName   string // key name in json input and output
		Jsonb      string // jsonb column name in database
		DataType   string // data type in database
		Exported   bool   // false if field name is lower case (unexported)
	}
)

var (
	ErrMustBePointer = errors.New("must be pointer")
)

// Initialize a Model from a struct. For available options, see SetOptions().
func NewModel(object interface{}, options ...interface{}) (m *Model) {
	m = &Model{
		modelInfo: &modelInfo{
			tableName: ToTableName(object),
		},
		structType: reflect.TypeOf(object),
	}
	if f, ok := object.(interface{ DataType(Model, string) string }); ok {
		m.structDataTypeFunc = f.DataType
	}
	m.SetColumnNamer(DefaultColumnNamer)
	m.SetOptions(options...)
	return
}

// Initialize a Model by defining table name only. Useful if you are calling
// functions that don't need fields, for example:
//  psql.NewModelTable("users", conn).MustCount()
// For available options, see SetOptions().
func NewModelTable(tableName string, options ...interface{}) (m *Model) {
	m = &Model{
		modelInfo: &modelInfo{
			tableName: tableName,
		},
		structType: nil,
	}
	m.SetColumnNamer(DefaultColumnNamer)
	m.SetOptions(options...)
	return
}

func (m Model) String() string {
	return `model (table: "` + m.tableName + `") has ` +
		strconv.Itoa(len(m.modelFields)) + " modelFields"
}

// Table name of the Model.
func (m Model) TableName() string {
	return m.tableName
}

// Type name of the Model.
func (m Model) TypeName() string {
	if m.structType != nil {
		return m.structType.Name()
	}
	return ""
}

// Get field by struct field name, nil will be returned if no such field.
func (m Model) FieldByName(name string) *Field {
	for _, f := range m.modelFields {
		if f.Name == name {
			return &f
		}
	}
	return nil
}

// Column names of the Model.
func (m Model) Columns() []string {
	columns := []string{}
	for _, f := range m.modelFields {
		if f.Jsonb != "" {
			continue
		}
		columns = append(columns, f.ColumnName)
	}
	for _, jsonbField := range m.jsonbColumns {
		columns = append(columns, jsonbField)
	}
	return columns
}

type (
	fieldDataTypeFunc func(fieldName, fieldType string) (dataType string)

	hasFieldDataTypeFunc interface {
		FieldDataType(fieldName, fieldType string) (dataType string)
	}
)

func (m Model) ColumnDataTypes() map[string]string {
	var dbDataTypeFunc fieldDataTypeFunc
	if c, ok := m.connection.(hasFieldDataTypeFunc); ok {
		dbDataTypeFunc = c.FieldDataType
	} else {
		dbDataTypeFunc = FieldDataType
	}
	dataTypes := map[string]string{}
	jsonbDataType := map[string]string{}
	for _, f := range m.modelFields {
		if f.Jsonb != "" {
			if _, ok := jsonbDataType[f.Jsonb]; !ok && f.DataType != "" {
				jsonbDataType[f.Jsonb] = f.DataType
			}
			continue
		}
		if m.structDataTypeFunc != nil {
			if dt := m.structDataTypeFunc(m, f.Name); dt != "" {
				dataTypes[f.ColumnName] = dt
				continue
			}
		}
		if f.DataType != "" {
			dataTypes[f.ColumnName] = f.DataType
			continue
		}
		dataTypes[f.ColumnName] = dbDataTypeFunc(f.ColumnName, f.ColumnType)
	}
	for _, jsonbField := range m.jsonbColumns {
		dataType := jsonbDataType[jsonbField]
		if dataType == "" {
			dataType = "jsonb DEFAULT '{}'::jsonb NOT NULL"
		}
		dataTypes[jsonbField] = dataType
	}
	return dataTypes
}

// Generate CREATE TABLE SQL statement from a Model.
//  | Go Type                                        | PostgreSQL Data Type |
//  |------------------------------------------------|----------------------|
//  | int8 / int16 / int32 / uint8 / uint16 / uint32 | integer              |
//  | int64 / uint64 / int / uint                    | bigint               |
//  | time.Time                                      | timestamptz          |
//  | float32 / float64 / decimal.Decimal            | numeric              |
//  | bool                                           | boolean              |
//  | other                                          | text                 |
// You can use "dataType" tag to customize the data type. "NOT NULL" is added
// if the struct field is not a pointer. You can also set SQL statements before
// or after this statement by defining "BeforeCreateSchema() string" (for
// example the CREATE EXTENSION statement) or "AfterCreateSchema() string" (for
// example the CREATE INDEX statement) function for the struct.
//  psql.NewModel(struct {
//  	__TABLE_NAME__ string `users`
//
//  	Id        int
//  	Name      string
//  	Age       *int
//  	Numbers   []int
//  	CreatedAt time.Time
//  	DeletedAt *time.Time `dataType:"timestamptz"`
//  	FullName  string     `jsonb:"meta"`
//  	NickName  string     `jsonb:"meta"`
//  }{}).Schema()
//  // CREATE TABLE users (
//  //         id SERIAL PRIMARY KEY,
//  //         name text DEFAULT ''::text NOT NULL,
//  //         age bigint DEFAULT 0,
//  //         numbers bigint[] DEFAULT '{}' NOT NULL,
//  //         created_at timestamptz DEFAULT NOW() NOT NULL,
//  //         deleted_at timestamptz,
//  //         meta jsonb DEFAULT '{}'::jsonb NOT NULL
//  // );
func (m Model) Schema() string {
	columns := m.Columns()
	dataTypes := m.ColumnDataTypes()
	sql := []string{}
	for _, column := range columns {
		sql = append(sql, "\t"+column+" "+dataTypes[column])
	}
	out := "CREATE TABLE " + m.tableName + " (\n" + strings.Join(sql, ",\n") + "\n);\n"
	if m.structType != nil {
		n := reflect.New(m.structType).Interface()
		if a, ok := n.(interface{ BeforeCreateSchema() string }); ok {
			out = a.BeforeCreateSchema() + "\n\n" + out
		} else if a, ok := n.(interface{ BeforeCreateSchema(Model) string }); ok {
			out = a.BeforeCreateSchema(m) + "\n\n" + out
		}
		if a, ok := n.(interface{ AfterCreateSchema() string }); ok {
			out += "\n" + a.AfterCreateSchema() + "\n"
		} else if a, ok := n.(interface{ AfterCreateSchema(Model) string }); ok {
			out += "\n" + a.AfterCreateSchema(m) + "\n"
		}
	}
	return out
}

// Generate DROP TABLE ("DROP TABLE IF EXISTS <table_name>;") SQL statement from a Model.
func (m Model) DropSchema() string {
	return "DROP TABLE IF EXISTS " + m.tableName + ";\n"
}

// Clone returns a copy of the model.
func (m *Model) Clone() *Model {
	return &Model{
		connection:         m.connection,
		logger:             m.logger,
		structType:         m.structType,
		structDataTypeFunc: m.structDataTypeFunc,
		modelInfo: &modelInfo{
			columnNamer:  m.columnNamer,
			tableName:    m.tableName,
			modelFields:  m.modelFields,
			jsonbColumns: m.jsonbColumns,
		},
	}
}

// Quiet returns a copy of the model without logger.
func (m *Model) Quiet() *Model {
	return m.Clone().SetLogger(nil)
}

// SetOptions sets database connection (see SetConnection()) and/or logger (see
// SetLogger()).
func (m *Model) SetOptions(options ...interface{}) *Model {
	for _, option := range options {
		switch o := option.(type) {
		case db.DB:
			m.SetConnection(o)
		case logger.Logger:
			m.SetLogger(o)
		}
	}
	return m
}

// Return database connection for the Model.
func (m *Model) Connection() db.DB {
	return m.connection
}

// Change the column namer function for the Model.
func (m *Model) SetColumnNamer(namer func(string) string) *Model {
	m.setColumnNamer(namer)
	m.updateColumnNames(m.structType)
	return m
}

// Set a database connection for the Model. ErrNoConnection is returned if no
// connection is set.
func (m *Model) SetConnection(db db.DB) *Model {
	m.connection = db
	return m
}

// Set the logger for the Model. Use logger.StandardLogger if you want to use
// Go's built-in standard logging package. By default, no logger is used, so
// the SQL statements are not printed to the console.
func (m *Model) SetLogger(logger logger.Logger) *Model {
	m.logger = logger
	return m
}

// MustExists is like Exists but panics if existence check operation fails.
// Returns true if record exists, false if not exists.
func (m Model) MustExists() bool {
	exists, err := m.Exists()
	if err != nil {
		panic(err)
	}
	return exists
}

// Create and execute a SELECT 1 AS one statement. Returns true if record
// exists, false if not exists.
func (m Model) Exists() (exists bool, err error) {
	return m.newSelect().Exists()
}

// MustCount is like Count but panics if count operation fails.
func (m Model) MustCount(optional ...string) int {
	count, err := m.Count(optional...)
	if err != nil {
		panic(err)
	}
	return count
}

// Create and execute a SELECT COUNT(*) statement, return number of rows.
func (m Model) Count(optional ...string) (count int, err error) {
	return m.newSelect().Count(optional...)
}

// MustAssign is like Assign but panics if assign operation fails.
func (m Model) MustAssign(i interface{}, lotsOfChanges ...interface{}) []interface{} {
	out, err := m.Assign(i, lotsOfChanges...)
	if err != nil {
		panic(err)
	}
	return out
}

// Assign changes to target object. Useful if you want to validate your struct.
//  func create(c echo.Context) error {
//  	var user models.User
//  	m := psql.NewModel(user, conn)
//  	changes := m.MustAssign(
//  		&user,
//  		m.Permit("Name").Filter(c.Request().Body),
//  	)
//  	if err := c.Validate(user); err != nil {
//  		panic(err)
//  	}
//  	var id int
//  	m.Insert(changes...).Returning("id").MustQueryRow(&id)
//  	// ...
//  }
func (m Model) Assign(target interface{}, lotsOfChanges ...interface{}) (out []interface{}, err error) {
	rt := reflect.TypeOf(target)
	if rt.Kind() != reflect.Ptr {
		err = ErrMustBePointer
		return
	}
	rv := reflect.ValueOf(target).Elem()
	for _, changes := range m.getChanges(lotsOfChanges) {
		for field, value := range changes {
			pointer := field.getFieldValueAddrFromStruct(rv)
			b, _ := json.Marshal(value)
			json.Unmarshal(b, pointer)
		}
		out = append(out, changes)
	}
	return
}

func (m Model) log(sql string, args []interface{}) {
	if m.logger == nil {
		return
	}
	var prefix string
	if idx := strings.Index(sql, " "); idx > -1 {
		prefix = strings.ToUpper(sql[:idx])
	} else {
		prefix = strings.ToUpper(sql)
	}
	var colored logger.ColoredString
	switch prefix {
	case "DELETE", "DROP", "ROLLBACK":
		colored = logger.RedString(sql)
	case "INSERT", "CREATE", "COMMIT":
		colored = logger.GreenString(sql)
	case "UPDATE", "ALTER":
		colored = logger.YellowString(sql)
	default:
		colored = logger.CyanString(sql)
	}
	if len(args) == 0 {
		m.logger.Debug(colored)
		return
	}
	m.logger.Debug(colored, args)
}

// Function to convert field name to name used in database.
func (mi modelInfo) ToColumnName(in string) string {
	if mi.columnNamer == nil {
		return in
	}
	return mi.columnNamer(in)
}

func (mi *modelInfo) setColumnNamer(namer func(string) string) {
	mi.columnNamer = namer
}

func (mi *modelInfo) updateColumnNames(structType reflect.Type) {
	if structType == nil {
		return
	}
	mi.modelFields, mi.jsonbColumns = mi.parseStruct(structType)
}

// parseStruct collects column names, json names and jsonb names
func (mi *modelInfo) parseStruct(obj interface{}) (fields []Field, jsonbColumns []string) {
	var rt reflect.Type
	if o, ok := obj.(reflect.Type); ok {
		rt = o
	} else {
		rt = reflect.TypeOf(obj)
	}
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	if rt.Kind() != reflect.Struct {
		return
	}

	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if f.Anonymous {
			f, j := mi.parseStruct(f.Type)
			fields = append(fields, f...)
			jsonbColumns = append(jsonbColumns, j...)
			continue
		}

		columnName := f.Tag.Get("column")
		if columnName == "-" {
			continue
		}
		if idx := strings.Index(columnName, ","); idx != -1 {
			columnName = columnName[:idx]
		}
		if columnName == "" {
			if f.PkgPath != "" {
				continue // ignore unexported field if no column specified
			}
			columnName = mi.ToColumnName(f.Name)
		}

		jsonName := f.Tag.Get("json")
		if jsonName == "-" {
			jsonName = ""
		} else {
			if idx := strings.Index(jsonName, ","); idx != -1 {
				jsonName = jsonName[:idx]
			}
			if jsonName == "" {
				jsonName = f.Name
			}
		}

		jsonb := f.Tag.Get("jsonb")
		if idx := strings.Index(jsonb, ","); idx != -1 {
			jsonb = jsonb[:idx]
		}
		jsonb = mi.ToColumnName(jsonb)
		if jsonb != "" {
			exists := false
			for _, column := range jsonbColumns {
				if column == jsonb {
					exists = true
					break
				}
			}
			if !exists {
				jsonbColumns = append(jsonbColumns, jsonb)
			}
		}

		fields = append(fields, Field{
			Name:       f.Name,
			Exported:   f.PkgPath == "",
			ColumnName: columnName,
			ColumnType: f.Type.String(),
			JsonName:   jsonName,
			Jsonb:      jsonb,
			DataType:   f.Tag.Get("dataType"),
		})
	}
	return
}

func (f Field) getFieldValueAddrFromStruct(structValue reflect.Value) interface{} {
	value := structValue.FieldByName(f.Name)
	if f.Exported {
		return value.Addr().Interface()
	}
	return reflect.NewAt(value.Type(), unsafe.Pointer(value.UnsafeAddr())).Interface()
}
