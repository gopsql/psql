package psql

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/gopsql/db"
	"github.com/gopsql/logger"
)

type (
	// Model represents a database table mapped from a Go struct.
	//
	// Table names are inferred from the struct name and converted to plural form
	// by default (e.g., User becomes users). To customize the table name:
	//   - Define a __TABLE_NAME__ field with the table name as its tag value
	//   - Implement a TableName() string method on the struct
	//
	// Column names are inferred from struct field names. To customize:
	//   - Use the "column" struct tag for individual fields
	//   - Call SetColumnNamer to set a naming function for all fields
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

	// Field represents a mapping between a struct field and a database column.
	Field struct {
		Name       string // Name is the struct field name.
		ColumnName string // ColumnName is the database column name (or JSONB key).
		ColumnType string // ColumnType is the Go type as a string (e.g., "int", "string").
		JsonName   string // JsonName is the key name used in JSON input/output.
		Jsonb      string // Jsonb is the JSONB column name if this field is stored in JSONB.
		DataType   string // DataType is the PostgreSQL data type from the dataType tag.
		Exported   bool   // Exported is true if the struct field is exported (capitalized).
		Strict     bool   // Strict enables JSON unmarshal error reporting for JSONB fields.
		Parent     string // Parent is the parent struct path for anonymous/embedded fields.
	}
)

var (
	// ErrMustBePointer is returned when a function expects a pointer argument
	// but receives a non-pointer value.
	ErrMustBePointer = errors.New("must be pointer")
)

// NewModel creates a new Model from a struct. The struct defines the table
// schema, with field names mapped to column names. Options can include a db.DB
// connection and/or a logger.Logger for query logging. See SetOptions for
// details on available options.
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

// NewModelTable creates a new Model with only a table name, without struct
// field mappings. This is useful for operations that don't require field
// information, such as counting rows or executing raw SQL:
//
//	psql.NewModelTable("users", conn).MustCount()
//
// Options can include a db.DB connection and/or a logger.Logger.
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

// New returns a reflect.Value representing a pointer to a new zero value for
// model's struct type.
func (m Model) New() reflect.Value {
	return reflect.New(m.structType)
}

// NewSlice returns a reflect.Value representing a pointer to a new
// zero-initialized slice value for model's struct type.
func (m Model) NewSlice() reflect.Value {
	slice := reflect.MakeSlice(reflect.SliceOf(m.structType), 0, 0)
	ret := reflect.New(slice.Type())
	ret.Elem().Set(slice)
	return ret
}

func (m Model) String() string {
	return `model (table: "` + m.tableName + `") has ` +
		strconv.Itoa(len(m.modelFields)) + " modelFields"
}

// TableName returns the database table name for this Model.
func (m Model) TableName() string {
	return m.tableName
}

// TypeName returns the Go struct type name for this Model, or an empty string
// if the Model was created with NewModelTable.
func (m Model) TypeName() string {
	if m.structType != nil {
		return m.structType.Name()
	}
	return ""
}

// FieldByName returns the Field with the given struct field name, or nil if
// no such field exists.
func (m Model) FieldByName(name string) *Field {
	for _, f := range m.modelFields {
		if f.Name == name {
			return &f
		}
	}
	return nil
}

// Columns returns all database column names for this Model, including JSONB
// columns but excluding fields stored within JSONB columns.
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

// ColumnDataTypes returns a map of column names to their PostgreSQL data type
// definitions. This is used by Schema to generate CREATE TABLE statements.
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
		if f.DataType == "-" {
			continue
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

// Schema generates a CREATE TABLE SQL statement from the Model's struct
// definition.
//
// Go types are mapped to PostgreSQL types as follows:
//
//	| Go Type                                        | PostgreSQL Data Type |
//	|------------------------------------------------|----------------------|
//	| int8 / int16 / int32 / uint8 / uint16 / uint32 | integer              |
//	| int64 / uint64 / int / uint                    | bigint               |
//	| time.Time                                      | timestamptz          |
//	| float32 / float64 / decimal.Decimal            | numeric              |
//	| bool                                           | boolean              |
//	| other                                          | text                 |
//
// Use the "dataType" struct tag to specify a custom PostgreSQL data type.
// Non-pointer fields automatically include "NOT NULL". Set dataType to "-"
// to exclude a field from schema generation.
//
// The struct may implement BeforeCreateSchema() string to prepend SQL (e.g.,
// CREATE EXTENSION) or AfterCreateSchema() string to append SQL (e.g.,
// CREATE INDEX).
//
//	psql.NewModel(struct {
//		__TABLE_NAME__ string `users`
//
//		Id        int
//		Name      string
//		Age       *int
//		Numbers   []int
//		CreatedAt time.Time
//		DeletedAt *time.Time `dataType:"timestamptz"`
//		FullName  string     `jsonb:"meta"`
//		NickName  string     `jsonb:"meta"`
//	}{}).Schema()
//	// CREATE TABLE users (
//	//         id SERIAL PRIMARY KEY,
//	//         name text DEFAULT ''::text NOT NULL,
//	//         age bigint DEFAULT 0,
//	//         numbers bigint[] DEFAULT '{}' NOT NULL,
//	//         created_at timestamptz DEFAULT NOW() NOT NULL,
//	//         deleted_at timestamptz,
//	//         meta jsonb DEFAULT '{}'::jsonb NOT NULL
//	// );
func (m Model) Schema() string {
	var before, after string
	if m.structType != nil {
		n := m.New().Interface()
		if a, ok := n.(interface{ Schema() string }); ok {
			return strings.TrimSpace(a.Schema()) + "\n"
		}

		if a, ok := n.(interface{ BeforeCreateSchema() string }); ok {
			before = a.BeforeCreateSchema() + "\n\n"
		} else if a, ok := n.(interface{ BeforeCreateSchema(Model) string }); ok {
			before = a.BeforeCreateSchema(m) + "\n\n"
		}

		if a, ok := n.(interface{ AfterCreateSchema() string }); ok {
			after = "\n" + a.AfterCreateSchema() + "\n"
		} else if a, ok := n.(interface{ AfterCreateSchema(Model) string }); ok {
			after = "\n" + a.AfterCreateSchema(m) + "\n"
		}
	}
	columns := m.Columns()
	dataTypes := m.ColumnDataTypes()
	sql := []string{}
	for _, column := range columns {
		if dataType, ok := dataTypes[column]; ok {
			sql = append(sql, "\t"+column+" "+dataType)
		}
	}
	return before + "CREATE TABLE " + m.tableName + " (\n" + strings.Join(sql, ",\n") + "\n);\n" + after
}

// DropSchema generates a DROP TABLE IF EXISTS SQL statement for this Model's
// table. If the struct implements DropSchema() string, that method is called
// instead.
func (m Model) DropSchema() string {
	if m.structType != nil {
		n := m.New().Interface()
		if a, ok := n.(interface{ DropSchema() string }); ok {
			return strings.TrimSpace(a.DropSchema()) + "\n"
		}
	}
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

// Fields returns field names of the Model. For JSONB fields, see JSONBFields().
func (m Model) Fields() []string {
	fields := []string{}
	for _, field := range m.modelFields {
		if field.Jsonb != "" {
			continue
		}
		fields = append(fields, field.ColumnName)
	}
	return fields
}

// JSONBFields returns JSONB field names of the Model.
func (m Model) JSONBFields() []string {
	fields := []string{}
	for _, jsonbField := range m.jsonbColumns {
		fields = append(fields, jsonbField)
	}
	return fields
}

// AddTableName prefixes each field name with the table name (e.g., "id"
// becomes "tablename.id"). This is useful for queries involving multiple
// tables to avoid column name ambiguity.
func (m Model) AddTableName(fields ...string) []string {
	out := make([]string, len(fields))
	for i, field := range fields {
		out[i] = m.tableName + "." + field
	}
	return out
}

// WithoutFields returns a copy of the model without given fields.
func (m *Model) WithoutFields(fieldNames ...string) *Model {
	cloned := m.Clone()
	var fields []Field
outer:
	for _, f := range cloned.modelFields {
		for _, name := range fieldNames {
			if f.Name == name {
				continue outer
			}
		}
		fields = append(fields, f)
	}
	cloned.modelFields = fields
	return cloned
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

// Connection returns the database connection for this Model.
func (m *Model) Connection() db.DB {
	return m.connection
}

// SetColumnNamer sets a function to transform struct field names into database
// column names. The function is applied to all fields when called. Pass nil to
// use field names as-is.
func (m *Model) SetColumnNamer(namer func(string) string) *Model {
	m.setColumnNamer(namer)
	m.updateColumnNames(m.structType)
	return m
}

// SetConnection sets the database connection for this Model. A connection is
// required for executing queries; ErrNoConnection is returned from query
// methods if no connection is set.
func (m *Model) SetConnection(db db.DB) *Model {
	m.connection = db
	return m
}

func (m *Model) convertValues(sql string, values []interface{}) (string, []interface{}) {
	if c, ok := m.connection.(db.ConvertParameters); ok {
		return c.ConvertParameters(sql, values)
	}
	return sql, values
}

// Logger returns the logger for this Model, or nil if no logger is set.
func (m *Model) Logger() logger.Logger {
	return m.logger
}

// SetLogger sets the logger for SQL query logging. Use logger.StandardLogger
// to log to the standard library's log package. By default, no logger is set
// and SQL statements are not logged. Pass nil to disable logging.
func (m *Model) SetLogger(logger logger.Logger) *Model {
	m.logger = logger
	return m
}

// MustExists is like Exists but panics if existence check operation fails.
// Returns true if record exists, false if not exists.
func (m Model) MustExists() bool {
	return m.MustExistsCtxTx(context.Background(), nil)
}

// MustExistsCtxTx is like ExistsCtxTx but panics if existence check operation fails.
// Returns true if record exists, false if not exists.
func (m Model) MustExistsCtxTx(ctx context.Context, tx Tx) bool {
	exists, err := m.ExistsCtxTx(ctx, tx)
	if err != nil {
		panic(err)
	}
	return exists
}

// Exists executes a SELECT 1 query and returns true if at least one row
// matches any WHERE conditions set on the Model, false otherwise.
func (m Model) Exists() (exists bool, err error) {
	return m.ExistsCtxTx(context.Background(), nil)
}

// ExistsCtxTx is like Exists but accepts a context and optional transaction.
func (m Model) ExistsCtxTx(ctx context.Context, tx Tx) (exists bool, err error) {
	return m.newSelect().ExistsCtxTx(ctx, tx)
}

// MustCount is like Count but panics if count operation fails.
func (m Model) MustCount(optional ...string) int {
	return m.MustCountCtxTx(context.Background(), nil, optional...)
}

// MustCountCtxTx is like CountCtxTx but panics if count operation fails.
func (m Model) MustCountCtxTx(ctx context.Context, tx Tx, optional ...string) int {
	count, err := m.CountCtxTx(ctx, tx, optional...)
	if err != nil {
		panic(err)
	}
	return count
}

// Count executes a SELECT COUNT(*) query and returns the number of rows
// matching any WHERE conditions set on the Model. Pass a custom expression
// for different counting, e.g., Count("COUNT(DISTINCT author_id)").
func (m Model) Count(optional ...string) (count int, err error) {
	return m.CountCtxTx(context.Background(), nil, optional...)
}

// CountCtxTx is like Count but accepts a context and optional transaction.
func (m Model) CountCtxTx(ctx context.Context, tx Tx, optional ...string) (count int, err error) {
	return m.newSelect().CountCtxTx(ctx, tx, optional...)
}

// MustAssign is like Assign but panics if assign operation fails.
func (m Model) MustAssign(i interface{}, lotsOfChanges ...interface{}) []interface{} {
	out, err := m.Assign(i, lotsOfChanges...)
	if err != nil {
		panic(err)
	}
	return out
}

// Assign applies changes to a target struct. This is useful when you need to
// validate the struct before inserting or updating. The target must be a
// pointer to a struct. Returns the changes as a slice suitable for passing
// to Insert or Update.
//
//	func create(c echo.Context) error {
//		var user models.User
//		m := psql.NewModel(user, conn)
//		changes := m.MustAssign(
//			&user,
//			m.Permit("Name").Filter(c.Request().Body),
//		)
//		if err := c.Validate(user); err != nil {
//			panic(err)
//		}
//		var id int
//		m.Insert(changes...).Returning("id").MustQueryRow(&id)
//		// ...
//	}
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

func (m Model) log(sql string, args []interface{}, elapsed time.Duration) {
	if m.logger == nil {
		return
	}
	var prefix string
	if idx := strings.Index(sql, " "); idx > -1 {
		prefix = strings.ToUpper(sql[:idx])
	} else {
		prefix = strings.ToUpper(sql)
	}
	if prefix == "EXPLAIN" {
		rest := strings.TrimSpace(sql[7:])
		if len(rest) > 0 && rest[0] == '(' {
			if idx := strings.Index(rest, ")"); idx > -1 {
				rest = strings.TrimSpace(rest[idx+1:])
			}
		}
		if idx := strings.Index(rest, " "); idx > -1 {
			prefix = strings.ToUpper(rest[:idx])
		} else if len(rest) > 0 {
			prefix = strings.ToUpper(rest)
		}
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
	if elapsed == 0 {
		if len(args) == 0 {
			m.logger.Debug(colored)
			return
		}
		m.logger.Debug(colored, args)
		return
	}
	var coloredElapsed logger.ColoredString
	ms := elapsed.Milliseconds()
	if ms > 1000 {
		coloredElapsed = logger.RedString(elapsed.String())
	} else if ms > 100 {
		coloredElapsed = logger.YellowString(elapsed.String())
	} else {
		coloredElapsed = logger.GreenString(elapsed.String())
	}
	if len(args) == 0 {
		m.logger.Debug(colored, coloredElapsed)
		return
	}
	m.logger.Debug(colored, args, coloredElapsed)
}

// ToColumnName converts a struct field name to a database column name using
// the configured column namer function. If no namer is set, returns the input
// unchanged.
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
	mi.modelFields, mi.jsonbColumns = mi.parseStruct(structType, nil)
}

// parseStruct collects column names, json names and jsonb names
func (mi *modelInfo) parseStruct(obj interface{}, parentColumnName *string) (fields []Field, jsonbColumns []string) {
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
			f, j := mi.parseStruct(f.Type, nil)
			fields = append(fields, f...)
			jsonbColumns = append(jsonbColumns, j...)
			continue
		}

		exported := f.PkgPath == ""

		columnParts := strings.Split(f.Tag.Get("column"), ",")
		columnName := columnParts[0]

		if columnName == "-" {
			continue
		}

		anonymous := false
		for _, option := range columnParts[1:] {
			if option == "anonymous" {
				anonymous = true
			}
		}

		if columnName == "" {
			if !exported && anonymous == false {
				continue // ignore unexported field if no column specified
			}
			columnName = mi.ToColumnName(f.Name)
		}

		if anonymous {
			var parent string
			if parentColumnName != nil {
				parent = *parentColumnName + "." + columnName
			} else {
				parent = columnName
			}
			f, j := mi.parseStruct(f.Type, &parent)
			for i := range f {
				if f[i].Parent == "" {
					f[i].Parent = parent
				}
				f[i].Exported = false // set to false in case any parent is unexported
			}
			fields = append(fields, f...)
			jsonbColumns = append(jsonbColumns, j...)
			continue
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

		jsonbParts := strings.Split(f.Tag.Get("jsonb"), ",")
		jsonb := mi.ToColumnName(jsonbParts[0])
		strict := false
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
			for _, option := range jsonbParts[1:] {
				if option == "strict" {
					strict = true
				}
			}
		}

		fields = append(fields, Field{
			Name:       f.Name,
			Exported:   exported,
			ColumnName: columnName,
			ColumnType: f.Type.String(),
			JsonName:   jsonName,
			Jsonb:      jsonb,
			DataType:   f.Tag.Get("dataType"),
			Strict:     strict,
		})
	}
	return
}

func (f Field) getFieldValueAddrFromStruct(structValue reflect.Value) interface{} {
	if f.Parent != "" {
		for _, parent := range strings.Split(f.Parent, ".") {
			structValue = structValue.FieldByName(parent)
			if structValue.Kind() == reflect.Ptr {
				structValue = structValue.Elem()
			}
		}
	}
	value := structValue.FieldByName(f.Name)
	if f.Exported {
		return value.Addr().Interface()
	}
	return reflect.NewAt(value.Type(), unsafe.Pointer(value.UnsafeAddr())).Interface()
}
