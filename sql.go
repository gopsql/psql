package psql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"unsafe"

	"github.com/gopsql/db"
)

var (
	ErrInvalidTarget       = errors.New("target must be pointer of a struct, slice or map")
	ErrNoConnection        = errors.New("no connection")
	ErrTypeAssertionFailed = errors.New("type assertion failed")
)

type (
	// SQL can be created with Model.NewSQL()
	SQL struct {
		main interface {
			String() string
		}
		model  *Model
		sql    string
		values []interface{}
	}

	jsonbRaw map[string]json.RawMessage

	fieldsFunc = func([]string, string) []string
)

// Can be used in Find(), add table name to all field names.
var AddTableName fieldsFunc = func(fields []string, tableName string) (out []string) {
	for _, field := range fields {
		if strings.Contains(field, ".") {
			out = append(out, field)
			continue
		}
		out = append(out, tableName+"."+field)
	}
	return
}

func (j *jsonbRaw) Scan(src interface{}) error { // necessary for github.com/lib/pq
	if src == nil {
		return nil
	}
	source, ok := src.([]byte)
	if !ok {
		return ErrTypeAssertionFailed
	}
	return json.Unmarshal(source, j)
}

// Create new SQL with SQL statement as first argument, The rest
// arguments are for any placeholder parameters in the statement.
func (m Model) NewSQL(sql string, values ...interface{}) *SQL {
	sql = strings.TrimSpace(sql)
	if c, ok := m.connection.(db.ConvertParameters); ok {
		sql, values = c.ConvertParameters(sql, values)
	}
	return &SQL{
		model:  &m,
		sql:    sql,
		values: values,
	}
}

// Perform operations on the chain.
func (s *SQL) Tap(funcs ...func(*SQL) *SQL) *SQL {
	for i := range funcs {
		s = funcs[i](s)
	}
	return s
}

func (s SQL) String() string {
	if s.main != nil {
		return s.main.String()
	}
	return s.sql
}

// MustQuery is like Query but panics if query operation fails.
func (s SQL) MustQuery(target interface{}) {
	if err := s.Query(target); err != nil {
		panic(err)
	}
}

// Query executes the SQL query and put the results into the target.
// Target must be a pointer to a struct, a slice or a map.
// For use cases, see Find() and Select().
func (s SQL) Query(target interface{}) error {
	return s.QueryCtxTx(context.Background(), nil, target)
}

// MustQueryCtxTx is like QueryCtxTx but panics if query operation fails.
func (s SQL) MustQueryCtxTx(ctx context.Context, tx db.Tx, target interface{}) {
	if err := s.QueryCtxTx(ctx, tx, target); err != nil {
		panic(err)
	}
}

// QueryCtxTx executes the SQL query and put the results into the target.
// Target must be a pointer to a struct, a slice or a map.
// For use cases, see Find() and Select().
func (s SQL) QueryCtxTx(ctx context.Context, tx db.Tx, target interface{}) error {
	sqlQuery := s.String()
	if sqlQuery == "" {
		return nil
	}

	if s.model.connection == nil {
		return ErrNoConnection
	}

	var rv reflect.Value
	var rt reflect.Type

	targetIsRV := false
	switch v := target.(type) {
	case *reflect.Value:
		rv = *v
		targetIsRV = true
	case reflect.Value:
		rv = v
		targetIsRV = true
	}

	if targetIsRV {
		rt = rv.Type()
		if rt.Kind() == reflect.Ptr {
			rv = reflect.Indirect(rv)
			rt = rv.Type()
		}
		if !rv.CanAddr() {
			return ErrInvalidTarget
		}
	} else {
		rv = reflect.Indirect(reflect.ValueOf(target))
		rt = reflect.TypeOf(target)
		if rt.Kind() != reflect.Ptr {
			return ErrInvalidTarget
		}
		rt = rt.Elem()
	}

	kind := rt.Kind()
	if kind == reflect.Slice {
		rt = rt.Elem()
	}

	var mi *modelInfo
	if s.model.structType != nil && rt == s.model.structType {
		// use model's existing info if type is the same
		mi = s.model.modelInfo
	} else {
		// different type of struct
		mi = &modelInfo{tableName: s.model.tableName}
		mi.modelFields, mi.jsonbColumns = parseStruct(rt)
	}

	if kind == reflect.Struct { // if target is not a slice, use QueryRow instead
		s.log(sqlQuery, s.values)
		if tx != nil {
			return mi.scan(rv, tx.QueryRowContext(ctx, sqlQuery, s.values...))
		}
		return mi.scan(rv, s.model.connection.QueryRow(sqlQuery, s.values...))
	} else if kind == reflect.Map {
		s.log(sqlQuery, s.values)
		var rows db.Rows
		var err error
		if tx != nil {
			rows, err = tx.QueryContext(ctx, sqlQuery, s.values...)
		} else {
			rows, err = s.model.connection.Query(sqlQuery, s.values...)
		}
		if err != nil {
			return err
		}

		defer rows.Close()
		columns, _ := rows.Columns()
		columnLen := len(columns)
		if rv.IsNil() {
			rv.Set(reflect.MakeMapWithSize(rt, 0))
		}
		mapKeyType, mapValueType := rt.Key(), rt.Elem()
		isSlice := mapValueType.Kind() == reflect.Slice
		valueTypes := mapValueTypes(rt)
		for rows.Next() {
			mapKeys, end, dests := newDestsForMapType(mapKeyType, mapValueType, columnLen)
			if err := rows.Scan(dests...); err != nil {
				return err
			}
			if isSlice {
				slice := rv.MapIndex(mapKeys[0])
				if !slice.IsValid() {
					slice = reflect.MakeSlice(valueTypes[0], 0, 0)
				}
				rv.SetMapIndex(mapKeys[0], reflect.Append(slice, end))
				continue
			}
			subMap := rv
			i := 0
			for ; i < len(mapKeys)-1; i++ { // map[type]map...
				if !subMap.MapIndex(mapKeys[i]).IsValid() {
					subMap.SetMapIndex(mapKeys[i], reflect.MakeMap(valueTypes[i]))
				}
				subMap = subMap.MapIndex(mapKeys[i])
			}
			subMap.SetMapIndex(mapKeys[i], end)
		}
		return rows.Err()
	} else if kind != reflect.Slice {
		return ErrInvalidTarget
	}

	s.log(sqlQuery, s.values)
	var rows db.Rows
	var err error
	if tx != nil {
		rows, err = tx.QueryContext(ctx, sqlQuery, s.values...)
	} else {
		rows, err = s.model.connection.Query(sqlQuery, s.values...)
	}
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		nv := reflect.New(rt).Elem()
		if err := mi.scan(nv, rows); err != nil {
			return err
		}
		rv.Set(reflect.Append(rv, nv))
	}
	return rows.Err()
}

// scan a scannable (Row or Rows) into every field of a struct
func (mi *modelInfo) scan(rv reflect.Value, scannable db.Scannable) error {
	if rv.Kind() != reflect.Struct || (len(mi.modelFields) == 0 && len(mi.jsonbColumns) == 0) {
		return scannable.Scan(rv.Addr().Interface())
	}
	f := rv.FieldByName(tableNameField)
	if f.Kind() == reflect.String {
		// hack
		reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem().SetString(mi.tableName)
	}
	dests := []interface{}{}
	for _, field := range mi.modelFields {
		if field.Jsonb != "" {
			continue
		}
		pointer := field.getFieldValueAddrFromStruct(rv)
		dests = append(dests, pointer)
	}
	jsonbValues := []jsonbRaw{}
	for range mi.jsonbColumns {
		jsonb := jsonbRaw{}
		dests = append(dests, &jsonb)
		jsonbValues = append(jsonbValues, jsonb)
	}
	if err := scannable.Scan(dests...); err != nil {
		return err
	}
	for _, jsonb := range jsonbValues {
		for _, field := range mi.modelFields {
			if field.Jsonb == "" {
				continue
			}
			val, ok := jsonb[field.ColumnName]
			if !ok {
				continue
			}
			pointer := field.getFieldValueAddrFromStruct(rv)
			if err := json.Unmarshal(val, pointer); err != nil {
				return err
			}
		}
	}
	return nil
}

// MustQueryRow is like QueryRow but panics if query row operation fails.
func (s SQL) MustQueryRow(dest ...interface{}) {
	if err := s.QueryRow(dest...); err != nil {
		panic(err)
	}
}

// QueryRow gets results from the first row, and put values of each column to
// corresponding dest. For use cases, see Insert().
//  var u struct {
//  	name string
//  	id   int
//  }
//  psql.NewModelTable("users", conn).Select("name, id").MustQueryRow(&u.name, &u.id)
func (s SQL) QueryRow(dest ...interface{}) error {
	return s.QueryRowCtxTx(context.Background(), nil, dest...)
}

// MustQueryRowCtxTx is like QueryRowCtxTx but panics if query row operation
// fails.
func (s SQL) MustQueryRowCtxTx(ctx context.Context, tx db.Tx, dest ...interface{}) {
	if err := s.QueryRowCtxTx(ctx, tx, dest...); err != nil {
		panic(err)
	}
}

// QueryRowCtxTx gets results from the first row, and put values of each column
// to corresponding dest. For use cases, see Insert().
func (s SQL) QueryRowCtxTx(ctx context.Context, tx db.Tx, dest ...interface{}) error {
	sqlQuery := s.String()
	if sqlQuery == "" {
		return nil
	}
	if s.model.connection == nil {
		return ErrNoConnection
	}
	s.log(sqlQuery, s.values)
	if tx != nil {
		return tx.QueryRowContext(ctx, sqlQuery, s.values...).Scan(dest...)
	}
	return s.model.connection.QueryRow(sqlQuery, s.values...).Scan(dest...)
}

// MustExecute is like Execute but panics if execute operation fails.
func (s SQL) MustExecute(dest ...interface{}) {
	if err := s.Execute(dest...); err != nil {
		panic(err)
	}
}

// Execute executes a query without returning any rows by an UPDATE, INSERT, or
// DELETE. You can get number of rows affected by providing pointer of int or
// int64 to the optional dest. For use cases, see Update().
func (s SQL) Execute(dest ...interface{}) error {
	return s.ExecuteCtxTx(context.Background(), nil, dest...)
}

// MustExecuteCtxTx is like ExecuteCtxTx but panics if execute operation fails.
func (s SQL) MustExecuteCtxTx(ctx context.Context, tx db.Tx, dest ...interface{}) {
	if err := s.ExecuteCtxTx(ctx, tx, dest...); err != nil {
		panic(err)
	}
}

// ExecuteCtxTx executes a query without returning any rows by an UPDATE,
// INSERT, or DELETE. You can get number of rows affected by providing pointer
// of int or int64 to the optional dest. For use cases, see Update().
func (s SQL) ExecuteCtxTx(ctx context.Context, tx db.Tx, dest ...interface{}) error {
	sqlQuery := s.String()
	if sqlQuery == "" {
		return nil
	}
	if s.model.connection == nil {
		return ErrNoConnection
	}
	s.log(sqlQuery, s.values)
	if tx != nil {
		return returnRowsAffected(dest)(tx.ExecContext(ctx, sqlQuery, s.values...))
	}
	return returnRowsAffected(dest)(s.model.connection.Exec(sqlQuery, s.values...))
}

func (s SQL) log(sql string, args []interface{}) {
	s.model.log(sql, args)
}

func returnRowsAffected(dest []interface{}) func(db.Result, error) error {
	return func(result db.Result, err error) error {
		if err != nil {
			return err
		}
		if len(dest) == 0 {
			return nil
		}
		ra, err := result.RowsAffected()
		if err != nil {
			return err
		}
		switch x := dest[0].(type) {
		case *int:
			*x = int(ra)
		case *int64:
			*x = ra
		}
		return nil
	}
}

// Get all element types of a map recursively, for example:
// mapValueTypes(reflect.TypeOf(map[string]map[int]map[bool]int{})) returns:
// [ map[int]map[bool]int, map[bool]int, int ]
func mapValueTypes(mapType reflect.Type) (types []reflect.Type) {
	if mapType.Kind() != reflect.Map {
		return
	}
	mapValueType := mapType.Elem()
	types = append(types, mapValueType)
	types = append(types, mapValueTypes(mapValueType)...)
	return
}

// Make new destination pointers from map type for Scannable. The "end" is the
// last non-map type value. Map keys are paths to the "end" value.
func newDestsForMapType(mapKeyType, mapValueType reflect.Type, columnLen int) (mapKeys []reflect.Value, end reflect.Value, dests []interface{}) {
	isSlice := mapValueType.Kind() == reflect.Slice
	if isSlice {
		mapValueType = mapValueType.Elem()
	}
	newMapKey := reflect.New(mapKeyType).Elem()
	newMapVal := reflect.New(mapValueType).Elem()
	switch mapKeyType.Kind() {
	case reflect.Struct:
		for i := 0; i < columnLen && i < mapKeyType.NumField(); i++ {
			dests = append(dests, getAddrOfStructField(mapKeyType.Field(i), newMapKey.Field(i)))
		}
	case reflect.Array:
		for i := 0; i < columnLen && i < mapKeyType.Len(); i++ {
			dests = append(dests, newMapKey.Index(i).Addr().Interface())
		}
	default:
		dests = append(dests, newMapKey.Addr().Interface())
	}
	mapKeys = append(mapKeys, newMapKey)
	end = newMapVal
	size := columnLen - len(dests)
	switch mapValueType.Kind() {
	case reflect.Struct:
		if size == 1 {
			if dest, ok := newMapVal.Addr().Interface().(sql.Scanner); ok {
				dests = append(dests, dest)
				return
			}
		}
		for i := 0; i < size; i++ {
			dests = append(dests, getAddrOfStructField(mapValueType.Field(i), newMapVal.Field(i)))
		}
	case reflect.Map:
		if isSlice {
			// can't handle this kind of data structure at the moment
			panic("sorry, but map[type][]map... is not yet supported")
		}
		k, e, d := newDestsForMapType(mapValueType.Key(), mapValueType.Elem(), size)
		mapKeys = append(mapKeys, k...)
		end = e
		dests = append(dests, d...)
	case reflect.Slice:
		newMapVal.Set(reflect.MakeSlice(reflect.SliceOf(mapValueType.Elem()), size, size))
		fallthrough
	case reflect.Array:
		for i := 0; i < size; i++ {
			dests = append(dests, newMapVal.Index(i).Addr().Interface())
		}
	default:
		if size > 0 {
			dests = append(dests, newMapVal.Addr().Interface())
		}
	}
	return
}

func getAddrOfStructField(field reflect.StructField, value reflect.Value) interface{} {
	if field.PkgPath == "" {
		return value.Addr().Interface()
	}
	return reflect.NewAt(value.Type(), unsafe.Pointer(value.UnsafeAddr())).Interface()
}
