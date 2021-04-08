package psql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unsafe"

	"github.com/gopsql/db"
	"github.com/gopsql/logger"
)

const (
	actionQueryRow = iota
	actionExecute
)

var (
	ErrInvalidTarget       = errors.New("target must be pointer of a struct or pointer of a slice of structs")
	ErrNoConnection        = errors.New("no connection")
	ErrTypeAssertionFailed = errors.New("type assertion failed")
)

type (
	// TxOptions can be used in QueryRowInTransaction or ExecuteInTransaction
	TxOptions struct {
		IsolationLevel string
		Before, After  func(context.Context, db.Tx) error
	}

	// SQLWithValues can be created with Model.NewSQLWithValues(sql, values...)
	SQLWithValues struct {
		model  *Model
		sql    string
		values []interface{}
	}

	jsonbRaw map[string]json.RawMessage
)

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

// Create new SQLWithValues with SQL statement as first argument, The rest
// arguments are for any placeholder parameters in the statement.
func (m Model) NewSQLWithValues(sql string, values ...interface{}) SQLWithValues {
	sql = strings.TrimSpace(sql)
	if c, ok := m.connection.(db.ConvertParameters); ok {
		sql, values = c.ConvertParameters(sql, values)
	}
	return SQLWithValues{
		model:  &m,
		sql:    sql,
		values: values,
	}
}

func (s SQLWithValues) String() string {
	return s.sql
}

// MustQuery is like Query but panics if query operation fails.
func (s SQLWithValues) MustQuery(target interface{}) {
	if err := s.Query(target); err != nil {
		panic(err)
	}
}

// Query executes the SQL query and put the results into the target. If target
// is pointer of a struct, at most one row of the query is returned. If target
// is a pointer of a slice, all rows of the query are returned. If target is a
// pointer of a map, first column in the SELECT list will be the key of the
// map, and the second column is the value of the map. For use cases, see
// Find() and Select().
func (s SQLWithValues) Query(target interface{}) error {
	if s.model.connection == nil {
		return ErrNoConnection
	}

	rt := reflect.TypeOf(target)
	if rt.Kind() != reflect.Ptr {
		return ErrInvalidTarget
	}
	rt = rt.Elem()

	kind := rt.Kind()
	if kind == reflect.Struct { // if target is not a slice, use QueryRow instead
		rv := reflect.Indirect(reflect.ValueOf(target))
		s.log(s.sql, s.values)
		return s.scan(rv, s.model.connection.QueryRow(s.sql, s.values...))
	} else if kind == reflect.Map {
		s.log(s.sql, s.values)
		rows, err := s.model.connection.Query(s.sql, s.values...)
		if err != nil {
			return err
		}
		defer rows.Close()
		rv := reflect.Indirect(reflect.ValueOf(target))
		if rv.IsNil() {
			rv.Set(reflect.MakeMapWithSize(rt, 0))
		}
		mapKeyType := rt.Key()
		mapValueType := rt.Elem()
		for rows.Next() {
			newKey := reflect.New(mapKeyType).Elem()
			newValue := reflect.New(mapValueType).Elem()
			if err := rows.Scan(newKey.Addr().Interface(), newValue.Addr().Interface()); err != nil {
				return err
			}
			rv.SetMapIndex(newKey, newValue)
		}
		return rows.Err()
	} else if kind != reflect.Slice {
		return ErrInvalidTarget
	}

	rt = rt.Elem()
	s.log(s.sql, s.values)
	rows, err := s.model.connection.Query(s.sql, s.values...)
	if err != nil {
		return err
	}
	defer rows.Close()
	v := reflect.Indirect(reflect.ValueOf(target))
	for rows.Next() {
		rv := reflect.New(rt).Elem()
		if err := s.scan(rv, rows); err != nil {
			return err
		}
		v.Set(reflect.Append(v, rv))
	}
	return rows.Err()
}

// scan a scannable (Row or Rows) into every field of a struct
func (s SQLWithValues) scan(rv reflect.Value, scannable db.Scannable) error {
	if rv.Kind() != reflect.Struct || (s.model.structType != nil && rv.Type() != s.model.structType) {
		return scannable.Scan(rv.Addr().Interface())
	}
	f := rv.FieldByName(tableNameField)
	if f.Kind() == reflect.String {
		// hack
		reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem().SetString(s.model.tableName)
	}
	dests := []interface{}{}
	for _, field := range s.model.modelFields {
		if field.Jsonb != "" {
			continue
		}
		f := rv.FieldByName(field.Name)
		var pointer interface{}
		if field.Exported {
			pointer = f.Addr().Interface()
		} else {
			pointer = reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Interface()
		}
		dests = append(dests, pointer)
	}
	jsonbValues := []jsonbRaw{}
	for range s.model.jsonbColumns {
		jsonb := jsonbRaw{}
		dests = append(dests, &jsonb)
		jsonbValues = append(jsonbValues, jsonb)
	}
	if s.model.structType == nil || len(dests) == 0 {
		rt := rv.Type()
		for i := 0; i < rt.NumField(); i++ {
			f := rv.Field(i)
			var pointer interface{}
			if rt.Field(i).PkgPath == "" {
				pointer = f.Addr().Interface()
			} else {
				pointer = reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Interface()
			}
			dests = append(dests, pointer)
		}
	}
	if err := scannable.Scan(dests...); err != nil {
		return err
	}
	for _, jsonb := range jsonbValues {
		for _, field := range s.model.modelFields {
			if field.Jsonb == "" {
				continue
			}
			val, ok := jsonb[field.ColumnName]
			if !ok {
				continue
			}
			f := rv.FieldByName(field.Name)
			var pointer interface{}
			if field.Exported {
				pointer = f.Addr().Interface()
			} else {
				pointer = reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Interface()
			}
			if err := json.Unmarshal(val, pointer); err != nil {
				return err
			}
		}
	}
	return nil
}

// MustQueryRow is like QueryRow but panics if query row operation fails.
func (s SQLWithValues) MustQueryRow(dest ...interface{}) {
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
func (s SQLWithValues) QueryRow(dest ...interface{}) error {
	return s.QueryRowInTransaction(nil, dest...)
}

// MustQueryRowInTransaction is like QueryRowInTransaction but panics if query
// row operation fails.
func (s SQLWithValues) MustQueryRowInTransaction(txOpts *TxOptions, dest ...interface{}) {
	if err := s.QueryRowInTransaction(txOpts, dest...); err != nil {
		panic(err)
	}
}

// QueryRowInTransaction is like QueryRow but executes the statement in a
// transaction, you can define IsolationLevel and statements Before and/or
// After it.
func (s SQLWithValues) QueryRowInTransaction(txOpts *TxOptions, dest ...interface{}) error {
	return s.execute(actionQueryRow, txOpts, dest...)
}

// MustExecute is like Execute but panics if execute operation fails.
func (s SQLWithValues) MustExecute(dest ...interface{}) {
	if err := s.Execute(dest...); err != nil {
		panic(err)
	}
}

// Execute executes a query without returning any rows by an UPDATE, INSERT, or
// DELETE. You can get number of rows affected by providing pointer of int or
// int64 to the optional dest. For use cases, see Update().
func (s SQLWithValues) Execute(dest ...interface{}) error {
	return s.ExecuteInTransaction(nil, dest...)
}

// MustExecuteInTransaction is like ExecuteInTransaction but panics if execute
// operation fails.
func (s SQLWithValues) MustExecuteInTransaction(txOpts *TxOptions, dest ...interface{}) {
	if err := s.ExecuteInTransaction(txOpts, dest...); err != nil {
		panic(err)
	}
}

// ExecuteInTransaction is like Execute but executes the statement in a
// transaction, you can define IsolationLevel and statements Before and/or
// After it.
func (s SQLWithValues) ExecuteInTransaction(txOpts *TxOptions, dest ...interface{}) error {
	return s.execute(actionExecute, txOpts, dest...)
}

// ExecTx executes a query in a transaction without returning any rows. You can
// get number of rows affected by providing pointer of int or int64 to the
// optional dest.
func (s SQLWithValues) ExecTx(tx db.Tx, ctx context.Context, dest ...interface{}) (err error) {
	if s.model.connection == nil {
		err = ErrNoConnection
		return
	}
	s.log(s.sql, s.values)
	err = returnRowsAffected(dest)(tx.ExecContext(ctx, s.sql, s.values...))
	return
}

// Query executes the SQL query and returns rows.
func (s SQLWithValues) QueryTx(tx db.Tx, ctx context.Context, dest ...interface{}) (rows db.Rows, err error) {
	if s.model.connection == nil {
		err = ErrNoConnection
		return
	}
	s.log(s.sql, s.values)
	rows, err = tx.QueryContext(ctx, s.sql, s.values...)
	return
}

func (s SQLWithValues) execute(action int, txOpts *TxOptions, dest ...interface{}) (err error) {
	if s.model.connection == nil {
		err = ErrNoConnection
		return
	}
	if txOpts == nil || (txOpts.Before == nil && txOpts.After == nil) {
		s.log(s.sql, s.values)
		if action == actionQueryRow {
			err = s.model.connection.QueryRow(s.sql, s.values...).Scan(dest...)
			return
		}
		err = returnRowsAffected(dest)(s.model.connection.Exec(s.sql, s.values...))
		return
	}
	ctx := context.Background()
	s.log("BEGIN", nil)
	var tx db.Tx
	tx, err = s.model.connection.BeginTx(ctx, txOpts.IsolationLevel)
	if err != nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			s.log("ROLLBACK", nil)
			tx.Rollback(ctx)
			err = errors.New(fmt.Sprint(r))
		} else if err != nil {
			s.log("ROLLBACK", nil)
			tx.Rollback(ctx)
		} else {
			s.log("COMMIT", nil)
			err = tx.Commit(ctx)
		}
	}()
	if txOpts.Before != nil {
		err = txOpts.Before(ctx, tx)
		if err != nil {
			return
		}
	}
	s.log(s.sql, s.values)
	if action == actionQueryRow {
		err = tx.QueryRowContext(ctx, s.sql, s.values...).Scan(dest...)
	} else {
		err = returnRowsAffected(dest)(tx.ExecContext(ctx, s.sql, s.values...))
	}
	if err != nil {
		return
	}
	if txOpts.After != nil {
		err = txOpts.After(ctx, tx)
		if err != nil {
			return
		}
	}
	return
}

func (s SQLWithValues) log(sql string, args []interface{}) {
	if s.model.logger == nil {
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
		s.model.logger.Debug(colored)
		return
	}
	s.model.logger.Debug(colored, args)
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
