package psql

import (
	"context"
	"database/sql"
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
	ErrInvalidTarget       = errors.New("target must be pointer of a struct, slice or map")
	ErrNoConnection        = errors.New("no connection")
	ErrTypeAssertionFailed = errors.New("type assertion failed")
)

type (
	// TxOptions can be used in QueryRowInTransaction or ExecuteInTransaction
	TxOptions struct {
		IsolationLevel string
		Before, After  func(context.Context, db.Tx) error
	}

	// SQL can be created with Model.NewSQL()
	SQL struct {
		main interface {
			String() string
		}
		model  *Model
		sql    string
		values []interface{}
	}

	sqlConditions struct {
		conditions []string
		args       []interface{}
	}

	sqlHavings struct {
		havings []string
	}

	// SelectSQL can be created with Model.NewSQL().AsSelect()
	SelectSQL struct {
		*SQL
		sqlConditions
		sqlHavings
		fields  []string
		join    string
		groupBy string
		orderBy string
		limit   string
		offset  string
	}

	// InsertSQL can be created with Model.NewSQL().AsInsert()
	InsertSQL struct {
		*SQL
		fields           []string
		outputExpression string
		conflictTargets  []string
		conflictActions  []string
	}

	// UpdateSQL can be created with Model.NewSQL().AsUpdate()
	UpdateSQL struct {
		*SQL
		sqlConditions
		changes          []interface{}
		outputExpression string
	}

	// DeleteSQL can be created with Model.NewSQL().AsDelete()
	DeleteSQL struct {
		*SQL
		sqlConditions
		usingList        string
		outputExpression string
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

// Convert SQL to InsertSQL. The optional fields will be used in Select().
func (s SQL) AsSelect(fields ...string) *SelectSQL {
	f := &SelectSQL{
		SQL:    &s,
		fields: fields,
	}
	f.SQL.main = f
	return f
}

// Convert SQL to InsertSQL. The optional fields will be used in DoUpdateAll().
func (s SQL) AsInsert(fields ...string) *InsertSQL {
	i := &InsertSQL{
		SQL:    &s,
		fields: fields,
	}
	i.SQL.main = i
	return i
}

// Convert SQL to UpdateSQL. The optional changes will be used in Reload().
func (s SQL) AsUpdate(changes ...interface{}) *UpdateSQL {
	u := &UpdateSQL{
		SQL:     &s,
		changes: changes,
	}
	u.SQL.main = u
	return u
}

// Convert SQL to DeleteSQL.
func (s SQL) AsDelete() *DeleteSQL {
	d := &DeleteSQL{
		SQL: &s,
	}
	d.SQL.main = d
	return d
}

// Update SQL and values in the DeleteSQL object due to changes of conditions.
func (s *SelectSQL) Reload() *SelectSQL {
	sql := "SELECT " + strings.Join(s.fields, ", ") + " FROM " + s.model.tableName
	if s.join != "" {
		sql += " " + s.join
	}
	sql += s.where()
	if s.groupBy != "" {
		sql += " GROUP BY " + s.groupBy + s.having()
	}
	n := s.model.NewSQL(sql, s.args...)
	s.sql = n.sql
	s.values = n.values
	return s
}

// Create a SELECT query statement with all fields of a Model. Options can be
// funtions like AddTableName or strings like "--no-reset" (use Select instead
// of ResetSelect).
func (s *SelectSQL) Find(options ...interface{}) *SelectSQL {
	fields := []string{}
	for _, field := range s.model.modelFields {
		if field.Jsonb != "" {
			continue
		}
		fields = append(fields, field.ColumnName)
	}
	for _, jsonbField := range s.model.jsonbColumns {
		fields = append(fields, jsonbField)
	}
	var noReset bool
	for _, opts := range options {
		switch f := opts.(type) {
		case fieldsFunc:
			fields = f(fields, s.model.tableName)
		case string:
			if f == "--no-reset" {
				noReset = true
			}
		}
	}
	if noReset {
		return s.Select(fields...)
	}
	return s.ResetSelect(fields...)
}

// Create a UPDATE statement from Where().
func (s *SelectSQL) Update(lotsOfChanges ...interface{}) *UpdateSQL {
	n := s.model.Update(lotsOfChanges...)
	n.conditions = s.conditions
	n.args = s.args
	return n.Reload()
}

// Create a DELETE statement from Where().
func (s *SelectSQL) Delete() *DeleteSQL {
	n := s.model.Delete()
	n.conditions = s.conditions
	n.args = s.args
	return n.Reload()
}

// MustExists is like Exists but panics if existence check operation fails.
// Returns true if record exists, false if not exists.
func (s *SelectSQL) MustExists() bool {
	exists, err := s.Exists()
	if err != nil {
		panic(err)
	}
	return exists
}

// Create and execute a SELECT 1 AS one statement. Returns true if record
// exists, false if not exists.
func (s *SelectSQL) Exists() (exists bool, err error) {
	var ret int
	err = s.ResetSelect("1 AS one").QueryRow(&ret)
	if err == s.model.connection.ErrNoRows() {
		err = nil
		return
	}
	exists = ret == 1
	return
}

// MustCount is like Count but panics if count operation fails.
func (s *SelectSQL) MustCount(optional ...string) int {
	count, err := s.Count(optional...)
	if err != nil {
		panic(err)
	}
	return count
}

// Create and execute a SELECT COUNT(*) statement, return number of rows.
// To count in a different way: Count("COUNT(DISTINCT authors.id)").
func (s *SelectSQL) Count(optional ...string) (count int, err error) {
	var expr string
	if len(optional) > 0 && optional[0] != "" {
		expr = optional[0]
	} else {
		expr = "COUNT(*)"
	}
	err = s.ResetSelect(expr).QueryRow(&count)
	return
}

// Set expressions to SELECT statement.
func (s *SelectSQL) ResetSelect(expressions ...string) *SelectSQL {
	s.fields = expressions
	return s.Reload()
}

// Add expressions to SELECT statement, before any existing jsonb columns.
func (s *SelectSQL) Select(expressions ...string) *SelectSQL {
	var idx int = -1
	for _, column := range s.model.jsonbColumns {
		for i, field := range s.fields {
			if field != column {
				continue
			}
			if idx == -1 || i < idx {
				idx = i
				break
			}
		}
	}
	if idx > -1 {
		s.fields = append(append(append([]string{}, s.fields[:idx]...), expressions...), s.fields[idx:]...)
	} else {
		s.fields = append(s.fields, expressions...)
	}
	return s.Reload()
}

// Adds GROUP BY to SELECT statement.
func (s *SelectSQL) GroupBy(expressions ...string) *SelectSQL {
	s.groupBy = strings.Join(expressions, ", ")
	return s.Reload()
}

// Adds HAVING to SELECT statement.
func (s *SelectSQL) Having(condition string, args ...interface{}) *SelectSQL {
	s.havings = append(s.havings, condition)
	s.args = append(s.args, args...)
	return s.Reload()
}

// Adds ORDER BY to SELECT statement.
func (s *SelectSQL) OrderBy(expressions ...string) *SelectSQL {
	s.orderBy = strings.Join(expressions, ", ")
	return s
}

// Adds LIMIT to SELECT statement.
func (s *SelectSQL) Limit(count interface{}) *SelectSQL {
	if count == nil {
		s.limit = ""
	} else {
		s.limit = fmt.Sprint(count)
	}
	return s
}

// Adds OFFSET to SELECT statement.
func (s *SelectSQL) Offset(start interface{}) *SelectSQL {
	if start == nil {
		s.offset = ""
	} else {
		s.offset = fmt.Sprint(start)
	}
	return s
}

// Adds condition to SELECT statement. Arguments should use positonal
// parameters like $1, $2. If only one argument is provided, "$?" in the
// condition will be replaced with the correct positonal parameter.
func (s *SelectSQL) Where(condition string, args ...interface{}) *SelectSQL {
	s.args = append(s.args, args...)
	if len(args) == 1 {
		condition = strings.Replace(condition, "$?", fmt.Sprintf("$%d", len(s.args)), -1)
	}
	s.conditions = append(s.conditions, condition)
	return s.Reload()
}

// Clears existing JOIN statements and set new JOIN statements.
func (s *SelectSQL) ResetJoin(expressions ...string) *SelectSQL {
	s.join = strings.Join(expressions, " ")
	return s.Reload()
}

// Adds join to SELECT statement.
func (s *SelectSQL) Join(expressions ...string) *SelectSQL {
	if s.join != "" && !strings.HasSuffix(s.join, " ") {
		s.join += " "
	}
	s.join += strings.Join(expressions, " ")
	return s.Reload()
}

// Perform operations on the chain.
func (s *SelectSQL) Tap(funcs ...func(*SelectSQL) *SelectSQL) *SelectSQL {
	for i := range funcs {
		s = funcs[i](s)
	}
	return s
}

func (s *SelectSQL) String() string {
	sql := s.sql
	if s.orderBy != "" {
		sql += " ORDER BY " + s.orderBy
	}
	if s.limit != "" {
		sql += " LIMIT " + s.limit
	}
	if s.offset != "" {
		sql += " OFFSET " + s.offset
	}
	return sql
}

// Adds RETURNING clause to INSERT INTO statement.
func (s *InsertSQL) Returning(expressions ...string) *InsertSQL {
	s.outputExpression = strings.Join(expressions, ", ")
	return s
}

// Used with DoNothing(), DoUpdate() or DoUpdateAll().
func (s *InsertSQL) OnConflict(targets ...string) *InsertSQL {
	s.conflictTargets = append([]string{}, targets...)
	return s
}

// Used with OnConflict(), adds ON CONFLICT DO NOTHING clause to INSERT INTO
// statement.
func (s *InsertSQL) DoNothing() *InsertSQL {
	s.conflictActions = []string{}
	return s
}

// Used with OnConflict(), adds custom expressions ON CONFLICT ... DO UPDATE
// SET ... clause to INSERT INTO statement.
func (s *InsertSQL) DoUpdate(expressions ...string) *InsertSQL {
	for _, expr := range expressions {
		s.conflictActions = append(s.conflictActions, expr)
	}
	return s
}

// DoUpdateAll is like DoUpdate but update every field.
func (s *InsertSQL) DoUpdateAll() *InsertSQL {
	for _, field := range s.fields {
		s.conflictActions = append(s.conflictActions, field+" = EXCLUDED."+field)
	}
	return s
}

// Perform operations on the chain.
func (s *InsertSQL) Tap(funcs ...func(*InsertSQL) *InsertSQL) *InsertSQL {
	for i := range funcs {
		s = funcs[i](s)
	}
	return s
}

func (s InsertSQL) String() string {
	sql := s.sql
	if s.conflictTargets != nil && s.conflictActions != nil {
		action := strings.Join(s.conflictActions, ", ")
		if action == "" {
			action = "DO NOTHING"
		} else {
			action = "DO UPDATE SET " + action
		}
		target := strings.Join(s.conflictTargets, ", ")
		if target != "" && !strings.HasPrefix(target, "(") {
			target = "(" + target + ")"
		}
		if target == "" {
			sql += " ON CONFLICT " + action
		} else {
			sql += " ON CONFLICT " + target + " " + action
		}
	}
	if s.outputExpression != "" {
		sql += " RETURNING " + s.outputExpression
	}
	return sql
}

// Adds RETURNING clause to UPDATE statement.
func (s *UpdateSQL) Returning(expressions ...string) *UpdateSQL {
	s.outputExpression = strings.Join(expressions, ", ")
	return s
}

// Adds condition to UPDATE statement. Arguments should use positonal
// parameters like $1, $2. If only one argument is provided, "$?" in the
// condition will be replaced with the correct positonal parameter.
func (s *UpdateSQL) Where(condition string, args ...interface{}) *UpdateSQL {
	s.args = append(s.args, args...)
	if len(args) == 1 {
		condition = strings.Replace(condition, "$?", fmt.Sprintf("$%d", len(s.args)), -1)
	}
	s.conditions = append(s.conditions, condition)
	return s.Reload()
}

// Perform operations on the chain.
func (s *UpdateSQL) Tap(funcs ...func(*UpdateSQL) *UpdateSQL) *UpdateSQL {
	for i := range funcs {
		s = funcs[i](s)
	}
	return s
}

func (s *UpdateSQL) String() string {
	sql := s.sql
	if s.outputExpression != "" {
		sql += " RETURNING " + s.outputExpression
	}
	return sql
}

// Adds condition to DELETE FROM statement. Arguments should use positonal
// parameters like $1, $2. If only one argument is provided, "$?" in the
// condition will be replaced with the correct positonal parameter.
func (s *DeleteSQL) Where(condition string, args ...interface{}) *DeleteSQL {
	s.args = append(s.args, args...)
	if len(args) == 1 {
		condition = strings.Replace(condition, "$?", fmt.Sprintf("$%d", len(s.args)), -1)
	}
	s.conditions = append(s.conditions, condition)
	return s.Reload()
}

// Adds RETURNING clause to DELETE FROM statement.
func (s *DeleteSQL) Using(list ...string) *DeleteSQL {
	s.usingList = strings.Join(list, ", ")
	return s.Reload()
}

// Adds RETURNING clause to DELETE FROM statement.
func (s *DeleteSQL) Returning(expressions ...string) *DeleteSQL {
	s.outputExpression = strings.Join(expressions, ", ")
	return s
}

// Perform operations on the chain.
func (s *DeleteSQL) Tap(funcs ...func(*DeleteSQL) *DeleteSQL) *DeleteSQL {
	for i := range funcs {
		s = funcs[i](s)
	}
	return s
}

func (s *DeleteSQL) String() string {
	sql := s.sql
	if s.outputExpression != "" {
		sql += " RETURNING " + s.outputExpression
	}
	return sql
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
// If Target must be a pointer to a struct, a slice or a map.
// For use cases, see Find() and Select().
func (s SQL) Query(target interface{}) error {
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
		s.log(s.String(), s.values)
		return mi.scan(rv, s.model.connection.QueryRow(s.String(), s.values...))
	} else if kind == reflect.Map {
		s.log(s.String(), s.values)
		rows, err := s.model.connection.Query(s.String(), s.values...)
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

	s.log(s.String(), s.values)
	rows, err := s.model.connection.Query(s.String(), s.values...)
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
	return s.QueryRowInTransaction(nil, dest...)
}

// MustQueryRowInTransaction is like QueryRowInTransaction but panics if query
// row operation fails.
func (s SQL) MustQueryRowInTransaction(txOpts *TxOptions, dest ...interface{}) {
	if err := s.QueryRowInTransaction(txOpts, dest...); err != nil {
		panic(err)
	}
}

// QueryRowInTransaction is like QueryRow but executes the statement in a
// transaction, you can define IsolationLevel and statements Before and/or
// After it.
func (s SQL) QueryRowInTransaction(txOpts *TxOptions, dest ...interface{}) error {
	return s.execute(actionQueryRow, txOpts, dest...)
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
	return s.ExecuteInTransaction(nil, dest...)
}

// MustExecuteInTransaction is like ExecuteInTransaction but panics if execute
// operation fails.
func (s SQL) MustExecuteInTransaction(txOpts *TxOptions, dest ...interface{}) {
	if err := s.ExecuteInTransaction(txOpts, dest...); err != nil {
		panic(err)
	}
}

// ExecuteInTransaction is like Execute but executes the statement in a
// transaction, you can define IsolationLevel and statements Before and/or
// After it.
func (s SQL) ExecuteInTransaction(txOpts *TxOptions, dest ...interface{}) error {
	return s.execute(actionExecute, txOpts, dest...)
}

// ExecTx executes a query in a transaction without returning any rows. You can
// get number of rows affected by providing pointer of int or int64 to the
// optional dest.
func (s SQL) ExecTx(tx db.Tx, ctx context.Context, dest ...interface{}) (err error) {
	if s.model.connection == nil {
		err = ErrNoConnection
		return
	}
	s.log(s.String(), s.values)
	err = returnRowsAffected(dest)(tx.ExecContext(ctx, s.String(), s.values...))
	return
}

// Query executes the SQL query and returns rows.
func (s SQL) QueryTx(tx db.Tx, ctx context.Context, dest ...interface{}) (rows db.Rows, err error) {
	if s.model.connection == nil {
		err = ErrNoConnection
		return
	}
	s.log(s.String(), s.values)
	rows, err = tx.QueryContext(ctx, s.String(), s.values...)
	return
}

func (s SQL) execute(action int, txOpts *TxOptions, dest ...interface{}) (err error) {
	if s.model.connection == nil {
		err = ErrNoConnection
		return
	}
	if txOpts == nil || (txOpts.Before == nil && txOpts.After == nil) {
		s.log(s.String(), s.values)
		if action == actionQueryRow {
			err = s.model.connection.QueryRow(s.String(), s.values...).Scan(dest...)
			return
		}
		err = returnRowsAffected(dest)(s.model.connection.Exec(s.String(), s.values...))
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
	s.log(s.String(), s.values)
	if action == actionQueryRow {
		err = tx.QueryRowContext(ctx, s.String(), s.values...).Scan(dest...)
	} else {
		err = returnRowsAffected(dest)(tx.ExecContext(ctx, s.String(), s.values...))
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

func (s SQL) log(sql string, args []interface{}) {
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

func (s sqlConditions) where() string {
	return conditionsToStr(s.conditions, " WHERE ")
}

func (s sqlHavings) having() string {
	return conditionsToStr(s.havings, " HAVING ")
}

func conditionsToStr(conds []string, prefix string) (out string) {
	moreThanOne := len(conds) > 1
	for i, conf := range conds {
		if i > 0 {
			out += " AND "
		}
		if moreThanOne {
			out += "(" + conf + ")"
		} else {
			out += conf
		}
	}
	if out != "" {
		out = prefix + out
	}
	return
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
