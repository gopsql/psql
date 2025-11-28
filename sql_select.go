package psql

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type (
	// SelectSQL can be created with Model.NewSQL().AsSelect()
	SelectSQL struct {
		*SQL
		sqlConditions
		sqlHavings
		fields  []string
		jfCount int // jsonb fields count
		from    string
		join    string
		with    string
		groupBy string
		orderBy string
		limit   string
		offset  string
	}

	sqlConditions struct {
		conditions []string
		args       []interface{}
	}

	sqlHavings struct {
		havings []string
	}
)

// Convert SQL to SelectSQL. The optional fields will be used in Select().
func (s SQL) AsSelect(fields ...string) *SelectSQL {
	f := &SelectSQL{
		SQL:    &s,
		fields: fields,
	}
	f.SQL.main = f
	return f
}

func (m Model) newSelect(fields ...string) *SelectSQL {
	return m.NewSQL("").AsSelect(fields...)
}

// Create a SELECT query statement with all fields of a Model. If you want to
// use other data type than the type of struct passed in NewModel(), see
// Select().
//
//	// put results into a slice
//	var users []models.User
//	psql.NewModel(models.User{}, conn).Find().MustQuery(&users)
//
//	// put results into a struct
//	var user models.User
//	psql.NewModel(models.User{}, conn).Find().Where("id = $1", 1).MustQuery(&user)
//
// You can pass options to modify Find(). For example, Find(psql.AddTableName)
// adds table name to every field.
func (m Model) Find(options ...interface{}) *SelectSQL {
	return m.newSelect().Find(options...)
}

// Select is like Find but can choose what columns to retrieve.
//
// To put results into a slice of strings:
//
//	var names []string
//	psql.NewModelTable("users", conn).Select("name").OrderBy("id ASC").MustQuery(&names)
//
// To put results into a slice of custom struct:
//
//	var users []struct {
//		name string
//		id   int
//	}
//	psql.NewModelTable("users", conn).Select("name", "id").OrderBy("id ASC").MustQuery(&users)
//
// To group results by the key:
//
//	var id2name map[int]string
//	psql.NewModelTable("users", conn).Select("id", "name").MustQuery(&id2name)
//
// If it is one-to-many, use slice as map's value:
//
//	var users map[[2]string][]struct {
//		id   int
//		name string
//	}
//	psql.NewModelTable("users", conn).Select("country, city, id, name").MustQuery(&users)
func (m Model) Select(fields ...string) *SelectSQL {
	return m.newSelect(fields...)
}

// Create a SELECT query statement with FROM items.
func (m Model) From(items ...string) *SelectSQL {
	return m.newSelect().From(items...)
}

// Create a SELECT query statement with joins.
func (m Model) Join(expressions ...string) *SelectSQL {
	return m.newSelect().Join(expressions...)
}

// Create a SELECT query statement with CTE (Common Table Expression).
func (m Model) With(expression string, args ...interface{}) *SelectSQL {
	return m.newSelect().With(expression, args...)
}

// Create a SELECT query statement with CTE (Common Table Expression).
func (m Model) WITH(name string, sql *SelectSQL) *SelectSQL {
	return m.newSelect().WITH(name, sql)
}

// Create a SELECT query statement with condition. Arguments should use
// positonal parameters like $1, $2. If only one argument is provided, "$?" in
// the condition will be replaced with the correct positonal parameter.
func (m Model) Where(condition string, args ...interface{}) *SelectSQL {
	return m.newSelect().Where(condition, args...)
}

// Create a SELECT query statement with condition.
//
// The args parameter contains field name, operator, value tuples with each
// tuple consisting of three consecutive elements: the field name as a string,
// an operator symbol as a string (e.g. "=", ">", "<="), and the value to match
// against that field.
//
// To generate a WHERE clause matching multiple fields, use more than one
// set of field/operator/value tuples in the args array. For example,
// WHERE("A", "=", 1, "B", "!=", 2) means "WHERE (A = 1) AND (B != 2)".
func (m Model) WHERE(args ...interface{}) *SelectSQL {
	return m.newSelect().WHERE(args...)
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
	s.jfCount = 0
	for _, jsonbField := range s.model.jsonbColumns {
		fields = append(fields, jsonbField)
		s.jfCount += 1
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
	return n
}

// Create a DELETE statement from Where().
func (s *SelectSQL) Delete() *DeleteSQL {
	n := s.model.Delete()
	n.conditions = s.conditions
	n.args = s.args
	return n
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
	return s
}

// Add expressions to SELECT statement, before any existing jsonb columns.
func (s *SelectSQL) Select(expressions ...string) *SelectSQL {
	if s.jfCount > 0 {
		idx := len(s.fields) - s.jfCount
		s.fields = append(append(append([]string{}, s.fields[:idx]...), expressions...), s.fields[idx:]...)
	} else {
		s.fields = append(s.fields, expressions...)
	}
	return s
}

// Replace old field names in existing SELECT statement with new.
func (s *SelectSQL) ReplaceSelect(old, new string) *SelectSQL {
	for i := range s.fields {
		if s.fields[i] == old {
			s.fields[i] = new
		}
	}
	return s
}

// Adds GROUP BY to SELECT statement.
func (s *SelectSQL) GroupBy(expressions ...string) *SelectSQL {
	s.groupBy = strings.Join(expressions, ", ")
	return s
}

// Adds HAVING to SELECT statement. Arguments should use positonal
// parameters like $1, $2. If only one argument is provided, "$?" in the
// condition will be replaced with the correct positonal parameter.
func (s *SelectSQL) Having(condition string, args ...interface{}) *SelectSQL {
	s.args = append(s.args, args...)
	if len(args) == 1 {
		condition = strings.Replace(condition, "$?", fmt.Sprintf("$%d", len(s.args)), -1)
	}
	s.havings = append(s.havings, condition)
	return s
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
	return s
}

// WHERE adds conditions to SELECT statement from variadic inputs.
//
// The args parameter contains field name, operator, value tuples with each
// tuple consisting of three consecutive elements: the field name as a string,
// an operator symbol as a string (e.g. "=", ">", "<="), and the value to match
// against that field.
//
// To generate a WHERE clause matching multiple fields, use more than one
// set of field/operator/value tuples in the args array. For example,
// WHERE("A", "=", 1, "B", "!=", 2) means "WHERE (A = 1) AND (B != 2)".
func (s *SelectSQL) WHERE(args ...interface{}) *SelectSQL {
	for i := 0; i < len(args)/3; i++ {
		var column string
		if c, ok := args[i*3].(string); ok {
			column = c
		}
		var operator string
		if o, ok := args[i*3+1].(string); ok {
			operator = o
		}
		if column == "" || operator == "" {
			continue
		}
		s.args = append(s.args, args[i*3+2])
		s.conditions = append(s.conditions, fmt.Sprintf("%s %s $%d", s.model.ToColumnName(column), operator, len(s.args)))
	}
	return s
}

// Clears existing FROM items and set new FROM items.
func (s *SelectSQL) ResetFrom(items ...string) *SelectSQL {
	s.from = strings.Join(items, ", ")
	return s
}

// Adds FROM items to SELECT statement.
func (s *SelectSQL) From(items ...string) *SelectSQL {
	if s.from == "" {
		s.from = s.model.tableName
	}
	if s.from != "" {
		s.from += ", "
	}
	s.from += strings.Join(items, ", ")
	return s
}

// Clears existing JOIN statements and set new JOIN statements.
func (s *SelectSQL) ResetJoin(expressions ...string) *SelectSQL {
	s.join = strings.Join(expressions, " ")
	return s
}

// Adds join to SELECT statement.
func (s *SelectSQL) Join(expressions ...string) *SelectSQL {
	if s.join != "" && !strings.HasSuffix(s.join, " ") {
		s.join += " "
	}
	s.join += strings.Join(expressions, " ")
	return s
}

// Adds WITH to SELECT statement.
func (s *SelectSQL) With(expression string, args ...interface{}) *SelectSQL {
	i := 1
	for range args {
		expression = strings.Replace(expression, "$?", fmt.Sprintf("$%d", i), 1)
		i += 1
	}
	if offset := len(s.args); offset > 0 {
		re := regexp.MustCompile(`\$(\d+)`)
		expression = re.ReplaceAllStringFunc(expression, func(s string) string {
			num, err := strconv.Atoi(s[1:])
			if err != nil { // this should not happen
				panic(err)
			}
			return fmt.Sprintf("$%d", num+offset)
		})
	}
	if s.with != "" {
		s.with += ", "
	}
	s.with += expression
	s.args = append(s.args, args...)
	return s
}

// Adds WITH from another SELECT statement to SELECT statement.
func (s *SelectSQL) WITH(name string, sql *SelectSQL) *SelectSQL {
	sqlQuery := sql.String()
	if offset := len(s.args); offset > 0 {
		re := regexp.MustCompile(`\$(\d+)`)
		sqlQuery = re.ReplaceAllStringFunc(sqlQuery, func(s string) string {
			num, err := strconv.Atoi(s[1:])
			if err != nil { // this should not happen
				panic(err)
			}
			return fmt.Sprintf("$%d", num+offset)
		})
	}
	if s.with != "" {
		s.with += ", "
	}
	s.with += name + " AS (" + sqlQuery + ")"
	s.args = append(s.args, sql.args...)
	return s
}

// Perform operations on the chain.
func (s *SelectSQL) Tap(funcs ...func(*SelectSQL) *SelectSQL) *SelectSQL {
	for i := range funcs {
		s = funcs[i](s)
	}
	return s
}

// Explain sets up EXPLAIN output collection. When Query, QueryRow, or Execute
// is called, an EXPLAIN statement will be executed first and the result will
// be written to the target. Target can be *string, io.Writer, logger.Logger,
// func(string), or func(...interface{}) (e.g. log.Println).
// Options can include ANALYZE, VERBOSE, BUFFERS, COSTS, TIMING, FORMAT JSON, etc.
func (s *SelectSQL) Explain(target interface{}, options ...string) *SelectSQL {
	s.SQL.Explain(target, options...)
	return s
}

// ExplainAnalyze is a shorthand for Explain(target, "ANALYZE", ...).
// Target can be *string, io.Writer, logger.Logger, func(string), or func(...interface{}).
// Note: The ANALYZE option causes the statement to be actually executed,
// not just planned. Use with caution on INSERT, UPDATE, DELETE statements
// as they will modify your data.
func (s *SelectSQL) ExplainAnalyze(target interface{}, options ...string) *SelectSQL {
	s.SQL.ExplainAnalyze(target, options...)
	return s
}

func (s *SelectSQL) String() string {
	var sql string
	if s.with != "" {
		sql += "WITH " + s.with + " "
	}
	if s.sql != "" {
		sql += s.formattedSQL()
	} else {
		sql += "SELECT " + strings.Join(s.fields, ", ") + " FROM "
		if s.from != "" {
			sql += s.from
		} else {
			sql += s.model.tableName
		}
	}
	if s.join != "" {
		sql += " " + s.join
	}
	sql += s.where()
	if s.groupBy != "" {
		sql += " GROUP BY " + s.groupBy + s.having()
	}
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

func (s *SelectSQL) StringValues() (string, []interface{}) {
	return s.model.convertValues(s.String(), s.args)
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
