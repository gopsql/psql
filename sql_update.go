package psql

import (
	"encoding/json"
	"fmt"
	"strings"
)

type (
	// UpdateSQL can be created with Model.NewSQL().AsUpdate()
	UpdateSQL struct {
		*SQL
		sqlConditions
		changes          []interface{}
		outputExpression string
	}
)

// Convert SQL to UpdateSQL. The optional changes will be used in Reload().
func (s SQL) AsUpdate(changes ...interface{}) *UpdateSQL {
	u := &UpdateSQL{
		SQL:     &s,
		changes: changes,
	}
	u.SQL.main = u
	return u
}

// Update builds an UPDATE statement with fields and values in the changes.
//
//	var rowsAffected int
//	m.Update(changes...).Where("user_id = $1", 1).MustExecute(&rowsAffected)
//
// Changes can be a list of field name and value pairs and can also be obtained
// from methods like Changes(), FieldChanges(), Assign(), Bind(), Filter().
//
//	m.Update("FieldA", 123, "FieldB", "other").MustExecute()
func (m Model) Update(lotsOfChanges ...interface{}) *UpdateSQL {
	return m.NewSQL("").AsUpdate(lotsOfChanges...)
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
	return s
}

// WHERE adds conditions to UPDATE statement from variadic inputs.
//
// The args parameter contains field name, operator, value tuples with each
// tuple consisting of three consecutive elements: the field name as a string,
// an operator symbol as a string (e.g. "=", ">", "<="), and the value to match
// against that field.
//
// To generate a WHERE clause matching multiple fields, use more than one
// set of field/operator/value tuples in the args array. For example,
// WHERE("A", "=", 1, "B", "!=", 2) means "WHERE (A = 1) AND (B != 2)".
func (s *UpdateSQL) WHERE(args ...interface{}) *UpdateSQL {
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

// Perform operations on the chain.
func (s *UpdateSQL) Tap(funcs ...func(*UpdateSQL) *UpdateSQL) *UpdateSQL {
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
func (s *UpdateSQL) Explain(target interface{}, options ...string) *UpdateSQL {
	s.SQL.Explain(target, options...)
	return s
}

// ExplainAnalyze is a shorthand for Explain(target, "ANALYZE", ...).
// Target can be *string, io.Writer, logger.Logger, func(string), or func(...interface{}).
// Note: The ANALYZE option causes the statement to be actually executed,
// not just planned. The UPDATE will actually modify data in the table.
func (s *UpdateSQL) ExplainAnalyze(target interface{}, options ...string) *UpdateSQL {
	s.SQL.ExplainAnalyze(target, options...)
	return s
}

func (s *UpdateSQL) String() string {
	sql, _ := s.StringValues()
	return sql
}

func (s *UpdateSQL) StringValues() (string, []interface{}) {
	fields := []string{}
	fieldsIndex := map[string]int{}
	values := []interface{}{}
	values = append(values, s.args...)
	jsonbFields := map[string]Changes{}
	i := len(s.args) + 1
	for _, changes := range s.model.getChanges(s.changes) {
		for field, value := range changes {
			if field.Jsonb != "" {
				if _, ok := jsonbFields[field.Jsonb]; !ok {
					jsonbFields[field.Jsonb] = Changes{}
				}
				jsonbFields[field.Jsonb][field] = value
				continue
			}
			if s, ok := value.(String); ok {
				fields = append(fields, fmt.Sprintf("%s = %s", field.ColumnName, s))
				continue
			}
			if idx, ok := fieldsIndex[field.Name]; ok { // prevent duplication
				switch v := value.(type) {
				case stringWithArg:
					str := strings.Replace(v.str, "$?", fmt.Sprintf("$%d", idx+1), -1)
					fields[idx] = fmt.Sprintf("%s = %s", field.ColumnName, str)
					values[idx] = v.arg
				default:
					values[idx] = v
				}
				continue
			}
			switch v := value.(type) {
			case stringWithArg:
				str := strings.Replace(v.str, "$?", fmt.Sprintf("$%d", i), -1)
				fields = append(fields, fmt.Sprintf("%s = %s", field.ColumnName, str))
				fieldsIndex[field.Name] = i - 1
				values = append(values, v.arg)
				i += 1
			default:
				fields = append(fields, fmt.Sprintf("%s = $%d", field.ColumnName, i))
				fieldsIndex[field.Name] = i - 1
				values = append(values, v)
				i += 1
			}
		}
	}
	for jsonbField, changes := range jsonbFields {
		var field = fmt.Sprintf("COALESCE(%s, '{}'::jsonb)", jsonbField)
		for f, value := range changes {
			if s, ok := value.(String); ok {
				field = fmt.Sprintf("jsonb_set(%s, '{%s}', %s)", field, f.ColumnName, s)
				continue
			}
			switch v := value.(type) {
			case stringWithArg:
				str := strings.Replace(v.str, "$?", fmt.Sprintf("$%d", i), -1)
				field = fmt.Sprintf("jsonb_set(%s, '{%s}', %s)", field, f.ColumnName, str)
				values = append(values, v.arg)
				i += 1
			default:
				field = fmt.Sprintf("jsonb_set(%s, '{%s}', $%d)", field, f.ColumnName, i)
				j, _ := json.Marshal(v)
				values = append(values, string(j))
				i += 1
			}
		}
		fields = append(fields, jsonbField+" = "+field)
	}
	var sql string
	if s.sql != "" {
		sql = s.sql
		for _, v := range s.values {
			sql = strings.Replace(sql, "$?", fmt.Sprintf("$%d", i), 1)
			i += 1
			values = append(values, v)
		}
	} else if len(fields) > 0 {
		sql = "UPDATE " + s.model.tableName + " SET " + strings.Join(fields, ", ")
	}
	if sql != "" {
		sql += s.where()
		if s.outputExpression != "" {
			sql += " RETURNING " + s.outputExpression
		}
	}
	return s.model.convertValues(sql, values)
}
