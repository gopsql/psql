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
//  var rowsAffected int
//  m.Update(changes...).Where("user_id = $1", 1).MustExecute(&rowsAffected)
//
// Changes can be a list of field name and value pairs and can also be obtained
// from methods like Changes(), FieldChanges(), Assign(), Bind(), Filter().
//
//  m.Update("FieldA", 123, "FieldB", "other").MustExecute()
//
func (m Model) Update(lotsOfChanges ...interface{}) *UpdateSQL {
	return m.NewSQL("").AsUpdate(lotsOfChanges...).Reload()
}

// Update SQL and values in the UpdateSQL object due to changes of columns and
// conditions.
func (s *UpdateSQL) Reload() *UpdateSQL {
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
			if idx, ok := fieldsIndex[field.Name]; ok { // prevent duplication
				values[idx] = value
				continue
			}
			fields = append(fields, fmt.Sprintf("%s = $%d", field.ColumnName, i))
			fieldsIndex[field.Name] = i - 1
			values = append(values, value)
			i += 1
		}
	}
	for jsonbField, changes := range jsonbFields {
		var field = fmt.Sprintf("COALESCE(%s, '{}'::jsonb)", jsonbField)
		for f, value := range changes {
			field = fmt.Sprintf("jsonb_set(%s, '{%s}', $%d)", field, f.ColumnName, i)
			j, _ := json.Marshal(value)
			values = append(values, string(j))
			i += 1
		}
		fields = append(fields, jsonbField+" = "+field)
	}
	var sql string
	if len(fields) > 0 {
		sql = "UPDATE " + s.model.tableName + " SET " + strings.Join(fields, ", ") + s.where()
	}
	n := s.model.NewSQL(sql, values...)
	s.sql = n.sql
	s.values = n.values
	return s
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
