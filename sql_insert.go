package psql

import (
	"encoding/json"
	"fmt"
	"strings"
)

type (
	// InsertSQL can be created with Model.NewSQL().AsInsert()
	InsertSQL struct {
		*SQL
		fields           []string
		outputExpression string
		conflictTargets  []string
		conflictActions  []string
	}
)

// Convert SQL to InsertSQL. The optional fields will be used in DoUpdateAll().
func (s SQL) AsInsert(fields ...string) *InsertSQL {
	i := &InsertSQL{
		SQL:    &s,
		fields: fields,
	}
	i.SQL.main = i
	return i
}

// Insert builds an INSERT INTO statement with fields and values in the
// changes.
//
//	var id int
//	m.Insert(changes...).Returning("id").MustQueryRow(&id)
//
// Changes can be a list of field name and value pairs and can also be obtained
// from methods like Changes(), FieldChanges(), Assign(), Bind(), Filter().
//
//	m.Insert("FieldA", 123, "FieldB", "other").MustExecute()
func (m Model) Insert(lotsOfChanges ...interface{}) *InsertSQL {
	fields := []string{}
	fieldsIndex := map[string]int{}
	numbers := []string{}
	values := []interface{}{}
	jsonbFields := map[string]Changes{}
	i := 1
	for _, changes := range m.getChanges(lotsOfChanges) {
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
			fields = append(fields, field.ColumnName)
			fieldsIndex[field.Name] = i - 1
			numbers = append(numbers, fmt.Sprintf("$%d", i))
			values = append(values, value)
			i += 1
		}
	}
	for jsonbField, changes := range jsonbFields {
		fields = append(fields, jsonbField)
		numbers = append(numbers, fmt.Sprintf("$%d", i))
		out := map[string]interface{}{}
		for field, value := range changes {
			out[field.ColumnName] = value
		}
		j, _ := json.Marshal(out)
		values = append(values, string(j))
		i += 1
	}
	var sql string
	if len(fields) > 0 {
		sql = "INSERT INTO " + m.tableName + " (" + strings.Join(fields, ", ") + ") VALUES (" + strings.Join(numbers, ", ") + ")"
	}
	return m.NewSQL(sql, values...).AsInsert(fields...)
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

// DoUpdateAllExcept is like DoUpdateAll but except some field names.
func (s *InsertSQL) DoUpdateAllExcept(fields ...string) *InsertSQL {
outer:
	for _, field := range s.fields {
		for _, f := range fields {
			if f == field {
				continue outer
			}
		}
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
		if sql == "" {
			return sql
		}
		if target == "" {
			sql += " ON CONFLICT " + action
		} else {
			sql += " ON CONFLICT " + target + " " + action
		}
	}
	if s.outputExpression != "" {
		if sql == "" {
			return sql
		}
		sql += " RETURNING " + s.outputExpression
	}
	return sql
}
