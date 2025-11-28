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
		changes          []interface{}
		outputExpression string
		conflictTargets  []string
		conflictActions  []string
		updateAll        bool
		updateAllExcept  []string
	}
)

// Convert SQL to InsertSQL. The optional fields will be used in DoUpdateAll().
func (s SQL) AsInsert(changes ...interface{}) *InsertSQL {
	i := &InsertSQL{
		SQL:     &s,
		changes: changes,
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
	return m.NewSQL("").AsInsert(lotsOfChanges...)
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
	s.conflictActions = append(s.conflictActions, expressions...)
	return s
}

// DoUpdateAll is like DoUpdate but update every field.
func (s *InsertSQL) DoUpdateAll() *InsertSQL {
	s.updateAll = true
	return s
}

// DoUpdateAllExcept is like DoUpdateAll but except some field names.
func (s *InsertSQL) DoUpdateAllExcept(fields ...string) *InsertSQL {
	s.updateAll = false
	s.updateAllExcept = append(s.updateAllExcept, fields...)
	return s
}

// Perform operations on the chain.
func (s *InsertSQL) Tap(funcs ...func(*InsertSQL) *InsertSQL) *InsertSQL {
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
func (s *InsertSQL) Explain(target interface{}, options ...string) *InsertSQL {
	s.SQL.Explain(target, options...)
	return s
}

// ExplainAnalyze is a shorthand for Explain(target, "ANALYZE", ...).
// Target can be *string, io.Writer, logger.Logger, func(string), or func(...interface{}).
// Note: The ANALYZE option causes the statement to be actually executed,
// not just planned. The INSERT will actually insert data into the table.
func (s *InsertSQL) ExplainAnalyze(target interface{}, options ...string) *InsertSQL {
	s.SQL.ExplainAnalyze(target, options...)
	return s
}

func (s InsertSQL) String() string {
	sql, _ := s.StringValues()
	return sql
}

func (s *InsertSQL) StringValues() (string, []interface{}) {
	fields := []string{}
	fieldsIndex := map[string]int{}
	numbers := []string{}
	values := []interface{}{}
	jsonbFields := map[string]Changes{}
	i := 1
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
		sql = "INSERT INTO " + s.model.tableName + " (" + strings.Join(fields, ", ") + ") VALUES (" + strings.Join(numbers, ", ") + ")"
	} else {
		sql = s.sql
		for _, v := range s.values {
			sql = strings.Replace(sql, "$?", fmt.Sprintf("$%d", i), 1)
			i += 1
			values = append(values, v)
		}
	}
	if sql != "" {
		if s.conflictTargets != nil {
			var actions []string
			if s.updateAll {
				for _, field := range fields {
					actions = append(actions, field+" = EXCLUDED."+field)
				}
			} else if len(s.updateAllExcept) > 0 {
			outer:
				for _, field := range fields {
					for _, except := range s.updateAllExcept {
						if field == except {
							continue outer
						}
					}
					actions = append(actions, field+" = EXCLUDED."+field)
				}
			}
			if s.conflictActions != nil {
				if actions == nil {
					actions = []string{}
				}
				actions = append(actions, s.conflictActions...)
			}
			if actions != nil {
				action := strings.Join(actions, ", ")
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
		}
		if s.outputExpression != "" {
			sql += " RETURNING " + s.outputExpression
		}
	}
	return s.model.convertValues(sql, values)
}
