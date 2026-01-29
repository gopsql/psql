package psql

import (
	"encoding/json"
	"fmt"
	"strings"
)

type (
	// InsertSQL represents an INSERT statement builder. Create instances using
	// Model.Insert or SQL.AsInsert.
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

// AsInsert converts a raw SQL statement to an InsertSQL builder with the given
// changes.
func (s SQL) AsInsert(changes ...interface{}) *InsertSQL {
	i := &InsertSQL{
		SQL:     &s,
		changes: changes,
	}
	i.SQL.main = i
	return i
}

// Insert creates an INSERT statement with the given field/value changes.
// Changes can be field name and value pairs, or Changes maps from Filter,
// Permit, etc.
//
//	// Using field/value pairs
//	users.Insert("Name", "Alice", "Email", "alice@example.com").MustExecute()
//
//	// Using Changes from Filter
//	changes := users.Permit("Name", "Email").Filter(input)
//	users.Insert(changes).Returning("id").MustQueryRow(&id)
func (m Model) Insert(lotsOfChanges ...interface{}) *InsertSQL {
	return m.NewSQL("").AsInsert(lotsOfChanges...)
}

// Returning adds a RETURNING clause to retrieve values from inserted rows.
func (s *InsertSQL) Returning(expressions ...string) *InsertSQL {
	s.outputExpression = strings.Join(expressions, ", ")
	return s
}

// OnConflict specifies conflict target columns for upsert operations. Use with
// DoNothing, DoUpdate, or DoUpdateAll.
func (s *InsertSQL) OnConflict(targets ...string) *InsertSQL {
	s.conflictTargets = append([]string{}, targets...)
	return s
}

// DoNothing adds ON CONFLICT DO NOTHING, ignoring rows that conflict. Must be
// used after OnConflict.
func (s *InsertSQL) DoNothing() *InsertSQL {
	s.conflictActions = []string{}
	return s
}

// DoUpdate adds ON CONFLICT DO UPDATE SET with custom expressions. Must be
// used after OnConflict.
func (s *InsertSQL) DoUpdate(expressions ...string) *InsertSQL {
	s.conflictActions = append(s.conflictActions, expressions...)
	return s
}

// DoUpdateAll adds ON CONFLICT DO UPDATE SET for all inserted fields. Must be
// used after OnConflict.
func (s *InsertSQL) DoUpdateAll() *InsertSQL {
	s.updateAll = true
	return s
}

// DoUpdateAllExcept is like DoUpdateAll but excludes specified fields from
// the update.
func (s *InsertSQL) DoUpdateAllExcept(fields ...string) *InsertSQL {
	s.updateAll = false
	s.updateAllExcept = append(s.updateAllExcept, fields...)
	return s
}

// Tap applies transformation functions to this InsertSQL, enabling custom
// method chaining.
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
