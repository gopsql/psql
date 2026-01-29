package psql

import (
	"fmt"
	"strings"
)

type (
	// DeleteSQL represents a DELETE statement builder. Create instances using
	// Model.Delete or SQL.AsDelete.
	DeleteSQL struct {
		*SQL
		sqlConditions
		usingList        string
		outputExpression string
	}
)

// AsDelete converts a raw SQL statement to a DeleteSQL builder.
func (s SQL) AsDelete() *DeleteSQL {
	d := &DeleteSQL{
		SQL: &s,
	}
	d.SQL.main = d
	return d
}

// Delete creates a DELETE statement. Use Where to specify which rows to delete.
//
//	// Delete with condition
//	users.Delete().Where("id = $1", 1).MustExecute()
//
//	// Delete with RETURNING clause
//	var ids []int
//	users.Delete().Where("status = $1", "inactive").Returning("id").MustQuery(&ids)
func (m Model) Delete() *DeleteSQL {
	return m.NewSQL("").AsDelete()
}

// Where adds a WHERE condition to the DELETE statement. Use $1, $2 for
// positional parameters, or $? which is auto-replaced when a single argument
// is provided.
func (s *DeleteSQL) Where(condition string, args ...interface{}) *DeleteSQL {
	s.args = append(s.args, args...)
	if len(args) == 1 {
		condition = strings.Replace(condition, "$?", fmt.Sprintf("$%d", len(s.args)), -1)
	}
	s.conditions = append(s.conditions, condition)
	return s
}

// WHERE adds conditions from field/operator/value tuples. Each tuple consists
// of three consecutive arguments: field name, operator, and value.
func (s *DeleteSQL) WHERE(args ...interface{}) *DeleteSQL {
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

// Using adds a USING clause for DELETE with joins.
func (s *DeleteSQL) Using(list ...string) *DeleteSQL {
	s.usingList = strings.Join(list, ", ")
	return s
}

// Returning adds a RETURNING clause to retrieve values from deleted rows.
func (s *DeleteSQL) Returning(expressions ...string) *DeleteSQL {
	s.outputExpression = strings.Join(expressions, ", ")
	return s
}

// Tap applies transformation functions to this DeleteSQL, enabling custom
// method chaining.
func (s *DeleteSQL) Tap(funcs ...func(*DeleteSQL) *DeleteSQL) *DeleteSQL {
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
func (s *DeleteSQL) Explain(target interface{}, options ...string) *DeleteSQL {
	s.SQL.Explain(target, options...)
	return s
}

// ExplainAnalyze is a shorthand for Explain(target, "ANALYZE", ...).
// Target can be *string, io.Writer, logger.Logger, func(string), or func(...interface{}).
// Note: The ANALYZE option causes the statement to be actually executed,
// not just planned. The DELETE will actually remove data from the table.
func (s *DeleteSQL) ExplainAnalyze(target interface{}, options ...string) *DeleteSQL {
	s.SQL.ExplainAnalyze(target, options...)
	return s
}

func (s *DeleteSQL) String() string {
	var sql string
	if s.sql != "" {
		sql = s.formattedSQL()
	} else {
		sql = "DELETE FROM " + s.model.tableName
	}
	if sql != "" {
		if s.usingList != "" {
			sql += " USING " + s.usingList
		}
		sql += s.where()
		if s.outputExpression != "" {
			sql += " RETURNING " + s.outputExpression
		}
	}
	return sql
}

func (s *DeleteSQL) StringValues() (string, []interface{}) {
	return s.model.convertValues(s.String(), s.args)
}
