package psql

import (
	"fmt"
	"strings"
)

type (
	// DeleteSQL can be created with Model.NewSQL().AsDelete()
	DeleteSQL struct {
		*SQL
		sqlConditions
		usingList        string
		outputExpression string
	}
)

// Convert SQL to DeleteSQL.
func (s SQL) AsDelete() *DeleteSQL {
	d := &DeleteSQL{
		SQL: &s,
	}
	d.SQL.main = d
	return d
}

// Delete builds a DELETE statement. You can add extra clause (like WHERE,
// RETURNING) to the statement as the first argument. The rest arguments are
// for any placeholder parameters in the statement.
//
//	var ids []int
//	psql.NewModelTable("reports", conn).Delete().Returning("id").MustQuery(&ids)
func (m Model) Delete() *DeleteSQL {
	return m.NewSQL("").AsDelete()
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
	return s
}

// WHERE adds conditions to DELETE statement from variadic inputs.
//
// The args parameter contains field name, operator, value tuples with each
// tuple consisting of three consecutive elements: the field name as a string,
// an operator symbol as a string (e.g. "=", ">", "<="), and the value to match
// against that field.
//
// To generate a WHERE clause matching multiple fields, use more than one
// set of field/operator/value tuples in the args array. For example,
// WHERE("A", "=", 1, "B", "!=", 2) means "WHERE (A = 1) AND (B != 2)".
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

// Adds RETURNING clause to DELETE FROM statement.
func (s *DeleteSQL) Using(list ...string) *DeleteSQL {
	s.usingList = strings.Join(list, ", ")
	return s
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
