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
//  var ids []int
//  psql.NewModelTable("reports", conn).Delete().Returning("id").MustQuery(&ids)
func (m Model) Delete() *DeleteSQL {
	return m.NewSQL("").AsDelete().Reload()
}

// Update SQL and values in the DeleteSQL object due to changes of conditions.
func (s *DeleteSQL) Reload() *DeleteSQL {
	sql := "DELETE FROM " + s.model.tableName
	if s.usingList != "" {
		sql += " USING " + s.usingList
	}
	sql += s.where()
	n := s.model.NewSQL(sql, s.args...)
	s.sql = n.sql
	s.values = n.values
	return s
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
