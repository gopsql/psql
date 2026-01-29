package psql

import (
	"encoding/json"
	"time"
)

type (
	// RawChanges is a map of string keys to values, used as input to Filter and
	// Changes methods. Keys should match either JSON tag names (for Changes) or
	// struct field names (for FieldChanges).
	RawChanges map[string]interface{}

	// Changes maps Field definitions to their values. It is the output of Filter
	// and the input to Insert and Update operations.
	Changes map[Field]interface{}
)

func (c Changes) MarshalJSON() ([]byte, error) {
	data := map[string]interface{}{}
	for field, value := range c {
		data[field.JsonName] = value
	}
	return json.Marshal(data)
}

func (c Changes) String() string {
	j, _ := json.MarshalIndent(c, "", "  ")
	return string(j)
}

type (
	// String is a raw SQL expression that will not be escaped or parameterized.
	// Use for expressions like "NOW()" or "column + 1".
	String string

	stringWithArg struct {
		str string
		arg interface{}
	}
)

// StringWithArg creates a raw SQL expression with a parameter placeholder.
// The $? in the string will be replaced with the proper positional parameter.
//
//	users.Update("views", psql.StringWithArg("views + $?", 1))
//	// UPDATE users SET views = views + $1
func StringWithArg(str string, arg interface{}) stringWithArg {
	return stringWithArg{
		str: str,
		arg: arg,
	}
}

func (s stringWithArg) String() string {
	return s.str
}

// Changes converts RawChanges to Changes using JSON tag names as keys.
// Use FieldChanges if your keys are struct field names instead.
//
//	changes := users.Changes(map[string]interface{}{
//		"name": "Alice",  // matches `json:"name"` tag
//	})
func (m Model) Changes(in RawChanges) (out Changes) {
	out = Changes{}
	for _, field := range m.modelFields {
		if _, ok := in[field.JsonName]; !ok {
			continue
		}
		out[field] = in[field.JsonName]
	}
	return
}

// FieldChanges converts RawChanges to Changes using struct field names as keys.
// Use Changes if your keys are JSON tag names instead.
func (m Model) FieldChanges(in RawChanges) (out Changes) {
	out = Changes{}
	for _, field := range m.modelFields {
		if _, ok := in[field.Name]; !ok {
			continue
		}
		out[field] = in[field.Name]
	}
	return
}

// CreatedAt returns Changes setting the CreatedAt field to the current UTC time.
func (m Model) CreatedAt() Changes {
	return m.Changes(RawChanges{
		"CreatedAt": time.Now().UTC(),
	})
}

// UpdatedAt returns Changes setting the UpdatedAt field to the current UTC time.
func (m Model) UpdatedAt() Changes {
	return m.Changes(RawChanges{
		"UpdatedAt": time.Now().UTC(),
	})
}

func (m Model) getChanges(in []interface{}) (out []Changes) {
	var key *string = nil
	for _, item := range in {
		if key == nil {
			if s, ok := item.(string); ok {
				key = &s
				continue
			}
			if i, ok := item.(Changes); ok {
				out = append(out, i)
			}
		} else {
			out = append(out, m.FieldChanges(map[string]interface{}{
				*key: item,
			}))
			key = nil
		}
	}
	return
}
