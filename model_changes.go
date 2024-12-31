package psql

import (
	"encoding/json"
	"time"
)

type (
	RawChanges map[string]interface{}
	Changes    map[Field]interface{}
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
	String string

	stringWithArg struct {
		str string
		arg interface{}
	}
)

func StringWithArg(str string, arg interface{}) stringWithArg {
	return stringWithArg{
		str: str,
		arg: arg,
	}
}

func (s stringWithArg) String() string {
	return s.str
}

// Convert RawChanges to Changes. Keys are JSON key names. See FieldChanges().
//
//	m := psql.NewModel(struct {
//		Age *int `json:"age"`
//	}{})
//	m.Changes(map[string]interface{}{
//		"age": 99,
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

// Convert RawChanges to Changes. Keys are field names. See Changes().
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

// Helper to add CreatedAt of current time changes.
func (m Model) CreatedAt() Changes {
	return m.Changes(RawChanges{
		"CreatedAt": time.Now().UTC(),
	})
}

// Helper to add UpdatedAt of current time changes.
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
