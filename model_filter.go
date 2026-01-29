package psql

import (
	"encoding/json"
	"io"
	"reflect"
)

type (
	// ModelWithPermittedFields wraps a Model with a whitelist of permitted
	// fields for mass assignment protection. Create instances using Permit or
	// PermitAllExcept, then use Filter to safely extract allowed fields from
	// user input.
	ModelWithPermittedFields struct {
		*Model
		permittedFieldsIdx []int
	}
)

// Permit creates a ModelWithPermittedFields that only allows the specified
// fields in Filter operations. This provides mass assignment protection similar
// to Rails strong parameters. If no field names are provided, no fields are
// permitted.
func (m Model) Permit(fieldNames ...string) *ModelWithPermittedFields {
	idx := []int{}
	for i, field := range m.modelFields {
		for _, fieldName := range fieldNames {
			if fieldName != field.Name {
				continue
			}
			idx = append(idx, i)
			break
		}
	}
	return &ModelWithPermittedFields{&m, idx}
}

// PermitAllExcept creates a ModelWithPermittedFields that allows all fields
// except the specified ones in Filter operations. If no field names are
// provided, all fields are permitted.
func (m Model) PermitAllExcept(fieldNames ...string) *ModelWithPermittedFields {
	idx := []int{}
	for i, field := range m.modelFields {
		found := false
		for _, fieldName := range fieldNames {
			if fieldName == field.Name {
				found = true
				break
			}
		}
		if !found {
			idx = append(idx, i)
		}
	}
	return &ModelWithPermittedFields{&m, idx}
}

// PermittedFields returns the list of field names that are permitted for
// mass assignment.
func (m ModelWithPermittedFields) PermittedFields() (out []string) {
	for _, i := range m.permittedFieldsIdx {
		field := m.modelFields[i]
		out = append(out, field.Name)
	}
	return
}

// MustBind is like Bind but panics if bind operation fails.
func (m ModelWithPermittedFields) MustBind(ctx interface{ Bind(interface{}) error }, target interface{}) Changes {
	c, err := m.Bind(ctx, target)
	if err != nil {
		panic(err)
	}
	return c
}

// Bind extracts permitted fields from an HTTP request using a Bind method
// (compatible with Echo and similar frameworks). Only permitted fields are
// copied to target; other fields retain their zero values. The target must be
// a pointer to a struct.
//
//	func handler(c echo.Context) error {
//		var user User
//		changes, err := users.Permit("Name", "Email").Bind(c, &user)
//		if err != nil {
//			return err
//		}
//		users.Insert(changes).MustExecute()
//		// ...
//	}
func (m ModelWithPermittedFields) Bind(ctx interface{ Bind(interface{}) error }, target interface{}) (Changes, error) {
	rt := reflect.TypeOf(target)
	if rt.Kind() != reflect.Ptr {
		return nil, ErrMustBePointer
	}
	rv := reflect.ValueOf(target).Elem()
	nv := reflect.New(rt.Elem())
	if err := ctx.Bind(nv.Interface()); err != nil {
		return nil, err
	}
	nv = nv.Elem()
	out := Changes{}
	for _, i := range m.permittedFieldsIdx {
		field := m.modelFields[i]
		v := nv.FieldByName(field.Name)
		rv.FieldByName(field.Name).Set(v)
		out[field] = v.Interface()
	}
	return out, nil
}

// Filter extracts only permitted fields from input data, providing mass
// assignment protection. Accepts multiple input types: RawChanges, JSON strings,
// []byte, io.Reader, or structs. Map/JSON keys must match the field's JSON tag
// name. The returned Changes can be passed to Insert or Update.
//
//	// Filter JSON from request body
//	changes := users.Permit("Name", "Email").Filter(requestBody)
//	users.Insert(changes).MustExecute()
//
//	// Filter from multiple sources (later values override earlier)
//	changes := users.Permit("Name").Filter(
//		map[string]interface{}{"name": "Alice"},
//		`{"name": "Bob"}`,
//	) // name will be "Bob"
func (m ModelWithPermittedFields) Filter(inputs ...interface{}) (out Changes) {
	out = Changes{}
	for _, input := range inputs {
		switch in := input.(type) {
		case RawChanges:
			m.filterPermits(in, &out)
		case map[string]interface{}:
			m.filterPermits(in, &out)
		case string:
			var c RawChanges
			if json.Unmarshal([]byte(in), &c) == nil {
				m.filterPermits(c, &out)
			}
		case []byte:
			var c RawChanges
			if json.Unmarshal(in, &c) == nil {
				m.filterPermits(c, &out)
			}
		case io.Reader:
			var c RawChanges
			if json.NewDecoder(in).Decode(&c) == nil {
				m.filterPermits(c, &out)
			}
		default:
			rt := reflect.TypeOf(in)
			if rt.Kind() == reflect.Struct {
				rv := reflect.ValueOf(in)
				fields := map[string]Field{}
				for _, i := range m.permittedFieldsIdx {
					field := m.modelFields[i]
					fields[field.Name] = field
				}
				for i := 0; i < rt.NumField(); i++ {
					if field, ok := fields[rt.Field(i).Name]; ok {
						out[field] = rv.Field(i).Interface()
					}
				}
			}

		}
	}
	return
}

func (m ModelWithPermittedFields) filterPermits(in RawChanges, out *Changes) {
	for _, i := range m.permittedFieldsIdx {
		field := m.modelFields[i]
		if _, ok := in[field.JsonName]; !ok {
			continue
		}
		if m.structType == nil {
			continue
		}
		f, ok := m.structType.FieldByName(field.Name)
		if !ok {
			continue
		}
		v, err := json.Marshal(in[field.JsonName])
		if err != nil {
			continue
		}
		x := reflect.New(f.Type)
		if err := json.Unmarshal(v, x.Interface()); err != nil {
			continue
		}
		(*out)[field] = x.Elem().Interface()
	}
}
