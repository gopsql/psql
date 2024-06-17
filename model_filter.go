package psql

import (
	"encoding/json"
	"io"
	"reflect"
)

type (
	ModelWithPermittedFields struct {
		*Model
		permittedFieldsIdx []int
	}
)

// Permits list of field names of a Model to limit Filter() which fields should
// be allowed for mass updating. If no field names are provided ("Permit()"),
// no fields are permitted.
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

// Permits all available fields except provided of a Model to limit Filter()
// which fields should be allowed for mass updating. If no field names are
// provided ("PermitAllExcept()"), all available fields are permitted.
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

// Returns list of permitted field names.
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

// Bind data of permitted fields to target structure using echo.Context#Bind
// function. The "target" must be a pointer to struct.
//
//	// request with ?name=x&age=10
//	func list(c echo.Context) error {
//		obj := struct {
//			Name string `query:"name"`
//			Age  int    `query:"age"`
//		}{}
//		m := psql.NewModel(obj)
//		fmt.Println(m.Permit("Name").Bind(c, &obj))
//		fmt.Println(obj) // "Name" is "x" and "Age" is 0 (default), because only "Name" is permitted to change
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

// Filter keeps data of permitted fields set by Permit() from multiple inputs.
// Inputs can be RawChanges (map[string]interface{}) or JSON-encoded data
// (string, []byte or io.Reader), their keys must be fields' JSON names. Input
// can also be a struct. The "Changes" outputs can be arguments for Insert() or
// Update().
//
//	m := psql.NewModel(struct {
//		Age *int `json:"age"`
//	}{})
//	m.Permit("Age").Filter(
//		psql.RawChanges{
//			"age": 10,
//		},
//		map[string]interface{}{
//			"age": 20,
//		},
//		`{"age": 30}`,
//		[]byte(`{"age": 40}`),
//		strings.NewReader(`{"age": 50}`),
//		struct{ Age int }{60},
//	) // Age is 60
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
