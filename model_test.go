package psql

import (
	"strings"
	"testing"
	"time"
)

type (
	test struct {
		*testing.T
		i int
	}

	userAlike struct {
		Id       int
		Name     string
		Password string
	}

	admin struct {
		userAlike
	}

	user struct {
		userAlike
		Phone string
	}

	category struct {
		Id        int
		Names     []map[string]string `jsonb:"meta"`
		Picture   string              `jsonb:"meta"`
		CreatedAt time.Time
		UpdatedAt time.Time
	}
)

func TestModel(_t *testing.T) {
	t := test{_t, 0}

	m0 := NewModelSlim(admin{})
	t.String(m0.tableName, "admins")
	t.Int(len(m0.modelFields), 0)
	p := m0.Permit("Id")
	t.Int(len(p.PermittedFields()), 0)

	m1 := NewModel(admin{})
	t.String(m1.FieldByName("Name").ColumnName, "name")
	t.Nil(m1.FieldByName("name"), (*Field)(nil))
	t.String(m1.tableName, "admins")
	t.Int(len(m1.modelFields), 3)
	p = m1.Permit()
	t.Int(len(p.PermittedFields()), 0)
	p = m1.Permit("Invalid")
	t.Int(len(p.PermittedFields()), 0)
	p = m1.Permit("Id")
	t.Int(len(p.PermittedFields()), 1)
	p = m1.Permit("Id", "Id")
	t.Int(len(p.PermittedFields()), 1)
	t.Int(len(p.Filter(RawChanges{
		"Id":   1,
		"Name": "haha",
	})), 1)
	t.Int(len(p.Filter(`{
		"Id":   1,
		"Name": "haha"
	}`)), 1)
	t.Int(len(p.Filter([]byte(`{
		"Id":   1,
		"Name": "haha"
	}`))), 1)
	t.Int(len(p.Filter(strings.NewReader(`{
		"Id":   1,
		"Name": "haha"
	}`))), 1)
	t.Int(len(p.Filter(struct {
		Id   int
		Name string
	}{})), 1)
	p = m1.Permit()
	t.Int(len(p.PermittedFields()), 0)
	t.Int(len(p.Filter(map[string]interface{}{
		"Name": "haha",
	})), 0)
	p = m1.PermitAllExcept("Password")
	t.Int(len(p.PermittedFields()), 2)
	p = m1.PermitAllExcept("Password", "Password")
	t.Int(len(p.PermittedFields()), 2)
	t.Int(len(p.Filter(map[string]interface{}{
		"Name":     "haha",
		"Password": "reset",
		"BadData":  "foobar",
	})), 1)
	p = m1.PermitAllExcept()
	t.Int(len(p.PermittedFields()), 3)
	p = m1.PermitAllExcept("Invalid")
	t.Int(len(p.PermittedFields()), 3)
	p = m1.Permit()
	c := m1.Changes(RawChanges{
		"Name":    "test",
		"BadData": "foobar",
	})
	t.Int(len(c), 1)
	var f Field
	for _f := range c {
		f = _f
		break
	}
	t.String(f.Name, "Name")
	t.String(m1.Find().String(), "SELECT id, name, password FROM admins")
	t.String(m1.Delete().String(), "DELETE FROM admins")
	t.String(m1.Delete("WHERE id = $1", 1).String(),
		"DELETE FROM admins WHERE id = $1")
	t.String(m1.Insert(c)().String(), "INSERT INTO admins (name) VALUES ($1)")
	t.String(m1.Update(c)().String(), "UPDATE admins SET name = $1")
	t.String(m1.Update(c)("WHERE id = $1", 1).String(),
		"UPDATE admins SET name = $2 WHERE id = $1")

	m2 := NewModel(category{})
	t.String(m2.tableName, "categories")
	p = m2.Permit("Names", "Picture")
	t.Int(len(p.PermittedFields()), 2)
	m2c := m2.Changes(RawChanges{
		"Picture": "https://hello/world",
	})
	t.String(m2.Insert(m2c)().String(), "INSERT INTO categories (meta) VALUES ($1)")
	t.String(m2.Insert(m2c)().values[0].(string), `{"picture":"https://hello/world"}`)
	t.String(m2.Update(m2c)().String(), "UPDATE categories SET meta = jsonb_set(COALESCE(meta, '{}'::jsonb), '{picture}', $1)")
	t.String(m2.Update(m2c)().values[0].(string), `"https://hello/world"`)
	t.String(m2.Update(m2c)("WHERE id = $1", 1).String(),
		"UPDATE categories SET meta = jsonb_set(COALESCE(meta, '{}'::jsonb), '{picture}', $2) WHERE id = $1")
	m2c2 := m2.Changes(RawChanges{
		"Names": []map[string]string{
			{
				"key":   "en_US",
				"value": "Category",
			},
		},
	})
	t.String(m2.Insert(m2c2)().String(), "INSERT INTO categories (meta) VALUES ($1)")
	t.String(m2.Insert(m2c2)().values[0].(string), `{"names":[{"key":"en_US","value":"Category"}]}`)
	t.String(m2.Update(m2c2)().String(), "UPDATE categories SET meta = jsonb_set(COALESCE(meta, '{}'::jsonb), '{names}', $1)")
	t.String(m2.Update(m2c2)().values[0].(string), `[{"key":"en_US","value":"Category"}]`)
	t.String(m2.Insert(
		m2c2,
		m2.CreatedAt(),
		m2.UpdatedAt(),
	)().String(), "INSERT INTO categories (created_at, updated_at, meta) VALUES ($1, $2, $3)")
	t.String(m2.Update(
		m2c2,
		m2.CreatedAt(),
		m2.UpdatedAt(),
	)().String(), "UPDATE categories SET created_at = $1, updated_at = $2, meta = jsonb_set(COALESCE(meta, '{}'::jsonb), '{names}', $3)")

	m3 := NewModel(user{})
	t.String(m3.tableName, "users")
	t.Int(len(m3.modelFields), 4)
}

func (t *test) String(got, expected string) {
	t.Helper()
	if got == expected {
		t.Logf("case %d passed", t.i)
	} else {
		t.Errorf("case %d failed, got %s", t.i, got)
	}
	t.i++
}

func (t *test) Int(got, expected int) {
	t.Helper()
	if got == expected {
		t.Logf("case %d passed", t.i)
	} else {
		t.Errorf("case %d failed, got %d", t.i, got)
	}
	t.i++
}

func (t *test) Nil(got, expected interface{}) {
	t.Helper()
	if got == expected {
		t.Logf("case %d passed", t.i)
	} else {
		t.Errorf("case %d failed, not nil: %+v", t.i, got)
	}
	t.i++
}
