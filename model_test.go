package psql

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func init() {
	DefaultColumnNamer = ToUnderscore
}

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

	product struct {
		Id    int    `json:"id"`
		Name  string `json:"name"`
		Price int    `json:"PRICE"`
	}
)

func TestModel(_t *testing.T) {
	t := test{_t, 0}

	m1 := NewModel(admin{})
	t.String(strings.Join(m1.Columns(), ","), "id,name,password")
	t.String(m1.ToColumnName("FooBar"), "foo_bar")
	m1.SetColumnNamer(nil)
	t.String(strings.Join(m1.Columns(), ","), "Id,Name,Password")
	t.String(m1.ToColumnName("FooBar"), "FooBar")
	m1.SetColumnNamer(DefaultColumnNamer)
	t.String(m1.FieldByName("Name").ColumnName, "name")
	t.Nil(m1.FieldByName("name"), (*Field)(nil))
	t.String(m1.tableName, "admins")
	t.Int(len(m1.modelFields), 3)
	p := m1.Permit()
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
	t.String(m1.Find(AddTableName).String(), "SELECT admins.id, admins.name, admins.password FROM admins")
	t.String(m1.Find().Where("id = $1", 1).String(), "SELECT id, name, password FROM admins WHERE id = $1")
	t.String(m1.Find().Select("name").String(), "SELECT id, name, password, name FROM admins")
	t.String(m1.Select("id").Where("id = $? AND id = $?", 1).String(), "SELECT id FROM admins WHERE id = $1 AND id = $1")
	t.String(m1.Select("id").Where("id = $? AND id = $?", 1, 2).String(), "SELECT id FROM admins WHERE id = $? AND id = $?")
	t.String(m1.Select("id").Where("id = $?", 1).Where("name = $?", "a").String(), "SELECT id FROM admins WHERE (id = $1) AND (name = $2)")
	t.String(m1.Select("name").Find().String(), "SELECT id, name, password FROM admins")
	t.String(m1.Select("name").Find("--no-reset").String(), "SELECT name, id, name, password FROM admins")
	t.String(m1.Where("id = $1", 1).Find().String(), "SELECT id, name, password FROM admins WHERE id = $1")
	t.String(m1.Where("id = $1", 1).Find(AddTableName).String(), "SELECT admins.id, admins.name, admins.password FROM admins WHERE id = $1")
	t.String(m1.Select("id", "name").String(), "SELECT id, name FROM admins")
	t.String(m1.Select("id", "name").Where("status = $1", "new").String(), "SELECT id, name FROM admins WHERE status = $1")
	t.String(m1.Where("status = $1", "new").Select("id", "name").String(), "SELECT id, name FROM admins WHERE status = $1")
	t.String(m1.Select("id").OrderBy("id DESC").String(), "SELECT id FROM admins ORDER BY id DESC")
	t.String(m1.Select("id").OrderBy("id DESC").Limit(2).String(), "SELECT id FROM admins ORDER BY id DESC LIMIT 2")
	t.String(m1.Select("id").OrderBy("id DESC").Limit(2).Limit(nil).String(), "SELECT id FROM admins ORDER BY id DESC")
	t.String(m1.Select("id").Offset("10").Limit(2).String(), "SELECT id FROM admins LIMIT 2 OFFSET 10")
	t.String(m1.Select("array_agg(id)").GroupBy("name", "status").String(), "SELECT array_agg(id) FROM admins GROUP BY name, status")
	t.String(m1.Select("sum(price)").Where("id > $1", 1).GroupBy("kind").Having("sum(price) < $2", 3).String(),
		"SELECT sum(price) FROM admins WHERE id > $1 GROUP BY kind HAVING sum(price) < $2")
	t.String(m1.Select("id").Join("JOIN a ON a.id = admins.id").String(), "SELECT id FROM admins JOIN a ON a.id = admins.id")
	t.String(m1.Select("id").Join("JOIN a ON a.id = admins.id").Join("JOIN b ON b.id = admins.id").String(),
		"SELECT id FROM admins JOIN a ON a.id = admins.id JOIN b ON b.id = admins.id")
	t.String(m1.Select("id").Join("JOIN a ON a.id = admins.id").ResetJoin("JOIN b ON b.id = admins.id").String(),
		"SELECT id FROM admins JOIN b ON b.id = admins.id")
	t.String(m1.Delete().String(), "DELETE FROM admins")
	t.String(m1.Delete().Returning("id").String(), "DELETE FROM admins RETURNING id")
	t.String(m1.Delete().Using("users", "orders").
		Where("admins.user_id = users.id").Where("admins.order_id = orders.id").
		Where("orders.name = $1", "foobar").Returning("admins.id").String(),
		"DELETE FROM admins USING users, orders WHERE (admins.user_id = users.id) "+
			"AND (admins.order_id = orders.id) AND (orders.name = $1) RETURNING admins.id")
	t.String(m1.Where("id = $1", 1).Delete().String(), "DELETE FROM admins WHERE id = $1")
	t.String(m1.Delete().Where("id = $1", 1).String(), "DELETE FROM admins WHERE id = $1")
	t.String(m1.Delete().Where("id = $1", 1).Where("name = $2", "foobar").String(),
		"DELETE FROM admins WHERE (id = $1) AND (name = $2)")
	t.String(m1.Insert(c).String(), "INSERT INTO admins (name) VALUES ($1)")
	t.String(m1.Insert(c).Returning("id").String(), "INSERT INTO admins (name) VALUES ($1) RETURNING id")
	t.String(m1.Insert(c).Returning("id AS foobar", "name").String(), "INSERT INTO admins (name) VALUES ($1) RETURNING id AS foobar, name")
	t.String(m1.Insert(c).OnConflict().String(), "INSERT INTO admins (name) VALUES ($1)")
	t.String(m1.Insert(c).DoNothing().String(), "INSERT INTO admins (name) VALUES ($1)")
	t.String(m1.Insert(c).OnConflict().DoNothing().String(), "INSERT INTO admins (name) VALUES ($1) ON CONFLICT DO NOTHING")
	t.String(m1.Insert(c).DoNothing().OnConflict().String(), "INSERT INTO admins (name) VALUES ($1) ON CONFLICT DO NOTHING")
	t.String(m1.Insert(c).Returning("id").OnConflict().DoNothing().String(),
		"INSERT INTO admins (name) VALUES ($1) ON CONFLICT DO NOTHING RETURNING id")
	t.String(m1.Insert(c).OnConflict("name").DoNothing().String(),
		"INSERT INTO admins (name) VALUES ($1) ON CONFLICT (name) DO NOTHING")
	t.String(m1.Insert(c).OnConflict("lower(name)").DoNothing().String(),
		"INSERT INTO admins (name) VALUES ($1) ON CONFLICT (lower(name)) DO NOTHING")
	t.String(m1.Insert(c).OnConflict("(name) WHERE TRUE").DoNothing().String(),
		"INSERT INTO admins (name) VALUES ($1) ON CONFLICT (name) WHERE TRUE DO NOTHING")
	t.String(m1.Insert(c).OnConflict("name", "password").DoNothing().String(),
		"INSERT INTO admins (name) VALUES ($1) ON CONFLICT (name, password) DO NOTHING")
	t.String(m1.Insert(c).OnConflict("name").DoUpdate("password = NULL").String(),
		"INSERT INTO admins (name) VALUES ($1) ON CONFLICT (name) DO UPDATE SET password = NULL")
	t.String(m1.Insert(c).OnConflict("name").DoUpdateAll().String(),
		"INSERT INTO admins (name) VALUES ($1) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name")
	t.String(m1.Insert(c).OnConflict("name").DoUpdateAll().DoUpdate("password = NULL").String(),
		"INSERT INTO admins (name) VALUES ($1) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name, password = NULL")
	t.String(m1.Update(c).String(), "UPDATE admins SET name = $1")
	t.String(m1.Update(c).Returning("id").String(), "UPDATE admins SET name = $1 RETURNING id")
	t.String(m1.Where("id = $1", 1).Update(c).String(),
		"UPDATE admins SET name = $2 WHERE id = $1")
	t.String(m1.Update(c).Where("id = $1", 1).String(),
		"UPDATE admins SET name = $2 WHERE id = $1")
	t.String(m1.Update(c).Where("name = $1", "foo").Where("id = $2", 1).String(),
		"UPDATE admins SET name = $3 WHERE (name = $1) AND (id = $2)")

	m2 := NewModel(category{})
	t.String(m2.Find().String(), "SELECT id, created_at, updated_at, meta FROM categories")
	t.String(m2.Find().Select("id").String(), "SELECT id, created_at, updated_at, id, meta FROM categories")
	t.String(m2.Find(AddTableName).Select("id").String(),
		"SELECT categories.id, categories.created_at, categories.updated_at, id, categories.meta FROM categories")
	t.String(m2.TypeName(), "category")
	t.String(m2.tableName, "categories")
	p = m2.Permit("Names", "Picture")
	t.Int(len(p.PermittedFields()), 2)
	m2c := m2.Changes(RawChanges{
		"Picture": "https://hello/world",
	})
	t.String(m2.Insert(m2c).String(), "INSERT INTO categories (meta) VALUES ($1)")
	t.String(m2.Insert(m2c).values[0].(string), `{"picture":"https://hello/world"}`)
	t.String(m2.Update(m2c).String(), "UPDATE categories SET meta = jsonb_set(COALESCE(meta, '{}'::jsonb), '{picture}', $1)")
	t.String(m2.Update(m2c).values[0].(string), `"https://hello/world"`)
	t.String(m2.Update(m2c).Where("id = $1", 1).String(),
		"UPDATE categories SET meta = jsonb_set(COALESCE(meta, '{}'::jsonb), '{picture}', $2) WHERE id = $1")
	m2c2 := m2.Changes(RawChanges{
		"Names": []map[string]string{
			{
				"key":   "en_US",
				"value": "Category",
			},
		},
	})
	t.String(m2.Insert(m2c2).String(), "INSERT INTO categories (meta) VALUES ($1)")
	t.String(m2.Insert(m2c2).values[0].(string), `{"names":[{"key":"en_US","value":"Category"}]}`)
	t.String(m2.Update(m2c2).String(), "UPDATE categories SET meta = jsonb_set(COALESCE(meta, '{}'::jsonb), '{names}', $1)")
	t.String(m2.Update(m2c2).values[0].(string), `[{"key":"en_US","value":"Category"}]`)
	t.String(m2.Insert(
		m2c2,
		m2.CreatedAt(),
		m2.UpdatedAt(),
	).String(), "INSERT INTO categories (created_at, updated_at, meta) VALUES ($1, $2, $3)")
	t.String(m2.Update(
		m2c2,
		m2.CreatedAt(),
		m2.UpdatedAt(),
	).String(), "UPDATE categories SET created_at = $1, updated_at = $2, meta = jsonb_set(COALESCE(meta, '{}'::jsonb), '{names}', $3)")

	m3 := NewModel(user{})
	t.String(m3.TypeName(), "user")
	t.String(m3.tableName, "users")
	t.Int(len(m3.modelFields), 4)

	m4 := NewModel(product{})
	t.String(m4.TypeName(), "product")
	t.String(m4.tableName, "products")
	t.Int(len(m4.modelFields), 3)
	x0 := "INSERT INTO products (name, price) VALUES ($1, $2)"
	x1 := m4.Insert(
		m4.Changes(RawChanges{"name": "test"}),
		m4.Changes(RawChanges{"PRICE": 2}),
	)
	t.String(x1.String(), x0)
	x2 := m4.Insert(
		m4.FieldChanges(RawChanges{"Name": "test"}),
		m4.FieldChanges(RawChanges{"Price": 2}),
	)
	t.String(x2.String(), x0)
	x3 := m4.Insert("Name", "test", "Price", 2)
	t.String(x3.String(), x0)
	x4 := m4.Insert(
		"PRICE", 1,
		m4.Changes(RawChanges{
			"name": "test",
		}),
		"Price", 2,
	)
	t.String(x4.String(), x0)
	x5 := m4.Insert(
		"Name", "foobar",
		"Price", 2, 3, 4,
		"Price", 10,
	)
	t.String(x5.String(), x0)
	t.String(fmt.Sprint(x5.values), "[foobar 10]")
	x6 := m4.Insert(
		m4.FieldChanges(RawChanges{"Name": "foobar"}),
		m4.FieldChanges(RawChanges{"Price": 10}),
	)
	t.String(x6.String(), x5.String())
	t.String(fmt.Sprint(x6.values), fmt.Sprint(x5.values))
	x7 := m4.Update(
		"Price", 1,
	)
	t.String(x7.String(), "UPDATE products SET price = $1")
	var pp product
	m4.MustAssign(
		&pp,
		"Price", 100,
	)
	t.Int(pp.Price, 100)
}

type (
	dataTypeTest struct {
		Test0 string
		Test1 string
		Test2 string `dataType:"test"`
		Test3 string `dataType:"hello"`
	}
)

func (dataTypeTest) DataType(m Model, fieldName string) string {
	if fieldName == "Test1" {
		return "foo"
	}
	if fieldName == "Test3" {
		return "world"
	}
	return ""
}

func TestDataType(_t *testing.T) {
	t := test{_t, 0}
	m := NewModel(dataTypeTest{})
	dataTypes := m.ColumnDataTypes()
	t.String(dataTypes["test0"], "text DEFAULT ''::text NOT NULL")
	t.String(dataTypes["test1"], "foo")
	t.String(dataTypes["test2"], "test")
	t.String(dataTypes["test3"], "world")
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
