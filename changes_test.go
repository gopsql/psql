package psql

import (
	"strings"
	"testing"
	"time"
)

// Test struct for Changes tests
type changesTestStruct struct {
	Id        int
	Name      string `json:"name"`
	Email     string `json:"email"`
	Age       int    `json:"age"`
	CreatedAt time.Time
	UpdatedAt time.Time
}

func TestChanges(t *testing.T) {
	t.Parallel()
	m := NewModel(changesTestStruct{})

	tests := []struct {
		name      string
		input     RawChanges
		wantCount int
		wantField string
	}{
		{
			name:      "single field by json name",
			input:     RawChanges{"name": "test"},
			wantCount: 1,
			wantField: "Name",
		},
		{
			name:      "multiple fields",
			input:     RawChanges{"name": "test", "email": "test@example.com"},
			wantCount: 2,
		},
		{
			name:      "invalid field is ignored",
			input:     RawChanges{"invalid": "test"},
			wantCount: 0,
		},
		{
			name:      "mixed valid and invalid",
			input:     RawChanges{"name": "test", "invalid": "ignored"},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			changes := m.Changes(tt.input)
			if len(changes) != tt.wantCount {
				t.Errorf("len(Changes()) = %d, want %d", len(changes), tt.wantCount)
			}
			if tt.wantField != "" {
				found := false
				for f := range changes {
					if f.Name == tt.wantField {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Changes() missing field %q", tt.wantField)
				}
			}
		})
	}
}

func TestFieldChanges(t *testing.T) {
	t.Parallel()
	m := NewModel(changesTestStruct{})

	tests := []struct {
		name      string
		input     RawChanges
		wantCount int
		wantField string
	}{
		{
			name:      "single field by field name",
			input:     RawChanges{"Name": "test"},
			wantCount: 1,
			wantField: "Name",
		},
		{
			name:      "multiple fields",
			input:     RawChanges{"Name": "test", "Email": "test@example.com"},
			wantCount: 2,
		},
		{
			name:      "json name does not work",
			input:     RawChanges{"name": "test"},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			changes := m.FieldChanges(tt.input)
			if len(changes) != tt.wantCount {
				t.Errorf("len(FieldChanges()) = %d, want %d", len(changes), tt.wantCount)
			}
		})
	}
}

func TestPermit(t *testing.T) {
	t.Parallel()
	m := NewModel(changesTestStruct{})

	tests := []struct {
		name          string
		permitFields  []string
		wantPermitted int
	}{
		{
			name:          "no fields permitted",
			permitFields:  []string{},
			wantPermitted: 0,
		},
		{
			name:          "single field",
			permitFields:  []string{"Name"},
			wantPermitted: 1,
		},
		{
			name:          "multiple fields",
			permitFields:  []string{"Name", "Email"},
			wantPermitted: 2,
		},
		{
			name:          "duplicate field",
			permitFields:  []string{"Name", "Name"},
			wantPermitted: 1,
		},
		{
			name:          "invalid field",
			permitFields:  []string{"Invalid"},
			wantPermitted: 0,
		},
		{
			name:          "mixed valid and invalid",
			permitFields:  []string{"Name", "Invalid"},
			wantPermitted: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := m.Permit(tt.permitFields...)
			permitted := p.PermittedFields()
			if len(permitted) != tt.wantPermitted {
				t.Errorf("len(PermittedFields()) = %d, want %d", len(permitted), tt.wantPermitted)
			}
		})
	}
}

func TestPermitAllExcept(t *testing.T) {
	t.Parallel()
	m := NewModel(changesTestStruct{})
	totalFields := len(m.modelFields)

	tests := []struct {
		name          string
		exceptFields  []string
		wantPermitted int
	}{
		{
			name:          "no exceptions",
			exceptFields:  []string{},
			wantPermitted: totalFields,
		},
		{
			name:          "single exception",
			exceptFields:  []string{"Name"},
			wantPermitted: totalFields - 1,
		},
		{
			name:          "multiple exceptions",
			exceptFields:  []string{"Name", "Email"},
			wantPermitted: totalFields - 2,
		},
		{
			name:          "duplicate exception",
			exceptFields:  []string{"Name", "Name"},
			wantPermitted: totalFields - 1,
		},
		{
			name:          "invalid exception",
			exceptFields:  []string{"Invalid"},
			wantPermitted: totalFields,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := m.PermitAllExcept(tt.exceptFields...)
			permitted := p.PermittedFields()
			if len(permitted) != tt.wantPermitted {
				t.Errorf("len(PermittedFields()) = %d, want %d", len(permitted), tt.wantPermitted)
			}
		})
	}
}

func TestFilter(t *testing.T) {
	t.Parallel()
	m := NewModel(changesTestStruct{})

	t.Run("from RawChanges", func(t *testing.T) {
		p := m.Permit("Name")
		changes := p.Filter(RawChanges{
			"name":  "test",
			"email": "should be filtered",
		})
		if len(changes) != 1 {
			t.Errorf("len(Filter()) = %d, want 1", len(changes))
		}
	})

	t.Run("from map[string]interface{}", func(t *testing.T) {
		p := m.Permit("Name")
		changes := p.Filter(map[string]interface{}{
			"name": "test",
		})
		if len(changes) != 1 {
			t.Errorf("len(Filter()) = %d, want 1", len(changes))
		}
	})

	t.Run("from JSON string", func(t *testing.T) {
		p := m.Permit("Name", "Age")
		changes := p.Filter(`{"name": "test", "age": 25}`)
		if len(changes) != 2 {
			t.Errorf("len(Filter()) = %d, want 2", len(changes))
		}
	})

	t.Run("from JSON bytes", func(t *testing.T) {
		p := m.Permit("Name")
		changes := p.Filter([]byte(`{"name": "test"}`))
		if len(changes) != 1 {
			t.Errorf("len(Filter()) = %d, want 1", len(changes))
		}
	})

	t.Run("from io.Reader", func(t *testing.T) {
		p := m.Permit("Name")
		changes := p.Filter(strings.NewReader(`{"name": "test"}`))
		if len(changes) != 1 {
			t.Errorf("len(Filter()) = %d, want 1", len(changes))
		}
	})

	t.Run("from struct", func(t *testing.T) {
		type inputStruct struct {
			Name  string
			Email string
		}
		p := m.Permit("Name")
		changes := p.Filter(inputStruct{Name: "test", Email: "ignored"})
		if len(changes) != 1 {
			t.Errorf("len(Filter()) = %d, want 1", len(changes))
		}
	})

	t.Run("multiple inputs", func(t *testing.T) {
		p := m.Permit("Name", "Email")
		changes := p.Filter(
			RawChanges{"name": "first"},
			RawChanges{"name": "second", "email": "test@example.com"},
		)
		if len(changes) != 2 {
			t.Errorf("len(Filter()) = %d, want 2", len(changes))
		}
		// The last value should win
		for f, v := range changes {
			if f.Name == "Name" && v != "second" {
				t.Errorf("Name value = %v, want 'second'", v)
			}
		}
	})

	t.Run("no permitted fields filters everything", func(t *testing.T) {
		p := m.Permit()
		changes := p.Filter(RawChanges{"name": "test"})
		if len(changes) != 0 {
			t.Errorf("len(Filter()) = %d, want 0", len(changes))
		}
	})

	t.Run("invalid JSON is ignored", func(t *testing.T) {
		p := m.Permit("Name")
		changes := p.Filter("invalid json")
		if len(changes) != 0 {
			t.Errorf("len(Filter()) = %d, want 0", len(changes))
		}
	})
}

func TestCreatedAt(t *testing.T) {
	t.Parallel()
	m := NewModel(changesTestStruct{})

	before := time.Now().UTC()
	changes := m.CreatedAt()
	after := time.Now().UTC()

	if len(changes) != 1 {
		t.Fatalf("len(CreatedAt()) = %d, want 1", len(changes))
	}

	var createdAt time.Time
	for f, v := range changes {
		if f.Name != "CreatedAt" {
			t.Errorf("field name = %q, want 'CreatedAt'", f.Name)
		}
		createdAt = v.(time.Time)
	}

	if createdAt.Before(before) || createdAt.After(after) {
		t.Errorf("CreatedAt value not within expected range")
	}
}

func TestUpdatedAt(t *testing.T) {
	t.Parallel()
	m := NewModel(changesTestStruct{})

	before := time.Now().UTC()
	changes := m.UpdatedAt()
	after := time.Now().UTC()

	if len(changes) != 1 {
		t.Fatalf("len(UpdatedAt()) = %d, want 1", len(changes))
	}

	var updatedAt time.Time
	for f, v := range changes {
		if f.Name != "UpdatedAt" {
			t.Errorf("field name = %q, want 'UpdatedAt'", f.Name)
		}
		updatedAt = v.(time.Time)
	}

	if updatedAt.Before(before) || updatedAt.After(after) {
		t.Errorf("UpdatedAt value not within expected range")
	}
}

func TestChangesMarshalJSON(t *testing.T) {
	t.Parallel()
	m := NewModel(changesTestStruct{})

	changes := m.Changes(RawChanges{"name": "test"})
	jsonBytes, err := changes.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON() error = %v", err)
	}

	json := string(jsonBytes)
	if !strings.Contains(json, `"name":"test"`) {
		t.Errorf("MarshalJSON() = %s, want to contain '\"name\":\"test\"'", json)
	}
}

func TestChangesString(t *testing.T) {
	t.Parallel()
	m := NewModel(changesTestStruct{})

	changes := m.Changes(RawChanges{"name": "test"})
	str := changes.String()
	if !strings.Contains(str, "name") {
		t.Errorf("String() = %s, want to contain 'name'", str)
	}
}

func TestStringWithArg(t *testing.T) {
	t.Parallel()

	swa := StringWithArg("name || $?", "suffix")
	if got := swa.String(); got != "name || $?" {
		t.Errorf("String() = %q, want %q", got, "name || $?")
	}
}

func TestAssign(t *testing.T) {
	t.Parallel()
	m := NewModel(changesTestStruct{})

	t.Run("assigns values to struct", func(t *testing.T) {
		var target changesTestStruct
		changes, err := m.Assign(&target, m.FieldChanges(RawChanges{"Name": "assigned"}))
		if err != nil {
			t.Fatalf("Assign() error = %v", err)
		}
		if target.Name != "assigned" {
			t.Errorf("target.Name = %q, want 'assigned'", target.Name)
		}
		if len(changes) != 1 {
			t.Errorf("len(changes) = %d, want 1", len(changes))
		}
	})

	t.Run("multiple changes", func(t *testing.T) {
		var target changesTestStruct
		_, err := m.Assign(&target,
			m.FieldChanges(RawChanges{"Name": "first"}),
			m.FieldChanges(RawChanges{"Name": "second", "Email": "test@example.com"}),
		)
		if err != nil {
			t.Fatalf("Assign() error = %v", err)
		}
		if target.Name != "second" {
			t.Errorf("target.Name = %q, want 'second'", target.Name)
		}
		if target.Email != "test@example.com" {
			t.Errorf("target.Email = %q, want 'test@example.com'", target.Email)
		}
	})

	t.Run("non-pointer returns error", func(t *testing.T) {
		var target changesTestStruct
		_, err := m.Assign(target, m.FieldChanges(RawChanges{"Name": "test"}))
		if err != ErrMustBePointer {
			t.Errorf("Assign() error = %v, want %v", err, ErrMustBePointer)
		}
	})
}

func TestMustAssign(t *testing.T) {
	t.Parallel()
	m := NewModel(changesTestStruct{})

	t.Run("assigns values to struct", func(t *testing.T) {
		var target changesTestStruct
		m.MustAssign(&target, m.FieldChanges(RawChanges{"Name": "assigned"}))
		if target.Name != "assigned" {
			t.Errorf("target.Name = %q, want 'assigned'", target.Name)
		}
	})

	t.Run("panics on error", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("MustAssign() did not panic")
			}
		}()
		var target changesTestStruct
		m.MustAssign(target, m.FieldChanges(RawChanges{"Name": "test"}))
	})
}
