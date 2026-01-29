// Package psql provides a PostgreSQL ORM-like query builder for Go.
//
// # Overview
//
// Package psql maps Go structs to PostgreSQL tables and provides fluent APIs
// for building and executing SELECT, INSERT, UPDATE, and DELETE queries.
// It supports multiple PostgreSQL drivers (pq, pgx, go-pg) and can be switched
// at runtime.
//
// Key features include:
//   - Model-based CRUD operations with automatic column name inference
//   - JSONB column support for storing multiple fields in a single column
//   - Mass assignment protection via Permit and Filter
//   - Query results scanning into structs, slices, and maps
//   - Transaction support with context
//   - Schema generation from struct definitions
//
// # Basic Usage
//
// Create a model from a struct and perform CRUD operations:
//
//	type User struct {
//		Id        int
//		Name      string
//		Email     string
//		CreatedAt time.Time
//	}
//
//	// Initialize model with database connection
//	users := psql.NewModel(User{}, conn)
//
//	// Insert a record
//	var id int
//	users.Insert("Name", "Alice", "Email", "alice@example.com").
//		Returning("id").MustQueryRow(&id)
//
//	// Find records
//	var user User
//	users.Find().Where("id = $1", id).MustQuery(&user)
//
//	// Update a record
//	users.Update("Name", "Bob").Where("id = $1", id).MustExecute()
//
//	// Delete a record
//	users.Delete().Where("id = $1", id).MustExecute()
//
// # Table and Column Naming
//
// Table names are derived from the struct name and converted to plural form
// by default (e.g., User becomes users). You can customize this by:
//   - Adding a __TABLE_NAME__ field with a tag specifying the table name
//   - Implementing a TableName() string method on the struct
//   - Setting DefaultTableNamer to a custom function
//
// Column names are derived from struct field names. Customize with:
//   - The "column" struct tag
//   - SetColumnNamer for model-specific naming
//   - DefaultColumnNamer for global naming
//
// # JSONB Support
//
// Store multiple struct fields in a single JSONB column using the "jsonb" tag:
//
//	type Product struct {
//		Id       int
//		Name     string
//		Price    int    `jsonb:"metadata"`  // stored in metadata column
//		Currency string `jsonb:"metadata"`  // stored in metadata column
//	}
//
// The JSONB column (metadata) is automatically created with appropriate defaults.
// When querying, JSONB fields are automatically unmarshaled into their struct fields.
//
// # Mass Assignment Protection
//
// Use Permit and Filter to safely handle user input, similar to Rails strong
// parameters:
//
//	// Only allow Name and Email to be set from user input
//	changes := users.Permit("Name", "Email").Filter(userInput)
//	users.Insert(changes).MustExecute()
//
// Filter accepts multiple input types: map[string]interface{}, JSON strings,
// []byte, io.Reader, or structs.
//
// # Query Results
//
// Query results can be scanned into various target types:
//
//	// Single struct
//	var user User
//	users.Find().Where("id = $1", 1).MustQuery(&user)
//
//	// Slice of structs
//	var userList []User
//	users.Find().MustQuery(&userList)
//
//	// Slice of single values
//	var ids []int
//	users.Select("id").MustQuery(&ids)
//
//	// Map for key-value pairs
//	var id2name map[int]string
//	users.Select("id", "name").MustQuery(&id2name)
//
//	// Map with struct keys/values for grouping
//	var byDept map[int][]User
//	users.Select("department_id", "id", "name").MustQuery(&byDept)
//
// # Schema Generation
//
// Generate CREATE TABLE statements from struct definitions:
//
//	fmt.Println(users.Schema())
//	// CREATE TABLE users (
//	//     id SERIAL PRIMARY KEY,
//	//     name text DEFAULT ''::text NOT NULL,
//	//     email text DEFAULT ''::text NOT NULL,
//	//     created_at timestamptz DEFAULT NOW() NOT NULL
//	// );
//
// Use the "dataType" tag to customize PostgreSQL data types:
//
//	type User struct {
//		Id    int
//		Score float64 `dataType:"numeric(10, 4)"`
//	}
//
// # Transactions
//
// Execute multiple operations in a transaction:
//
//	users.MustTransaction(func(ctx context.Context, tx db.Tx) error {
//		users.Insert("Name", "Alice").MustExecuteCtxTx(ctx, tx)
//		users.Insert("Name", "Bob").MustExecuteCtxTx(ctx, tx)
//		return nil // commit; return error to rollback
//	})
//
// # Database Drivers
//
// Package psql supports multiple PostgreSQL drivers through the db.DB interface:
//   - github.com/lib/pq via github.com/gopsql/pq
//   - github.com/jackc/pgx via github.com/gopsql/pgx
//   - github.com/go-pg/pg via github.com/gopsql/gopg
//
// For minimal dependencies, use database/sql with lib/pq:
//
//	import (
//		"database/sql"
//		"github.com/gopsql/standard"
//		_ "github.com/lib/pq"
//	)
//
//	db, _ := sql.Open("postgres", connStr)
//	conn := standard.NewDB("postgres", db)
//	users := psql.NewModel(User{}, conn)
package psql
