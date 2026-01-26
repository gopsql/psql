// Package psql_test contains integration tests for the psql package.
//
// These tests require a PostgreSQL database to be available. The connection
// string can be configured via the DBCONNSTR environment variable.
//
// # Running Tests
//
// To run integration tests with a custom database:
//
//	DBCONNSTR="postgres://user:pass@host:5432/dbname?sslmode=disable" go test ./tests/...
//
// If DBCONNSTR is not set, tests will attempt to connect to:
//
//	postgres://localhost:5432/gopsqltests?sslmode=disable
//
// # Test Organization
//
//   - datatypes_test.go: Tests for various Go data types and JSONB fields
//   - integration_test.go: CRUD operations, transactions, and driver compatibility
//   - query_test.go: Query result scanning into various target types (slices, maps, structs)
//
// # Database Drivers
//
// Integration tests run against multiple PostgreSQL drivers when available:
//   - github.com/lib/pq
//   - github.com/jackc/pgx
//   - github.com/go-pg/pg
//
// Tests will skip unavailable drivers rather than failing.
package psql_test
