module github.com/gopsql/psql/tests

go 1.16

replace github.com/gopsql/psql => ../

require (
	github.com/go-pg/pg/v10 v10.10.6
	github.com/gopsql/db v1.2.1
	github.com/gopsql/gopg v1.2.1
	github.com/gopsql/logger v1.0.0
	github.com/gopsql/pgx v1.2.1
	github.com/gopsql/pq v1.2.1
	github.com/gopsql/psql v0.0.0
	github.com/gopsql/standard v1.2.1
	github.com/jackc/pgx/v4 v4.14.1
	github.com/lib/pq v1.10.4
	github.com/shopspring/decimal v1.2.0
)
