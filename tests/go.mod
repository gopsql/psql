module github.com/gopsql/psql/tests

go 1.16

replace github.com/gopsql/psql => ../

require (
	github.com/go-pg/pg/v10 v10.10.6
	github.com/gopsql/db v1.2.1
	github.com/gopsql/gopg v1.2.1
	github.com/gopsql/logger v1.0.0
	github.com/gopsql/pgx v1.5.0
	github.com/gopsql/pq v1.2.1
	github.com/gopsql/psql v0.0.0
	github.com/gopsql/standard v1.2.1
	github.com/jackc/pgx/v5 v5.4.3
	github.com/lib/pq v1.10.4
	github.com/shopspring/decimal v1.2.0
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
)
