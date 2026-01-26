package psql_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"
	"testing"
	"text/template"

	"github.com/go-pg/pg/v10"
	"github.com/gopsql/db"
	"github.com/gopsql/gopg"
	"github.com/gopsql/pgx"
	"github.com/gopsql/pq"
	"github.com/gopsql/psql"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/lib/pq"
)

const (
	numberRows  = 100
	selectQuery = "SELECT data FROM bench LIMIT 100"
)

var dbConn string

func init() {
	dbConn = os.Getenv("DBCONNSTR")
	if dbConn == "" {
		dbConn = "postgres://localhost:5432/gopsqltests?sslmode=disable"
	}

	conn := pq.MustOpen(dbConn)
	defer conn.Close()
	fmt.Println("Recreating bench table")
	_, err := conn.Exec(`DROP TABLE IF EXISTS bench`)
	if err != nil {
		panic(err)
	}
	_, err = conn.Exec(`CREATE TABLE IF NOT EXISTS bench (id SERIAL PRIMARY KEY, data text)`)
	if err != nil {
		panic(err)
	}
	fmt.Println("Inserting", numberRows, "records")
	for i := 0; i < numberRows; i++ {
		_, err = conn.Exec("INSERT INTO bench (data) VALUES ($1)", randomString())
		if err != nil {
			panic(err)
		}
	}
	fmt.Println("Init done")
}

func randomString() string {
	randomBytes := make([]byte, 10)
	if _, err := rand.Read(randomBytes); err != nil {
		panic(err)
	}
	return hex.EncodeToString(randomBytes)
}

func benchmark(b *testing.B, conn db.DB) {
	b.ReportAllocs()
	m := psql.NewModelTable("bench", conn)
	for i := 0; i < b.N; i++ {
		var randomStrings []string
		if err := m.Select("data").Limit(100).Query(&randomStrings); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPQ(b *testing.B) {
	conn := pq.MustOpen(dbConn)
	defer conn.Close()
	benchmark(b, conn)
}

func BenchmarkPGX(b *testing.B) {
	conn := pgx.MustOpen(dbConn)
	defer conn.Close()
	benchmark(b, conn)
}

func BenchmarkGOPG(b *testing.B) {
	conn := gopg.MustOpen(dbConn)
	defer conn.Close()
	benchmark(b, conn)
}

func BenchmarkPQNative(b *testing.B) {
	c, err := sql.Open("postgres", dbConn)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	if err := c.Ping(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rows, err := c.Query(selectQuery)
		if err != nil {
			b.Fatal(err)
		}
		randomStrings := make([]string, 0, numberRows)
		for rows.Next() {
			var randomString string
			rows.Scan(&randomString)
			randomStrings = append(randomStrings, randomString)
		}
		if err := rows.Err(); err != nil {
			b.Fatal(err)
		}
		rows.Close()
	}
}

func BenchmarkPGXNative(b *testing.B) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbConn)
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Close()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rows, err := pool.Query(ctx, selectQuery)
		if err != nil {
			b.Fatal(err)
		}
		randomStrings := make([]string, 0, numberRows)
		for rows.Next() {
			var randomString string
			rows.Scan(&randomString)
			randomStrings = append(randomStrings, randomString)
		}
		if err := rows.Err(); err != nil {
			b.Fatal(err)
		}
		rows.Close()
	}
}

func BenchmarkGOPGNative(b *testing.B) {
	opt, err := pg.ParseURL(dbConn)
	if err != nil {
		b.Fatal(err)
	}
	conn := pg.Connect(opt)
	if err := conn.Ping(context.Background()); err != nil {
		b.Fatal(err)
	}
	defer conn.Close()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var randomStrings []string
		conn.Query(&randomStrings, selectQuery)
	}
}

type (
	Column struct {
		Name     string
		Bench1   func(*testing.B)
		Bench2   func(*testing.B)
		NsPerOp1 int64
		NsPerOp2 int64
	}

	Chart struct {
		N                                    int
		W, H, TX, TY, SX, SY, AX, AY, BX, BY float64
		Bars                                 []Bar
	}

	Bar struct {
		X1, Y1, W1, H1, N1X, N1Y float64
		X2, Y2, W2, H2, N2X, N2Y float64
		TX, TY                   float64
		N1, N2, T                string
	}
)

// GENERATE=1 go test -v ./benchmark_test.go
func TestGenerateChart(t *testing.T) {
	if os.Getenv("GENERATE") != "1" {
		t.Log("Skipped unless env GENERATE=1")
		t.SkipNow()
		return
	}

	f, err := os.OpenFile("benchmark.svg", os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	svg := template.Must(template.New("").Parse(tpl))
	err = svg.Execute(io.MultiWriter(os.Stdout, f), generateChart())
	if err != nil {
		panic(err)
	}
}

const tpl = `<svg x="0" y="0" width="{{.W}}" height="{{.H}}" viewBox="0, 0, {{.W}}, {{.H}}" xmlns="http://www.w3.org/2000/svg">
  <rect width="100%" height="100%" fill="#000" />
  <defs>
    <linearGradient id="g1" x1="0" x2="0" y1="0" y2="1">
      <stop stop-color="#19aade" offset="0%"/>
      <stop stop-color="#37d4f0" offset="100%"/>
    </linearGradient>
    <linearGradient id="g2" x1="0" x2="0" y1="0" y2="1">
      <stop stop-color="#11998e" offset="0%"/>
      <stop stop-color="#38ef7d" offset="100%"/>
    </linearGradient>
  </defs>
  <g>
    <text font-family="sans-serif" font-size="12" x="{{.TX}}" y="{{.TY}}" fill="#fff">Select {{.N}} rows</text>
    <text font-family="sans-serif" font-size="10" x="{{.SX}}" y="{{.SY}}" fill="#fff">ns/op, less is better</text>
  </g>
  <g>
    <rect x="{{.AX}}" y="{{.AY}}" width="10" height="10" fill="url(#g2)"></rect>
    <text font-family="sans-serif" font-size="10" x="{{.BX}}" y="{{.BY}}" fill="#fff">native</text>
  </g>
  {{- range .Bars}}
  <g>
    <rect x="{{.X1}}" y="{{.Y1}}" width="{{.W1}}" height="{{.H1}}" fill="url(#g1)"></rect>
    <rect x="{{.X2}}" y="{{.Y2}}" width="{{.W2}}" height="{{.H2}}" fill="url(#g2)"></rect>
    <text font-family="sans-serif" font-size="12" text-anchor="middle" x="{{.TX}}" y="{{.TY}}" fill="#fff">{{.T}}</text>
    <text font-family="sans-serif" font-size="8" text-anchor="middle" x="{{.N1X}}" y="{{.N1Y}}" fill="#fff">{{.N1}}</text>
    <text font-family="sans-serif" font-size="8" text-anchor="middle" x="{{.N2X}}" y="{{.N2Y}}" fill="#fff">{{.N2}}</text>
  </g>
  {{- end}}
</svg>
`

func generateChart() Chart {
	columns := []Column{
		{
			Name:   "pq",
			Bench1: BenchmarkPQ,
			Bench2: BenchmarkPQNative,
		},
		{
			Name:   "pgx",
			Bench1: BenchmarkPGX,
			Bench2: BenchmarkPGXNative,
		},
		{
			Name:   "gopg",
			Bench1: BenchmarkGOPG,
			Bench2: BenchmarkGOPGNative,
		},
	}

	var maxNsPerOp int64
	for i := range columns {
		fmt.Println("benchmarking", columns[i].Name)

		result := testing.Benchmark(columns[i].Bench1)
		columns[i].NsPerOp1 = result.NsPerOp()

		result = testing.Benchmark(columns[i].Bench2)
		columns[i].NsPerOp2 = result.NsPerOp()

		if columns[i].NsPerOp1 > maxNsPerOp {
			maxNsPerOp = columns[i].NsPerOp1
		}
		if columns[i].NsPerOp2 > maxNsPerOp {
			maxNsPerOp = columns[i].NsPerOp2
		}
	}

	padLeft, padRight := 25.0, 25.0
	padTop, padBottom := 55.0, 20.0
	bars := []Bar{}
	x1 := padLeft
	barWidth := 30.0
	barGap := 5.0
	columnGap := 30.0
	barMaxHeight := 100.0
	barMaxY := barMaxHeight + padTop
	textY := barMaxY + 15.0
	for _, column := range columns {
		x2 := x1 + barWidth + barGap
		h1 := float64(column.NsPerOp1*1000/maxNsPerOp) / 10
		h2 := float64(column.NsPerOp2*1000/maxNsPerOp) / 10
		bars = append(bars, Bar{
			X1:  x1,
			W1:  barWidth,
			Y1:  barMaxY - h1,
			H1:  h1,
			N1:  strconv.Itoa(int(column.NsPerOp1)),
			N1X: x1 + barWidth/2,
			N1Y: barMaxY - h1 - 6,

			X2:  x2,
			W2:  barWidth,
			Y2:  barMaxY - h2,
			H2:  h2,
			N2:  strconv.Itoa(int(column.NsPerOp2)),
			N2X: x2 + barWidth/2,
			N2Y: barMaxY - h2 - 6,

			TX: x2 - barGap/2,
			TY: textY,
			T:  column.Name,
		})
		x1 = x2 + barWidth + columnGap
	}
	chartWidth := x1 - columnGap + padRight
	chartHeight := textY + padBottom
	return Chart{
		N:    numberRows,
		W:    chartWidth,
		H:    chartHeight,
		TX:   padLeft,
		TY:   20.0,
		SX:   padLeft,
		SY:   32.0,
		AX:   chartWidth - 55.0,
		AY:   10.0,
		BX:   chartWidth - 40.0,
		BY:   18.0,
		Bars: bars,
	}
}
