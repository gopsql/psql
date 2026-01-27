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
	selectQuery = "SELECT data FROM benchs LIMIT 100"
)

type bench struct {
	Id   int
	Data string
}

var dbConn string

func init() {
	dbConn = os.Getenv("DBCONNSTR")
	if dbConn == "" {
		dbConn = "postgres://localhost:5432/gopsqltests?sslmode=disable"
	}

	conn := pq.MustOpen(dbConn)
	defer conn.Close()
	fmt.Println("Recreating table")
	model := psql.NewModel(bench{})
	_, err := conn.Exec(model.DropSchema())
	if err != nil {
		panic(err)
	}
	_, err = conn.Exec(model.Schema())
	if err != nil {
		panic(err)
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

// INSERT benchmarks

func benchmarkInsert(b *testing.B, conn db.DB) {
	b.ReportAllocs()
	m := psql.NewModel(bench{}, conn)
	for i := 0; i < b.N; i++ {
		for j := 0; j < numberRows; j++ {
			m.Insert("Data", randomString()).MustExecute()
		}
	}
}

func BenchmarkInsertPQ(b *testing.B) {
	conn := pq.MustOpen(dbConn)
	defer conn.Close()
	benchmarkInsert(b, conn)
}

func BenchmarkInsertPGX(b *testing.B) {
	conn := pgx.MustOpen(dbConn)
	defer conn.Close()
	benchmarkInsert(b, conn)
}

func BenchmarkInsertGOPG(b *testing.B) {
	conn := gopg.MustOpen(dbConn)
	defer conn.Close()
	benchmarkInsert(b, conn)
}

func BenchmarkInsertPQNative(b *testing.B) {
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
		for j := 0; j < numberRows; j++ {
			_, err := c.Exec("INSERT INTO benchs (data) VALUES ($1)", randomString())
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkInsertPGXNative(b *testing.B) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbConn)
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Close()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < numberRows; j++ {
			_, err := pool.Exec(ctx, "INSERT INTO benchs (data) VALUES ($1)", randomString())
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkInsertGOPGNative(b *testing.B) {
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
		for j := 0; j < numberRows; j++ {
			_, err := conn.Exec("INSERT INTO benchs (data) VALUES (?)", randomString())
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

// UPDATE benchmarks

func benchmarkUpdate(b *testing.B, conn db.DB) {
	b.ReportAllocs()
	m := psql.NewModel(bench{}, conn)
	for i := 0; i < b.N; i++ {
		for j := 0; j < numberRows; j++ {
			m.Update("Data", randomString()).Where("id = $1", j+1).MustExecute()
		}
	}
}

func BenchmarkUpdatePQ(b *testing.B) {
	conn := pq.MustOpen(dbConn)
	defer conn.Close()
	benchmarkUpdate(b, conn)
}

func BenchmarkUpdatePGX(b *testing.B) {
	conn := pgx.MustOpen(dbConn)
	defer conn.Close()
	benchmarkUpdate(b, conn)
}

func BenchmarkUpdateGOPG(b *testing.B) {
	conn := gopg.MustOpen(dbConn)
	defer conn.Close()
	benchmarkUpdate(b, conn)
}

func BenchmarkUpdatePQNative(b *testing.B) {
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
		for j := 0; j < numberRows; j++ {
			_, err := c.Exec("UPDATE benchs SET data = $1 WHERE id = $2", randomString(), j+1)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkUpdatePGXNative(b *testing.B) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbConn)
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Close()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < numberRows; j++ {
			_, err := pool.Exec(ctx, "UPDATE benchs SET data = $1 WHERE id = $2", randomString(), j+1)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkUpdateGOPGNative(b *testing.B) {
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
		for j := 0; j < numberRows; j++ {
			_, err := conn.Exec("UPDATE benchs SET data = ? WHERE id = ?", randomString(), j+1)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

// SELECT benchmarks

func benchmarkSelect(b *testing.B, conn db.DB) {
	b.ReportAllocs()
	m := psql.NewModel(bench{}, conn)
	for i := 0; i < b.N; i++ {
		var randomStrings []string
		if err := m.Select("data").Limit(100).Query(&randomStrings); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectPQ(b *testing.B) {
	conn := pq.MustOpen(dbConn)
	defer conn.Close()
	benchmarkSelect(b, conn)
}

func BenchmarkSelectPGX(b *testing.B) {
	conn := pgx.MustOpen(dbConn)
	defer conn.Close()
	benchmarkSelect(b, conn)
}

func BenchmarkSelectGOPG(b *testing.B) {
	conn := gopg.MustOpen(dbConn)
	defer conn.Close()
	benchmarkSelect(b, conn)
}

func BenchmarkSelectPQNative(b *testing.B) {
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

func BenchmarkSelectPGXNative(b *testing.B) {
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

func BenchmarkSelectGOPGNative(b *testing.B) {
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

	Section struct {
		Title          string
		Unit           string
		OffsetX        float64
		TX, TY, SX, SY float64
		Bars           []Bar
	}

	Chart struct {
		N              int
		W, H           float64
		AX, AY, BX, BY float64
		CX, CY, DX, DY float64
		Sections       []Section
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
    <rect x="{{.AX}}" y="{{.AY}}" width="10" height="10" fill="url(#g1)"></rect>
    <text font-family="sans-serif" font-size="10" x="{{.BX}}" y="{{.BY}}" fill="#fff">psql</text>
  </g>
  <g>
    <rect x="{{.CX}}" y="{{.CY}}" width="10" height="10" fill="url(#g2)"></rect>
    <text font-family="sans-serif" font-size="10" x="{{.DX}}" y="{{.DY}}" fill="#fff">native</text>
  </g>
  {{- range .Sections}}
  <g transform="translate({{.OffsetX}}, 0)">
    <text font-family="sans-serif" font-size="12" x="{{.TX}}" y="{{.TY}}" fill="#fff">{{.Title}}</text>
    <text font-family="sans-serif" font-size="10" x="{{.SX}}" y="{{.SY}}" fill="#fff">{{.Unit}}, less is better</text>
    {{- range .Bars}}
    <g>
      <rect x="{{.X1}}" y="{{.Y1}}" width="{{.W1}}" height="{{.H1}}" fill="url(#g1)"></rect>
      <rect x="{{.X2}}" y="{{.Y2}}" width="{{.W2}}" height="{{.H2}}" fill="url(#g2)"></rect>
      <text font-family="sans-serif" font-size="12" text-anchor="middle" x="{{.TX}}" y="{{.TY}}" fill="#fff">{{.T}}</text>
      <text font-family="sans-serif" font-size="7" text-anchor="middle" x="{{.N1X}}" y="{{.N1Y}}" fill="#fff">{{.N1}}</text>
      <text font-family="sans-serif" font-size="7" text-anchor="middle" x="{{.N2X}}" y="{{.N2Y}}" fill="#fff">{{.N2}}</text>
    </g>
    {{- end}}
  </g>
  {{- end}}
</svg>
`

type BenchmarkSet struct {
	Title   string
	Unit    string  // "ns/op" or "μs/op"
	Divisor float64 // 1 for ns, 1000 for μs
	Columns []Column
}

func generateChart() Chart {
	benchmarkSets := []BenchmarkSet{
		{
			Title:   fmt.Sprintf("Insert %d rows", numberRows),
			Unit:    "μs/op",
			Divisor: 1000,
			Columns: []Column{
				{Name: "pq", Bench1: BenchmarkInsertPQ, Bench2: BenchmarkInsertPQNative},
				{Name: "pgx", Bench1: BenchmarkInsertPGX, Bench2: BenchmarkInsertPGXNative},
				{Name: "gopg", Bench1: BenchmarkInsertGOPG, Bench2: BenchmarkInsertGOPGNative},
			},
		},
		{
			Title:   fmt.Sprintf("Update %d rows", numberRows),
			Unit:    "μs/op",
			Divisor: 1000,
			Columns: []Column{
				{Name: "pq", Bench1: BenchmarkUpdatePQ, Bench2: BenchmarkUpdatePQNative},
				{Name: "pgx", Bench1: BenchmarkUpdatePGX, Bench2: BenchmarkUpdatePGXNative},
				{Name: "gopg", Bench1: BenchmarkUpdateGOPG, Bench2: BenchmarkUpdateGOPGNative},
			},
		},
		{
			Title:   fmt.Sprintf("Select %d rows", numberRows),
			Unit:    "ns/op",
			Divisor: 1,
			Columns: []Column{
				{Name: "pq", Bench1: BenchmarkSelectPQ, Bench2: BenchmarkSelectPQNative},
				{Name: "pgx", Bench1: BenchmarkSelectPGX, Bench2: BenchmarkSelectPGXNative},
				{Name: "gopg", Bench1: BenchmarkSelectGOPG, Bench2: BenchmarkSelectGOPGNative},
			},
		},
	}

	// Run all benchmarks and find max for each section
	for setIdx := range benchmarkSets {
		for colIdx := range benchmarkSets[setIdx].Columns {
			col := &benchmarkSets[setIdx].Columns[colIdx]
			fmt.Printf("benchmarking %s - %s\n", benchmarkSets[setIdx].Title, col.Name)

			result := testing.Benchmark(col.Bench1)
			col.NsPerOp1 = result.NsPerOp()

			result = testing.Benchmark(col.Bench2)
			col.NsPerOp2 = result.NsPerOp()
		}
	}

	// Narrower dimensions for each section
	padLeft, padRight := 15.0, 15.0
	padTop, padBottom := 55.0, 20.0
	barWidth := 20.0
	barGap := 3.0
	columnGap := 15.0
	barMaxHeight := 100.0
	barMaxY := barMaxHeight + padTop
	textY := barMaxY + 15.0
	sectionGap := 10.0

	var sections []Section
	offsetX := 0.0

	for _, set := range benchmarkSets {
		// Find max for this section
		var sectionMaxNsPerOp int64
		for _, column := range set.Columns {
			if column.NsPerOp1 > sectionMaxNsPerOp {
				sectionMaxNsPerOp = column.NsPerOp1
			}
			if column.NsPerOp2 > sectionMaxNsPerOp {
				sectionMaxNsPerOp = column.NsPerOp2
			}
		}

		bars := []Bar{}
		x1 := padLeft

		for _, column := range set.Columns {
			x2 := x1 + barWidth + barGap
			h1 := float64(column.NsPerOp1*1000/sectionMaxNsPerOp) / 10
			h2 := float64(column.NsPerOp2*1000/sectionMaxNsPerOp) / 10
			// Display value with divisor applied
			displayVal1 := float64(column.NsPerOp1) / set.Divisor
			displayVal2 := float64(column.NsPerOp2) / set.Divisor
			bars = append(bars, Bar{
				X1:  x1,
				W1:  barWidth,
				Y1:  barMaxY - h1,
				H1:  h1,
				N1:  strconv.Itoa(int(displayVal1)),
				N1X: x1 + barWidth/2,
				N1Y: barMaxY - h1 - 6,

				X2:  x2,
				W2:  barWidth,
				Y2:  barMaxY - h2,
				H2:  h2,
				N2:  strconv.Itoa(int(displayVal2)),
				N2X: x2 + barWidth/2,
				N2Y: barMaxY - h2 - 6,

				TX: x2 - barGap/2,
				TY: textY,
				T:  column.Name,
			})
			x1 = x2 + barWidth + columnGap
		}

		sectionWidth := x1 - columnGap + padRight
		sections = append(sections, Section{
			Title:   set.Title,
			Unit:    set.Unit,
			OffsetX: offsetX,
			TX:      padLeft,
			TY:      20.0,
			SX:      padLeft,
			SY:      32.0,
			Bars:    bars,
		})
		offsetX += sectionWidth + sectionGap
	}

	chartWidth := offsetX - sectionGap
	chartHeight := textY + padBottom

	return Chart{
		N:        numberRows,
		W:        chartWidth,
		H:        chartHeight,
		AX:       chartWidth - 55.0,
		AY:       10.0,
		BX:       chartWidth - 40.0,
		BY:       18.0,
		CX:       chartWidth - 55.0,
		CY:       25.0,
		DX:       chartWidth - 40.0,
		DY:       33.0,
		Sections: sections,
	}
}
