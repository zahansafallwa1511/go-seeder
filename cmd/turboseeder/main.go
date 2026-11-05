// Command turboseeder checks a database connection and benchmarks seeding
// strategies against it.
//
//	turboseeder check     -driver pgx    -dsn 'postgres://localhost/app?sslmode=disable'
//	turboseeder benchmark -driver mysql  -dsn 'root:secret@tcp(127.0.0.1:3306)/app' -rows 1000000
//	turboseeder benchmark -driver sqlite -dsn ./bench.db -rows 500000
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"

	ts "github.com/zahansafallwa1511/go-seeder"
	_ "github.com/zahansafallwa1511/go-seeder/loaders/mysqlcsv"
	_ "github.com/zahansafallwa1511/go-seeder/loaders/pgcopy"
	"github.com/zahansafallwa1511/go-seeder/turbodata"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "check":
		err = check(os.Args[2:])
	case "benchmark", "bench":
		err = benchmark(os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  turboseeder check     -driver pgx|mysql|sqlite -dsn DSN
  turboseeder benchmark -driver pgx|mysql|sqlite -dsn DSN [-rows N] [-strategies insert,csv] [-workers 1,4] [-chunk N]`)
	os.Exit(2)
}

type connFlags struct{ driver, dsn string }

func (c *connFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&c.driver, "driver", "", "database driver: pgx (postgres), mysql, sqlite")
	fs.StringVar(&c.dsn, "dsn", "", "data source name")
}

func (c *connFlags) open(ctx context.Context) (*sql.DB, error) {
	driver := map[string]string{"postgres": "pgx", "pg": "pgx", "pgsql": "pgx", "pgx": "pgx",
		"mysql": "mysql", "mariadb": "mysql", "sqlite": "sqlite", "sqlite3": "sqlite"}[strings.ToLower(c.driver)]
	if driver == "" || c.dsn == "" {
		return nil, fmt.Errorf("-driver (pgx, mysql, sqlite) and -dsn are required")
	}
	db, err := sql.Open(driver, c.dsn)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("connect: %w", err)
	}
	return db, nil
}

func check(args []string) error {
	fs := flag.NewFlagSet("check", flag.ExitOnError)
	var c connFlags
	c.register(fs)
	_ = fs.Parse(args) // ExitOnError: Parse exits instead of returning an error
	ctx := context.Background()
	db, err := c.open(ctx)
	if err != nil {
		return err
	}
	defer db.Close()

	d := ts.DetectDialect(db)
	versionSQL := "SELECT version()"
	if d == ts.SQLite {
		versionSQL = "SELECT sqlite_version()"
	}
	var version string
	if err := db.QueryRowContext(ctx, versionSQL).Scan(&version); err != nil {
		return err
	}
	fmt.Printf("connection     ok\ndialect        %s\nserver         %s\n", d, version)
	if err := ts.CheckCSV(ctx, db); err != nil {
		fmt.Printf("csv strategy   unavailable: %s (INSERT strategy will be used)\n", err)
	} else {
		fmt.Printf("csv strategy   available\n")
	}
	return nil
}

func benchmark(args []string) error {
	fs := flag.NewFlagSet("benchmark", flag.ExitOnError)
	var c connFlags
	c.register(fs)
	rows := fs.Int("rows", 1_000_000, "rows per run")
	strategies := fs.String("strategies", "insert,csv", "comma-separated strategies to compare")
	workers := fs.String("workers", "1", "comma-separated worker counts to compare, e.g. 1,4")
	chunk := fs.Int("chunk", 0, "chunk size (0 = dialect default)")
	table := fs.String("table", "turbo_seeder_benchmark", "temporary table (dropped afterwards)")
	_ = fs.Parse(args) // ExitOnError: Parse exits instead of returning an error

	ctx := context.Background()
	db, err := c.open(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	d := ts.DetectDialect(db)
	if err := createBenchTable(ctx, db, d, *table); err != nil {
		return err
	}
	defer func() {
		if _, err := db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+d.QuoteIdent(*table)); err != nil {
			fmt.Fprintf(os.Stderr, "warning: could not drop %s: %v\n", *table, err)
		}
	}()

	email := turbodata.UniqueEmail("bench", "example.com")
	status := turbodata.Weighted(map[string]int{"active": 70, "pending": 25, "banned": 5})
	from, to := turbodata.Date("2023-01-01"), turbodata.Date("2025-01-01")
	gen := func(i int) []any {
		return []any{
			"User " + strconv.Itoa(i), email(i), status(),
			turbodata.RandomFloat(2, 0, 1000), turbodata.DateRange(from, to),
		}
	}

	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintf(tw, "strategy\tworkers\trows\ttime\trows/s\tpeak heap\t\n")
	for _, st := range strings.Split(*strategies, ",") {
		for _, w := range strings.Split(*workers, ",") {
			n, err := strconv.Atoi(strings.TrimSpace(w))
			if err != nil {
				return fmt.Errorf("bad -workers value %q", w)
			}
			fmt.Fprintf(os.Stderr, "running %s with %d worker(s)...\n", st, n)
			s := ts.ForTable(db, *table).
				Columns("name", "email", "status", "score", "created_at").
				Generate(gen).Count(*rows).Parallel(n).Truncate().Logger(quiet).
				OnProgress(ts.ProgressBar(os.Stderr))
			if *chunk > 0 {
				s.ChunkSize(*chunk)
			}
			if strings.TrimSpace(st) == "csv" {
				s.UseCSVStrategy()
			}
			res, err := s.Run(ctx)
			if err != nil {
				return err
			}
			label := res.Strategy.String()
			if res.FallbackReason != "" {
				label += " (csv fell back)"
			}
			fmt.Fprintf(tw, "%s\t%d\t%d\t%s\t%.0f\t%.1f MB\t\n", label, res.Workers, res.Rows,
				res.Duration.Round(time.Millisecond), res.RowsPerSecond(), res.PeakHeapMB())
		}
	}
	fmt.Println()
	return tw.Flush()
}

func createBenchTable(ctx context.Context, db *sql.DB, d ts.Dialect, table string) error {
	id, stamp := "INTEGER PRIMARY KEY", "DATETIME"
	switch d {
	case ts.Postgres:
		id, stamp = "BIGSERIAL PRIMARY KEY", "TIMESTAMPTZ"
	case ts.MySQL:
		id, stamp = "BIGINT AUTO_INCREMENT PRIMARY KEY", "DATETIME"
	}
	q := d.QuoteIdent(table)
	for _, stmt := range []string{
		"DROP TABLE IF EXISTS " + q,
		fmt.Sprintf(`CREATE TABLE %s (
			id %s,
			name VARCHAR(255) NOT NULL,
			email VARCHAR(255) NOT NULL UNIQUE,
			status VARCHAR(20) NOT NULL,
			score DOUBLE PRECISION NOT NULL,
			created_at %s NOT NULL)`, q, id, stamp),
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("create benchmark table: %w", err)
		}
	}
	return nil
}
