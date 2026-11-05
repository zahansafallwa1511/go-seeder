package turboseeder_test

import (
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"

	ts "github.com/zahansafallwa1511/go-seeder"
	_ "github.com/zahansafallwa1511/go-seeder/loaders/mysqlcsv"
	_ "github.com/zahansafallwa1511/go-seeder/loaders/pgcopy"
)

// env is one database under test. SQLite always runs; Postgres and MySQL
// run when TURBOSEEDER_PG_DSN / TURBOSEEDER_MYSQL_DSN are set, e.g.
//
//	TURBOSEEDER_PG_DSN=postgres://localhost/turboseeder_test?sslmode=disable
//	TURBOSEEDER_MYSQL_DSN='root:secret@tcp(127.0.0.1:3306)/turboseeder_test?parseTime=true'
type env struct {
	name string
	db   *sql.DB
	d    ts.Dialect
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func forEachDB(t *testing.T, fn func(t *testing.T, e env)) {
	t.Helper()
	t.Run("sqlite", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.db")
		db, err := sql.Open("sqlite", "file:"+path+"?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		fn(t, env{"sqlite", db, ts.SQLite})
	})
	for _, c := range []struct {
		name, driver, envVar string
		d                    ts.Dialect
	}{
		{"postgres", "pgx", "TURBOSEEDER_PG_DSN", ts.Postgres},
		{"mysql", "mysql", "TURBOSEEDER_MYSQL_DSN", ts.MySQL},
	} {
		t.Run(c.name, func(t *testing.T) {
			dsn := os.Getenv(c.envVar)
			if dsn == "" {
				t.Skipf("%s not set", c.envVar)
			}
			db, err := sql.Open(c.driver, dsn)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { db.Close() })
			if err := db.Ping(); err != nil {
				t.Fatalf("ping %s: %v", c.name, err)
			}
			fn(t, env{c.name, db, c.d})
		})
	}
}

func (e env) exec(t *testing.T, q string, args ...any) {
	t.Helper()
	if _, err := e.db.Exec(q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

func (e env) count(t *testing.T, table string, where ...string) int {
	t.Helper()
	q := "SELECT COUNT(*) FROM " + table
	if len(where) > 0 {
		q += " WHERE " + where[0]
	}
	var n int
	if err := e.db.QueryRow(q).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}

func (e env) one(t *testing.T, q string, dst ...any) {
	t.Helper()
	if err := e.db.QueryRow(q).Scan(dst...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

// table creates a fresh table from a portable column spec. Tokens:
// ID = auto-increment primary key, BOOL, TS = timestamp, JSON = text.
func (e env) table(t *testing.T, name, spec string) {
	t.Helper()
	var id, stamp string
	switch e.d {
	case ts.SQLite:
		id, stamp = "INTEGER PRIMARY KEY", "DATETIME"
	case ts.Postgres:
		id, stamp = "BIGSERIAL PRIMARY KEY", "TIMESTAMPTZ"
	case ts.MySQL:
		id, stamp = "BIGINT AUTO_INCREMENT PRIMARY KEY", "DATETIME(6)"
	}
	r := strings.NewReplacer(" ID", " "+id, "BOOL", "BOOLEAN", " TS", " "+stamp, "JSON", "TEXT")
	drop := "DROP TABLE IF EXISTS " + name
	if e.d == ts.Postgres {
		drop += " CASCADE"
	}
	e.exec(t, drop)
	e.exec(t, fmt.Sprintf("CREATE TABLE %s (%s)", name, r.Replace(spec)))
	t.Cleanup(func() {
		if _, err := e.db.Exec(drop); err != nil {
			t.Logf("cleanup %s: %v", name, err)
		}
	})
}

const usersSpec = `id ID,
	name VARCHAR(255) NOT NULL,
	email VARCHAR(255) NOT NULL UNIQUE,
	password VARCHAR(255) NULL,
	active BOOL NOT NULL DEFAULT TRUE,
	meta JSON NULL,
	score DOUBLE PRECISION NULL,
	created_at TS NULL,
	updated_at TS NULL`

// seeder returns a quiet ForTable builder.
func (e env) seeder(table string) *ts.Seeder {
	return ts.ForTable(e.db, table).Logger(quiet)
}
