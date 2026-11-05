// Package mysqlcsv registers a CSV-strategy loader for MySQL/MariaDB that
// streams rows through LOAD DATA LOCAL INFILE from an in-memory pipe
// (go-sql-driver/mysql's Reader:: handler), so nothing touches the disk.
//
//	import (
//		_ "github.com/go-sql-driver/mysql"
//		_ "github.com/zahansafallwa1511/go-seeder/loaders/mysqlcsv"
//	)
//
// The server must allow it: SET GLOBAL local_infile = 1. Preflight checks
// this and the seeder falls back to INSERT when it is off. No client-side
// allowAllFiles flag is needed for Reader:: handlers.
package mysqlcsv

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync/atomic"

	"github.com/go-sql-driver/mysql"
	"github.com/zahansafallwa1511/go-seeder"
)

func init() { turboseeder.RegisterLoader(Loader{}) }

// Loader implements turboseeder.Loader with LOAD DATA LOCAL INFILE.
type Loader struct{}

func (Loader) Dialect() turboseeder.Dialect { return turboseeder.MySQL }

func (Loader) Preflight(ctx context.Context, conn *sql.Conn) error {
	err := conn.Raw(func(dc any) error {
		if t := reflect.TypeOf(dc); !strings.Contains(derefPkg(t), "go-sql-driver/mysql") {
			return fmt.Errorf("mysqlcsv needs go-sql-driver/mysql, got %T", dc)
		}
		return nil
	})
	if err != nil {
		return err
	}
	var on string
	if err := conn.QueryRowContext(ctx, "SELECT @@GLOBAL.local_infile").Scan(&on); err != nil {
		return fmt.Errorf("check local_infile: %w", err)
	}
	if on != "1" && !strings.EqualFold(on, "ON") {
		return errors.New("server has local_infile disabled (SET GLOBAL local_infile = 1)")
	}
	return nil
}

func derefPkg(t reflect.Type) string {
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == nil {
		return ""
	}
	return t.PkgPath()
}

var seq atomic.Uint64

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (Loader) Load(ctx context.Context, job turboseeder.LoadJob) (int64, error) {
	var ex execer = job.Conn
	if job.Tx != nil {
		ex = job.Tx
	}
	name := fmt.Sprintf("turboseeder_%d", seq.Add(1))
	pr, pw := io.Pipe()
	mysql.RegisterReaderHandler(name, func() io.Reader { return pr })
	defer mysql.DeregisterReaderHandler(name)

	encErr := make(chan error, 1)
	go func() {
		err := encode(pw, job)
		pw.CloseWithError(err)
		encErr <- err
	}()

	d := job.Dialect
	query := fmt.Sprintf(`LOAD DATA LOCAL INFILE 'Reader::%s' INTO TABLE %s CHARACTER SET utf8mb4 `+
		`FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\' `+
		`LINES TERMINATED BY '\n' (%s)`, name, d.QuoteIdent(job.Table), d.QuoteIdents(job.Columns))
	res, err := ex.ExecContext(ctx, query)
	pr.CloseWithError(io.ErrClosedPipe) // unblock the encoder if the server stopped reading
	if eerr := <-encErr; eerr != nil && eerr != io.ErrClosedPipe {
		return 0, eerr
	}
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	// LOCAL loads turn errors (bad values, duplicate keys) into warnings and
	// keep going. Seeding should be exact, so surface them as an error.
	if msg := warnings(ctx, ex); msg != "" {
		return n, fmt.Errorf("LOAD DATA produced warnings: %s", msg)
	}
	return n, nil
}

func warnings(ctx context.Context, ex execer) string {
	rows, err := ex.QueryContext(ctx, "SHOW WARNINGS LIMIT 5")
	if err != nil {
		return ""
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var level, msg string
		var code int
		if rows.Scan(&level, &code, &msg) == nil {
			out = append(out, fmt.Sprintf("%s %d: %s", level, code, msg))
		}
	}
	return strings.Join(out, "; ")
}

const progressEvery = 1024

// encode writes rows in MySQL's LOAD DATA format: NULL is an unquoted \N and
// every other value is quoted with backslash escapes, so a string such as
// `\N` is written as "\\N" and can never be read back as NULL.
func encode(w io.Writer, job turboseeder.LoadJob) error {
	bw := bufio.NewWriterSize(w, 256<<10)
	pending := 0
	for row, err := range job.Rows {
		if err != nil {
			return err
		}
		for i, v := range row {
			if i > 0 {
				bw.WriteByte(',')
			}
			s, null, err := turboseeder.TextValue(v, turboseeder.MySQL)
			if err != nil {
				return fmt.Errorf("column %s: %w", job.Columns[i], err)
			}
			if null {
				bw.WriteString(`\N`)
				continue
			}
			bw.WriteByte('"')
			writeEscaped(bw, s)
			bw.WriteByte('"')
		}
		if err := bw.WriteByte('\n'); err != nil {
			return err
		}
		if pending++; pending == progressEvery {
			job.OnRows(pending)
			pending = 0
		}
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	job.OnRows(pending)
	return nil
}

func writeEscaped(bw *bufio.Writer, s string) {
	start := 0
	for i := 0; i < len(s); i++ {
		var esc string
		switch s[i] {
		case '\\':
			esc = `\\`
		case '"':
			esc = `\"`
		case 0:
			esc = `\0`
		case '\n':
			esc = `\n`
		case '\r':
			esc = `\r`
		case '\t':
			esc = `\t`
		case 0x1a:
			esc = `\Z`
		default:
			continue
		}
		bw.WriteString(s[start:i])
		bw.WriteString(esc)
		start = i + 1
	}
	bw.WriteString(s[start:])
}
