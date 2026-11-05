// Package pgcopy registers a CSV-strategy loader for Postgres that streams
// rows through COPY ... FROM STDIN using the pgx driver. No temporary files
// and no superuser privileges are needed.
//
//	import (
//		_ "github.com/jackc/pgx/v5/stdlib"
//		_ "github.com/zahansafallwa1511/go-seeder/loaders/pgcopy"
//	)
//
// The *sql.DB must be opened with the "pgx" driver.
package pgcopy

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/zahansafallwa1511/go-seeder"
)

func init() { turboseeder.RegisterLoader(Loader{}) }

// Loader implements turboseeder.Loader with COPY FROM STDIN (FORMAT csv).
type Loader struct{}

func (Loader) Dialect() turboseeder.Dialect { return turboseeder.Postgres }

func (Loader) Preflight(_ context.Context, conn *sql.Conn) error {
	return conn.Raw(func(dc any) error {
		if _, ok := dc.(*stdlib.Conn); !ok {
			return fmt.Errorf("pgcopy needs the pgx stdlib driver, got %T", dc)
		}
		return nil
	})
}

func (Loader) Load(ctx context.Context, job turboseeder.LoadJob) (int64, error) {
	d := job.Dialect
	query := fmt.Sprintf("COPY %s (%s) FROM STDIN WITH (FORMAT csv)", d.QuoteIdent(job.Table), d.QuoteIdents(job.Columns))
	var n int64
	err := job.Conn.Raw(func(dc any) error {
		c, ok := dc.(*stdlib.Conn)
		if !ok {
			return fmt.Errorf("pgcopy needs the pgx stdlib driver, got %T", dc)
		}
		pr, pw := io.Pipe()
		encErr := make(chan error, 1)
		go func() {
			err := encode(pw, job)
			pw.CloseWithError(err)
			encErr <- err
		}()
		tag, err := c.Conn().PgConn().CopyFrom(ctx, pr, query)
		pr.CloseWithError(io.ErrClosedPipe) // unblock the encoder if COPY stopped reading
		if eerr := <-encErr; eerr != nil && eerr != io.ErrClosedPipe {
			return eerr // generator/encoding error is the root cause
		}
		n = tag.RowsAffected()
		return err
	})
	return n, err
}

const progressEvery = 1024

// encode writes rows as RFC 4180 CSV. Every non-NULL field is quoted, and
// NULL is an unquoted empty field — COPY's CSV default — so no string value
// can ever be mistaken for NULL.
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
			s, null, err := turboseeder.TextValue(v, turboseeder.Postgres)
			if err != nil {
				return fmt.Errorf("column %s: %w", job.Columns[i], err)
			}
			if null {
				continue
			}
			bw.WriteByte('"')
			if strings.IndexByte(s, '"') >= 0 {
				s = strings.ReplaceAll(s, `"`, `""`)
			}
			bw.WriteString(s)
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
