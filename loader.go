package turboseeder

import (
	"context"
	"database/sql"
	"errors"
	"iter"
	"sync"
)

// Loader implements the CSV strategy for one dialect: a streaming bulk-load
// path such as Postgres COPY FROM STDIN or MySQL LOAD DATA LOCAL INFILE.
//
// Loaders live in sub-packages so the core library never depends on a
// specific driver. Register one with a blank import:
//
//	import _ "github.com/zahansafallwa1511/go-seeder/loaders/pgcopy"
type Loader interface {
	// Dialect is the database flavour this loader targets.
	Dialect() Dialect
	// Preflight checks the loader can run on this connection (driver type,
	// server settings). A non-nil error triggers the fallback to the
	// default INSERT strategy unless fallback is disabled.
	Preflight(ctx context.Context, conn *sql.Conn) error
	// Load streams rows into the table and returns the number of rows the
	// server reports as written.
	Load(ctx context.Context, job LoadJob) (int64, error)
}

// LoadJob is one bulk-load call: a contiguous range of generated rows.
type LoadJob struct {
	Conn    *sql.Conn
	Tx      *sql.Tx // nil when transactions are disabled
	Dialect Dialect
	Table   string
	Columns []string
	// Rows yields normalized values in column order. The slice is reused
	// after yield returns, so loaders must encode it before continuing.
	Rows iter.Seq2[[]any, error]
	// Expected is the number of rows Rows will yield.
	Expected int
	// OnRows is called as rows are handed to the database, for progress.
	OnRows func(n int)
}

var (
	loadersMu sync.RWMutex
	loaders   = map[Dialect]Loader{}
)

// RegisterLoader makes a CSV-strategy loader available for its dialect.
func RegisterLoader(l Loader) {
	loadersMu.Lock()
	loaders[l.Dialect()] = l
	loadersMu.Unlock()
}

func lookupLoader(d Dialect) Loader {
	loadersMu.RLock()
	defer loadersMu.RUnlock()
	return loaders[d]
}

// CheckCSV reports whether the CSV strategy can run against db: a loader is
// registered for its dialect and the loader's preflight passes. A nil error
// means UseCSVStrategy will not fall back.
func CheckCSV(ctx context.Context, db *sql.DB) error {
	d := DetectDialect(db)
	if l, reason := (&Seeder{db: db}).resolveLoader(ctx, d); l == nil {
		return errors.New(reason)
	}
	return nil
}
