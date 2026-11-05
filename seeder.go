// Package turboseeder seeds databases with millions of rows fast.
//
// It is a Go take on laravel-turbo-seeder: it bypasses ORMs entirely and
// writes rows with multi-row INSERTs (prepared once, reused per chunk) or,
// with the CSV strategy, streams them through Postgres COPY / MySQL
// LOAD DATA LOCAL INFILE without touching the disk.
//
//	res, err := turboseeder.ForTable(db, "users").
//		Columns("name", "email", "created_at").
//		Generate(func(i int) []any {
//			return []any{fmt.Sprintf("User %d", i), email(i), now}
//		}).
//		Count(1_000_000).
//		Run(ctx)
package turboseeder

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"database/sql"
)

// Strategy selects how rows are written.
type Strategy int

const (
	// StrategyInsert writes multi-row INSERT statements. Works everywhere.
	StrategyInsert Strategy = iota
	// StrategyCSV streams rows through the dialect's bulk loader
	// (COPY FROM STDIN, LOAD DATA LOCAL INFILE). Requires a registered Loader.
	StrategyCSV
)

func (s Strategy) String() string {
	if s == StrategyCSV {
		return "csv"
	}
	return "insert"
}

type txMode int

const (
	txSingle txMode = iota
	txCommitEvery
	txNone
)

// Seeder is a fluent builder for one seeding run. Builder methods record the
// first configuration error, which Run returns, so calls can be chained.
type Seeder struct {
	db      *sql.DB
	table   string
	dialect Dialect

	columns           []string
	columnsFromSchema bool
	gen               generator
	timestamps        []string

	count     int
	offset    int
	chunkSize int
	workers   int

	strategy    Strategy
	csvFallback bool

	truncate     bool
	tx           txMode
	txExplicit   bool
	commitEvery  int
	upsertBy     []string
	upsertUpdate []string
	upsert       bool
	dryRun       bool

	disableFK     bool
	disableUnique bool

	retryAttempts int
	retryIf       func(error) bool

	validateColumns bool
	force           bool

	progress func(Progress)
	logger   *slog.Logger

	err error
}

// ForTable starts a seeder for the given table ("users" or "schema.users").
func ForTable(db *sql.DB, table string) *Seeder {
	return &Seeder{
		db:              db,
		table:           table,
		csvFallback:     true,
		disableFK:       true,
		retryAttempts:   3,
		validateColumns: true,
		workers:         1,
	}
}

func (s *Seeder) fail(format string, args ...any) *Seeder {
	if s.err == nil {
		s.err = fmt.Errorf("turboseeder: "+format, args...)
	}
	return s
}

// Table overrides the target table (useful with FromModel).
func (s *Seeder) Table(name string) *Seeder { s.table = name; return s }

// Dialect overrides automatic dialect detection.
func (s *Seeder) Dialect(d Dialect) *Seeder { s.dialect = d; return s }

// Columns sets the column order for the generated rows.
func (s *Seeder) Columns(cols ...string) *Seeder { s.columns = cols; return s }

// ColumnsFromSchema reads the insertable columns (everything except
// auto-increment, identity and generated columns) from the database.
func (s *Seeder) ColumnsFromSchema() *Seeder { s.columnsFromSchema = true; return s }

// Generate sets a row generator returning values in Columns() order.
// This is the fastest path. With Parallel(n) it must be safe for
// concurrent use; it is always called with increasing i within a chunk.
func (s *Seeder) Generate(fn func(i int) []any) *Seeder {
	s.gen = sliceGen(fn)
	return s
}

// GenerateMap sets a row generator returning column => value maps. When
// Columns() is not set, the columns are the sorted keys of the first row.
func (s *Seeder) GenerateMap(fn func(i int) map[string]any) *Seeder {
	s.gen = &mapGen{fn: fn}
	return s
}

// WithTimestamps appends created_at and updated_at (or the given columns),
// set to the time the run started, unless the generator already provides them.
func (s *Seeder) WithTimestamps(cols ...string) *Seeder {
	if len(cols) == 0 {
		cols = []string{"created_at", "updated_at"}
	}
	s.timestamps = cols
	return s
}

// WithoutTimestamps undoes WithTimestamps.
func (s *Seeder) WithoutTimestamps() *Seeder { s.timestamps = nil; return s }

// Count sets how many rows to generate.
func (s *Seeder) Count(n int) *Seeder {
	if n < 1 {
		return s.fail("Count must be >= 1, got %d", n)
	}
	s.count = n
	return s
}

// Offset sets the first index passed to the generator (default 0). Handy for
// appending to an already-seeded table without colliding unique values.
func (s *Seeder) Offset(n int) *Seeder { s.offset = n; return s }

// ChunkSize sets rows per INSERT statement. It is clamped to the driver's
// bind-parameter limit. Defaults: MySQL 1000, Postgres 800, SQLite 500.
func (s *Seeder) ChunkSize(n int) *Seeder {
	if n < 1 {
		return s.fail("ChunkSize must be >= 1, got %d", n)
	}
	s.chunkSize = n
	return s
}

// Parallel writes with n connections at once, each handling a disjoint
// range of indexes. The generator must be safe for concurrent use.
// SQLite is always seeded with one connection (it has a single writer).
// In the default single-transaction mode every worker keeps its transaction
// open until all succeed, then all commit; this is best-effort atomicity,
// not a distributed transaction.
func (s *Seeder) Parallel(n int) *Seeder {
	if n < 1 {
		return s.fail("Parallel must be >= 1, got %d", n)
	}
	s.workers = n
	return s
}

// UseCSVStrategy streams rows through the dialect's bulk loader. Import the
// loader package for your database (loaders/pgcopy, loaders/mysqlcsv).
func (s *Seeder) UseCSVStrategy() *Seeder { s.strategy = StrategyCSV; return s }

// UseDefaultStrategy uses multi-row INSERT statements.
func (s *Seeder) UseDefaultStrategy() *Seeder { s.strategy = StrategyInsert; return s }

// Strategy sets the strategy explicitly.
func (s *Seeder) Strategy(st Strategy) *Seeder { s.strategy = st; return s }

// CSVFallback controls whether a failing CSV preflight falls back to the
// INSERT strategy (default true) or fails the run.
func (s *Seeder) CSVFallback(enabled bool) *Seeder { s.csvFallback = enabled; return s }

// Truncate empties the table first. MySQL uses TRUNCATE (resets
// AUTO_INCREMENT); Postgres and SQLite use DELETE and keep sequences.
func (s *Seeder) Truncate() *Seeder { s.truncate = true; return s }

// UseTransactions wraps the whole run in a single transaction (default).
func (s *Seeder) UseTransactions() *Seeder {
	if s.txExplicit && s.tx != txSingle {
		return s.fail("UseTransactions conflicts with an earlier CommitEvery/WithoutTransactions")
	}
	s.tx, s.txExplicit = txSingle, true
	return s
}

// WithoutTransactions runs every chunk in autocommit mode.
func (s *Seeder) WithoutTransactions() *Seeder {
	if s.txExplicit && s.tx != txNone {
		return s.fail("WithoutTransactions conflicts with an earlier UseTransactions/CommitEvery")
	}
	s.tx, s.txExplicit = txNone, true
	return s
}

// CommitEvery commits after every n chunks instead of once at the end.
func (s *Seeder) CommitEvery(chunks int) *Seeder {
	if chunks < 1 {
		return s.fail("CommitEvery must be >= 1, got %d", chunks)
	}
	if s.txExplicit && s.tx != txCommitEvery {
		return s.fail("CommitEvery conflicts with an earlier UseTransactions/WithoutTransactions")
	}
	s.tx, s.txExplicit, s.commitEvery = txCommitEvery, true, chunks
	return s
}

// Upsert turns inserts into upserts keyed on the given unique columns.
// By default every other column is updated; pass Update() to narrow it.
func (s *Seeder) Upsert(uniqueBy ...string) *Seeder {
	if len(uniqueBy) == 0 {
		return s.fail("Upsert needs at least one unique column")
	}
	s.upsert, s.upsertBy = true, uniqueBy
	return s
}

// Update limits which columns an Upsert overwrites on conflict.
func (s *Seeder) Update(cols ...string) *Seeder { s.upsertUpdate = cols; return s }

// DryRun generates and inserts everything inside a transaction that is
// rolled back, validating data and constraints without keeping any rows.
func (s *Seeder) DryRun() *Seeder { s.dryRun = true; return s }

// DisableForeignKeyChecks turns off FK checks while seeding (default on).
// MySQL: FOREIGN_KEY_CHECKS=0. SQLite: PRAGMA foreign_keys=OFF.
// Postgres: SET CONSTRAINTS ALL DEFERRED (affects DEFERRABLE constraints only).
func (s *Seeder) DisableForeignKeyChecks() *Seeder { s.disableFK = true; return s }

// EnableForeignKeyChecks keeps FK checks on while seeding.
func (s *Seeder) EnableForeignKeyChecks() *Seeder { s.disableFK = false; return s }

// DisableUniqueChecks sets UNIQUE_CHECKS=0 on MySQL (opt-in; ignored elsewhere).
func (s *Seeder) DisableUniqueChecks() *Seeder { s.disableUnique = true; return s }

// EnableUniqueChecks undoes DisableUniqueChecks.
func (s *Seeder) EnableUniqueChecks() *Seeder { s.disableUnique = false; return s }

// RetryAttempts sets total attempts (1-10, default 3) for a unit of work that
// fails with a deadlock, lock timeout, serialization failure or SQLITE_BUSY.
// A unit is the transaction being written, so retries never leave gaps.
func (s *Seeder) RetryAttempts(n int) *Seeder {
	if n < 1 || n > 10 {
		return s.fail("RetryAttempts must be between 1 and 10, got %d", n)
	}
	s.retryAttempts = n
	return s
}

// RetryIf replaces the built-in retryable-error detection.
func (s *Seeder) RetryIf(fn func(error) bool) *Seeder { s.retryIf = fn; return s }

// WithoutColumnValidation skips the pre-run schema check.
func (s *Seeder) WithoutColumnValidation() *Seeder { s.validateColumns = false; return s }

// Force allows running when APP_ENV/GO_ENV/ENV says production.
func (s *Seeder) Force() *Seeder { s.force = true; return s }

// OnProgress registers a callback invoked about every 100ms and once at the end.
// Callbacks run on a single goroutine.
func (s *Seeder) OnProgress(fn func(Progress)) *Seeder { s.progress = fn; return s }

// WithProgressBar renders a progress bar on stderr.
func (s *Seeder) WithProgressBar() *Seeder { s.progress = ProgressBar(nil); return s }

// WithoutProgressTracking removes any progress callback.
func (s *Seeder) WithoutProgressTracking() *Seeder { s.progress = nil; return s }

// Logger sets where warnings (e.g. strategy fallbacks) go. Default slog.Default().
func (s *Seeder) Logger(l *slog.Logger) *Seeder { s.logger = l; return s }

// When applies fn to the builder if cond is true.
func (s *Seeder) When(cond bool, fn func(*Seeder)) *Seeder {
	if cond {
		fn(s)
	}
	return s
}

// Unless applies fn to the builder if cond is false.
func (s *Seeder) Unless(cond bool, fn func(*Seeder)) *Seeder { return s.When(!cond, fn) }

// validate checks option combinations that cannot work together.
func (s *Seeder) validate() error {
	if s.err != nil {
		return s.err
	}
	switch {
	case s.db == nil:
		return errors.New("turboseeder: nil *sql.DB")
	case s.table == "":
		return errors.New("turboseeder: no table set")
	case s.gen == nil:
		return errors.New("turboseeder: no generator set (Generate, GenerateMap or FromModel)")
	case s.count < 1:
		return errors.New("turboseeder: Count is required")
	case s.dryRun && s.truncate:
		return errors.New("turboseeder: DryRun cannot be combined with Truncate")
	case s.dryRun && s.tx != txSingle:
		return errors.New("turboseeder: DryRun requires a single transaction (drop CommitEvery/WithoutTransactions)")
	case s.upsert && s.strategy == StrategyCSV:
		return errors.New("turboseeder: Upsert is not supported with the CSV strategy")
	case len(s.upsertUpdate) > 0 && !s.upsert:
		return errors.New("turboseeder: Update requires Upsert")
	}
	return nil
}

// generator produces rows for a fixed column list.
type generator interface {
	// columns returns the columns the generator inherently knows, if any.
	columns() []string
	// bind prepares a row writer for cols (excluding timestamp columns).
	bind(cols []string) (rowFunc, error)
}

// rowFunc writes the values for index idx into dst (len(dst) == len(cols)).
type rowFunc func(idx int, dst []any) error

type sliceGen func(i int) []any

func (g sliceGen) columns() []string { return nil }

func (g sliceGen) bind(cols []string) (rowFunc, error) {
	n := len(cols)
	return func(idx int, dst []any) error {
		vals := g(idx)
		if len(vals) != n {
			return fmt.Errorf("row %d: generator returned %d values for %d columns", idx, len(vals), n)
		}
		copy(dst, vals)
		return nil
	}, nil
}

type mapGen struct {
	fn func(i int) map[string]any
}

func (g *mapGen) columns() []string { return nil }

// inferColumns calls the generator once for idx so its keys become the
// column list (the row itself is generated again when it is written).
func (g *mapGen) inferColumns(idx int) []string {
	first := g.fn(idx)
	cols := make([]string, 0, len(first))
	for k := range first {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	return cols
}

func (g *mapGen) bind(cols []string) (rowFunc, error) {
	return func(idx int, dst []any) error {
		m := g.fn(idx)
		for i, c := range cols {
			v, ok := m[c]
			if !ok {
				return fmt.Errorf("row %d: missing column %q%s", idx, c, extraKeys(m, cols))
			}
			dst[i] = v
		}
		if len(m) != len(cols) {
			return fmt.Errorf("row %d:%s", idx, extraKeys(m, cols))
		}
		return nil
	}, nil
}

// extraKeys describes map keys that are not columns (usually typos).
func extraKeys(m map[string]any, cols []string) string {
	var extra []string
	for k := range m {
		if !contains(cols, k) {
			extra = append(extra, fmt.Sprintf("%q", k))
		}
	}
	if len(extra) == 0 {
		return ""
	}
	sort.Strings(extra)
	return " (unexpected column " + strings.Join(extra, ", ") + ")"
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// Result describes a completed run. Equivalent of SeederResultDTO.
type Result struct {
	Table     string
	Dialect   Dialect
	Strategy  Strategy // the strategy actually used (after any fallback)
	Rows      int64    // rows written (or that would have been, for DryRun)
	Duration  time.Duration
	DryRun    bool
	Workers   int
	ChunkSize int
	Retries   int
	// FallbackReason is set when the CSV strategy fell back to INSERT.
	FallbackReason string
	// PeakHeapBytes is the highest live-heap size sampled during the run.
	PeakHeapBytes uint64
}

// RowsPerSecond is the average throughput of the run.
func (r Result) RowsPerSecond() float64 {
	if r.Duration <= 0 {
		return 0
	}
	return float64(r.Rows) / r.Duration.Seconds()
}

// PeakHeapMB returns PeakHeapBytes in mebibytes.
func (r Result) PeakHeapMB() float64 { return float64(r.PeakHeapBytes) / (1 << 20) }

func (r Result) String() string {
	dry := ""
	if r.DryRun {
		dry = " (dry run, rolled back)"
	}
	return fmt.Sprintf("%s: %d rows in %s (%.0f rows/s, %s, %d worker(s), peak heap %.1f MB)%s",
		r.Table, r.Rows, r.Duration.Round(time.Millisecond), r.RowsPerSecond(),
		r.Strategy, r.Workers, r.PeakHeapMB(), dry)
}
