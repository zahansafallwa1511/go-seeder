package turboseeder

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Run executes the seeder. On failure it returns the error together with a
// Result describing how far the run got.
func (s *Seeder) Run(ctx context.Context) (Result, error) {
	if err := s.validate(); err != nil {
		return Result{}, err
	}
	if !s.force && isProduction() {
		return Result{}, errors.New("turboseeder: refusing to seed in production (APP_ENV/GO_ENV/ENV); call Force() to override")
	}
	d := s.dialect
	if d == Unknown {
		if d = DetectDialect(s.db); d == Unknown {
			return Result{}, fmt.Errorf("turboseeder: cannot detect dialect for driver %T; call Dialect()", s.db.Driver())
		}
	}
	log := s.logger
	if log == nil {
		log = slog.Default()
	}
	res := Result{Table: s.table, Dialect: d, DryRun: s.dryRun, Strategy: StrategyInsert}

	p := &plan{s: s, d: d, log: log}

	if s.strategy == StrategyCSV {
		l, reason := s.resolveLoader(ctx, d)
		if l == nil {
			if !s.csvFallback {
				return res, fmt.Errorf("turboseeder: CSV strategy unavailable: %s", reason)
			}
			log.Warn("turboseeder: CSV strategy unavailable, falling back to INSERT", "table", s.table, "reason", reason)
			res.FallbackReason = reason
		} else {
			p.loader = l
			res.Strategy = StrategyCSV
		}
	}

	if err := p.resolveColumns(ctx); err != nil {
		return res, err
	}

	p.chunk = s.chunkSize
	if p.chunk == 0 {
		p.chunk = d.defaultChunkSize()
	}
	if maxRows := d.maxBindParams(ctx, s.db) / len(p.cols); p.loader == nil && p.chunk > maxRows {
		p.chunk = maxRows
	}
	res.ChunkSize = p.chunk

	workers := s.workers
	if d == SQLite && workers > 1 {
		log.Warn("turboseeder: SQLite has a single writer; Parallel ignored", "table", s.table)
		workers = 1
	}
	if maxOpen := s.db.Stats().MaxOpenConnections; maxOpen > 0 && workers > maxOpen {
		log.Warn("turboseeder: Parallel clamped to the pool's MaxOpenConns", "requested", workers, "max", maxOpen)
		workers = maxOpen
	}

	if s.truncate {
		if err := p.truncate(ctx); err != nil {
			return res, err
		}
	}

	p.prepareSQL()
	p.now = time.Now()
	p.tracker = newTracker(s.table, int64(s.count), s.progress)
	err := p.execute(ctx, workers, &res)
	res.Duration = p.tracker.finish(err == nil)
	res.Rows = p.tracker.done.Load()
	res.PeakHeapBytes = p.tracker.peak.Load()
	res.Retries = int(p.retries.Load())
	if err != nil {
		return res, fmt.Errorf("turboseeder: seeding %s failed: %w", s.table, err)
	}
	return res, nil
}

func isProduction() bool {
	for _, k := range []string{"APP_ENV", "GO_ENV", "ENV", "ENVIRONMENT"} {
		switch strings.ToLower(os.Getenv(k)) {
		case "production", "prod":
			return true
		}
	}
	return false
}

func (s *Seeder) resolveLoader(ctx context.Context, d Dialect) (Loader, string) {
	l := lookupLoader(d)
	if l == nil {
		return nil, fmt.Sprintf("no CSV loader registered for %s (import go-seeder/loaders/pgcopy or go-seeder/loaders/mysqlcsv)", d)
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, err.Error()
	}
	defer conn.Close()
	if err := l.Preflight(ctx, conn); err != nil {
		return nil, err.Error()
	}
	return l, ""
}

// plan holds everything resolved before writing starts.
type plan struct {
	s      *Seeder
	d      Dialect
	log    *slog.Logger
	loader Loader

	cols  []string // all columns written, timestamps last
	nUser int      // leading columns filled by the generator
	rf    rowFunc
	now   time.Time
	chunk int

	fullSQL      string // INSERT for a full chunk
	insertPrefix string
	upsertSuffix string

	tracker *tracker
	retries atomic.Int64
}

func (p *plan) resolveColumns(ctx context.Context) error {
	s := p.s
	var (
		info    []ColumnInfo
		infoErr error
		loaded  bool
	)
	getInfo := func() ([]ColumnInfo, error) {
		if !loaded {
			info, infoErr = Introspect(ctx, s.db, p.d, s.table)
			loaded = true
		}
		return info, infoErr
	}

	isTS := func(c string) bool {
		for _, t := range s.timestamps {
			if strings.EqualFold(t, c) {
				return true
			}
		}
		return false
	}

	cols := s.columns
	if len(cols) == 0 {
		switch g := s.gen.(type) {
		case *mapGen:
			if s.columnsFromSchema {
				break
			}
			cols = g.inferColumns(s.offset)
		default:
			if gc := g.columns(); gc != nil { // model
				cols = gc
				if info, err := getInfo(); err == nil {
					cols = dropNonInsertable(cols, info)
				} else if s.validateColumns {
					return err
				}
			}
		}
	}
	if len(cols) == 0 && s.columnsFromSchema {
		info, err := getInfo()
		if err != nil {
			return err
		}
		for _, c := range info {
			if c.Insertable() && !isTS(c.Name) {
				cols = append(cols, c.Name)
			}
		}
	}
	if len(cols) == 0 {
		return errors.New("turboseeder: no columns: call Columns() or ColumnsFromSchema()")
	}

	rf, err := s.gen.bind(cols)
	if err != nil {
		return err
	}
	p.rf, p.nUser = rf, len(cols)
	p.cols = append([]string(nil), cols...)
	for _, t := range s.timestamps {
		if !containsFold(cols, t) {
			p.cols = append(p.cols, t)
		}
	}

	if s.validateColumns {
		info, err := getInfo()
		if err != nil {
			return err
		}
		if err := checkColumns(s.table, p.cols, info); err != nil {
			return err
		}
	}
	if s.upsert {
		for _, u := range s.upsertBy {
			if !containsFold(p.cols, u) {
				return fmt.Errorf("turboseeder: upsert key %q is not one of the seeded columns", u)
			}
		}
	}
	return nil
}

func dropNonInsertable(cols []string, info []ColumnInfo) []string {
	skip := map[string]bool{}
	for _, c := range info {
		if !c.Insertable() {
			skip[strings.ToLower(c.Name)] = true
		}
	}
	out := cols[:0:0]
	for _, c := range cols {
		if !skip[strings.ToLower(c)] {
			out = append(out, c)
		}
	}
	return out
}

func containsFold(list []string, s string) bool {
	for _, x := range list {
		if strings.EqualFold(x, s) {
			return true
		}
	}
	return false
}

func (p *plan) truncate(ctx context.Context) error {
	conn, err := p.s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	settings, err := p.d.sessionSettings(ctx, conn, p.s.disableFK, false)
	if err != nil {
		return err
	}
	defer restoreSettings(conn, settings)
	for _, st := range settings {
		if _, err := conn.ExecContext(ctx, st.apply); err != nil {
			return err
		}
	}
	if _, err := conn.ExecContext(ctx, p.d.truncateSQL(p.s.table)); err != nil {
		return fmt.Errorf("turboseeder: truncate %s: %w", p.s.table, err)
	}
	return nil
}

func restoreSettings(conn *sql.Conn, settings []sessionSetting) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, st := range settings {
		conn.ExecContext(ctx, st.restore) //nolint:errcheck // best effort; conn is discarded on failure
	}
}

func (p *plan) prepareSQL() {
	d, s := p.d, p.s
	p.insertPrefix = "INSERT INTO " + d.QuoteIdent(s.table) + " (" + d.QuoteIdents(p.cols) + ") VALUES "
	if s.upsert {
		update := s.upsertUpdate
		if len(update) == 0 {
			for _, c := range p.cols {
				if !containsFold(s.upsertBy, c) {
					update = append(update, c)
				}
			}
		}
		var b strings.Builder
		if d == MySQL {
			b.WriteString(" ON DUPLICATE KEY UPDATE ")
			if len(update) == 0 {
				q := d.QuoteIdent(s.upsertBy[0])
				b.WriteString(q + " = " + q)
			}
			for i, c := range update {
				if i > 0 {
					b.WriteString(", ")
				}
				q := d.QuoteIdent(c)
				b.WriteString(q + " = VALUES(" + q + ")")
			}
		} else {
			b.WriteString(" ON CONFLICT (" + d.QuoteIdents(s.upsertBy) + ") DO ")
			if len(update) == 0 {
				b.WriteString("NOTHING")
			} else {
				b.WriteString("UPDATE SET ")
				for i, c := range update {
					if i > 0 {
						b.WriteString(", ")
					}
					q := d.QuoteIdent(c)
					b.WriteString(q + " = EXCLUDED." + q)
				}
			}
		}
		p.upsertSuffix = b.String()
	}
	if p.loader == nil {
		p.fullSQL = p.insertSQL(p.chunk)
	}
}

func (p *plan) insertSQL(rows int) string {
	var b strings.Builder
	b.Grow(len(p.insertPrefix) + rows*len(p.cols)*7 + len(p.upsertSuffix))
	b.WriteString(p.insertPrefix)
	p.d.writePlaceholders(&b, rows, len(p.cols))
	b.WriteString(p.upsertSuffix)
	return b.String()
}

// unit is a contiguous range of row positions [start, end) written as one
// transaction (or one autocommit stream when transactions are off).
type unit struct{ start, end int }

func (u unit) size() int { return u.end - u.start }

func (p *plan) units(workers int) []unit {
	s := p.s
	size := (s.count + workers - 1) / workers
	if s.tx == txCommitEvery {
		size = s.commitEvery * p.chunk
	}
	var out []unit
	for start := 0; start < s.count; start += size {
		out = append(out, unit{start, min(start+size, s.count)})
	}
	return out
}

// execute runs all units across the workers and finalises transactions.
func (p *plan) execute(parent context.Context, workers int, res *Result) error {
	units := p.units(workers)
	workers = min(workers, len(units))
	res.Workers = workers

	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)

	ws := make([]*worker, 0, workers)
	defer func() {
		for _, w := range ws {
			w.close()
		}
	}()
	for range workers {
		w, err := p.newWorker(ctx)
		if err != nil {
			return err
		}
		ws = append(ws, w)
	}

	queue := make(chan unit, len(units))
	for _, u := range units {
		queue <- u
	}
	close(queue)

	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	for _, w := range ws {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for u := range queue {
				if ctx.Err() != nil {
					return
				}
				if err := w.runUnit(ctx, u); err != nil {
					errOnce.Do(func() { firstErr = err })
					cancel(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	if err := parent.Err(); err != nil {
		return err
	}

	// Single-transaction mode: every worker holds an open transaction.
	committed := 0
	for _, w := range ws {
		if w.pending == nil {
			continue
		}
		tx := w.pending
		w.pending = nil
		if p.s.dryRun {
			if err := tx.Rollback(); err != nil {
				return fmt.Errorf("dry run rollback: %w", err)
			}
			continue
		}
		if err := tx.Commit(); err != nil {
			if committed > 0 {
				return fmt.Errorf("commit failed after %d of %d worker transactions committed (partial data remains): %w", committed, len(ws), err)
			}
			return fmt.Errorf("commit: %w", err)
		}
		committed++
	}
	return nil
}

// worker owns one connection for the whole run.
type worker struct {
	p        *plan
	conn     *sql.Conn
	settings []sessionSetting
	pending  *sql.Tx   // open transaction awaiting the final commit
	connStmt *sql.Stmt // prepared full-chunk INSERT when transactions are off
	bufs     chan *chunkBuf
}

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

const pipelineDepth = 3 // chunks generated ahead of the database

func (p *plan) newWorker(ctx context.Context) (*worker, error) {
	conn, err := p.s.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	w := &worker{p: p, conn: conn, bufs: make(chan *chunkBuf, pipelineDepth)}
	w.settings, err = p.d.sessionSettings(ctx, conn, p.s.disableFK, p.s.disableUnique)
	if err != nil {
		conn.Close()
		return nil, err
	}
	for i, st := range w.settings {
		if _, err := conn.ExecContext(ctx, st.apply); err != nil {
			restoreSettings(conn, w.settings[:i])
			conn.Close()
			return nil, fmt.Errorf("apply %q: %w", st.apply, err)
		}
	}
	for range pipelineDepth {
		w.bufs <- &chunkBuf{args: make([]any, p.chunk*len(p.cols))}
	}
	return w, nil
}

func (w *worker) close() {
	if w.pending != nil {
		w.pending.Rollback() //nolint:errcheck
	}
	if w.connStmt != nil {
		w.connStmt.Close()
	}
	restoreSettings(w.conn, w.settings)
	w.conn.Close()
}

// runUnit writes one unit, retrying the whole unit on transient errors
// (a deadlock aborts the entire transaction, so partial retries are unsafe).
func (w *worker) runUnit(ctx context.Context, u unit) error {
	p := w.p
	retryable := p.s.retryIf
	if retryable == nil {
		retryable = IsRetryable
	}
	for attempt := 0; ; attempt++ {
		written, err := w.attempt(ctx, u)
		if err == nil {
			return nil
		}
		if p.s.tx != txNone {
			p.tracker.add(-written) // the transaction rolled back
		}
		if ctx.Err() != nil || attempt+1 >= p.s.retryAttempts || !retryable(err) {
			return err
		}
		p.retries.Add(1)
		p.log.Warn("turboseeder: retrying after transient error", "table", p.s.table,
			"rows", fmt.Sprintf("%d-%d", u.start, u.end-1), "attempt", attempt+2, "error", err)
		if err := backoff(ctx, attempt); err != nil {
			return err
		}
	}
}

func (w *worker) attempt(ctx context.Context, u unit) (written int64, err error) {
	p := w.p
	var (
		tx *sql.Tx
		ex execer = w.conn
	)
	if p.s.tx != txNone {
		if tx, err = w.conn.BeginTx(ctx, nil); err != nil {
			return 0, err
		}
		defer func() {
			if err != nil {
				tx.Rollback() //nolint:errcheck
			}
		}()
		for _, q := range p.d.txSettings(p.s.disableFK) {
			if _, err = tx.ExecContext(ctx, q); err != nil {
				return 0, err
			}
		}
		ex = tx
	}

	if p.loader != nil {
		written, err = w.load(ctx, tx, u)
	} else {
		written, err = w.insert(ctx, ex, tx != nil, u)
	}
	if err != nil {
		return written, err
	}
	switch p.s.tx {
	case txCommitEvery:
		err = tx.Commit()
	case txSingle:
		w.pending = tx
	}
	return written, err
}

func (w *worker) insert(ctx context.Context, ex execer, inTx bool, u unit) (written int64, err error) {
	p := w.p
	st := w.stream(ctx, u)
	defer st.stop()

	var stmt *sql.Stmt
	if inTx {
		defer func() {
			if stmt != nil {
				stmt.Close()
			}
		}()
	} else {
		stmt = w.connStmt
		defer func() { w.connStmt = stmt }()
	}

	for b := range st.out {
		if b.err != nil {
			st.release(b)
			return written, b.err
		}
		err := w.execChunk(ctx, ex, &stmt, b)
		// Without a transaction each statement is atomic, so a transient
		// failure can be retried on the same, already generated chunk.
		for attempt := 0; err != nil && !inTx && attempt+1 < p.s.retryAttempts && ctx.Err() == nil && w.retryable(err); attempt++ {
			p.retries.Add(1)
			p.log.Warn("turboseeder: retrying chunk after transient error", "table", p.s.table, "attempt", attempt+2, "error", err)
			if berr := backoff(ctx, attempt); berr != nil {
				break
			}
			err = w.execChunk(ctx, ex, &stmt, b)
		}
		st.release(b)
		if err != nil {
			return written, err
		}
		written += int64(b.rows)
		p.tracker.add(int64(b.rows))
	}
	if int(written) != u.size() {
		return written, cmpErr(ctx, errors.New("row stream ended early"))
	}
	return written, nil
}

func (w *worker) retryable(err error) bool {
	if w.p.s.retryIf != nil {
		return w.p.s.retryIf(err)
	}
	return IsRetryable(err)
}

func cmpErr(ctx context.Context, fallback error) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	return fallback
}

func (w *worker) execChunk(ctx context.Context, ex execer, stmt **sql.Stmt, b *chunkBuf) error {
	p := w.p
	args := b.args[:b.rows*len(p.cols)]
	if b.rows == p.chunk {
		if *stmt == nil {
			s, err := ex.PrepareContext(ctx, p.fullSQL)
			if err != nil {
				return err
			}
			*stmt = s
		}
		_, err := (*stmt).ExecContext(ctx, args...)
		return err
	}
	_, err := ex.ExecContext(ctx, p.insertSQL(b.rows), args...)
	return err
}

func (w *worker) load(ctx context.Context, tx *sql.Tx, u unit) (int64, error) {
	p := w.p
	st := w.stream(ctx, u)
	defer st.stop()
	ncols := len(p.cols)

	rows := func(yield func([]any, error) bool) {
		for b := range st.out {
			if b.err != nil {
				st.release(b)
				yield(nil, b.err)
				return
			}
			for r := range b.rows {
				if !yield(b.args[r*ncols:(r+1)*ncols], nil) {
					st.release(b)
					return
				}
			}
			st.release(b)
		}
		if err := ctx.Err(); err != nil {
			yield(nil, err)
		}
	}

	var sent atomic.Int64
	n, err := p.loader.Load(ctx, LoadJob{
		Conn: w.conn, Tx: tx, Dialect: p.d, Table: p.s.table, Columns: p.cols,
		Rows: rows, Expected: u.size(),
		OnRows: func(k int) { sent.Add(int64(k)); p.tracker.add(int64(k)) },
	})
	if err != nil {
		return sent.Load(), err
	}
	if n != int64(u.size()) {
		return sent.Load(), fmt.Errorf("bulk load wrote %d rows, expected %d (rows were skipped as duplicates or invalid; check SHOW WARNINGS / server logs)", n, u.size())
	}
	return sent.Load(), nil
}

// chunkBuf is a reusable buffer holding one chunk of generated rows.
type chunkBuf struct {
	args []any
	rows int
	err  error
}

type stream struct {
	out     chan *chunkBuf
	bufs    chan *chunkBuf
	cancel  context.CancelFunc
	done    chan struct{}
	stopped bool
}

func (st *stream) release(b *chunkBuf) { st.bufs <- b }

// stop cancels the producer and waits for it, so the generator is never
// running for a stale unit while the next one starts.
func (st *stream) stop() {
	if st.stopped {
		return
	}
	st.stopped = true
	st.cancel()
	<-st.done // out is unbuffered, so the producer returned any buffer it held
}

// stream generates the unit's rows on a separate goroutine, pipelining
// row generation with database writes.
func (w *worker) stream(parent context.Context, u unit) *stream {
	p := w.p
	ctx, cancel := context.WithCancel(parent)
	st := &stream{out: make(chan *chunkBuf), bufs: w.bufs, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(st.done)
		defer close(st.out)
		ncols := len(p.cols)
		for i := u.start; i < u.end; {
			var b *chunkBuf
			select {
			case <-ctx.Done():
				return
			case b = <-st.bufs:
			}
			if ctx.Err() != nil {
				st.bufs <- b
				return
			}
			n := min(p.chunk, u.end-i)
			b.rows, b.err = n, p.fillChunk(b.args[:n*ncols], p.s.offset+i, n)
			select {
			case st.out <- b:
			case <-ctx.Done():
				st.bufs <- b
				return
			}
			if b.err != nil {
				return
			}
			i += n
		}
	}()
	return st
}

// fillChunk generates n rows starting at generator index first.
func (p *plan) fillChunk(dst []any, first, n int) (err error) {
	ncols := len(p.cols)
	idx := first
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("row %d: generator panicked: %v", idx, r)
		}
	}()
	for r := range n {
		idx = first + r
		row := dst[r*ncols : (r+1)*ncols]
		if err := p.rf(idx, row[:p.nUser]); err != nil {
			return err
		}
		for c := range p.nUser {
			v, err := Normalize(row[c])
			if err != nil {
				return fmt.Errorf("row %d, column %s: %w", idx, p.cols[c], err)
			}
			row[c] = v
		}
		for c := p.nUser; c < ncols; c++ {
			row[c] = p.now
		}
	}
	return nil
}
