# go-seeder

[![CI](https://github.com/zahansafallwa1511/go-seeder/actions/workflows/ci.yml/badge.svg)](https://github.com/zahansafallwa1511/go-seeder/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/zahansafallwa1511/go-seeder.svg)](https://pkg.go.dev/github.com/zahansafallwa1511/go-seeder)
[![Go Report Card](https://goreportcard.com/badge/github.com/zahansafallwa1511/go-seeder)](https://goreportcard.com/report/github.com/zahansafallwa1511/go-seeder)
[![Go 1.26+](https://img.shields.io/badge/go-1.26%2B-00ADD8)](https://go.dev/dl/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Seed millions of rows into MySQL, PostgreSQL or SQLite, fast, from Go.

A Go take on [laravel-turbo-seeder](https://github.com/iz-ahmad/laravel-turbo-seeder):
same builder API and feature set, plus things Go makes cheap — row generation
pipelined with database writes, parallel writer connections, and bulk loads
streamed straight from memory with no temp files.

```go
email := turbodata.UniqueEmail("user", "example.com")

res, err := turboseeder.ForTable(db, "users").
    Columns("name", "email", "password", "created_at").
    Generate(func(i int) []any {
        return []any{"User " + strconv.Itoa(i), email(i), turbodata.HashedPassword(), turbodata.NowOnce()}
    }).
    Count(1_000_000).
    WithProgressBar().
    Run(ctx)

fmt.Println(res) // users: 1000000 rows in 5.7s (176088 rows/s, insert, 1 worker(s), peak heap 3.8 MB)
```

## Install

```sh
go get github.com/zahansafallwa1511/go-seeder
```

```go
import (
    turboseeder "github.com/zahansafallwa1511/go-seeder"
    "github.com/zahansafallwa1511/go-seeder/turbodata"
)
```

The package is named `turboseeder`, so the alias above is optional but makes the
import read clearly. Requires Go 1.26+. Works with any `*sql.DB` from
go-sql-driver/mysql, pgx (`stdlib`), lib/pq, modernc.org/sqlite,
mattn/go-sqlite3 and friends. The core package imports no database driver;
only the optional loader packages do.

## Generating rows

Three ways in. Any of them works with any strategy.

**`Generate`**: positional values, fastest:

```go
turboseeder.ForTable(db, "posts").
    Columns("user_id", "title", "created_at").
    Generate(func(i int) []any { return []any{userIDs.Cycle(i), "Post " + strconv.Itoa(i), now} })
```

**`GenerateMap`**: column → value maps. Without `Columns()`, the columns are
the sorted keys of the first row. Typos are reported (`unexpected column "emial"`).

```go
turboseeder.ForTable(db, "users").
    GenerateMap(func(i int) map[string]any { return map[string]any{"name": "U", "email": email(i)} })
```

**`FromModel`**: the Go equivalent of `fromFactory()`. You pass a struct factory:

```go
type User struct {
    ID        int64     `db:"id"`         // auto-increment: dropped via schema introspection
    Name      string                      // -> name
    Email     string    `db:"email"`
    Meta      map[string]any `db:"meta"`  // JSON-encoded
    Posts     []Post                      // relation-looking: skipped
    CreatedAt time.Time `gorm:"column:created_at"`
}
func (User) TableName() string { return "users" } // optional; default is snake_case plural

turboseeder.FromModel(db, func(i int) User { return User{Name: ..., Email: email(i)} }).Count(50_000).Run(ctx)
```

Generators get the row index `i` (starting at `Offset()`, default 0). With
`Parallel(n)` they must be safe for concurrent use; every `turbodata` helper is.

## Strategies

| | Default (`INSERT`) | CSV (`UseCSVStrategy()`) |
|---|---|---|
| Mechanism | multi-row `INSERT`, prepared once per transaction | Postgres `COPY FROM STDIN`, MySQL `LOAD DATA LOCAL INFILE` |
| Disk | none | none: CSV is streamed through an `io.Pipe` |
| Setup | none | import the loader package; MySQL needs `SET GLOBAL local_infile = 1` |
| Upsert | yes | no (returns an error) |

Loaders are opt-in packages, so the core never imports a driver:

```go
import (
    _ "github.com/jackc/pgx/v5/stdlib"
    _ "github.com/zahansafallwa1511/go-seeder/loaders/pgcopy"   // needs the "pgx" driver
    _ "github.com/zahansafallwa1511/go-seeder/loaders/mysqlcsv" // needs go-sql-driver/mysql
)
```

A preflight runs before the CSV strategy. It checks that a loader is registered, the driver
matches, and `local_infile` is on. If the preflight fails, the run falls back to `INSERT`
with a warning and records why in `Result.FallbackReason`. `CSVFallback(false)`
turns the fallback into an error. SQLite has no loader and always uses `INSERT`, which is
already the fast path there.

The CSV encoding needs no NULL marker. On Postgres every value is quoted and NULL is the
unquoted empty field. On MySQL every value is quoted and backslash-escaped and NULL is a
bare `\N`. Because of this, a string such as `\N` can never turn into NULL. MySQL's
`LOAD DATA LOCAL` would normally downgrade bad values and duplicates to warnings, so
the loader turns any warning into an error.

## Builder reference

| Laravel | Go | Notes |
|---|---|---|
| `forTable()` | `ForTable(db, "users")`, `.Table()` | `"schema.table"` supported |
| `fromFactory()` | `FromModel[T](db, fn)` | struct tags `db` / `gorm:"column:"` |
| `columns()` / `columnsFromSchema()` | `Columns(...)` / `ColumnsFromSchema()` | schema skips auto-increment & generated columns |
| `generate()` | `Generate(fn)` / `GenerateMap(fn)` | |
| `withTimestamps()` | `WithTimestamps(cols...)` | default `created_at`, `updated_at`, set once per run |
| `count()` / `chunkSize()` | `Count(n)` / `ChunkSize(n)` | chunk clamped to bind limit (65,535; SQLite auto-detected) |
| — | `Offset(n)` | start index, for appending without unique collisions |
| — | `Parallel(n)` | n writer connections over disjoint ranges |
| `truncate()` | `Truncate()` | MySQL `TRUNCATE`; Postgres/SQLite `DELETE` (keeps sequences) |
| `useTransactions()` / `withoutTransactions()` | same names | default: one transaction |
| `commitEvery()` | `CommitEvery(chunks)` | conflicts with the other transaction modes |
| `upsert()` | `Upsert(keys...)` + optional `Update(cols...)` | `ON CONFLICT` / `ON DUPLICATE KEY UPDATE` |
| `dryRun()` | `DryRun()` | runs everything, then rolls back |
| `disable/enableForeignKeyChecks()` | same names | default disabled (see below) |
| `disable/enableUniqueChecks()` | same names | MySQL only, opt-in |
| `retryAttempts()` | `RetryAttempts(1..10)`, `RetryIf(fn)` | default 3 attempts |
| `withoutColumnValidation()` | same | |
| `force()` | `Force()` | |
| `withProgressTracking()` | `OnProgress(fn)` / `WithProgressBar()` | |
| `when()` / `unless()` | `When(cond, fn)` / `Unless(cond, fn)` | |
| `useCsvStrategy()` / `useDefaultStrategy()` | same, plus `Strategy(s)`, `CSVFallback(bool)` | |
| `ValueFormatter::extend()` | `RegisterFormatter(fn)` | |
| `SeederResultDTO` | `Result` | `Rows`, `Duration`, `RowsPerSecond()`, `PeakHeapMB()`, `Retries`, … |
| events | `Run` returns `(Result, error)` | |
| `turbo-seeder:test-connection` / `:benchmark` | `cmd/turboseeder check` / `benchmark` | |

**Foreign keys.** MySQL uses `FOREIGN_KEY_CHECKS=0` and SQLite uses
`PRAGMA foreign_keys=OFF`. Both are set on the seeder's own connections and restored
afterwards. Postgres runs `SET CONSTRAINTS ALL DEFERRED` in each transaction, which only
affects constraints declared `DEFERRABLE`. Postgres offers no non-superuser way to
skip FK checks.

**Validation.** Before anything is written, the table is introspected. The run fails if a
column you name doesn't exist, if a column is generated, or if a `NOT NULL` column without
a default is missing.

**Production safety.** If `APP_ENV`, `GO_ENV`, `ENV` or `ENVIRONMENT` is `production`
or `prod`, `Run` refuses to run unless `Force()` was called.

## How a run works

1. Columns are resolved and validated, the chunk size is clamped, and the table is truncated if requested.
2. The rows are split into **units**:
   - one per worker in single-transaction mode
   - `CommitEvery × ChunkSize` rows each with `CommitEvery`
   - one per worker without transactions
3. Each worker holds one connection. A producer goroutine generates the next chunks
   into reused buffers (3 deep) while the current chunk is being written. That's why
   heap stays at a few MB no matter how many rows you seed.
4. Full chunks reuse one prepared statement. Only the last, partial chunk builds its own SQL.
5. **Retries happen at the unit level.** A deadlock aborts the whole transaction, so the
   unit is regenerated from its first index in a new transaction. Partial data never survives a retry.
   Without transactions, each statement is atomic, so a failed chunk is retried in place.
6. **Parallel commits are best-effort.** In single-transaction mode with `Parallel(n)`,
   each worker's transaction stays open until every worker has succeeded, then they all
   commit. If any worker fails, they all roll back. This is not a distributed transaction:
   a failure *during* the final commits is reported as partial.

## `turbodata` helpers

```go
import td "github.com/zahansafallwa1511/go-seeder/turbodata"
```

| Kind | Helpers |
|---|---|
| Index factories (create once, call with `i`) | `CycleFrom`, `UniqueEmail`, `UniqueUsername`, `UniqueSlug`, `UniqueUUID`, `SequentialDate`, `StreamFromTable` |
| Loaded pools | `FromTable[T]`, `FromQuery[T]` → `Pool[T]` with `.Cycle(i)` / `.Random()` |
| Per-row | `Weighted` (returns a picker), `RandomFrom`, `RandomInt`, `RandomFloat`, `RandomBool`, `Chance`, `Nullable`, `DateRange`, `Date` |
| Cached | `NowOnce`, `HashedPassword` (bcrypt of `"password"`, precomputed) |

The unique helpers derive uniqueness from the index, so they hold no state. Laravel's
Faker `unique()` keeps every value it has produced in memory; these don't.
`UniqueUUID` produces valid v4 UUIDs that are unique by construction: random bits plus the index.

## CLI

```sh
go install github.com/zahansafallwa1511/go-seeder/cmd/turboseeder@latest

turboseeder check     -driver pgx   -dsn 'postgres://localhost/app?sslmode=disable'
turboseeder benchmark -driver mysql -dsn 'root:secret@tcp(127.0.0.1:3306)/app' -rows 1000000 -workers 1,4
```

`benchmark` creates and drops its own `turbo_seeder_benchmark` table.

## Benchmarks

These are from one machine, so treat them as relative. The setup was an Apple Silicon laptop
with 10 cores, Postgres 17 (Homebrew defaults), MySQL 8.4 in Docker, and SQLite 3.54
(modernc). Each run was 1,000,000 rows into a 6-column table with a unique index,
using `turboseeder benchmark`:

| Database | INSERT ×1 | INSERT ×4 | CSV ×1 | CSV ×4 |
|---|---|---|---|---|
| PostgreSQL | 102k rows/s | 251k rows/s | 147k rows/s | 139k rows/s |
| MySQL | 176k rows/s | 349k rows/s | 255k rows/s | 357k rows/s |
| SQLite | 622k rows/s | — | — | — |

Peak heap stayed between 3 and 8 MB in every run. In the Postgres CSV run, the client used
1.4 s of CPU over 8.5 s of wall time, so throughput was bound by the database server, not
the library. Tune the server (WAL size, `synchronous_commit`, indexes) for further gains.

## Testing

```sh
go test ./...   # SQLite only

TURBOSEEDER_PG_DSN='postgres://localhost/turboseeder_test?sslmode=disable' \
TURBOSEEDER_MYSQL_DSN='root:secret@tcp(127.0.0.1:3306)/turboseeder_test' \
go test -race ./...   # all three databases
```

The MySQL tests need `local_infile=1`, for example with
`docker run -e MYSQL_ROOT_PASSWORD=secret -e MYSQL_DATABASE=turboseeder_test -p 3306:3306 mysql:8.4 --local-infile=1`.

CI runs the full suite against real Postgres and MySQL services on every push,
and fails if either suite is silently skipped.

## License

MIT. See [LICENSE](LICENSE).
