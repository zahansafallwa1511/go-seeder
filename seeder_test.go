package turboseeder_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ts "github.com/zahansafallwa1511/go-seeder"
	"github.com/zahansafallwa1511/go-seeder/turbodata"
)

var ctx = context.Background()

func userRow(i int) []any {
	return []any{fmt.Sprintf("User %d", i), fmt.Sprintf("user%d@example.com", i)}
}

func TestGenerateInsertsExactRows(t *testing.T) {
	forEachDB(t, func(t *testing.T, e env) {
		e.table(t, "users", usersSpec)
		const n = 10_007 // not a multiple of the chunk size
		res, err := e.seeder("users").
			Columns("name", "email").
			Generate(userRow).
			Count(n).
			ChunkSize(1000).
			Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if res.Rows != n || e.count(t, "users") != n {
			t.Fatalf("rows: result %d, table %d, want %d", res.Rows, e.count(t, "users"), n)
		}
		var name string
		e.one(t, "SELECT name FROM users WHERE email = 'user10006@example.com'", &name)
		if name != "User 10006" {
			t.Fatalf("last row name %q", name)
		}
		if res.Strategy != ts.StrategyInsert || res.Dialect != e.d || res.ChunkSize != 1000 {
			t.Fatalf("unexpected result %+v", res)
		}
	})
}

func TestCSVStrategy(t *testing.T) {
	forEachDB(t, func(t *testing.T, e env) {
		e.table(t, "users", usersSpec)
		res, err := e.seeder("users").
			Columns("name", "email", "password", "meta", "score", "active", "created_at").
			Generate(func(i int) []any {
				var meta any = map[string]any{"i": i, "q": `he said "hi", \N`}
				if i%3 == 0 {
					meta = nil
				}
				return []any{
					fmt.Sprintf("User, \"%d\"\n\\N", i), // commas, quotes, newline, backslash
					fmt.Sprintf("user%d@example.com", i),
					`\N`, // literal backslash-N must stay a string, not NULL
					meta,
					float64(i) / 4,
					i%2 == 0,
					turbodata.NowOnce(),
				}
			}).
			Count(5_000).
			UseCSVStrategy().
			Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if e.d == ts.SQLite {
			if res.Strategy != ts.StrategyInsert || res.FallbackReason == "" {
				t.Fatalf("sqlite should fall back to INSERT, got %+v", res)
			}
		} else if res.Strategy != ts.StrategyCSV {
			t.Fatalf("expected CSV strategy, fell back: %s", res.FallbackReason)
		}
		if got := e.count(t, "users"); got != 5000 {
			t.Fatalf("count %d", got)
		}
		var name, pw string
		e.one(t, "SELECT name, password FROM users WHERE email = 'user7@example.com'", &name, &pw)
		if name != "User, \"7\"\n\\N" || pw != `\N` {
			t.Fatalf("round trip failed: name=%q password=%q", name, pw)
		}
		if got := e.count(t, "users", "meta IS NULL"); got != 1667 {
			t.Fatalf("null meta rows %d, want 1667", got)
		}
		if got := e.count(t, "users", "password IS NULL"); got != 0 {
			t.Fatalf("literal \\N became NULL in %d rows", got)
		}
		var meta string
		e.one(t, "SELECT meta FROM users WHERE email = 'user1@example.com'", &meta)
		if !strings.Contains(meta, `"i":1`) {
			t.Fatalf("meta %q", meta)
		}
	})
}

func TestCSVFallbackDisabled(t *testing.T) {
	forEachDB(t, func(t *testing.T, e env) {
		if e.d != ts.SQLite {
			t.Skip("only SQLite lacks a loader")
		}
		e.table(t, "users", usersSpec)
		_, err := e.seeder("users").Columns("name", "email").Generate(userRow).Count(10).
			UseCSVStrategy().CSVFallback(false).Run(ctx)
		if err == nil || !strings.Contains(err.Error(), "CSV strategy unavailable") {
			t.Fatalf("want unavailable error, got %v", err)
		}
	})
}

func TestGenerateMapInfersColumns(t *testing.T) {
	forEachDB(t, func(t *testing.T, e env) {
		e.table(t, "users", usersSpec)
		email := turbodata.UniqueEmail("", "")
		_, err := e.seeder("users").
			GenerateMap(func(i int) map[string]any {
				return map[string]any{"name": "U", "email": email(i), "score": turbodata.Nullable(1.5, 0.5)}
			}).
			Count(300).
			Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if got := e.count(t, "users"); got != 300 {
			t.Fatalf("count %d", got)
		}
	})
}

func TestGenerateMapMissingKey(t *testing.T) {
	forEachDB(t, func(t *testing.T, e env) {
		e.table(t, "users", usersSpec)
		_, err := e.seeder("users").
			Columns("name", "email").
			GenerateMap(func(i int) map[string]any { return map[string]any{"name": "x", "emial": "typo"} }).
			Count(5).
			Run(ctx)
		if err == nil || !strings.Contains(err.Error(), `unexpected column "emial"`) {
			t.Fatalf("want unexpected column error, got %v", err)
		}
	})
}

type Base struct {
	ID        int64     `db:"id"`
	CreatedAt time.Time `db:"created_at"`
}

type Account struct {
	Base
	Name     string
	Email    string `db:"email"`
	Score    *float64
	Meta     map[string]any `db:"meta"`
	Secret   string         `db:"-"`
	Friends  []Account      // relation: skipped
	internal int            //nolint:unused // proves unexported fields are skipped
}

func (Account) TableName() string { return "users" }

func TestFromModel(t *testing.T) {
	forEachDB(t, func(t *testing.T, e env) {
		e.table(t, "users", usersSpec)
		now := turbodata.NowOnce()
		res, err := ts.FromModel(e.db, func(i int) Account {
			s := float64(i)
			return Account{
				Base:  Base{ID: 999, CreatedAt: now}, // ID must be dropped: auto-increment
				Name:  fmt.Sprint("acc", i),
				Email: fmt.Sprintf("a%d@x.io", i),
				Score: &s,
				Meta:  map[string]any{"tier": "gold"},
			}
		}).Logger(quiet).Count(1234).Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if res.Table != "users" || e.count(t, "users") != 1234 {
			t.Fatalf("table %s count %d", res.Table, e.count(t, "users"))
		}
		var maxID int64
		e.one(t, "SELECT MAX(id) FROM users", &maxID)
		if maxID != 1234 {
			t.Fatalf("ids should be auto-assigned, max id %d", maxID)
		}
		if got := e.count(t, "users", "meta LIKE '%gold%' AND created_at IS NOT NULL AND score IS NOT NULL"); got != 1234 {
			t.Fatalf("model fields not written: %d", got)
		}
	})
}

func TestWithTimestamps(t *testing.T) {
	forEachDB(t, func(t *testing.T, e env) {
		e.table(t, "users", usersSpec)
		_, err := e.seeder("users").Columns("name", "email").Generate(userRow).
			WithTimestamps().Count(50).Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if got := e.count(t, "users", "created_at IS NOT NULL AND updated_at IS NOT NULL"); got != 50 {
			t.Fatalf("timestamped rows %d", got)
		}
	})
}

func TestUpsert(t *testing.T) {
	forEachDB(t, func(t *testing.T, e env) {
		e.table(t, "users", usersSpec)
		gen := func(prefix string) func(int) []any {
			return func(i int) []any { return []any{prefix + fmt.Sprint(i), fmt.Sprintf("u%d@x.io", i)} }
		}
		if _, err := e.seeder("users").Columns("name", "email").Generate(gen("old")).Count(100).Run(ctx); err != nil {
			t.Fatal(err)
		}
		_, err := e.seeder("users").Columns("name", "email").Generate(gen("new")).
			Count(150).ChunkSize(40).Upsert("email").Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if got := e.count(t, "users"); got != 150 {
			t.Fatalf("count %d, want 150", got)
		}
		if got := e.count(t, "users", "name LIKE 'old%'"); got != 0 {
			t.Fatalf("%d rows not updated", got)
		}
	})
}

func TestDryRunRollsBack(t *testing.T) {
	forEachDB(t, func(t *testing.T, e env) {
		e.table(t, "users", usersSpec)
		res, err := e.seeder("users").Columns("name", "email").Generate(userRow).
			Count(2000).Parallel(2).DryRun().Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !res.DryRun || res.Rows != 2000 {
			t.Fatalf("result %+v", res)
		}
		if got := e.count(t, "users"); got != 0 {
			t.Fatalf("dry run left %d rows", got)
		}
	})
}

func TestDryRunCatchesConstraintViolation(t *testing.T) {
	forEachDB(t, func(t *testing.T, e env) {
		e.table(t, "users", usersSpec)
		_, err := e.seeder("users").Columns("name", "email").
			Generate(func(i int) []any { return []any{"x", fmt.Sprintf("dup%d@x.io", i%10)} }).
			Count(100).DryRun().Run(ctx)
		if err == nil {
			t.Fatal("expected unique violation")
		}
		if got := e.count(t, "users"); got != 0 {
			t.Fatalf("left %d rows", got)
		}
	})
}

func TestTruncate(t *testing.T) {
	forEachDB(t, func(t *testing.T, e env) {
		e.table(t, "users", usersSpec)
		e.table(t, "posts", "id ID, user_id BIGINT NOT NULL REFERENCES users(id), title VARCHAR(100) NOT NULL")
		run := func() {
			_, err := e.seeder("users").Columns("name", "email").Generate(userRow).Count(100).Truncate().Run(ctx)
			if err != nil {
				t.Fatal(err)
			}
		}
		run()
		e.exec(t, "INSERT INTO posts (user_id, title) VALUES (1, 'p')")
		e.exec(t, "DELETE FROM posts")
		run() // would fail on a duplicate email without truncation
		if got := e.count(t, "users"); got != 100 {
			t.Fatalf("count %d", got)
		}
	})
}

func TestColumnValidation(t *testing.T) {
	forEachDB(t, func(t *testing.T, e env) {
		e.table(t, "users", usersSpec)
		_, err := e.seeder("users").Columns("name").
			Generate(func(i int) []any { return []any{"x"} }).Count(10).Run(ctx)
		if err == nil || !strings.Contains(err.Error(), "not provided: email") {
			t.Fatalf("want missing email error, got %v", err)
		}
		_, err = e.seeder("users").Columns("name", "email", "nope").
			Generate(func(i int) []any { return []any{"x", "y", "z"} }).Count(10).Run(ctx)
		if err == nil || !strings.Contains(err.Error(), "unknown columns: nope") {
			t.Fatalf("want unknown column error, got %v", err)
		}
		_, err = e.seeder("missing_table").Columns("a").
			Generate(func(i int) []any { return []any{1} }).Count(1).Run(ctx)
		if err == nil || !strings.Contains(err.Error(), "not found") {
			t.Fatalf("want table not found, got %v", err)
		}
		if got := e.count(t, "users"); got != 0 {
			t.Fatalf("validation must fail before writing, found %d rows", got)
		}
	})
}

func TestColumnsFromSchema(t *testing.T) {
	forEachDB(t, func(t *testing.T, e env) {
		e.table(t, "tags", "id ID, slug VARCHAR(50) NOT NULL, weight INTEGER NOT NULL, created_at TS NULL, updated_at TS NULL")
		slug := turbodata.UniqueSlug("tag")
		_, err := e.seeder("tags").ColumnsFromSchema().WithTimestamps().
			Generate(func(i int) []any { return []any{slug(i), i} }). // schema order: slug, weight
			Count(77).Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if got := e.count(t, "tags", "weight >= 0 AND created_at IS NOT NULL"); got != 77 {
			t.Fatalf("count %d", got)
		}
	})
}

func TestGeneratorErrorRollsBackSingleTransaction(t *testing.T) {
	forEachDB(t, func(t *testing.T, e env) {
		e.table(t, "users", usersSpec)
		_, err := e.seeder("users").Columns("name", "email").
			Generate(func(i int) []any {
				if i == 2500 {
					panic("boom")
				}
				return userRow(i)
			}).
			Count(5000).ChunkSize(500).Run(ctx)
		if err == nil || !strings.Contains(err.Error(), "row 2500: generator panicked: boom") {
			t.Fatalf("want panic error, got %v", err)
		}
		if got := e.count(t, "users"); got != 0 {
			t.Fatalf("single transaction should roll back, found %d rows", got)
		}
	})
}

func TestCommitEveryKeepsEarlierBatches(t *testing.T) {
	forEachDB(t, func(t *testing.T, e env) {
		e.table(t, "users", usersSpec)
		_, err := e.seeder("users").Columns("name", "email").
			Generate(func(i int) []any {
				if i == 2500 {
					return []any{"only one value"}
				}
				return userRow(i)
			}).
			Count(5000).ChunkSize(500).CommitEvery(2).RetryAttempts(1).Run(ctx)
		if err == nil || !strings.Contains(err.Error(), "returned 1 values for 2 columns") {
			t.Fatalf("want length error, got %v", err)
		}
		// Batches of 2 chunks x 500 rows: 0-999 and 1000-1999 committed,
		// 2000-2999 (containing row 2500) rolled back.
		if got := e.count(t, "users"); got != 2000 {
			t.Fatalf("committed %d rows, want 2000", got)
		}
	})
}

func TestWithoutTransactions(t *testing.T) {
	forEachDB(t, func(t *testing.T, e env) {
		e.table(t, "users", usersSpec)
		res, err := e.seeder("users").Columns("name", "email").Generate(userRow).
			Count(3333).WithoutTransactions().Run(ctx)
		if err != nil || res.Rows != 3333 || e.count(t, "users") != 3333 {
			t.Fatalf("err %v rows %d", err, res.Rows)
		}
	})
}

func TestParallel(t *testing.T) {
	forEachDB(t, func(t *testing.T, e env) {
		for _, strategy := range []ts.Strategy{ts.StrategyInsert, ts.StrategyCSV} {
			t.Run(strategy.String(), func(t *testing.T) {
				e.table(t, "users", usersSpec)
				res, err := e.seeder("users").Columns("name", "email").Generate(userRow).
					Count(20_001).Parallel(4).Strategy(strategy).Run(ctx)
				if err != nil {
					t.Fatal(err)
				}
				want := 4
				if e.d == ts.SQLite {
					want = 1
				}
				if res.Workers != want {
					t.Fatalf("workers %d, want %d", res.Workers, want)
				}
				if got := e.count(t, "users"); got != 20_001 {
					t.Fatalf("count %d", got)
				}
				var lo, hi int
				e.one(t, "SELECT COUNT(DISTINCT email), COUNT(*) FROM users", &lo, &hi)
				if lo != hi {
					t.Fatalf("duplicate rows: %d distinct of %d", lo, hi)
				}
			})
		}
	})
}

func TestRetryReplaysUnit(t *testing.T) {
	forEachDB(t, func(t *testing.T, e env) {
		e.table(t, "users", usersSpec)
		var failed atomic.Bool
		res, err := e.seeder("users").Columns("name", "email").
			Generate(func(i int) []any {
				if i == 700 && failed.CompareAndSwap(false, true) {
					panic("transient")
				}
				return userRow(i)
			}).
			RetryIf(func(err error) bool { return strings.Contains(err.Error(), "transient") }).
			Count(1000).ChunkSize(100).Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if res.Retries != 1 || res.Rows != 1000 || e.count(t, "users") != 1000 {
			t.Fatalf("retries %d rows %d table %d", res.Retries, res.Rows, e.count(t, "users"))
		}
	})
}

func TestChunkSizeClampedToBindLimit(t *testing.T) {
	forEachDB(t, func(t *testing.T, e env) {
		e.table(t, "users", usersSpec)
		res, err := e.seeder("users").Columns("name", "email").Generate(userRow).
			Count(100).ChunkSize(1_000_000).Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if res.ChunkSize*2 > 65535 {
			t.Fatalf("chunk size %d exceeds bind limit", res.ChunkSize)
		}
	})
}

func TestOffsetAppends(t *testing.T) {
	forEachDB(t, func(t *testing.T, e env) {
		e.table(t, "users", usersSpec)
		for _, off := range []int{0, 100} {
			if _, err := e.seeder("users").Columns("name", "email").Generate(userRow).
				Count(100).Offset(off).Run(ctx); err != nil {
				t.Fatal(err)
			}
		}
		if got := e.count(t, "users"); got != 200 {
			t.Fatalf("count %d", got)
		}
	})
}

func TestProgressCallback(t *testing.T) {
	forEachDB(t, func(t *testing.T, e env) {
		e.table(t, "users", usersSpec)
		var last ts.Progress
		calls := 0
		_, err := e.seeder("users").Columns("name", "email").Generate(userRow).Count(5000).
			OnProgress(func(p ts.Progress) { last = p; calls++ }).Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !last.Final || last.Done != 5000 || last.Total != 5000 || last.Fraction() != 1 || calls == 0 {
			t.Fatalf("final progress %+v after %d calls", last, calls)
		}
	})
}

func TestContextCancel(t *testing.T) {
	forEachDB(t, func(t *testing.T, e env) {
		e.table(t, "users", usersSpec)
		c, cancel := context.WithCancel(ctx)
		_, err := e.seeder("users").Columns("name", "email").
			Generate(func(i int) []any {
				if i == 3000 {
					cancel()
				}
				return userRow(i)
			}).
			Count(100_000).ChunkSize(500).Run(c)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
		if got := e.count(t, "users"); got != 0 {
			t.Fatalf("cancelled run left %d rows", got)
		}
	})
}

func TestValueTypes(t *testing.T) {
	type Status string
	forEachDB(t, func(t *testing.T, e env) {
		e.table(t, "users", usersSpec)
		score := 9.5
		_, err := e.seeder("users").Columns("name", "email", "meta", "score", "active", "password").
			Generate(func(i int) []any {
				return []any{Status("named"), fmt.Sprint(i), []string{"a", "b"}, &score, false, (*string)(nil)}
			}).Count(3).Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if got := e.count(t, "users", `name = 'named' AND meta = '["a","b"]' AND score = 9.5 AND password IS NULL`); got != 3 {
			t.Fatalf("value conversion mismatch, %d rows matched", got)
		}
	})
}

func TestProductionGuard(t *testing.T) {
	forEachDB(t, func(t *testing.T, e env) {
		e.table(t, "users", usersSpec)
		t.Setenv("APP_ENV", "production")
		s := func() *ts.Seeder { return e.seeder("users").Columns("name", "email").Generate(userRow).Count(1) }
		if _, err := s().Run(ctx); err == nil || !strings.Contains(err.Error(), "production") {
			t.Fatalf("want production refusal, got %v", err)
		}
		if _, err := s().Force().Run(ctx); err != nil {
			t.Fatal(err)
		}
	})
}

func TestBuilderValidation(t *testing.T) {
	cases := map[string]*ts.Seeder{
		"Count must be":          ts.ForTable(nil, "t").Count(0),
		"nil *sql.DB":            ts.ForTable(nil, "t").Count(1).Generate(userRow),
		"conflicts":              ts.ForTable(nil, "t").CommitEvery(2).WithoutTransactions(),
		"RetryAttempts must be":  ts.ForTable(nil, "t").RetryAttempts(11),
		"Upsert needs":           ts.ForTable(nil, "t").Upsert(),
		"needs a struct type":    ts.FromModel(nil, func(int) int { return 0 }),
		"Parallel must be >= 1":  ts.ForTable(nil, "t").Parallel(0),
		"ChunkSize must be >= 1": ts.ForTable(nil, "t").ChunkSize(-1),
	}
	for want, s := range cases {
		if _, err := s.Run(ctx); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("want %q, got %v", want, err)
		}
	}
}

func TestOptionConflicts(t *testing.T) {
	forEachDB(t, func(t *testing.T, e env) {
		base := func() *ts.Seeder { return e.seeder("users").Columns("name", "email").Generate(userRow).Count(1) }
		cases := map[string]*ts.Seeder{
			"DryRun cannot be combined with Truncate": base().DryRun().Truncate(),
			"DryRun requires a single transaction":    base().DryRun().CommitEvery(1),
			"Upsert is not supported with the CSV":    base().Upsert("email").UseCSVStrategy(),
			"Update requires Upsert":                  base().Update("name"),
		}
		for want, s := range cases {
			if _, err := s.Run(ctx); err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("want %q, got %v", want, err)
			}
		}
	})
}
