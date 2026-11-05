package turbodata

import (
	"context"
	"database/sql"
	"math"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestUniqueUUID(t *testing.T) {
	gen := UniqueUUID()
	re := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	seen := map[string]bool{}
	for i := range 100_000 {
		u := gen(i)
		if !re.MatchString(u) {
			t.Fatalf("not a v4 uuid: %s", u)
		}
		if seen[u] {
			t.Fatalf("duplicate at %d", i)
		}
		seen[u] = true
	}
	if a, b := gen(5), gen(5); a != b {
		t.Fatalf("same index should be deterministic: %s vs %s", a, b)
	}
}

func TestIndexFactories(t *testing.T) {
	if got := UniqueEmail("", "")(7); got != "user7@example.com" {
		t.Error(got)
	}
	if got := UniqueUsername("dev")(3); got != "dev_3" {
		t.Error(got)
	}
	if got := UniqueSlug("post")(9); got != "post-9" {
		t.Error(got)
	}
	c := CycleFrom("a", "b", "c")
	if c(0) != "a" || c(4) != "b" || c(-1) != "c" {
		t.Error("CycleFrom")
	}
	start := Date("2024-01-01")
	if got := SequentialDate(start, time.Hour)(3); !got.Equal(start.Add(3 * time.Hour)) {
		t.Error(got)
	}
}

func TestWeighted(t *testing.T) {
	pick := Weighted(map[string]int{"a": 90, "b": 10, "never": 0})
	counts := map[string]int{}
	for range 100_000 {
		counts[pick()]++
	}
	if counts["never"] != 0 || counts["a"] < 88_000 || counts["a"] > 92_000 {
		t.Fatalf("distribution off: %v", counts)
	}
}

func TestRandomHelpers(t *testing.T) {
	from, to := Date("2023-01-01"), Date("2023-12-31")
	for range 10_000 {
		if n := RandomInt(5, 7); n < 5 || n > 7 {
			t.Fatal(n)
		}
		if f := RandomFloat(2, 1, 2); f < 1 || f > 2 || math.Abs(f*100-math.Round(f*100)) > 1e-9 {
			t.Fatal(f)
		}
		if d := DateRange(from, to); d.Before(from) || !d.Before(to) {
			t.Fatal(d)
		}
	}
	if Nullable(1, 1) != nil || Nullable(1, 0) != 1 {
		t.Fatal("Nullable")
	}
	first := NowOnce()
	time.Sleep(10 * time.Millisecond)
	if second := NowOnce(); !first.Equal(second) {
		t.Fatalf("NowOnce changed: %v then %v", first, second)
	}
	if len(HashedPassword()) != 60 {
		t.Fatal("bcrypt hash length")
	}
}

func TestFromTableAndStream(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY);
		WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < 25) INSERT INTO users SELECT i FROM n`); err != nil {
		t.Fatal(err)
	}
	ids, err := FromTable[int64](ctx, db, "users", "id")
	if err != nil || len(ids) != 25 || ids.Cycle(25) != ids[0] {
		t.Fatalf("FromTable: %v %v", ids, err)
	}
	if _, err := FromQuery[int64](ctx, db, "SELECT id FROM users WHERE id > 100"); err == nil {
		t.Fatal("empty FromQuery should error")
	}
	next := StreamFromTable[int64](ctx, db, "users", "id", 10)
	for want := int64(1); want <= 60; want++ {
		exp := (want-1)%25 + 1
		if got := next(0); got != exp {
			t.Fatalf("stream value %d: got %d want %d", want, got, exp)
		}
	}
}
