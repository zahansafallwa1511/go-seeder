package turboseeder

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"
)

type status string
type level int
type valuer struct{ s string }

func (v valuer) Value() (driver.Value, error) { return "v:" + v.s, nil }

func TestNormalize(t *testing.T) {
	ts := time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC)
	s := "x"
	var nilPtr *string
	var nilValuer *valuer
	cases := []struct {
		in, want any
	}{
		{nil, nil},
		{"a", "a"},
		{status("active"), "active"},
		{level(3), int64(3)},
		{uint8(7), uint8(7)},
		{&s, "x"},
		{nilPtr, nil},
		{ts, ts},
		{map[string]int{"a": 1}, `{"a":1}`},
		{[]int{1, 2}, "[1,2]"},
		{struct{ A int }{1}, `{"A":1}`},
		{json.RawMessage(`{"x":1}`), `{"x":1}`},
		{[]byte("raw"), []byte("raw")},
		{map[string]int(nil), nil},
		{[]int(nil), nil},
	}
	for _, c := range cases {
		got, err := Normalize(c.in)
		if err != nil {
			t.Fatalf("Normalize(%#v): %v", c.in, err)
		}
		if fmt.Sprintf("%#v", got) != fmt.Sprintf("%#v", c.want) {
			t.Errorf("Normalize(%#v) = %#v, want %#v", c.in, got, c.want)
		}
	}
	if got, _ := Normalize(valuer{"a"}); got != (valuer{"a"}) {
		t.Errorf("valuer should pass through, got %#v", got)
	}
	if got, _ := Normalize(nilValuer); got != nil {
		t.Errorf("nil valuer pointer should be NULL, got %#v", got)
	}
	if _, err := Normalize(make(chan int)); err == nil {
		t.Error("chan should be unsupported")
	}
}

func TestTextValue(t *testing.T) {
	ts := time.Date(2024, 5, 6, 7, 8, 9, 120000000, time.FixedZone("X", 3600))
	cases := []struct {
		in   any
		d    Dialect
		want string
		null bool
	}{
		{nil, MySQL, "", true},
		{true, Postgres, "1", false},
		{int64(-5), MySQL, "-5", false},
		{int16(9), MySQL, "9", false},
		{0.25, MySQL, "0.25", false},
		{1e21, Postgres, "1000000000000000000000", false},
		{[]byte{0xde, 0xad}, Postgres, `\xdead`, false},
		{ts, MySQL, "2024-05-06 06:08:09.12", false},
		{ts, Postgres, "2024-05-06 07:08:09.12+01:00", false},
		{valuer{"q"}, MySQL, "v:q", false},
	}
	for _, c := range cases {
		got, null, err := TextValue(c.in, c.d)
		if err != nil || got != c.want || null != c.null {
			t.Errorf("TextValue(%#v, %s) = %q, %v, %v; want %q, %v", c.in, c.d, got, null, err, c.want, c.null)
		}
	}
}

func TestRegisterFormatter(t *testing.T) {
	type money struct{ cents int }
	RegisterFormatter(func(v any) (any, bool) {
		if m, ok := v.(money); ok {
			return fmt.Sprintf("%d.%02d", m.cents/100, m.cents%100), true
		}
		return nil, false
	})
	got, err := Normalize(money{1234})
	if err != nil || got != "12.34" {
		t.Fatalf("got %#v, %v", got, err)
	}
}

func TestSnakeAndPlural(t *testing.T) {
	for in, want := range map[string]string{
		"UserID": "user_id", "BlogPost": "blog_post", "HTTPServer": "http_server", "ID": "id", "Address2": "address2",
	} {
		if got := snakeCase(in); got != want {
			t.Errorf("snakeCase(%q) = %q, want %q", in, got, want)
		}
	}
	for in, want := range map[string]string{
		"user": "users", "category": "categories", "day": "days", "box": "boxes", "match": "matches", "blog_post": "blog_posts",
	} {
		if got := pluralize(in); got != want {
			t.Errorf("pluralize(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestQuoteIdent(t *testing.T) {
	if got := MySQL.QuoteIdent("db.us`ers"); got != "`db`.`us``ers`" {
		t.Errorf("mysql quote %s", got)
	}
	if got := Postgres.QuoteIdent(`public.us"ers`); got != `"public"."us""ers"` {
		t.Errorf("pg quote %s", got)
	}
}

type sqlStateErr string

func (e sqlStateErr) Error() string    { return "pg error" }
func (e sqlStateErr) SQLState() string { return string(e) }

func TestIsRetryable(t *testing.T) {
	yes := []error{
		sqlStateErr("40P01"),
		fmt.Errorf("wrapped: %w", sqlStateErr("40001")),
		errors.New("Error 1213 (40001): Deadlock found when trying to get lock; try restarting transaction"),
		errors.New("Error 1205 (HY000): Lock wait timeout exceeded; try restarting transaction"),
		errors.New("database is locked (5) (SQLITE_BUSY)"),
	}
	for _, err := range yes {
		if !IsRetryable(err) {
			t.Errorf("should retry: %v", err)
		}
	}
	for _, err := range []error{nil, sqlStateErr("23505"), errors.New("duplicate key")} {
		if IsRetryable(err) {
			t.Errorf("should not retry: %v", err)
		}
	}
}

func TestUnitsPlan(t *testing.T) {
	p := &plan{s: &Seeder{count: 10, tx: txSingle}, chunk: 3}
	if u := p.units(3); len(u) != 3 || u[0] != (unit{0, 4}) || u[2] != (unit{8, 10}) {
		t.Errorf("single tx units %v", u)
	}
	p.s.tx, p.s.commitEvery = txCommitEvery, 2
	if u := p.units(1); len(u) != 2 || u[0] != (unit{0, 6}) || u[1] != (unit{6, 10}) {
		t.Errorf("commit-every units %v", u)
	}
}

func TestPlaceholders(t *testing.T) {
	p := &plan{d: Postgres, cols: []string{"a", "b"}, insertPrefix: "X "}
	if got := p.insertSQL(2); got != "X ($1, $2), ($3, $4)" {
		t.Errorf("pg placeholders %q", got)
	}
	p.d = MySQL
	if got := p.insertSQL(2); got != "X (?, ?), (?, ?)" {
		t.Errorf("mysql placeholders %q", got)
	}
}
