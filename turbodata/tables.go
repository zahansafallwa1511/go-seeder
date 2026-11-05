package turbodata

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"sync"

	"github.com/zahansafallwa1511/go-seeder"
)

// Pool is a set of values loaded from the database, e.g. foreign keys.
type Pool[T any] []T

// Cycle returns pool[i % len(pool)]: deterministic, even distribution.
func (p Pool[T]) Cycle(i int) T { return p[mod(i, len(p))] }

// Random returns a random element.
func (p Pool[T]) Random() T { return p[rand.IntN(len(p))] }

// FromTable loads every value of column from table, e.g. user IDs to use as
// foreign keys:
//
//	userIDs, err := turbodata.FromTable[int64](ctx, db, "users", "id")
//	... "user_id": userIDs.Cycle(i) ...
//
// The whole column is held in memory; for very large tables (millions of
// values) consider StreamFromTable.
func FromTable[T any](ctx context.Context, db *sql.DB, table, column string) (Pool[T], error) {
	d := turboseeder.DetectDialect(db)
	q := fmt.Sprintf("SELECT %s FROM %s", d.QuoteIdent(column), d.QuoteIdent(table))
	return FromQuery[T](ctx, db, q)
}

// FromQuery loads the first column of every row returned by query.
func FromQuery[T any](ctx context.Context, db *sql.DB, query string, args ...any) (Pool[T], error) {
	out, err := queryColumn[T](ctx, db, query, args...)
	if err == nil && len(out) == 0 {
		err = fmt.Errorf("turbodata: query returned no rows: %s", query)
	}
	return out, err
}

func queryColumn[T any](ctx context.Context, db *sql.DB, query string, args ...any) (Pool[T], error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out Pool[T]
	for rows.Next() {
		var v T
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// StreamFromTable pages through column in ascending order (keyset
// pagination, pageSize rows at a time) and hands out values sequentially,
// wrapping around at the end. Memory stays at one page regardless of table
// size. The returned function ignores i and is safe for concurrent use.
//
// It queries the database while seeding runs, so the pool needs a free
// connection beyond the seeder's (it cannot work with SetMaxOpenConns(1)).
// Errors panic, which the seeder reports as a failed row.
func StreamFromTable[T any](ctx context.Context, db *sql.DB, table, column string, pageSize int) func(i int) T {
	if pageSize < 1 {
		pageSize = 10_000
	}
	d := turboseeder.DetectDialect(db)
	col, tbl := d.QuoteIdent(column), d.QuoteIdent(table)
	ph := "?"
	if d == turboseeder.Postgres {
		ph = "$1"
	}
	first := fmt.Sprintf("SELECT %s FROM %s ORDER BY %s LIMIT %d", col, tbl, col, pageSize)
	next := fmt.Sprintf("SELECT %s FROM %s WHERE %s > %s ORDER BY %s LIMIT %d", col, tbl, col, ph, col, pageSize)

	var (
		mu   sync.Mutex
		page []T
		pos  int
	)
	load := func(q string, args ...any) []T {
		vals, err := queryColumn[T](ctx, db, q, args...)
		if err == nil && len(vals) == 0 && args == nil {
			err = fmt.Errorf("table is empty")
		}
		if err != nil {
			panic(fmt.Errorf("turbodata: StreamFromTable %s.%s: %w", table, column, err))
		}
		return vals
	}
	return func(int) T {
		mu.Lock()
		defer mu.Unlock()
		if pos >= len(page) {
			if len(page) == 0 {
				page = load(first)
			} else if page = load(next, page[len(page)-1]); len(page) == 0 {
				page = load(first) // wrap around
			}
			pos = 0
		}
		v := page[pos]
		pos++
		return v
	}
}
