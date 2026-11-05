// Package turbodata provides fast, allocation-light value generators for
// seeding — the Go counterpart of laravel-turbo-seeder's TurboData helpers.
//
// Three kinds of helpers exist:
//
//   - Index factories return a func(i int) T. Uniqueness is derived from the
//     row index, so they never keep seen values in memory (unlike Faker's
//     unique()). Create them once, outside the generator.
//   - Per-row helpers (RandomInt, DateRange, ...) are called inside the generator.
//   - Cached values (NowOnce, HashedPassword) are computed once per process.
//
// All helpers are safe for concurrent use, so they work with Parallel(n).
package turbodata

import (
	"encoding/hex"
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"strconv"
	"sync"
	"time"
)

// CycleFrom returns values[i % len(values)].
func CycleFrom[T any](values ...T) func(i int) T {
	if len(values) == 0 {
		panic("turbodata: CycleFrom needs at least one value")
	}
	return func(i int) T { return values[mod(i, len(values))] }
}

func mod(i, n int) int {
	m := i % n
	if m < 0 {
		m += n
	}
	return m
}

// UniqueEmail returns "<prefix><i>@<domain>" (defaults "user", "example.com").
func UniqueEmail(prefix, domain string) func(i int) string {
	if prefix == "" {
		prefix = "user"
	}
	if domain == "" {
		domain = "example.com"
	}
	suffix := "@" + domain
	return func(i int) string { return prefix + strconv.Itoa(i) + suffix }
}

// UniqueUsername returns "<prefix>_<i>" (default prefix "user").
func UniqueUsername(prefix string) func(i int) string {
	if prefix == "" {
		prefix = "user"
	}
	return func(i int) string { return prefix + "_" + strconv.Itoa(i) }
}

// UniqueSlug returns "<base>-<i>".
func UniqueSlug(base string) func(i int) string {
	return func(i int) string { return base + "-" + strconv.Itoa(i) }
}

// UniqueUUID returns RFC 4122 version-4 UUID strings that are unique by
// construction: 74 random bits fixed per call to UniqueUUID plus the row
// index in the last 48 bits, so no collision check is needed.
func UniqueUUID() func(i int) string {
	var base [16]byte
	for k := range base {
		base[k] = byte(rand.Uint32())
	}
	base[6] = base[6]&0x0f | 0x40 // version 4
	base[8] = base[8]&0x3f | 0x80 // variant 10
	return func(i int) string {
		u := base
		x := uint64(i)
		for k := 15; k >= 10; k-- {
			u[k] = byte(x)
			x >>= 8
		}
		var buf [36]byte
		hex.Encode(buf[0:8], u[0:4])
		buf[8] = '-'
		hex.Encode(buf[9:13], u[4:6])
		buf[13] = '-'
		hex.Encode(buf[14:18], u[6:8])
		buf[18] = '-'
		hex.Encode(buf[19:23], u[8:10])
		buf[23] = '-'
		hex.Encode(buf[24:], u[10:])
		return string(buf[:])
	}
}

// SequentialDate returns start + i*step.
func SequentialDate(start time.Time, step time.Duration) func(i int) time.Time {
	return func(i int) time.Time { return start.Add(time.Duration(i) * step) }
}

// Weighted picks values in proportion to their weights:
//
//	status := turbodata.Weighted(map[string]int{"active": 70, "banned": 5, "pending": 25})
//	... status() ...
func Weighted[T comparable](weights map[T]int) func() T {
	type entry struct {
		v   T
		cum int
	}
	keys := make([]T, 0, len(weights))
	for k, w := range weights {
		if w < 0 {
			panic(fmt.Sprintf("turbodata: negative weight %d", w))
		}
		if w > 0 {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		panic("turbodata: Weighted needs at least one positive weight")
	}
	// Stable order so the distribution is reproducible with a seeded source.
	sort.Slice(keys, func(a, b int) bool { return fmt.Sprint(keys[a]) < fmt.Sprint(keys[b]) })
	entries := make([]entry, len(keys))
	total := 0
	for i, k := range keys {
		total += weights[k]
		entries[i] = entry{k, total}
	}
	return func() T {
		r := rand.IntN(total)
		j := sort.Search(len(entries), func(j int) bool { return entries[j].cum > r })
		return entries[j].v
	}
}

// RandomFrom returns one of values at random.
func RandomFrom[T any](values ...T) T {
	return values[rand.IntN(len(values))]
}

// RandomInt returns a random int in [lo, hi].
func RandomInt(lo, hi int) int {
	if hi < lo {
		lo, hi = hi, lo
	}
	return lo + rand.IntN(hi-lo+1)
}

// RandomFloat returns a random float in [lo, hi] rounded to decimals places.
func RandomFloat(decimals int, lo, hi float64) float64 {
	f := lo + rand.Float64()*(hi-lo)
	p := math.Pow10(decimals)
	return math.Round(f*p) / p
}

// RandomBool returns true with 50% probability.
func RandomBool() bool { return rand.Uint32()&1 == 1 }

// Chance returns true with the given probability in [0,1].
func Chance(p float64) bool { return rand.Float64() < p }

// Nullable returns nil with probability nullChance, otherwise v.
func Nullable(v any, nullChance float64) any {
	if rand.Float64() < nullChance {
		return nil
	}
	return v
}

// DateRange returns a random time in [from, to).
func DateRange(from, to time.Time) time.Time {
	span := to.Sub(from)
	if span <= 0 {
		return from
	}
	return from.Add(time.Duration(rand.Int64N(int64(span))))
}

// Date parses "2006-01-02" or RFC 3339 and panics on bad input; handy for
// literals: turbodata.DateRange(turbodata.Date("2023-01-01"), turbodata.Date("2024-12-31")).
func Date(s string) time.Time {
	for _, layout := range []string{"2006-01-02", time.RFC3339, "2006-01-02 15:04:05"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	panic(fmt.Sprintf("turbodata: cannot parse date %q", s))
}

var (
	nowOnce sync.Once
	now     time.Time
)

// NowOnce returns the same time.Now() for the life of the process, so every
// row gets an identical timestamp without a clock call per row.
func NowOnce() time.Time {
	nowOnce.Do(func() { now = time.Now().UTC().Truncate(time.Second) })
	return now
}

// HashedPassword returns a precomputed bcrypt hash (cost 10) of "password",
// the same convenience Laravel factories provide, at zero cost per row.
func HashedPassword() string { return passwordHash }

const passwordHash = "$2a$10$RGzhYNdp4lEfTMLJXiGY../efa9hGKbbeD06la4G5Pu4ql46hYS46"
