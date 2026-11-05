package turboseeder

import (
	"context"
	"errors"
	"math/rand/v2"
	"strings"
	"time"
)

// IsRetryable reports whether err is a transient concurrency failure worth
// retrying: deadlocks, lock-wait timeouts, serialization failures and
// SQLITE_BUSY / SQLITE_LOCKED. It works without importing any driver.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	var st interface{ SQLState() string } // pgconn.PgError, lib/pq via Error()
	if errors.As(err, &st) {
		switch st.SQLState() {
		case "40001", "40P01", "55P03": // serialization_failure, deadlock_detected, lock_not_available
			return true
		}
	}
	msg := strings.ToLower(err.Error())
	for _, s := range []string{
		"deadlock",                   // MySQL 1213, Postgres 40P01
		"lock wait timeout exceeded", // MySQL 1205
		"could not serialize access", // Postgres 40001
		"database is locked",         // SQLITE_BUSY
		"database table is locked",   // SQLITE_LOCKED
		"sqlite_busy",
	} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// backoff sleeps 50ms·2^attempt (+ jitter), capped at 2s, or until ctx ends.
func backoff(ctx context.Context, attempt int) error {
	d := min(50*time.Millisecond<<attempt, 2*time.Second)
	d += time.Duration(rand.Int64N(int64(d) / 2))
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
