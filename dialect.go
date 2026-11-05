package turboseeder

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// Dialect identifies the SQL flavour of the target database.
type Dialect int

const (
	// Unknown means the dialect could not be detected from the driver.
	Unknown Dialect = iota
	MySQL
	Postgres
	SQLite
)

func (d Dialect) String() string {
	switch d {
	case MySQL:
		return "mysql"
	case Postgres:
		return "postgres"
	case SQLite:
		return "sqlite"
	default:
		return "unknown"
	}
}

// defaultChunkSize mirrors laravel-turbo-seeder's per-driver defaults.
func (d Dialect) defaultChunkSize() int {
	switch d {
	case MySQL:
		return 1000
	case Postgres:
		return 800
	default:
		return 500
	}
}

// DetectDialect inspects the driver behind db and reports its dialect.
// It recognises go-sql-driver/mysql, pgx (v4/v5 stdlib), lib/pq, modernc
// sqlite, mattn/go-sqlite3, ncruces/go-sqlite3 and glebarez/go-sqlite.
func DetectDialect(db *sql.DB) Dialect {
	t := reflect.TypeOf(db.Driver())
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	id := strings.ToLower(t.PkgPath() + "." + t.Name())
	switch {
	case strings.Contains(id, "mysql"):
		return MySQL
	case strings.Contains(id, "pgx"), strings.Contains(id, "lib/pq"),
		strings.Contains(id, "postgres"):
		return Postgres
	case strings.Contains(id, "sqlite"):
		return SQLite
	}
	return Unknown
}

// QuoteIdent quotes a possibly schema-qualified identifier ("public.users").
func (d Dialect) QuoteIdent(name string) string {
	parts := strings.Split(name, ".")
	q, esc := `"`, `""`
	if d == MySQL {
		q, esc = "`", "``"
	}
	for i, p := range parts {
		parts[i] = q + strings.ReplaceAll(p, q, esc) + q
	}
	return strings.Join(parts, ".")
}

// QuoteIdents quotes each name and joins them with ", ".
func (d Dialect) QuoteIdents(names []string) string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = d.QuoteIdent(n)
	}
	return strings.Join(out, ", ")
}

// maxBindParams returns the maximum number of bind parameters per statement.
func (d Dialect) maxBindParams(ctx context.Context, db *sql.DB) int {
	switch d {
	case MySQL, Postgres:
		return 65535
	case SQLite:
		return sqliteMaxVariables(ctx, db)
	}
	return 999
}

// sqliteMaxVariables reads SQLITE_MAX_VARIABLE_NUMBER from the compile
// options, falling back to the documented defaults for the library version.
func sqliteMaxVariables(ctx context.Context, db *sql.DB) int {
	rows, err := db.QueryContext(ctx, "PRAGMA compile_options")
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var opt string
			if rows.Scan(&opt) == nil && strings.HasPrefix(opt, "MAX_VARIABLE_NUMBER=") {
				if n, err := strconv.Atoi(strings.TrimPrefix(opt, "MAX_VARIABLE_NUMBER=")); err == nil && n > 0 {
					return n
				}
			}
		}
	}
	var version string
	if err := db.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&version); err == nil {
		var major, minor int
		if _, err := fmt.Sscanf(version, "%d.%d", &major, &minor); err == nil &&
			(major > 3 || (major == 3 && minor >= 32)) {
			return 32766
		}
	}
	return 999
}

// sessionSetting is a connection-scoped tweak and the statement that undoes it.
type sessionSetting struct {
	apply, restore string
}

// sessionSettings returns the connection-level performance settings to apply
// before seeding. The returned restore statements are run before the
// connection goes back to the pool. Postgres has no session-level FK switch
// that works without superuser; it uses SET CONSTRAINTS inside each tx.
func (d Dialect) sessionSettings(ctx context.Context, conn *sql.Conn, disableFK, disableUnique bool) ([]sessionSetting, error) {
	var out []sessionSetting
	switch d {
	case MySQL:
		if disableFK {
			out = append(out, sessionSetting{"SET FOREIGN_KEY_CHECKS=0", "SET FOREIGN_KEY_CHECKS=1"})
		}
		if disableUnique {
			out = append(out, sessionSetting{"SET UNIQUE_CHECKS=0", "SET UNIQUE_CHECKS=1"})
		}
	case SQLite:
		if disableFK {
			var on int
			if err := conn.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&on); err != nil {
				return nil, err
			}
			if on == 1 {
				out = append(out, sessionSetting{"PRAGMA foreign_keys = OFF", "PRAGMA foreign_keys = ON"})
			}
		}
	}
	return out, nil
}

// txSettings are run right after BEGIN on every transaction.
func (d Dialect) txSettings(disableFK bool) []string {
	if d == Postgres && disableFK {
		// Only affects constraints declared DEFERRABLE; harmless otherwise.
		return []string{"SET CONSTRAINTS ALL DEFERRED"}
	}
	return nil
}

func (d Dialect) truncateSQL(table string) string {
	if d == MySQL {
		// Resets AUTO_INCREMENT. Postgres/SQLite use DELETE so tables that
		// are referenced by foreign keys can still be cleared, matching the
		// behaviour of laravel-turbo-seeder.
		return "TRUNCATE TABLE " + d.QuoteIdent(table)
	}
	return "DELETE FROM " + d.QuoteIdent(table)
}

// placeholders writes rows*cols placeholders as "(?, ?), (?, ?)" or "($1, $2), ...".
func (d Dialect) writePlaceholders(b *strings.Builder, rows, cols int) {
	n := 1
	for r := 0; r < rows; r++ {
		if r > 0 {
			b.WriteString(", ")
		}
		b.WriteByte('(')
		for c := 0; c < cols; c++ {
			if c > 0 {
				b.WriteString(", ")
			}
			if d == Postgres {
				b.WriteByte('$')
				b.WriteString(strconv.Itoa(n))
				n++
			} else {
				b.WriteByte('?')
			}
		}
		b.WriteByte(')')
	}
}
