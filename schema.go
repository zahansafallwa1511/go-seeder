package turboseeder

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// ColumnInfo describes one table column as seen by the introspector.
type ColumnInfo struct {
	Name          string
	Nullable      bool
	HasDefault    bool
	AutoIncrement bool // AUTO_INCREMENT, IDENTITY, serial, INTEGER PRIMARY KEY
	Generated     bool // computed / generated columns
}

// Insertable reports whether the seeder should ever write this column
// when columns are inferred.
func (c ColumnInfo) Insertable() bool { return !c.AutoIncrement && !c.Generated }

// Required reports whether an INSERT that omits this column would fail.
func (c ColumnInfo) Required() bool { return c.Insertable() && !c.Nullable && !c.HasDefault }

// Introspect lists a table's columns in ordinal order.
func Introspect(ctx context.Context, db *sql.DB, d Dialect, table string) ([]ColumnInfo, error) {
	schema, name := splitTable(table)
	var (
		rows *sql.Rows
		err  error
	)
	switch d {
	case MySQL:
		rows, err = db.QueryContext(ctx, `
			SELECT COLUMN_NAME,
			       IS_NULLABLE = 'YES',
			       COLUMN_DEFAULT IS NOT NULL,
			       EXTRA LIKE '%auto_increment%',
			       (EXTRA LIKE '%VIRTUAL GENERATED%' OR EXTRA LIKE '%STORED GENERATED%')
			FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = COALESCE(?, DATABASE()) AND TABLE_NAME = ?
			ORDER BY ORDINAL_POSITION`, schema, name)
	case Postgres:
		rows, err = db.QueryContext(ctx, `
			SELECT column_name,
			       is_nullable = 'YES',
			       column_default IS NOT NULL,
			       is_identity = 'YES' OR COALESCE(column_default LIKE 'nextval(%', false),
			       is_generated <> 'NEVER'
			FROM information_schema.columns
			WHERE table_schema = COALESCE($1::text, current_schema()) AND table_name = $2
			ORDER BY ordinal_position`, schema, name)
	case SQLite:
		return introspectSQLite(ctx, db, schema, name)
	default:
		return nil, fmt.Errorf("turboseeder: cannot introspect dialect %s", d)
	}
	if err != nil {
		return nil, fmt.Errorf("turboseeder: introspect %s: %w", table, err)
	}
	defer rows.Close()
	var out []ColumnInfo
	for rows.Next() {
		var c ColumnInfo
		if err := rows.Scan(&c.Name, &c.Nullable, &c.HasDefault, &c.AutoIncrement, &c.Generated); err != nil {
			return nil, fmt.Errorf("turboseeder: introspect %s: %w", table, err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("turboseeder: table %q not found", table)
	}
	return out, nil
}

func introspectSQLite(ctx context.Context, db *sql.DB, schema *string, name string) ([]ColumnInfo, error) {
	s := "main"
	if schema != nil {
		s = *schema
	}
	rows, err := db.QueryContext(ctx,
		`SELECT name, upper(type), "notnull", dflt_value IS NOT NULL, pk, hidden FROM pragma_table_xinfo(?, ?)`, name, s)
	if err != nil {
		return nil, fmt.Errorf("turboseeder: introspect %s: %w", name, err)
	}
	defer rows.Close()
	var (
		out     []ColumnInfo
		pkCount int
		pkIdx   = -1
		pkType  string
	)
	for rows.Next() {
		var (
			c          ColumnInfo
			typ        string
			notNull    bool
			pk, hidden int
		)
		if err := rows.Scan(&c.Name, &typ, &notNull, &c.HasDefault, &pk, &hidden); err != nil {
			return nil, err
		}
		if hidden == 1 { // hidden column of a virtual table
			continue
		}
		c.Nullable = !notNull
		c.Generated = hidden == 2 || hidden == 3
		if pk > 0 {
			pkCount++
			pkIdx, pkType = len(out), typ
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// A lone INTEGER PRIMARY KEY is an alias for rowid and auto-assigned.
	if pkCount == 1 && pkType == "INTEGER" {
		out[pkIdx].AutoIncrement = true
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("turboseeder: table %q not found", name)
	}
	return out, nil
}

func splitTable(table string) (schema *string, name string) {
	if i := strings.LastIndexByte(table, '.'); i >= 0 {
		s := table[:i]
		return &s, table[i+1:]
	}
	return nil, table
}

// checkColumns fails when cols names a column the table lacks, or omits a
// NOT NULL column that has no default and is not auto-generated.
func checkColumns(table string, cols []string, info []ColumnInfo) error {
	known := make(map[string]ColumnInfo, len(info))
	for _, c := range info {
		known[strings.ToLower(c.Name)] = c
	}
	var unknown, generated, missing []string
	given := make(map[string]bool, len(cols))
	for _, c := range cols {
		lc := strings.ToLower(c)
		given[lc] = true
		ci, ok := known[lc]
		switch {
		case !ok:
			unknown = append(unknown, c)
		case ci.Generated:
			generated = append(generated, c)
		}
	}
	for _, c := range info {
		if c.Required() && !given[strings.ToLower(c.Name)] {
			missing = append(missing, c.Name)
		}
	}
	var problems []string
	if len(unknown) > 0 {
		problems = append(problems, "unknown columns: "+strings.Join(unknown, ", "))
	}
	if len(generated) > 0 {
		problems = append(problems, "generated columns cannot be written: "+strings.Join(generated, ", "))
	}
	if len(missing) > 0 {
		problems = append(problems, "NOT NULL columns without a default are not provided: "+strings.Join(missing, ", "))
	}
	if len(problems) > 0 {
		return fmt.Errorf("turboseeder: column validation failed for %s: %s", table, strings.Join(problems, "; "))
	}
	return nil
}
