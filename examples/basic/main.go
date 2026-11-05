// Example: seed 100k users and 1M posts that reference them.
//
//	go run ./examples/basic                     # SQLite file in a temp dir
//	go run ./examples/basic -driver pgx -dsn 'postgres://localhost/app?sslmode=disable'
//	go run ./examples/basic -driver mysql -dsn 'root:secret@tcp(127.0.0.1:3306)/app'
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"

	ts "github.com/zahansafallwa1511/go-seeder"
	_ "github.com/zahansafallwa1511/go-seeder/loaders/mysqlcsv"
	_ "github.com/zahansafallwa1511/go-seeder/loaders/pgcopy"
	td "github.com/zahansafallwa1511/go-seeder/turbodata"
)

// User is a model "factory" target for FromModel.
type User struct {
	ID        int64  `db:"id"` // auto-increment: dropped automatically
	Name      string `db:"name"`
	Email     string `db:"email"`
	Password  string `db:"password"`
	Status    string `db:"status"`
	CreatedAt any    `db:"created_at"`
}

func main() {
	driver := flag.String("driver", "sqlite", "pgx, mysql or sqlite")
	dsn := flag.String("dsn", "", "data source name (default: temp SQLite file)")
	flag.Parse()
	if *dsn == "" {
		*dsn = filepath.Join(os.TempDir(), "turboseeder-example.db")
		os.Remove(*dsn)
	}

	ctx := context.Background()
	db, err := sql.Open(*driver, *dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	mustSchema(ctx, db)

	// 1. Users from a struct factory (like fromFactory()).
	email := td.UniqueEmail("user", "example.com")
	status := td.Weighted(map[string]int{"active": 80, "pending": 15, "banned": 5})
	res, err := ts.FromModel(db, func(i int) User {
		return User{
			Name:      "User " + strconv.Itoa(i),
			Email:     email(i),
			Password:  td.HashedPassword(),
			Status:    status(),
			CreatedAt: td.DateRange(td.Date("2023-01-01"), td.Date("2025-01-01")),
		}
	}).Count(100_000).Truncate().WithProgressBar().Run(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(res)

	// 2. Posts with foreign keys pulled from the seeded users (like generate()).
	userIDs, err := td.FromTable[int64](ctx, db, "users", "id")
	if err != nil {
		log.Fatal(err)
	}
	slug := td.UniqueSlug("post")
	res, err = ts.ForTable(db, "posts").
		Columns("user_id", "title", "slug", "views", "published_at").
		Generate(func(i int) []any {
			return []any{
				userIDs.Cycle(i),
				"Post " + strconv.Itoa(i),
				slug(i),
				td.RandomInt(0, 10_000),
				td.Nullable(td.DateRange(td.Date("2024-01-01"), td.Date("2025-01-01")), 0.2),
			}
		}).
		Count(1_000_000).
		Truncate().
		UseCSVStrategy(). // COPY on Postgres, LOAD DATA on MySQL, INSERT fallback on SQLite
		WithProgressBar().
		Run(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(res)
}

func mustSchema(ctx context.Context, db *sql.DB) {
	d := ts.DetectDialect(db)
	id, stamp := "INTEGER PRIMARY KEY", "DATETIME"
	switch d {
	case ts.Postgres:
		id, stamp = "BIGSERIAL PRIMARY KEY", "TIMESTAMPTZ"
	case ts.MySQL:
		id, stamp = "BIGINT AUTO_INCREMENT PRIMARY KEY", "DATETIME"
	}
	for _, q := range []string{
		"DROP TABLE IF EXISTS posts",
		"DROP TABLE IF EXISTS users",
		fmt.Sprintf(`CREATE TABLE users (id %s, name VARCHAR(255) NOT NULL, email VARCHAR(255) NOT NULL UNIQUE,
			password VARCHAR(255) NOT NULL, status VARCHAR(20) NOT NULL, created_at %s NULL)`, id, stamp),
		fmt.Sprintf(`CREATE TABLE posts (id %s, user_id BIGINT NOT NULL REFERENCES users(id), title VARCHAR(255) NOT NULL,
			slug VARCHAR(255) NOT NULL UNIQUE, views INTEGER NOT NULL, published_at %s NULL)`, id, stamp),
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			log.Fatalf("%s: %v", q, err)
		}
	}
}
