// Command resetdeps prepares throwaway state for an end-to-end run: it drops and
// recreates one PostgreSQL database and flushes one Redis logical database.
//
// It exists so scripts/e2e-*.sh work the same whether PostgreSQL and Redis come
// from docker compose or from anywhere else, without needing psql or redis-cli on
// the machine running the tests.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

func main() {
	adminURL := flag.String("postgres", "", "administrative PostgreSQL URL (a database other than the one to reset)")
	dbName := flag.String("db", "", "database to drop and recreate")
	redisURL := flag.String("redis", "", "redis:// URL whose logical database is flushed")
	flag.Parse()

	if err := run(*adminURL, *dbName, *redisURL); err != nil {
		fmt.Fprintln(os.Stderr, "resetdeps:", err)
		os.Exit(1)
	}
}

var safeName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

func run(adminURL, dbName, redisURL string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if adminURL != "" {
		if !safeName.MatchString(dbName) {
			return fmt.Errorf("refusing database name %q", dbName)
		}
		var conn *pgx.Conn
		var err error
		// The database may still be starting; wait for it rather than fail.
		for i := 0; i < 60; i++ {
			if conn, err = pgx.Connect(ctx, adminURL); err == nil {
				break
			}
			time.Sleep(time.Second)
		}
		if err != nil {
			return fmt.Errorf("connecting to PostgreSQL: %w", err)
		}
		defer func() { _ = conn.Close(ctx) }()
		for _, stmt := range []string{
			fmt.Sprintf(`DROP DATABASE IF EXISTS %q WITH (FORCE)`, dbName),
			fmt.Sprintf(`CREATE DATABASE %q`, dbName),
		} {
			if _, err := conn.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("%s: %w", stmt, err)
			}
		}
	}
	if redisURL != "" {
		opts, err := redis.ParseURL(redisURL)
		if err != nil {
			return err
		}
		c := redis.NewClient(opts)
		defer func() { _ = c.Close() }()
		for i := 0; i < 30; i++ {
			if err = c.Ping(ctx).Err(); err == nil {
				break
			}
			time.Sleep(time.Second)
		}
		if err != nil {
			return fmt.Errorf("connecting to Redis: %w", err)
		}
		if err := c.FlushDB(ctx).Err(); err != nil {
			return err
		}
	}
	return nil
}
