// Command nebula-migrate applies NEBULA's database migrations.
//
// Migrations are embedded in the binary, so this is a single static artifact with
// no files to ship alongside it. In Kubernetes it runs as a Helm pre-install and
// pre-upgrade hook Job that must succeed before application pods roll.
//
// Usage:
//
//	nebula-migrate up            apply every pending migration
//	nebula-migrate down [n]      revert the last n migrations (default 1)
//	nebula-migrate status        list every migration and whether it is applied
//	nebula-migrate version       print the current schema version
//	nebula-migrate validate      verify applied migrations match this binary
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"text/tabwriter"

	"log/slog"

	"github.com/adityasatwar321/nebula/migrations"
	"github.com/adityasatwar321/nebula/packages/config"
	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/db/migrate"
	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/packages/version"
)

// cmdDown is the one subcommand named in more than one place: the step-count
// argument is consumed before flag parsing, and the switch dispatches on it.
const cmdDown = "down"

const serviceName = "nebula-migrate"

func main() {
	if err := run(os.Args[1:]); err != nil {
		if errors.Is(err, config.ErrHelpRequested) {
			os.Exit(0)
		}
		_, _ = fmt.Fprintf(os.Stderr, "%s: %v\n", serviceName, err)
		os.Exit(1)
	}
}

func usage() string {
	return `usage: nebula-migrate <command> [flags]

commands:
  up            apply every pending migration
  down [n]      revert the last n migrations (default 1)
  status        list every migration and whether it is applied
  version       print the current schema version
  validate      verify that applied migrations match this binary

Connection settings come from NEBULA_DATABASE_URL or a config file; see
docs/deployment-architecture.md §9.`
}

func run(args []string) error {
	if len(args) == 0 {
		_, _ = fmt.Fprintln(os.Stderr, usage())
		return errors.New("no command given")
	}
	cmd, rest := args[0], args[1:]

	switch cmd {
	case "-h", "--help", "help":
		fmt.Println(usage())
		return nil
	case "up", cmdDown, "status", "version", "validate":
	default:
		_, _ = fmt.Fprintln(os.Stderr, usage())
		return fmt.Errorf("unknown command %q", cmd)
	}

	info := version.Get(serviceName)

	// Flags that are not the subcommand are passed through to the config loader;
	// a positional step count for `down` is consumed here first.
	steps := 1
	flagArgs := make([]string, 0, len(rest))
	for _, a := range rest {
		if cmd == cmdDown && a != "" && a[0] != '-' {
			n, err := strconv.Atoi(a)
			if err != nil || n < 1 {
				return fmt.Errorf("down: step count must be a positive integer, got %q", a)
			}
			steps = n
			continue
		}
		flagArgs = append(flagArgs, a)
	}

	cfg, err := config.Loader{Service: serviceName, Args: flagArgs}.Load()
	if err != nil {
		return err
	}

	logger, err := telemetry.NewLogger(os.Stderr, cfg.Log, info, "cli")
	if err != nil {
		return fmt.Errorf("configuring logger: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	loaded, err := migrate.Load(migrations.FS)
	if err != nil {
		return fmt.Errorf("loading embedded migrations: %w", err)
	}

	pool, err := db.Open(ctx, cfg.Database, serviceName, logger)
	if err != nil {
		return err
	}
	defer pool.Close()

	runner := migrate.New(pool, loaded, logger)

	switch cmd {
	case "up":
		applied, err := runner.Up(ctx)
		if err != nil {
			return err
		}
		if len(applied) == 0 {
			fmt.Printf("no pending migrations; schema is at version %d\n", runner.Target())
			return nil
		}
		fmt.Printf("applied %d migration(s); schema is now at version %d\n", len(applied), runner.Target())
		return nil

	case cmdDown:
		reverted, err := runner.Down(ctx, steps)
		if err != nil {
			return err
		}
		if len(reverted) == 0 {
			fmt.Println("nothing to revert")
			return nil
		}
		current, _, err := runner.Version(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("reverted %d migration(s); schema is now at version %d\n", len(reverted), current)
		return nil

	case "status":
		st, err := runner.Status(ctx)
		if err != nil {
			return err
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		_, _ = fmt.Fprintln(w, "VERSION\tNAME\tSTATUS\tAPPLIED AT")
		for _, s := range st {
			status := "pending"
			at := "-"
			switch {
			case s.Drifted:
				status = "DRIFTED"
				at = s.AppliedAt.UTC().Format("2006-01-02 15:04:05Z")
			case s.Applied:
				status = "applied"
				at = s.AppliedAt.UTC().Format("2006-01-02 15:04:05Z")
			}
			_, _ = fmt.Fprintf(w, "%06d\t%s\t%s\t%s\n", s.Version, s.Name, status, at)
		}
		return w.Flush()

	case "version":
		current, applied, err := runner.Version(ctx)
		if err != nil {
			return err
		}
		if !applied {
			fmt.Printf("no migrations applied; this binary carries up to version %d\n", runner.Target())
			return nil
		}
		fmt.Printf("schema version %d (this binary carries up to %d)\n", current, runner.Target())
		if current != runner.Target() {
			logger.Warn("schema version differs from this binary's target",
				slog.Int64("current_version", current), slog.Int64("target_version", runner.Target()))
		}
		return nil

	case "validate":
		if err := runner.Validate(ctx); err != nil {
			return err
		}
		fmt.Println("all applied migrations match this binary")
		return nil
	}
	return nil
}
