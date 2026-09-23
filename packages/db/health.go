package db

import (
	"context"
	"fmt"
	"time"
)

// HealthChecker reports PostgreSQL usability for readiness probes.
//
// It runs a real query rather than only checking the pool's internal state: a
// pool can hold connections that the server has since closed, and "the struct
// thinks it is fine" is not evidence.
type HealthChecker struct {
	Pool *Pool
	// Timeout bounds the check. Kept short: a readiness probe that hangs is worse
	// than one that fails, because the pod stays in service while unresponsive.
	Timeout time.Duration
	// ExpectedSchema, when non-zero, also asserts the schema version. A pod
	// running against the wrong schema must not receive traffic.
	ExpectedSchema int64
}

// Name implements telemetry.Checker.
func (c HealthChecker) Name() string { return "postgres" }

// Critical implements telemetry.Checker: no database means no service.
func (c HealthChecker) Critical() bool { return true }

// Check implements telemetry.Checker.
func (c HealthChecker) Check(ctx context.Context) error {
	if c.Pool == nil {
		return fmt.Errorf("no database pool configured")
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var one int
	if err := c.Pool.QueryRow(ctx, `SELECT 1`).Scan(&one); err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	if one != 1 {
		return fmt.Errorf("unexpected result from SELECT 1: %d", one)
	}

	if c.ExpectedSchema > 0 {
		if err := AssertSchemaVersion(ctx, c.Pool, c.ExpectedSchema); err != nil {
			return err
		}
	}
	return nil
}
