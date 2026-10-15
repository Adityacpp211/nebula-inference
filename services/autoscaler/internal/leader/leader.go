// Package leader elects one active autoscaler with a PostgreSQL advisory lock.
//
// Two autoscalers deciding the same deployment would each see the other's change
// as load moving, and the guards would no longer bound anything. The controller
// uses a Kubernetes Lease; the autoscaler has no Kubernetes access at all
// (docs/security-boundaries.md §5), and it already depends on PostgreSQL for
// every decision, so the lock lives there. A session-level advisory lock is held
// for exactly as long as the connection that took it: a crashed or partitioned
// leader loses it when its connection does.
package leader

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Key is the advisory lock's id: "nebulaas" in ASCII.
const Key int64 = 0x6e65_6275_6c61_6173

// Elector holds the lock on one dedicated connection.
type Elector struct {
	Pool   *pgxpool.Pool
	Logger *slog.Logger

	conn   *pgxpool.Conn
	leader atomic.Bool
}

// Lead reports whether this replica leads, taking the lock if it is free and
// verifying it still holds it if it already did. It never blocks for longer than
// one round trip.
func (e *Elector) Lead(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if e.conn != nil {
		if _, err := e.conn.Exec(ctx, "SELECT 1"); err == nil {
			return true
		}
		e.Logger.WarnContext(ctx, "lost the leadership connection: standing down")
		e.drop(ctx)
		return false
	}
	conn, err := e.Pool.Acquire(ctx)
	if err != nil {
		return false
	}
	var got bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", Key).Scan(&got); err != nil || !got {
		conn.Release()
		return false
	}
	e.conn = conn
	e.leader.Store(true)
	e.Logger.InfoContext(ctx, "acquired leadership")
	return true
}

// IsLeader reports the last known state, for probes.
func (e *Elector) IsLeader() bool { return e.leader.Load() }

// Release gives the lock up, for a clean shutdown.
func (e *Elector) Release(ctx context.Context) {
	if e.conn == nil {
		return
	}
	_, _ = e.conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", Key)
	e.conn.Release()
	e.conn = nil
	e.leader.Store(false)
}

// drop closes a connection that may still hold the lock in a broken session:
// closing it is what guarantees the server releases the lock.
func (e *Elector) drop(ctx context.Context) {
	_ = e.conn.Conn().Close(ctx)
	e.conn.Release()
	e.conn = nil
	e.leader.Store(false)
}
