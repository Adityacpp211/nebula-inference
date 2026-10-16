package db_test

import (
	"testing"

	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/testsupport/dbtest"
)

// The horizon moves forward and running it twice changes nothing.
func TestEnsurePartitionsExtendsTheHorizon(t *testing.T) {
	t.Parallel()
	pool := dbtest.Migrated(t)
	ctx := dbtest.Context(t)
	count := func() int {
		var n int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM pg_inherits i JOIN pg_class p ON p.oid = i.inhparent
			 WHERE p.relname IN ('requests', 'audit_logs')`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	before := count()
	if err := db.EnsurePartitions(ctx, pool, 5); err != nil {
		t.Fatal(err)
	}
	after := count()
	if after != 2*6 {
		t.Fatalf("partitions %d -> %d, want 12 (six months of two tables)", before, after)
	}
	if err := db.EnsurePartitions(ctx, pool, 5); err != nil || count() != after {
		t.Fatalf("a second run must change nothing: %v %d", err, count())
	}
	// A row five months out now has somewhere to go.
	if _, err := pool.Exec(ctx, `
		INSERT INTO requests (id, org_id, created_at, status_code, duration_ms)
		VALUES (gen_random_uuid(), gen_random_uuid(), date_trunc('month', now()) + interval '5 months 1 day', 200, 1)`); err != nil {
		t.Fatalf("insert at the new horizon: %v", err)
	}
}
