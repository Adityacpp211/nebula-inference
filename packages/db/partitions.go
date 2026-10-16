package db

import (
	"context"
	"fmt"
)

// PartitionedTables are the tables partitioned by month (migration 000006). They
// have no default partition on purpose: an insert past the horizon fails loudly
// rather than landing where it would later block attaching the right partition.
var PartitionedTables = []string{"requests", "audit_logs"}

// EnsurePartitions creates this month's partition and the next monthsAhead for
// every monthly-partitioned table. Idempotent; it needs the privilege to create
// tables, so it runs as the schema owner (nebula-migrate up), never as a service
// role.
func EnsurePartitions(ctx context.Context, q Querier, monthsAhead int) error {
	for _, table := range PartitionedTables {
		_, err := q.Exec(ctx, `
			SELECT nebula_ensure_month_partition($1::regclass,
			       (date_trunc('month', now()) + make_interval(months => m))::date)
			  FROM generate_series(0, $2::int) AS m`, table, monthsAhead)
		if err != nil {
			return fmt.Errorf("ensuring %s partitions: %w", table, err)
		}
	}
	return nil
}
