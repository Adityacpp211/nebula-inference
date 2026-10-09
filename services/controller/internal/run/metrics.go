package run

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/adityasatwar321/nebula/packages/telemetry"
)

// observe records one reconcile pass (docs/observability.md §2.4).
func (c *Controller) observe(started time.Time, err error) {
	m := c.o.Metrics
	if m == nil {
		return
	}
	result := "ok"
	if err != nil {
		result = "error"
		m.Counter("nebula_reconcile_errors_total").WithLabelValues("deployment", errorClass(err)).Inc()
	}
	m.Histogram("nebula_reconcile_duration_seconds").WithLabelValues("deployment", result).
		Observe(time.Since(started).Seconds())
}

// errorClass is a closed set, so a new error cannot invent a series.
func errorClass(err error) string {
	var status apierrors.APIStatus
	var pg *pgconn.PgError
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.As(err, &status):
		return "kubernetes"
	case errors.As(err, &pg):
		return "database"
	}
	return "other"
}

// registerFleetMetrics exports the work queue depth and, from PostgreSQL, each
// deployment's generation lag, replicas and state. The fleet query runs at most
// every ten seconds however often Prometheus scrapes.
func (c *Controller) registerFleetMetrics() {
	m := c.o.Metrics
	if m == nil {
		return
	}
	m.GaugeFunc("nebula_reconcile_queue_depth", func() []telemetry.Sample {
		return []telemetry.Sample{{Labels: []string{"deployment"}, Value: float64(c.queue.Len())}}
	})

	var mu sync.Mutex
	var at time.Time
	var rows []fleetRow
	fleet := func() []fleetRow {
		mu.Lock()
		defer mu.Unlock()
		if time.Since(at) > 10*time.Second {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if fresh, err := c.fleet(ctx); err == nil {
				rows, at = fresh, time.Now()
			}
		}
		return rows
	}
	m.GaugeFunc("nebula_generation_lag", func() []telemetry.Sample {
		var out []telemetry.Sample
		for _, r := range fleet() {
			out = append(out, telemetry.Sample{Labels: []string{r.Name}, Value: float64(r.Generation - r.Observed)})
		}
		return out
	})
	m.GaugeFunc("nebula_deployment_replicas", func() []telemetry.Sample {
		var out []telemetry.Sample
		for _, r := range fleet() {
			out = append(out,
				telemetry.Sample{Labels: []string{r.Name, "desired"}, Value: float64(r.Desired)},
				telemetry.Sample{Labels: []string{r.Name, "ready"}, Value: float64(r.Ready)},
				telemetry.Sample{Labels: []string{r.Name, "updated"}, Value: float64(r.Updated)})
		}
		return out
	})
	m.GaugeFunc("nebula_deployment_state", func() []telemetry.Sample {
		var out []telemetry.Sample
		for _, r := range fleet() {
			out = append(out, telemetry.Sample{Labels: []string{r.Name, r.State}, Value: 1})
		}
		return out
	})
}

type fleetRow struct {
	Name                    string
	State                   string
	Generation, Observed    int64
	Desired, Ready, Updated int32
}

func (c *Controller) fleet(ctx context.Context) ([]fleetRow, error) {
	rows, err := c.o.Store.Pool().Query(ctx, `
		SELECT d.name, d.state::text, d.generation, d.observed_generation,
		       d.desired_replicas, d.ready_replicas, d.updated_replicas
		  FROM deployments d
		 WHERE d.deleted_at IS NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []fleetRow
	for rows.Next() {
		var r fleetRow
		if err := rows.Scan(&r.Name, &r.State, &r.Generation, &r.Observed, &r.Desired, &r.Ready, &r.Updated); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
