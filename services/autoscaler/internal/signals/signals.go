// Package signals reads the autoscaler's inputs from Prometheus and turns them
// into autoscale.Signals (docs/architecture.md §7.2).
//
// Every deployment's signals come from a handful of instant queries per tick,
// grouped by deployment_id, so the cost does not grow with the number of
// deployments. A deployment absent from a result is missing, never zero, with one
// documented exception: the gateway's admission queue reports only deployments it
// has queued for, so an absent queue depth is an empty queue.
package signals

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/autoscale"
)

// Signal names, as they appear in autoscaling_events.signals and in the
// nebula_autoscale_signal_ratio metric.
const (
	ConcurrencyPerReplica = "concurrency_per_replica"
	QueueWaitP95MS        = "queue_wait_p95_ms"
)

// Reading is one pass over Prometheus.
type Reading struct {
	// WorkerLoad is requests running or queued at the workers.
	WorkerLoad map[uuid.UUID]float64
	// GatewayQueued is requests waiting in the gateways' admission queues.
	GatewayQueued map[uuid.UUID]float64
	// Slots is the workers' declared parallel slots, the default concurrency target.
	Slots map[uuid.UUID]float64
	// QueueWaitMS is the p95 admission-queue wait over the signal window.
	QueueWaitMS map[uuid.UUID]float64
	// Err is set when Prometheus could not be read at all: every signal is missing.
	Err error
}

// Prometheus reads signals over the HTTP API.
type Prometheus struct {
	BaseURL string
	Client  *http.Client
	// Window is the rate window for histogram signals.
	Window time.Duration
}

// Queries are exported for the documentation and the tests.
func (p *Prometheus) Queries() map[string]string {
	w := promDuration(p.Window)
	return map[string]string{
		"worker_load":    `sum by (deployment_id) (nebula_worker_in_flight{deployment_id!=""} + nebula_worker_queue_depth{deployment_id!=""})`,
		"gateway_queued": `sum by (deployment_id) (nebula_queue_depth{deployment_id!=""})`,
		"slots":          `avg by (deployment_id) (nebula_worker_slots_total{deployment_id!=""})`,
		"queue_wait_ms": `1000 * histogram_quantile(0.95, sum by (deployment_id, le) (rate(nebula_queue_wait_seconds_bucket{deployment_id!=""}[` +
			w + `])))`,
	}
}

// Read runs every query. A failed query fails the whole reading: signals from a
// half-answered pass would compare a fresh number with a missing one.
func (p *Prometheus) Read(ctx context.Context) Reading {
	var r Reading
	q := p.Queries()
	for _, step := range []struct {
		name string
		into *map[uuid.UUID]float64
	}{
		{"worker_load", &r.WorkerLoad}, {"gateway_queued", &r.GatewayQueued},
		{"slots", &r.Slots}, {"queue_wait_ms", &r.QueueWaitMS},
	} {
		m, err := p.query(ctx, q[step.name])
		if err != nil {
			return Reading{Err: fmt.Errorf("%s: %w", step.name, err)}
		}
		*step.into = m
	}
	return r
}

type apiResponse struct {
	Status    string `json:"status"`
	ErrorType string `json:"errorType"`
	Error     string `json:"error"`
	Data      struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  [2]any            `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

// query runs one instant query and returns its finite values by deployment_id.
// NaN (a quantile over no requests) and Inf are dropped: they are absences.
func (p *Prometheus) query(ctx context.Context, q string) (map[uuid.UUID]float64, error) {
	u := strings.TrimRight(p.BaseURL, "/") + "/api/v1/query?query=" + url.QueryEscape(q)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, http.NoBody)
	if err != nil {
		return nil, err
	}
	client := p.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	var body apiResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("decoding the response (HTTP %d): %w", resp.StatusCode, err)
	}
	if body.Status != "success" {
		return nil, fmt.Errorf("prometheus: %s: %s", body.ErrorType, body.Error)
	}
	if body.Data.ResultType != "vector" {
		return nil, fmt.Errorf("prometheus: want a vector, got %q", body.Data.ResultType)
	}
	out := make(map[uuid.UUID]float64, len(body.Data.Result))
	for _, s := range body.Data.Result {
		id, err := uuid.Parse(s.Metric["deployment_id"])
		if err != nil {
			continue
		}
		raw, ok := s.Value[1].(string)
		if !ok {
			continue
		}
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
			continue
		}
		out[id] = v
	}
	return out, nil
}

// For builds one deployment's signal vector from a reading.
//
// Concurrency is total demand — running and queued at the workers plus waiting at
// the gateways — divided by the desired replica count, so a replica that is still
// starting counts as one carrying nothing. That is the conservative choice: it
// makes a scale-up look smaller, never larger.
func For(r Reading, id uuid.UUID, current int32, s autoscale.Settings) []autoscale.Signal {
	conc := autoscale.Signal{Name: ConcurrencyPerReplica, Source: "prometheus", Stale: true}
	wait := autoscale.Signal{Name: QueueWaitP95MS, Source: "prometheus", Stale: true, Target: s.TargetQueueWaitMS}
	if r.Err != nil {
		return []autoscale.Signal{conc, wait}
	}

	target := s.TargetConcurrency
	if target <= 0 {
		target = r.Slots[id]
	}
	conc.Target = target
	if load, ok := r.WorkerLoad[id]; ok && target > 0 {
		replicas := float64(current)
		if replicas < 1 {
			replicas = 1
		}
		conc.Observed = math.Round((load+r.GatewayQueued[id])/replicas*100) / 100
		conc.Stale = false
	}
	if v, ok := r.QueueWaitMS[id]; ok {
		wait.Observed = math.Round(v*10) / 10
		wait.Stale = false
	}
	return []autoscale.Signal{conc, wait}
}

// promDuration renders a duration in PromQL's syntax.
func promDuration(d time.Duration) string {
	if d <= 0 {
		d = time.Minute
	}
	return strconv.Itoa(int(d.Seconds())) + "s"
}
