package server

import (
	"errors"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/adityasatwar321/nebula/packages/httpx"
	"github.com/adityasatwar321/nebula/packages/queue"
	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/services/gateway/internal/admission"
	"github.com/adityasatwar321/nebula/services/gateway/internal/router"
	"github.com/adityasatwar321/nebula/services/gateway/internal/usage"
)

// gwMetrics is the gateway's slice of the metric catalogue (docs/observability.md
// §2.1 and §2.2). Every series is declared in telemetry.Catalog; this type only
// decides when each is recorded.
type gwMetrics struct {
	requests      *prometheus.CounterVec
	duration      *prometheus.HistogramVec
	ttft          *prometheus.HistogramVec
	tokens        *prometheus.CounterVec
	tps           *prometheus.HistogramVec
	inflight      *prometheus.GaugeVec
	attempts      *prometheus.CounterVec
	interrupted   *prometheus.CounterVec
	cancellations *prometheus.CounterVec
	decisions     *prometheus.CounterVec
	staleness     *prometheus.HistogramVec
	queueWait     *prometheus.HistogramVec
	queueDrops    *prometheus.CounterVec
	rateLimited   *prometheus.CounterVec
}

func newGatewayMetrics(m *telemetry.Metrics, rt *router.Router, q *admission.Queues) *gwMetrics {
	g := &gwMetrics{
		requests:      m.Counter("nebula_requests_total"),
		duration:      m.Histogram("nebula_request_duration_seconds"),
		ttft:          m.Histogram("nebula_ttft_seconds"),
		tokens:        m.Counter("nebula_tokens_total"),
		tps:           m.Histogram("nebula_tokens_per_second"),
		inflight:      m.Gauge("nebula_inflight_requests"),
		attempts:      m.Counter("nebula_request_attempts_total"),
		interrupted:   m.Counter("nebula_streams_interrupted_total"),
		cancellations: m.Counter("nebula_client_cancellations_total"),
		decisions:     m.Counter("nebula_route_decisions_total"),
		staleness:     m.Histogram("nebula_endpoint_staleness_seconds"),
		queueWait:     m.Histogram("nebula_queue_wait_seconds"),
		queueDrops:    m.Counter("nebula_queue_drops_total"),
		rateLimited:   m.Counter("nebula_ratelimit_rejections_total"),
	}

	// Computed at scrape time from state the router and the queues already hold.
	m.GaugeFunc("nebula_endpoints", func() []telemetry.Sample {
		counts := map[[2]string]float64{}
		for _, e := range rt.Report() {
			counts[[2]string{e.Deployment, e.State}]++
		}
		out := make([]telemetry.Sample, 0, len(counts))
		for k, v := range counts {
			out = append(out, telemetry.Sample{Labels: []string{k[0], k[1]}, Value: v})
		}
		return out
	})
	m.GaugeFunc("nebula_breaker_state", func() []telemetry.Sample {
		var out []telemetry.Sample
		for _, e := range rt.Report() {
			// The endpoint id is the pod name for discovered endpoints, the URL for
			// static ones; both labels carry it so either query works.
			out = append(out, telemetry.Sample{Labels: []string{e.Deployment, e.Endpoint, e.Endpoint},
				Value: float64(breakerValue(e.Breaker))})
		}
		return out
	})
	m.GaugeFunc("nebula_queue_depth", func() []telemetry.Sample {
		var out []telemetry.Sample
		for key, s := range q.Stats() {
			name := rt.DeploymentName(key)
			for _, p := range []queue.Priority{queue.Low, queue.Normal, queue.High} {
				out = append(out, telemetry.Sample{Labels: []string{name, p.String()}, Value: float64(s.ByPrio[p.String()])})
			}
		}
		return out
	})
	m.GaugeFunc("nebula_queue_oldest_age_seconds", func() []telemetry.Sample {
		var out []telemetry.Sample
		for key, s := range q.Stats() {
			out = append(out, telemetry.Sample{Labels: []string{rt.DeploymentName(key)}, Value: s.OldestAge.Seconds()})
		}
		return out
	})
	return g
}

func breakerValue(s interface{ String() string }) int {
	switch s.String() {
	case "half_open":
		return 1
	case "open":
		return 2
	}
	return 0
}

// finished records a request that reached dispatch, from its usage record: the
// record is already the single, complete account of what happened.
func (m *gwMetrics) finished(rec *usage.Record, streamed bool) {
	errClass := rec.ErrorClass
	m.requests.WithLabelValues(rec.Route, rec.Deployment, rec.ModelVersion, rec.Endpoint,
		telemetry.StatusClass(rec.StatusCode), errClass, rec.Variant).Inc()
	telemetry.Observe(m.duration.WithLabelValues(rec.Route, rec.Deployment, rec.Endpoint, strconv.FormatBool(streamed)),
		float64(rec.DurationMS)/1000, rec.TraceID)
	if rec.TTFT != nil {
		telemetry.Observe(m.ttft.WithLabelValues(rec.Route, rec.Deployment, rec.ModelVersion),
			float64(*rec.TTFT)/1000, rec.TraceID)
	}
	if rec.PromptTokens != nil && rec.CompletionTokens != nil {
		m.tokens.WithLabelValues("prompt", rec.Deployment, rec.ModelVersion).Add(float64(*rec.PromptTokens))
		m.tokens.WithLabelValues("completion", rec.Deployment, rec.ModelVersion).Add(float64(*rec.CompletionTokens))
		if rec.ComputeMS != nil && *rec.ComputeMS > 0 && *rec.CompletionTokens > 0 {
			m.tps.WithLabelValues(rec.Deployment, rec.ModelVersion).
				Observe(float64(*rec.CompletionTokens) / (float64(*rec.ComputeMS) / 1000))
		}
	}
	if rec.Outcome == usage.OutcomeClientCancelled {
		m.cancellations.WithLabelValues(rec.Deployment).Inc()
	}
}

// refused records a request that never reached dispatch.
func (m *gwMetrics) refused(route, endpoint string, err error) {
	var apiErr *httpx.APIError
	status, class := 500, string(httpx.TypeInternal)
	if errors.As(err, &apiErr) {
		status, class = apiErr.Status, string(apiErr.Type)
	}
	m.requests.WithLabelValues(route, "", "", endpoint, telemetry.StatusClass(status), class, "").Inc()
}

func (m *gwMetrics) decision(route, strategy, outcome string) {
	m.decisions.WithLabelValues(route, strategy, outcome).Inc()
}

func (m *gwMetrics) attempt(deployment string, outcome router.Outcome) {
	m.attempts.WithLabelValues(deployment, outcomeName(outcome)).Inc()
}

func outcomeName(o router.Outcome) string {
	switch o {
	case router.Unreachable:
		return "unreachable"
	case router.Failed:
		return "failed"
	case router.Refused:
		return "refused"
	case router.Abandoned:
		return "abandoned"
	}
	return "succeeded"
}

func (m *gwMetrics) selected(sel *router.Selection) {
	if sel.HeartbeatAge >= 0 {
		m.staleness.WithLabelValues(sel.Target.Deployment).Observe(sel.HeartbeatAge.Seconds())
	}
}

func (m *gwMetrics) waited(deployment string, p queue.Priority, d time.Duration, traceID string) {
	telemetry.Observe(m.queueWait.WithLabelValues(deployment, p.String()), d.Seconds(), traceID)
}

func (m *gwMetrics) dropped(deployment string, err error) {
	var full *queue.FullError
	reason := queue.ReasonCancelled
	switch {
	case errors.As(err, &full) && full.WouldWait:
		reason = queue.ReasonRejected
	case errors.As(err, &full):
		reason = queue.ReasonFull
	case errors.Is(err, queue.ErrTimeout):
		reason = queue.ReasonTimeout
	}
	m.queueDrops.WithLabelValues(deployment, reason).Inc()
}

func (m *gwMetrics) rateLimitedBy(scope, reason string) {
	limit := "concurrency"
	switch reason {
	case "rate_limited_rpm":
		limit = "rpm"
	case "rate_limited_tpm":
		limit = "tpm"
	}
	m.rateLimited.WithLabelValues(scope, limit).Inc()
}
