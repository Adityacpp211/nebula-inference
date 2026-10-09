package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics are NEBULA's Prometheus metrics (docs/observability.md §2).
//
// Every metric a Go service exports is declared once, in Catalog, with its type
// and its complete label set. A service asks for a metric by name and gets the
// declared vector; asking for an undeclared name, or with the wrong type, panics
// at startup. That is the cardinality budget of §2.5 enforced in code: a label
// cannot appear on a metric without a change to this file, which is where review
// looks, and a contract test keeps this file and the documented catalogue in step
// so a rename cannot silently empty a dashboard.

// Kind is a metric type.
type Kind int

// Metric kinds.
const (
	KindCounter Kind = iota
	KindGauge
	KindHistogram
)

func (k Kind) String() string {
	switch k {
	case KindGauge:
		return "gauge"
	case KindHistogram:
		return "histogram"
	}
	return "counter"
}

// Def declares one metric.
type Def struct {
	Name    string
	Kind    Kind
	Help    string
	Labels  []string
	Buckets []float64
}

var (
	latencyBuckets  = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120}
	ttftBuckets     = []float64{0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10}
	tpsBuckets      = []float64{1, 5, 10, 25, 50, 100, 200, 500}
	waitBuckets     = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}
	staleBuckets    = []float64{0.25, 0.5, 1, 2, 3, 5, 10, 30}
	reconcileBucket = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 10}
)

// Catalog is every metric the Go services export. Keep it in the order of
// docs/observability.md §2; TestCatalogMatchesTheDocumentedCatalogue compares them.
var Catalog = []Def{
	// §2.1 request path (gateway)
	{Name: "nebula_requests_total", Kind: KindCounter, Help: "Inference requests, by outcome.",
		Labels: []string{"route", "deployment", "model_version", "endpoint", "status_class", "error_class", "variant"}},
	{Name: "nebula_request_duration_seconds", Kind: KindHistogram, Help: "Inference request duration.",
		Labels: []string{"route", "deployment", "endpoint", "streamed"}, Buckets: latencyBuckets},
	{Name: "nebula_ttft_seconds", Kind: KindHistogram, Help: "Time to first token, as the worker measured it.",
		Labels: []string{"route", "deployment", "model_version"}, Buckets: ttftBuckets},
	{Name: "nebula_tokens_total", Kind: KindCounter, Help: "Tokens, as the runtime counted them.",
		Labels: []string{"direction", "deployment", "model_version"}},
	{Name: "nebula_tokens_per_second", Kind: KindHistogram, Help: "Decode throughput per request.",
		Labels: []string{"deployment", "model_version"}, Buckets: tpsBuckets},
	{Name: "nebula_inflight_requests", Kind: KindGauge, Help: "Requests this gateway has dispatched and not finished.",
		Labels: []string{"deployment"}},
	{Name: "nebula_request_attempts_total", Kind: KindCounter, Help: "Dispatch attempts, by outcome.",
		Labels: []string{"deployment", "outcome"}},
	{Name: "nebula_streams_interrupted_total", Kind: KindCounter, Help: "Streams that failed after they started.",
		Labels: []string{"deployment", "reason"}},
	{Name: "nebula_client_cancellations_total", Kind: KindCounter, Help: "Requests the client abandoned. Not errors.",
		Labels: []string{"deployment"}},

	// §2.2 routing, queue, reliability
	{Name: "nebula_route_decisions_total", Kind: KindCounter, Help: "Routing decisions, by outcome.",
		Labels: []string{"route", "strategy", "outcome"}},
	{Name: "nebula_endpoints", Kind: KindGauge, Help: "Endpoints in the router's view, by state.",
		Labels: []string{"deployment", "state"}},
	{Name: "nebula_endpoint_staleness_seconds", Kind: KindHistogram, Help: "Heartbeat age at selection time.",
		Labels: []string{"deployment"}, Buckets: staleBuckets},
	{Name: "nebula_queue_depth", Kind: KindGauge, Help: "Requests waiting in the admission queue.",
		Labels: []string{"deployment", "priority"}},
	{Name: "nebula_queue_wait_seconds", Kind: KindHistogram, Help: "Time spent waiting in the admission queue.",
		Labels: []string{"deployment", "priority"}, Buckets: waitBuckets},
	{Name: "nebula_queue_oldest_age_seconds", Kind: KindGauge, Help: "Age of the oldest waiting request.",
		Labels: []string{"deployment"}},
	{Name: "nebula_queue_drops_total", Kind: KindCounter, Help: "Requests the admission queue refused or dropped.",
		Labels: []string{"deployment", "reason"}},
	{Name: "nebula_breaker_state", Kind: KindGauge, Help: "Per-endpoint breaker: 0 closed, 1 half-open, 2 open.",
		Labels: []string{"deployment", "endpoint", "pod"}},
	{Name: "nebula_ratelimit_rejections_total", Kind: KindCounter, Help: "Requests refused by a rate limit.",
		Labels: []string{"scope", "limit"}},

	// §2.4 control plane and controller
	{Name: "nebula_reconcile_duration_seconds", Kind: KindHistogram, Help: "One reconcile pass.",
		Labels: []string{"reconciler", "result"}, Buckets: reconcileBucket},
	{Name: "nebula_reconcile_errors_total", Kind: KindCounter, Help: "Failed reconcile passes.",
		Labels: []string{"reconciler", "error_class"}},
	{Name: "nebula_reconcile_queue_depth", Kind: KindGauge, Help: "Items waiting for the reconciler.",
		Labels: []string{"reconciler"}},
	{Name: "nebula_generation_lag", Kind: KindGauge, Help: "generation minus observed_generation.",
		Labels: []string{"deployment"}},
	{Name: "nebula_deployment_replicas", Kind: KindGauge, Help: "Replicas, desired and ready.",
		Labels: []string{"deployment", "state"}},
	{Name: "nebula_deployment_state", Kind: KindGauge, Help: "1 for a deployment's current lifecycle state.",
		Labels: []string{"deployment", "state"}},
	{Name: "nebula_capacity_admission_total", Kind: KindCounter, Help: "Capacity admission decisions.",
		Labels: []string{"result", "reason"}},
	{Name: "nebula_schema_version", Kind: KindGauge, Help: "The schema version this service expects.",
		Labels: []string{"service"}},
}

// Lookup returns a metric's declaration.
func Lookup(name string) (Def, bool) {
	for _, d := range Catalog {
		if d.Name == name {
			return d, true
		}
	}
	return Def{}, false
}

// Metrics is one service's registry.
type Metrics struct {
	reg *prometheus.Registry

	mu         sync.Mutex
	counters   map[string]*prometheus.CounterVec
	gauges     map[string]*prometheus.GaugeVec
	histograms map[string]*prometheus.HistogramVec
}

// NewMetrics builds a registry with the Go runtime and process collectors.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return &Metrics{reg: reg, counters: map[string]*prometheus.CounterVec{},
		gauges: map[string]*prometheus.GaugeVec{}, histograms: map[string]*prometheus.HistogramVec{}}
}

func declared(name string, kind Kind) Def {
	d, ok := Lookup(name)
	if !ok {
		panic(fmt.Sprintf("telemetry: metric %q is not declared in telemetry.Catalog", name))
	}
	if d.Kind != kind {
		panic(fmt.Sprintf("telemetry: metric %q is a %s, not a %s", name, d.Kind, kind))
	}
	return d
}

// Counter returns a declared counter.
func (m *Metrics) Counter(name string) *prometheus.CounterVec {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c := m.counters[name]; c != nil {
		return c
	}
	d := declared(name, KindCounter)
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: d.Name, Help: d.Help}, d.Labels)
	m.reg.MustRegister(c)
	m.counters[name] = c
	return c
}

// Gauge returns a declared gauge.
func (m *Metrics) Gauge(name string) *prometheus.GaugeVec {
	m.mu.Lock()
	defer m.mu.Unlock()
	if g := m.gauges[name]; g != nil {
		return g
	}
	d := declared(name, KindGauge)
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: d.Name, Help: d.Help}, d.Labels)
	m.reg.MustRegister(g)
	m.gauges[name] = g
	return g
}

// Histogram returns a declared histogram.
func (m *Metrics) Histogram(name string) *prometheus.HistogramVec {
	m.mu.Lock()
	defer m.mu.Unlock()
	if h := m.histograms[name]; h != nil {
		return h
	}
	d := declared(name, KindHistogram)
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: d.Name, Help: d.Help, Buckets: d.Buckets}, d.Labels)
	m.reg.MustRegister(h)
	m.histograms[name] = h
	return h
}

// Observe records a histogram sample, attaching the trace id as an exemplar when
// there is one, so a Prometheus bucket links to the trace behind it.
func Observe(o prometheus.Observer, v float64, traceID string) {
	if eo, ok := o.(prometheus.ExemplarObserver); ok && traceID != "" {
		eo.ObserveWithExemplar(v, prometheus.Labels{"trace_id": traceID})
		return
	}
	o.Observe(v)
}

// Sample is one value of a computed gauge.
type Sample struct {
	Labels []string
	Value  float64
}

// GaugeFunc registers a declared gauge whose values are computed at scrape time,
// for state another component already holds (the router's endpoints, a queue's
// depth). fn must return label values in the declared order.
func (m *Metrics) GaugeFunc(name string, fn func() []Sample) {
	d := declared(name, KindGauge)
	m.reg.MustRegister(&funcCollector{desc: prometheus.NewDesc(d.Name, d.Help, d.Labels, nil), fn: fn})
}

type funcCollector struct {
	desc *prometheus.Desc
	fn   func() []Sample
}

// Describe implements prometheus.Collector.
func (c *funcCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

// Collect implements prometheus.Collector.
func (c *funcCollector) Collect(ch chan<- prometheus.Metric) {
	for _, s := range c.fn() {
		if m, err := prometheus.NewConstMetric(c.desc, prometheus.GaugeValue, s.Value, s.Labels...); err == nil {
			ch <- m
		}
	}
}

// Handler serves the registry in the Prometheus text format, with exemplars when
// the scraper asks for OpenMetrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{EnableOpenMetrics: true})
}

// Registered lists the NEBULA metric names this registry has, sorted; for tests.
func (m *Metrics) Registered() []string {
	fams, _ := m.reg.Gather()
	out := make([]string, 0, len(fams))
	for _, f := range fams {
		out = append(out, f.GetName())
	}
	sort.Strings(out)
	return out
}

// StatusClass maps an HTTP status to the status_class label.
func StatusClass(code int) string {
	switch {
	case code >= 500:
		return "5xx"
	case code >= 400:
		return "4xx"
	case code >= 300:
		return "3xx"
	case code >= 200:
		return "2xx"
	}
	return "other"
}

// ServeMetrics serves /metrics on its own listener until ctx ends. A separate
// port keeps metrics off the public listener: the gateway's main port is the
// internet-facing edge, and /metrics is for in-cluster scraping only
// (docs/api.md §3).
func ServeMetrics(ctx context.Context, addr string, m *Metrics, logger *slog.Logger) {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", m.Handler())
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	logger.InfoContext(ctx, "metrics listening", slog.String("addr", addr))
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.ErrorContext(ctx, "metrics listener stopped", slog.String("cause", err.Error()))
	}
}
