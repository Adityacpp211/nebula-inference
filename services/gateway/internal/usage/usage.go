// Package usage emits one usage record per inference request, in the shape of
// nebula.usage.record.v1 (docs/events.md §4.1).
//
// Phase 4 has a record and no broker: NATS JetStream and the usage ingester that
// consumes it arrive together (Phase 6 brings NATS, Phase 13 the ingester), and a
// publisher with no consumer would be scaffolding shaped like a feature. Until then
// the record is written as one structured log line, "usage.record", carrying the
// full payload. That is a real, parseable record — Loki can already count it — and
// the Sink interface is where the JetStream publisher slots in without the request
// path changing. TODO(NEB-140) tracks the publisher.
//
// The rules that make a usage record trustworthy hold already:
//
//   - One record per request, emitted after the response completes OR aborts.
//   - Partial usage is always emitted: a client disconnect, a worker dying
//     mid-stream and a deadline all produce a record with the tokens the runtime
//     actually generated, because the compute happened.
//   - token_source says where the counts came from. "runtime" is the only source
//     that produces numbers; when a stream is cut before the runtime reported, the
//     counts are absent and token_source is "unavailable" — never an estimate
//     (axiom A2).
package usage

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/adityasatwar321/nebula/packages/telemetry"
)

// Outcomes, per docs/events.md §4.1.
const (
	OutcomeCompleted         = "completed"
	OutcomeClientCancelled   = "client_cancelled"
	OutcomeStreamInterrupted = "stream_interrupted"
	OutcomeDeadlineExceeded  = "deadline_exceeded"
	OutcomeRejected          = "rejected"
	OutcomeFailed            = "failed"
)

// Token sources.
const (
	TokenSourceRuntime     = "runtime"
	TokenSourceUnavailable = "unavailable"
)

// Record is one request's usage. Pointer fields are absent when not measured.
type Record struct {
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	SchemaVersion int       `json:"schema_version"`
	OccurredAt    time.Time `json:"occurred_at"`
	TraceID       string    `json:"trace_id,omitempty"`
	OrgID         string    `json:"org_id"`

	RequestID    string `json:"request_id"`
	APIKeyID     string `json:"api_key_id"`
	RouteID      string `json:"route_id,omitempty"`
	Route        string `json:"route,omitempty"`
	Deployment   string `json:"deployment,omitempty"`
	DeploymentID string `json:"deployment_id,omitempty"`
	ModelVersion string `json:"model_version,omitempty"`
	Variant      string `json:"variant,omitempty"`
	Endpoint     string `json:"endpoint"`
	Priority     string `json:"priority"`
	Streamed     bool   `json:"streamed"`
	StatusCode   int    `json:"status_code"`
	ErrorClass   string `json:"error_class,omitempty"`
	Outcome      string `json:"outcome"`

	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	QueueWait  *int64    `json:"queue_wait_ms,omitempty"`
	TTFT       *int64    `json:"ttft_ms,omitempty"`
	DurationMS int64     `json:"duration_ms"`
	ComputeMS  *int64    `json:"compute_ms,omitempty"`
	Attempts   int       `json:"attempts"`

	PromptTokens     *int    `json:"prompt_tokens,omitempty"`
	CompletionTokens *int    `json:"completion_tokens,omitempty"`
	TokenSource      string  `json:"token_source"`
	FinishReason     string  `json:"finish_reason,omitempty"`
	Runtime          string  `json:"runtime,omitempty"`
	User             string  `json:"user,omitempty"`
	SampleRate       float64 `json:"sample_rate"`
}

// EventType is the record's schema name.
const EventType = "nebula.usage.record.v1"

// Sink receives records. Emit must not block the request path.
type Sink interface {
	Emit(ctx context.Context, r Record)
}

// LogSink writes each record as a structured log line.
type LogSink struct{ Logger *slog.Logger }

// Emit implements Sink.
func (s LogSink) Emit(ctx context.Context, r Record) {
	complete(&r)
	s.Logger.LogAttrs(ctx, slog.LevelInfo, "usage.record", slog.Any("usage", r))
}

// MemorySink keeps records in memory, for tests.
type MemorySink struct {
	mu      sync.Mutex
	records []Record
	notify  chan struct{}
}

// NewMemorySink returns an empty sink.
func NewMemorySink() *MemorySink { return &MemorySink{notify: make(chan struct{}, 1024)} }

// Emit implements Sink.
func (m *MemorySink) Emit(_ context.Context, r Record) {
	complete(&r)
	m.mu.Lock()
	m.records = append(m.records, r)
	m.mu.Unlock()
	select {
	case m.notify <- struct{}{}:
	default:
	}
}

// Records returns a copy of everything emitted.
func (m *MemorySink) Records() []Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Record(nil), m.records...)
}

// Wait blocks until n records exist or the timeout passes.
func (m *MemorySink) Wait(n int, timeout time.Duration) []Record {
	deadline := time.After(timeout)
	for {
		if rs := m.Records(); len(rs) >= n {
			return rs
		}
		select {
		case <-m.notify:
		case <-deadline:
			return m.Records()
		}
	}
}

func complete(r *Record) {
	if r.EventID == "" {
		r.EventID = telemetry.NewRequestID()
	}
	r.EventType = EventType
	r.SchemaVersion = 1
	if r.OccurredAt.IsZero() {
		r.OccurredAt = time.Now().UTC()
	}
	if r.SampleRate == 0 {
		r.SampleRate = 1.0
	}
	if r.TokenSource == "" {
		r.TokenSource = TokenSourceUnavailable
	}
}
