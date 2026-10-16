// Package usagectrl ingests usage records from JetStream into PostgreSQL
// (docs/events.md §4.1): one row per request in requests, with its estimated
// cost, and the hourly usage_records rollups the cost and usage APIs read.
//
// The stream is a WorkQueue with exactly one durable consumer, so a record is
// processed by one ingester and removed once acknowledged. Delivery is
// at-least-once; the store makes a redelivered batch a no-op. A batch is
// acknowledged only after its transaction commits, so a crash between the two
// redelivers it rather than losing it.
package usagectrl

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/services/controller/internal/store"
)

// Stream and consumer names, and the subject (docs/events.md §2).
const (
	StreamName   = "NEBULA_USAGE"
	Subject      = "nebula.usage.records"
	ConsumerName = "usage-ingester"
	BatchSize    = 500
)

// StreamConfig is the usage stream: a WorkQueue kept 7 days, deduplicating on
// the message id (the record's event id) within 2 minutes.
func StreamConfig() jetstream.StreamConfig {
	return jetstream.StreamConfig{Name: StreamName, Subjects: []string{Subject},
		Retention: jetstream.WorkQueuePolicy, MaxAge: 7 * 24 * time.Hour, Duplicates: 2 * time.Minute,
		Storage: jetstream.FileStorage}
}

// Store is what the ingester needs from PostgreSQL.
type Store interface {
	IngestUsage(ctx context.Context, rows []store.UsageRow) (store.IngestResult, error)
}

// Ingester consumes the usage stream.
type Ingester struct {
	URL      string
	Store    Store
	Logger   *slog.Logger
	Metrics  *telemetry.Metrics
	IsLeader func() bool
}

// Run ingests until ctx ends.
func (i *Ingester) Run(ctx context.Context) error {
	nc, err := nats.Connect(i.URL, nats.Name("nebula-controller-usage"), nats.MaxReconnects(-1),
		nats.RetryOnFailedConnect(true), nats.ReconnectWait(time.Second))
	if err != nil {
		return err
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	var cons jetstream.Consumer
	for cons == nil {
		if ctx.Err() != nil {
			return nil
		}
		cons, err = ensure(ctx, js)
		if err != nil {
			i.Logger.WarnContext(ctx, "usage stream not ready; retrying", slog.String("cause", err.Error()))
			sleep(ctx, 5*time.Second)
		}
	}
	i.Logger.InfoContext(ctx, "usage ingester consuming", slog.String("stream", StreamName))
	for ctx.Err() == nil {
		if i.IsLeader != nil && !i.IsLeader() {
			sleep(ctx, 2*time.Second)
			continue
		}
		if err := i.batch(ctx, cons); err != nil && ctx.Err() == nil {
			i.Logger.WarnContext(ctx, "usage batch not ingested; it will be redelivered", slog.String("cause", err.Error()))
			sleep(ctx, 2*time.Second)
		}
	}
	return nil
}

func ensure(ctx context.Context, js jetstream.JetStream) (jetstream.Consumer, error) {
	if _, err := js.CreateOrUpdateStream(ctx, StreamConfig()); err != nil {
		return nil, err
	}
	return js.CreateOrUpdateConsumer(ctx, StreamName, jetstream.ConsumerConfig{Durable: ConsumerName,
		AckPolicy: jetstream.AckExplicitPolicy, AckWait: time.Minute, MaxAckPending: 10 * BatchSize})
}

// batch ingests up to BatchSize records in one transaction, then acknowledges
// them; on failure nothing is acknowledged and the batch is redelivered.
func (i *Ingester) batch(ctx context.Context, cons jetstream.Consumer) error {
	msgs, err := cons.Fetch(BatchSize, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		return err
	}
	batch := make([]jetstream.Msg, 0, BatchSize)
	rows := make([]store.UsageRow, 0, BatchSize)
	oldest := time.Time{}
	for m := range msgs.Messages() {
		row, occurred, err := decode(m.Data())
		if err != nil {
			// A record no ingester can read will not improve: remove it, loudly.
			i.Logger.ErrorContext(ctx, "dropping an unreadable usage record", slog.String("cause", err.Error()))
			if i.Metrics != nil {
				i.Metrics.Counter("nebula_usage_events_dropped_total").WithLabelValues("unreadable").Inc()
			}
			_ = m.Term()
			continue
		}
		batch = append(batch, m)
		rows = append(rows, row)
		if oldest.IsZero() || occurred.Before(oldest) {
			oldest = occurred
		}
	}
	if err := msgs.Error(); err != nil && !errors.Is(err, jetstream.ErrNoMessages) && len(batch) == 0 {
		return err
	}
	if len(rows) == 0 {
		if i.Metrics != nil {
			i.Metrics.Gauge("nebula_usage_ingest_lag_seconds").WithLabelValues().Set(0)
		}
		return nil
	}
	res, err := i.Store.IngestUsage(ctx, rows)
	if err != nil {
		for _, m := range batch {
			_ = m.NakWithDelay(5 * time.Second)
		}
		return err
	}
	for _, m := range batch {
		_ = m.Ack()
	}
	if i.Metrics != nil {
		i.Metrics.Gauge("nebula_usage_ingest_lag_seconds").WithLabelValues().Set(time.Since(oldest).Seconds())
		cost := i.Metrics.Counter("nebula_estimated_cost_micros_total")
		for k, v := range res.CostByDeployment {
			cost.WithLabelValues(k[0], k[1]).Add(float64(v))
		}
	}
	i.Logger.DebugContext(ctx, "usage batch ingested", slog.Int("records", len(rows)), slog.Int("new", res.Inserted),
		slog.Int("rollups", res.Buckets))
	return nil
}

// record is the part of nebula.usage.record.v1 the ingester stores.
type record struct {
	EventID          string    `json:"event_id"`
	OccurredAt       time.Time `json:"occurred_at"`
	TraceID          string    `json:"trace_id"`
	OrgID            string    `json:"org_id"`
	RequestID        string    `json:"request_id"`
	APIKeyID         string    `json:"api_key_id"`
	RouteID          string    `json:"route_id"`
	DeploymentID     string    `json:"deployment_id"`
	ModelVersion     string    `json:"model_version"`
	Variant          string    `json:"variant"`
	Endpoint         string    `json:"endpoint"`
	Priority         string    `json:"priority"`
	Streamed         bool      `json:"streamed"`
	StatusCode       int       `json:"status_code"`
	ErrorClass       string    `json:"error_class"`
	Outcome          string    `json:"outcome"`
	StartedAt        time.Time `json:"started_at"`
	QueueWait        *int64    `json:"queue_wait_ms"`
	TTFT             *int64    `json:"ttft_ms"`
	DurationMS       int64     `json:"duration_ms"`
	ComputeMS        *int64    `json:"compute_ms"`
	Attempts         int       `json:"attempts"`
	PromptTokens     *int64    `json:"prompt_tokens"`
	CompletionTokens *int64    `json:"completion_tokens"`
	SampleRate       float64   `json:"sample_rate"`
}

// requestNamespace derives a stable row id for a request id that is not a UUID,
// so a redelivery of the same record maps to the same row.
var requestNamespace = uuid.MustParse("2f6c0f2a-62b4-4c3e-9d0b-6e6562756c61")

var outcomes = map[string]bool{"completed": true, "client_cancelled": true, "stream_interrupted": true,
	"deadline_exceeded": true, "queue_timeout": true, "rejected": true, "failed": true}

func decode(b []byte) (store.UsageRow, time.Time, error) {
	var r record
	if err := json.Unmarshal(b, &r); err != nil {
		return store.UsageRow{}, time.Time{}, err
	}
	org, err := uuid.Parse(r.OrgID)
	if err != nil {
		return store.UsageRow{}, time.Time{}, errors.New("org_id is not a UUID")
	}
	if r.RequestID == "" {
		return store.UsageRow{}, time.Time{}, errors.New("no request_id")
	}
	id, err := uuid.Parse(r.RequestID)
	if err != nil {
		id = uuid.NewSHA1(requestNamespace, []byte(r.RequestID))
	}
	created := r.StartedAt
	if created.IsZero() {
		created = r.OccurredAt
	}
	if created.IsZero() {
		return store.UsageRow{}, time.Time{}, errors.New("no time")
	}
	row := store.UsageRow{ID: id, OrgID: org, CreatedAt: created.UTC(), ModelVersion: r.ModelVersion,
		Endpoint: r.Endpoint, Priority: strings.ToUpper(r.Priority), StatusCode: r.StatusCode, Outcome: r.Outcome,
		Streamed: r.Streamed, TTFTMS: r.TTFT, DurationMS: max(r.DurationMS, 0), Attempts: max(r.Attempts, 1),
		SampleRate: r.SampleRate}
	row.RouteID, row.DeploymentID, row.APIKeyID = optUUID(r.RouteID), optUUID(r.DeploymentID), optUUID(r.APIKeyID)
	row.Variant, row.ErrorClass, row.TraceID = optString(r.Variant), optString(r.ErrorClass), optString(r.TraceID)
	if row.Endpoint == "" {
		row.Endpoint = "chat.completions"
	}
	if row.Priority != "LOW" && row.Priority != "HIGH" {
		row.Priority = "NORMAL"
	}
	if !outcomes[row.Outcome] {
		row.Outcome = "failed"
	}
	if r.QueueWait != nil {
		row.QueueWaitMS = max(*r.QueueWait, 0)
	}
	if r.ComputeMS != nil {
		row.ComputeMS = max(*r.ComputeMS, 0)
	}
	if r.PromptTokens != nil {
		row.PromptTokens = max(*r.PromptTokens, 0)
	}
	if r.CompletionTokens != nil {
		row.CompletionTokens = max(*r.CompletionTokens, 0)
	}
	if row.SampleRate <= 0 || row.SampleRate > 1 {
		row.SampleRate = 1
	}
	occurred := r.OccurredAt
	if occurred.IsZero() {
		occurred = created
	}
	return row, occurred, nil
}

func optUUID(s string) *uuid.UUID {
	if id, err := uuid.Parse(s); err == nil {
		return &id
	}
	return nil
}

func optString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
