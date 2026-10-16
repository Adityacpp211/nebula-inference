package usage

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
)

// Subject is where usage records are published (docs/events.md §4.1).
const Subject = "nebula.usage.records"

// BufferSize bounds the records held while JetStream cannot take them.
const BufferSize = 10_000

// JetStreamSink publishes every record to the usage stream, with the record's
// event id as the message id so a redelivery within the stream's 2-minute
// duplicate window is dropped by JetStream itself.
//
// Emit never blocks the request path: it hands the record to a bounded buffer.
// While JetStream is unreachable the buffer holds records and the publisher
// retries; a full buffer sheds — counted in nebula_usage_events_dropped_total and
// logged as an error, so lost cost data is visible, never silent. The local-file
// spool of docs/events.md is not implemented: the gateway's root filesystem is
// read-only, and a spool belongs on a volume the chart does not yet give it.
type JetStreamSink struct {
	URL     string
	Logger  *slog.Logger
	Dropped *prometheus.CounterVec // labels: reason

	buf      chan Record
	shedding atomic.Bool
}

// NewJetStreamSink builds a sink; Run must be started for it to publish.
func NewJetStreamSink(url string, logger *slog.Logger, dropped *prometheus.CounterVec) *JetStreamSink {
	return &JetStreamSink{URL: url, Logger: logger, Dropped: dropped, buf: make(chan Record, BufferSize)}
}

// Emit implements Sink.
func (s *JetStreamSink) Emit(ctx context.Context, r Record) {
	complete(&r)
	select {
	case s.buf <- r:
		s.shedding.Store(false)
	default:
		s.drop("buffer_full")
		if !s.shedding.Swap(true) {
			s.Logger.ErrorContext(ctx, "usage buffer full: shedding usage records; cost data for this period is incomplete",
				slog.Int("buffer", BufferSize))
		}
	}
}

func (s *JetStreamSink) drop(reason string) {
	if s.Dropped != nil {
		s.Dropped.WithLabelValues(reason).Inc()
	}
}

// Pending is how many records are waiting to be published.
func (s *JetStreamSink) Pending() int { return len(s.buf) }

// Run publishes until ctx ends, then flushes what it can for a few seconds.
func (s *JetStreamSink) Run(ctx context.Context) error {
	nc, err := nats.Connect(s.URL, nats.Name("nebula-gateway-usage"), nats.MaxReconnects(-1),
		nats.RetryOnFailedConnect(true), nats.ReconnectWait(time.Second))
	if err != nil {
		return err
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	failing := false
	for {
		var r Record
		select {
		case <-ctx.Done():
			s.flush(ctx, js)
			return nil
		case r = <-s.buf:
		}
		for backoff := 100 * time.Millisecond; ; backoff = min(backoff*2, 5*time.Second) {
			err := s.publish(ctx, js, r)
			if err == nil {
				if failing {
					failing = false
					s.Logger.InfoContext(ctx, "usage publishing recovered", slog.Int("pending", len(s.buf)))
				}
				break
			}
			if !failing {
				failing = true
				s.Logger.WarnContext(ctx, "cannot publish usage records; holding them in the buffer",
					slog.String("cause", err.Error()), slog.Int("pending", len(s.buf)))
			}
			select {
			case <-ctx.Done():
				// The record in hand is lost with the process; say so.
				s.drop("shutdown")
				s.flush(ctx, js)
				return nil
			case <-time.After(backoff):
			}
		}
	}
}

func (s *JetStreamSink) publish(ctx context.Context, js jetstream.JetStream, r Record) error {
	b, err := json.Marshal(r)
	if err != nil {
		s.drop("unencodable")
		return nil
	}
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = js.Publish(pctx, Subject, b, jetstream.WithMsgID(r.EventID))
	return err
}

// flush publishes what is buffered for at most a few seconds on shutdown; the
// rest is counted as dropped.
func (s *JetStreamSink) flush(parent context.Context, js jetstream.JetStream) {
	// The parent is already cancelled at shutdown; the flush gets its own bound.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
	defer cancel()
	for {
		select {
		case r := <-s.buf:
			if err := s.publish(ctx, js, r); err != nil {
				s.drop("shutdown")
				n := len(s.buf)
				for range n {
					<-s.buf
					s.drop("shutdown")
				}
				if n > 0 {
					s.Logger.ErrorContext(ctx, "usage records lost at shutdown", slog.Int("records", n+1))
				}
				return
			}
		default:
			return
		}
	}
}

// Tee emits to every sink, completing the record once so every sink sees the
// same event id: the log line and the published record are the same record.
type Tee []Sink

// Emit implements Sink.
func (t Tee) Emit(ctx context.Context, r Record) {
	complete(&r)
	for _, s := range t {
		s.Emit(ctx, r)
	}
}
