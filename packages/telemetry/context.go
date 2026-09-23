package telemetry

import (
	"context"
	"log/slog"

	"github.com/google/uuid"
)

type ctxKey int

const (
	ctxKeyRequestID ctxKey = iota
	ctxKeyTrace
	ctxKeyOrgID
	ctxKeyLogger
)

// NewRequestID mints a request identifier.
//
// UUIDv7 rather than v4: the identifier is time-ordered, so it indexes well as a
// primary key on the high-volume requests table and sorts chronologically in a
// log view (ADR-0023).
func NewRequestID() string {
	id, err := uuid.NewV7()
	if err != nil {
		// NewV7 only fails if the entropy source fails. Falling back to v4 keeps
		// the request traceable, which matters more here than time-ordering.
		return uuid.NewString()
	}
	return id.String()
}

// WithRequestID returns a context carrying the request ID.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKeyRequestID, id)
}

// RequestID returns the request ID, or "" when absent.
func RequestID(ctx context.Context) string {
	v, _ := ctx.Value(ctxKeyRequestID).(string)
	return v
}

// WithTrace returns a context carrying W3C trace context.
func WithTrace(ctx context.Context, tc TraceContext) context.Context {
	return context.WithValue(ctx, ctxKeyTrace, tc)
}

// Trace returns the trace context, and whether one was present.
func Trace(ctx context.Context) (TraceContext, bool) {
	tc, ok := ctx.Value(ctxKeyTrace).(TraceContext)
	return tc, ok
}

// WithOrgID returns a context carrying the tenant. Set by the authentication
// middleware in Phase 2; read here so the log schema is complete from the start.
func WithOrgID(ctx context.Context, orgID string) context.Context {
	return context.WithValue(ctx, ctxKeyOrgID, orgID)
}

// OrgID returns the tenant, or "" when the request is unauthenticated.
func OrgID(ctx context.Context) string {
	v, _ := ctx.Value(ctxKeyOrgID).(string)
	return v
}

// WithLogger stores a logger on the context.
func WithLogger(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, ctxKeyLogger, l)
}

// Logger returns the context's logger enriched with whatever correlation
// identifiers the context carries, falling back to base when none was stored.
//
// This is the function service code should call. It is why every log line in a
// request carries request_id and trace_id without any call site remembering to
// add them (axiom A5).
func Logger(ctx context.Context, base *slog.Logger) *slog.Logger {
	l, ok := ctx.Value(ctxKeyLogger).(*slog.Logger)
	if !ok || l == nil {
		l = base
	}
	if l == nil {
		l = slog.Default()
	}
	var attrs []any
	if id := RequestID(ctx); id != "" {
		attrs = append(attrs, slog.String(FieldRequestID, id))
	}
	if tc, ok := Trace(ctx); ok {
		attrs = append(attrs, slog.String(FieldTraceID, tc.TraceID), slog.String(FieldSpanID, tc.SpanID))
	}
	if org := OrgID(ctx); org != "" {
		attrs = append(attrs, slog.String(FieldOrgID, org))
	}
	if len(attrs) == 0 {
		return l
	}
	return l.With(attrs...)
}
