// Package telemetry provides the logging, correlation and health-probe
// primitives every NEBULA service shares.
//
// It exists so that "every service must have structured logging and a health
// endpoint" is satisfied by construction rather than by each service
// reimplementing it slightly differently (docs/roadmap.md Phase 1).
//
// Metrics and OpenTelemetry tracing land in Phase 8; the log schema here already
// carries trace_id and span_id so correlation works the moment tracing arrives.
package telemetry

import (
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/adityasatwar321/nebula/packages/config"
	"github.com/adityasatwar321/nebula/packages/version"
)

// Mandatory log fields. Named constants because dashboards, Loki queries and
// tests all depend on the exact spelling (docs/observability.md §4.1).
const (
	FieldTime         = "ts"
	FieldLevel        = "level"
	FieldMessage      = "msg"
	FieldService      = "service"
	FieldVersion      = "version"
	FieldInstance     = "instance"
	FieldRequestID    = "request_id"
	FieldTraceID      = "trace_id"
	FieldSpanID       = "span_id"
	FieldOrgID        = "org_id"
	FieldErrorClass   = "error_class"
	FieldDurationMS   = "duration_ms"
	FieldHTTPMethod   = "http_method"
	FieldHTTPPath     = "http_path"
	FieldHTTPStatus   = "http_status"
	FieldBytesWritten = "bytes_written"
	FieldRemoteIP     = "remote_ip"
)

// NewLogger builds the service logger.
//
// JSON is the only production format; text exists for local development and is
// rejected by config validation when NEBULA_ENV=production.
func NewLogger(w io.Writer, cfg config.LogConfig, info version.Info, instance string) (*slog.Logger, error) {
	level, err := parseLevel(cfg.Level)
	if err != nil {
		return nil, err
	}

	opts := &slog.HandlerOptions{
		Level:       level,
		AddSource:   cfg.AddSource,
		ReplaceAttr: replaceAttr,
	}

	var h slog.Handler
	switch strings.ToLower(cfg.Format) {
	case "json":
		h = slog.NewJSONHandler(w, opts)
	case "text":
		h = slog.NewTextHandler(w, opts)
	default:
		return nil, fmt.Errorf("unknown log format %q (want json or text)", cfg.Format)
	}

	// Base attributes appear on every line, which is what makes a Loki query
	// scoped to one service and one build possible.
	return slog.New(h).With(
		slog.String(FieldService, info.Service),
		slog.String(FieldVersion, info.Version),
		slog.String(FieldInstance, instance),
	), nil
}

func parseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info", "":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unknown log level %q (want debug, info, warn or error)", s)
	}
}

// replaceAttr normalises slog's built-in keys to the documented schema: "time"
// becomes "ts", and levels are lowercase so queries do not have to case-fold.
func replaceAttr(groups []string, a slog.Attr) slog.Attr {
	if len(groups) > 0 {
		return a
	}
	switch a.Key {
	case slog.TimeKey:
		a.Key = FieldTime
	case slog.LevelKey:
		if lv, ok := a.Value.Any().(slog.Level); ok {
			a.Value = slog.StringValue(strings.ToLower(lv.String()))
		}
	}
	return a
}

// Discard returns a logger that throws everything away. For tests that assert on
// behaviour rather than output.
func Discard() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}
