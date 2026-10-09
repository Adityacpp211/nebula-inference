package httpx

import (
	"errors"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"slices"
	"strings"
	"time"

	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/packages/version"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// Middleware wraps a handler.
type Middleware func(http.Handler) http.Handler

// Chain composes middleware so that the first argument is the outermost wrapper.
// Reading order therefore matches execution order.
func Chain(mw ...Middleware) Middleware {
	return func(next http.Handler) http.Handler {
		for i := len(mw) - 1; i >= 0; i-- {
			next = mw[i](next)
		}
		return next
	}
}

// maxRequestIDLen bounds a client-supplied request ID.
const maxRequestIDLen = 64

// RequestID adopts a client-supplied X-Request-Id or mints a UUIDv7, puts it on
// the context, and echoes it on the response.
//
// A client-supplied value is validated before use: it reaches log lines and
// response headers, so an unvalidated one is a log-injection and
// header-injection vector.
func RequestID() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := r.Header.Get(HeaderRequestID)
			if !validRequestID(id) {
				id = telemetry.NewRequestID()
			}
			ctx := telemetry.WithRequestID(r.Context(), id)
			w.Header().Set(HeaderRequestID, id)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// validRequestID accepts a conservative token: printable ASCII limited to
// characters that cannot break a header or a JSON log line.
func validRequestID(s string) bool {
	if s == "" || len(s) > maxRequestIDLen {
		return false
	}
	for i := range len(s) {
		c := s[i]
		ok := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '_' || c == '.'
		if !ok {
			return false
		}
	}
	return true
}

// Trace continues an incoming W3C trace or starts a new one, opens the service's
// root span (spanName, e.g. "gateway.request"), and puts the trace context on the
// request so every log line carries trace_id and span_id. The span is closed on
// every path, with the response status, and marked as an error for a 5xx.
func Trace(spanName string) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tc, err := telemetry.ParseTraceparent(r.Header.Get(telemetry.HeaderTraceparent))
			if err != nil || !tc.Valid() {
				// An absent or unparseable header starts a fresh trace rather than
				// propagating identifiers we did not understand.
				tc, err = telemetry.NewTraceContext(true)
				if err != nil {
					next.ServeHTTP(w, r) // correlation is best-effort, never fatal
					return
				}
			} else {
				tc.TraceState = r.Header.Get(telemetry.HeaderTracestate)
			}
			ctx, span, tc := telemetry.StartRequestSpan(r.Context(), spanName, tc)
			defer span.End()
			span.SetAttributes(attribute.String("http.request.method", r.Method),
				attribute.String("url.path", r.URL.Path))
			if id := telemetry.RequestID(ctx); id != "" {
				span.SetAttributes(attribute.String("request_id", id))
			}
			rec := &statusRecorder{ResponseWriter: w}
			next.ServeHTTP(rec, r.WithContext(telemetry.WithTrace(ctx, tc)))
			span.SetAttributes(attribute.Int("http.response.status_code", rec.status))
			if rec.status >= 500 {
				span.SetStatus(codes.Error, http.StatusText(rec.status))
			}
		})
	}
}

// WithLogger stores the base logger on the request context so handlers can pick
// up a correlated logger with telemetry.Logger(ctx, nil).
func WithLogger(base *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := telemetry.WithLogger(r.Context(), base)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// APIVersion advertises the contract version on every response, so a client can
// detect skew without a separate call.
func APIVersion() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(HeaderAPIVersion, version.Contract)
			next.ServeHTTP(w, r)
		})
	}
}

// MaxBody caps the request body size. Without this, a large body is an
// unbounded allocation driven by the caller.
func MaxBody(limit int64) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if limit > 0 && r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, limit)
			}
			next.ServeHTTP(w, r)
		})
	}
}

// Recover converts a panic into a 500 with the standard envelope, logs the
// stack, and keeps the process alive.
//
// A panic in one request must not take down a replica serving others; this is the
// only place in the codebase that recovers (axiom: no panic outside main).
func Recover(logger *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// contextcheck cannot see that the deferred closure acts on the same
			// request it was created for: there is no other context available inside a
			// recover, and passing one in would be passing r.Context() to itself.
			defer func() { //nolint:contextcheck // recovers for this request only
				rec := recover()
				if rec == nil {
					return
				}
				// http.ErrAbortHandler is the documented way to abort a response;
				// re-panic so the server handles it as intended.
				// errors.Is rather than ==: a handler may legitimately wrap it, and a
				// wrapped ErrAbortHandler that is not re-panicked becomes a 500 with a
				// stack trace instead of the silent abort the caller asked for.
				if err, isErr := rec.(error); isErr && errors.Is(err, http.ErrAbortHandler) {
					panic(rec)
				}
				if logger != nil {
					telemetry.Logger(r.Context(), logger).Error("panic recovered in HTTP handler",
						slog.Any("panic", rec),
						slog.String("stack", string(debug.Stack())),
						slog.String(telemetry.FieldHTTPMethod, r.Method),
						slog.String(telemetry.FieldHTTPPath, r.URL.Path),
					)
				}
				WriteError(w, r, ErrInternal(nil), nil) // already logged above
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// statusRecorder captures the status code and bytes written for the access log.
type statusRecorder struct {
	http.ResponseWriter
	status  int
	written int64
}

// WriteHeader records the first status written and passes it through. First only:
// a handler that writes a header twice has a bug, and the access log should report
// what the client actually received.
func (s *statusRecorder) WriteHeader(code int) {
	if s.status == 0 {
		s.status = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	n, err := s.ResponseWriter.Write(b)
	s.written += int64(n)
	return n, err
}

// Flush forwards to the underlying writer when it supports flushing, so SSE
// still streams through the middleware chain (needed from Phase 4 onward).
func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap exposes the underlying writer to http.ResponseController, so a streaming
// handler can set per-write deadlines through the middleware chain. Without it the
// controller stops at this wrapper and SetWriteDeadline reports "not supported".
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// AccessLog logs one structured line per request after it completes.
//
// skipPaths suppresses probe endpoints: kubelet polls them every few seconds and
// the resulting volume would bury real events while costing Loki storage.
func AccessLog(logger *slog.Logger, skipPaths ...string) Middleware {
	skip := slices.Clone(skipPaths)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if slices.Contains(skip, r.URL.Path) {
				next.ServeHTTP(w, r)
				return
			}
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w}
			next.ServeHTTP(rec, r)

			if rec.status == 0 {
				rec.status = http.StatusOK
			}
			l := telemetry.Logger(r.Context(), logger)
			attrs := []any{
				slog.String(telemetry.FieldHTTPMethod, r.Method),
				slog.String(telemetry.FieldHTTPPath, r.URL.Path),
				slog.Int(telemetry.FieldHTTPStatus, rec.status),
				slog.Int64(telemetry.FieldDurationMS, time.Since(start).Milliseconds()),
				slog.Int64(telemetry.FieldBytesWritten, rec.written),
				slog.String(telemetry.FieldRemoteIP, clientIP(r)),
			}
			switch {
			case rec.status >= 500:
				l.Error("request failed", attrs...)
			case rec.status >= 400:
				l.Warn("request rejected", attrs...)
			default:
				l.Info("request completed", attrs...)
			}
		})
	}
}

// clientIP returns the peer address. It deliberately does NOT trust
// X-Forwarded-For: that header is caller-controlled, and trusting it without a
// configured trusted-proxy list turns rate limiting and audit logs into fiction.
// Proxy-aware resolution arrives with the gateway in Phase 4, where the trusted
// hop is known.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return strings.TrimSpace(r.RemoteAddr)
	}
	return host
}
