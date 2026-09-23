// Package httpx holds the HTTP primitives shared by every NEBULA service: the
// error envelope, the middleware chain, and a server with correct drain
// semantics.
//
// It is a package rather than per-service code because the error envelope and the
// request-ID contract are part of the public API surface (docs/api.md §1), and a
// second implementation would eventually disagree with the first.
package httpx

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/adityasatwar321/nebula/packages/telemetry"
)

// ErrorType is the stable machine-readable error category returned to clients.
// The set is closed and OpenAI-compatible so existing SDK error handling works.
type ErrorType string

// Error types. See docs/api.md §1 for the HTTP status each maps to.
const (
	TypeInvalidRequest     ErrorType = "invalid_request_error"
	TypeAuthentication     ErrorType = "authentication_error"
	TypePermission         ErrorType = "permission_error"
	TypeNotFound           ErrorType = "not_found_error"
	TypeConflict           ErrorType = "conflict_error"
	TypeUnprocessable      ErrorType = "unprocessable_entity"
	TypeRateLimit          ErrorType = "rate_limit_error"
	TypeInternal           ErrorType = "internal_error"
	TypeUpstream           ErrorType = "upstream_error"
	TypeServiceUnavailable ErrorType = "service_unavailable"
	TypeTimeout            ErrorType = "timeout_error"
)

// HeaderRequestID is echoed on every response, including errors.
const (
	HeaderRequestID  = "X-Request-Id"
	HeaderReason     = "X-Nebula-Reason"
	HeaderAPIVersion = "X-Nebula-Api-Version"
)

// APIError is a client-facing error.
//
// It carries only what is safe to expose: no pod names, no node names, no SQL
// detail, no upstream response bodies. Internal detail belongs in logs and
// traces, which are operator-only (docs/security-boundaries.md §2, B1).
type APIError struct {
	// Status is the HTTP status code.
	Status int `json:"-"`
	// Message is human-readable and safe to show a caller.
	Message string `json:"message"`
	// Type is the stable category.
	Type ErrorType `json:"type"`
	// Code is a stable, specific identifier, e.g. "context_length_exceeded".
	Code string `json:"code,omitempty"`
	// Param names the offending request field, when there is one.
	Param string `json:"param,omitempty"`
	// RequestID correlates the error with logs and traces.
	RequestID string `json:"request_id,omitempty"`
	// Reason populates X-Nebula-Reason for a closed set of operational causes.
	Reason string `json:"-"`
	// internal is logged, never serialised.
	internal error `json:"-"`
}

func (e *APIError) Error() string {
	if e.internal != nil {
		return string(e.Type) + ": " + e.Message + ": " + e.internal.Error()
	}
	return string(e.Type) + ": " + e.Message
}

// Unwrap exposes the internal cause to errors.Is/As without exposing it to
// clients.
func (e *APIError) Unwrap() error { return e.internal }

// WithInternal attaches a cause for logging.
func (e *APIError) WithInternal(err error) *APIError {
	e.internal = err
	return e
}

// errorEnvelope is the wire shape: {"error": {...}}.
type errorEnvelope struct {
	Error *APIError `json:"error"`
}

// Constructors for the errors Phase 1 can actually produce. More are added by the
// phases that introduce them, rather than speculatively now.

// ErrInvalidRequest reports a malformed or semantically invalid request.
func ErrInvalidRequest(message, code, param string) *APIError {
	return &APIError{Status: http.StatusBadRequest, Message: message, Type: TypeInvalidRequest, Code: code, Param: param}
}

// ErrNotFound reports a missing resource. Also used for resources in another
// tenant, so existence is not disclosed (docs/security-boundaries.md §4.2).
func ErrNotFound(message string) *APIError {
	return &APIError{Status: http.StatusNotFound, Message: message, Type: TypeNotFound}
}

// ErrMethodNotAllowed reports an unsupported method.
func ErrMethodNotAllowed(message string) *APIError {
	return &APIError{Status: http.StatusMethodNotAllowed, Message: message, Type: TypeInvalidRequest, Code: "method_not_allowed"}
}

// ErrInternal reports an unexpected server-side failure. The message is
// deliberately generic; the cause goes to the logs.
func ErrInternal(cause error) *APIError {
	return &APIError{
		Status:   http.StatusInternalServerError,
		Message:  "an internal error occurred; quote the request_id when reporting it",
		Type:     TypeInternal,
		internal: cause,
	}
}

// ErrServiceUnavailable reports a dependency-driven inability to serve.
func ErrServiceUnavailable(message, reason string) *APIError {
	return &APIError{Status: http.StatusServiceUnavailable, Message: message, Type: TypeServiceUnavailable, Reason: reason}
}

// ErrPayloadTooLarge reports a body over the configured limit.
func ErrPayloadTooLarge(limit int64) *APIError {
	return &APIError{
		Status:  http.StatusRequestEntityTooLarge,
		Message: "request body exceeds the configured limit",
		Type:    TypeInvalidRequest,
		Code:    "payload_too_large",
		Param:   "body",
	}
}

// WriteError renders an error in the standard envelope and logs the internal
// cause. Any non-APIError is treated as an internal error, so a raw error can
// never leak its text to a client.
func WriteError(w http.ResponseWriter, r *http.Request, err error, logger *slog.Logger) {
	apiErr, ok := err.(*APIError)
	if !ok {
		apiErr = ErrInternal(err)
	}
	apiErr.RequestID = telemetry.RequestID(r.Context())

	if logger != nil {
		l := telemetry.Logger(r.Context(), logger)
		attrs := []any{
			slog.String(telemetry.FieldErrorClass, string(apiErr.Type)),
			slog.Int(telemetry.FieldHTTPStatus, apiErr.Status),
		}
		if apiErr.Code != "" {
			attrs = append(attrs, slog.String("error_code", apiErr.Code))
		}
		if apiErr.internal != nil {
			attrs = append(attrs, slog.String("cause", apiErr.internal.Error()))
		}
		if apiErr.Status >= 500 {
			l.Error(apiErr.Message, attrs...)
		} else {
			l.Warn(apiErr.Message, attrs...)
		}
	}

	if apiErr.Reason != "" {
		w.Header().Set(HeaderReason, apiErr.Reason)
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(apiErr.Status)
	// A failed write here cannot be reported to the client; the access log
	// already records the status, so it is deliberately ignored.
	_ = json.NewEncoder(w).Encode(errorEnvelope{Error: apiErr})
}

// WriteJSON renders a successful JSON response.
func WriteJSON(w http.ResponseWriter, status int, v any) error {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(v)
}
