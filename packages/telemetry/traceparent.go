package telemetry

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// HeaderTraceparent and HeaderTracestate are the W3C Trace Context headers.
const (
	HeaderTraceparent = "traceparent"
	HeaderTracestate  = "tracestate"
)

// TraceContext is a parsed W3C traceparent.
//
// Implemented against the specification with the standard library rather than
// pulling in the OpenTelemetry SDK: Phase 1 needs correlation identifiers, not a
// span pipeline. When the SDK arrives in Phase 8 it consumes and produces the
// same header, so nothing here is wasted or has to change.
type TraceContext struct {
	TraceID    string // 32 lowercase hex characters
	SpanID     string // 16 lowercase hex characters
	Sampled    bool
	TraceState string // opaque, propagated unchanged
}

// ErrInvalidTraceparent is returned for a malformed or unsupported header.
var ErrInvalidTraceparent = errors.New("invalid traceparent")

// ParseTraceparent parses a traceparent header value.
//
// Unknown future versions are rejected rather than guessed at; the caller then
// starts a fresh trace, which the specification permits and which is safer than
// propagating identifiers we did not understand.
func ParseTraceparent(v string) (TraceContext, error) {
	parts := strings.Split(strings.TrimSpace(v), "-")
	if len(parts) != 4 {
		return TraceContext{}, fmt.Errorf("%w: want 4 fields, got %d", ErrInvalidTraceparent, len(parts))
	}
	ver, traceID, spanID, flags := parts[0], parts[1], parts[2], parts[3]

	if ver != "00" {
		return TraceContext{}, fmt.Errorf("%w: unsupported version %q", ErrInvalidTraceparent, ver)
	}
	if !isLowerHex(traceID, 32) {
		return TraceContext{}, fmt.Errorf("%w: trace-id must be 32 lowercase hex characters", ErrInvalidTraceparent)
	}
	if !isLowerHex(spanID, 16) {
		return TraceContext{}, fmt.Errorf("%w: span-id must be 16 lowercase hex characters", ErrInvalidTraceparent)
	}
	if isAllZero(traceID) {
		return TraceContext{}, fmt.Errorf("%w: trace-id must not be all zeros", ErrInvalidTraceparent)
	}
	if isAllZero(spanID) {
		return TraceContext{}, fmt.Errorf("%w: span-id must not be all zeros", ErrInvalidTraceparent)
	}
	if !isLowerHex(flags, 2) {
		return TraceContext{}, fmt.Errorf("%w: trace-flags must be 2 lowercase hex characters", ErrInvalidTraceparent)
	}
	b, err := hex.DecodeString(flags)
	if err != nil {
		return TraceContext{}, fmt.Errorf("%w: trace-flags: %w", ErrInvalidTraceparent, err)
	}

	return TraceContext{TraceID: traceID, SpanID: spanID, Sampled: b[0]&0x01 == 0x01}, nil
}

// NewTraceContext starts a new trace with a fresh trace and span identifier.
func NewTraceContext(sampled bool) (TraceContext, error) {
	traceID, err := randomHex(16)
	if err != nil {
		return TraceContext{}, err
	}
	spanID, err := randomHex(8)
	if err != nil {
		return TraceContext{}, err
	}
	return TraceContext{TraceID: traceID, SpanID: spanID, Sampled: sampled}, nil
}

// Child returns a context in the same trace with a new span identifier, for the
// outbound leg of a call.
func (tc TraceContext) Child() (TraceContext, error) {
	spanID, err := randomHex(8)
	if err != nil {
		return TraceContext{}, err
	}
	tc.SpanID = spanID
	return tc, nil
}

// Header renders the traceparent header value.
func (tc TraceContext) Header() string {
	flags := "00"
	if tc.Sampled {
		flags = "01"
	}
	return "00-" + tc.TraceID + "-" + tc.SpanID + "-" + flags
}

// Valid reports whether the context carries usable identifiers.
func (tc TraceContext) Valid() bool {
	return isLowerHex(tc.TraceID, 32) && isLowerHex(tc.SpanID, 16) &&
		!isAllZero(tc.TraceID) && !isAllZero(tc.SpanID)
}

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generating trace identifier: %w", err)
	}
	return hex.EncodeToString(b), nil
}

func isLowerHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for i := range len(s) {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func isAllZero(s string) bool {
	for i := range len(s) {
		if s[i] != '0' {
			return false
		}
	}
	return s != ""
}
