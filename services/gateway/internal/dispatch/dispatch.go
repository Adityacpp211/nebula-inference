// Package dispatch is the gateway's client for the internal worker API
// (docs/api.md §6).
//
// It speaks the protocol and nothing more: it sets the five required headers, maps
// the worker's error envelope into a typed error, and turns an SSE body into a
// sequence of chunks. Deciding what an error means for the caller is the
// handler's job, and deciding whether to retry is Phase 10's reliability package.
//
// What never crosses this boundary is as important as what does
// (docs/security-boundaries.md §2, B3): no API key, no org, no user, no scope. The
// Call type has no field that could carry them.
package dispatch

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Header names of the worker protocol.
const (
	HeaderRequestID    = "X-Request-Id"
	HeaderTraceparent  = "traceparent"
	HeaderDeadline     = "X-Nebula-Deadline"
	HeaderPriority     = "X-Nebula-Priority"
	HeaderModelVersion = "X-Nebula-Model-Version"
	HeaderProtocol     = "X-Nebula-Worker-Protocol"
	HeaderReason       = "X-Nebula-Reason"

	// ProtocolVersion is the worker protocol this gateway speaks.
	ProtocolVersion = "1"
)

// Body is the generation request body. It mirrors the worker's GenerateBody, which
// refuses unknown fields, so a misspelt field here fails loudly in tests.
type Body struct {
	Prompt      string         `json:"prompt"`
	MaxTokens   int            `json:"max_tokens"`
	Temperature float64        `json:"temperature"`
	TopP        float64        `json:"top_p"`
	Stop        []string       `json:"stop,omitempty"`
	Seed        *int64         `json:"seed,omitempty"`
	Extra       map[string]any `json:"extra,omitempty"`
}

// Call is one dispatch: where, with which context, and what to generate.
type Call struct {
	Endpoint     string
	RequestID    string
	Traceparent  string
	Deadline     time.Time
	Priority     string
	ModelVersion string
	Body         Body
}

// Usage is the runtime's own token count.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
}

// Timing is the worker's measurement of one generation.
type Timing struct {
	QueueMS   *int64 `json:"queue_ms"`
	PrefillMS *int64 `json:"prefill_ms"`
	DecodeMS  *int64 `json:"decode_ms"`
	TTFTMS    *int64 `json:"ttft_ms"`
	TotalMS   *int64 `json:"total_ms"`
}

// Runtime identifies the engine that served a request.
type Runtime struct {
	Name         string `json:"name"`
	Version      string `json:"version"`
	ModelVersion string `json:"model_version"`
	Slot         *int   `json:"slot"`
}

// Result is a completed non-streamed generation.
type Result struct {
	Text         string   `json:"text"`
	FinishReason string   `json:"finish_reason"`
	Usage        *Usage   `json:"usage"`
	Timing       *Timing  `json:"timing"`
	Runtime      *Runtime `json:"runtime"`
}

// Chunk is one event of a worker stream. The final chunk has Stop set and carries
// FinishReason and Usage (docs/api.md §7, obligation 2).
type Chunk struct {
	Content      string   `json:"content"`
	Index        int      `json:"index"`
	Stop         bool     `json:"stop"`
	FinishReason string   `json:"finish_reason,omitempty"`
	Usage        *Usage   `json:"usage,omitempty"`
	Timing       *Timing  `json:"timing,omitempty"`
	Runtime      *Runtime `json:"runtime,omitempty"`
}

// Error is a failure reported by, or on the way to, a worker.
type Error struct {
	// Status is the worker's HTTP status, or 0 when no response arrived.
	Status int
	// Type and Code come from the worker's error envelope.
	Type, Code, Message string
	// Reason is the worker's X-Nebula-Reason, e.g. worker_saturated.
	Reason string
	// RetryAfter is the worker's Retry-After, when it sent one.
	RetryAfter time.Duration
	// Transport is set when the request never got an HTTP response: refused,
	// reset, timed out dialling. These are the classically retryable failures.
	Transport error
	// MidStream is set when the failure arrived as an error event after the stream
	// had started, which is never retryable (docs/architecture.md §6.4).
	MidStream bool
}

func (e *Error) Error() string {
	if e.Transport != nil {
		return "worker unreachable: " + e.Transport.Error()
	}
	return fmt.Sprintf("worker answered %d %s/%s: %s", e.Status, e.Type, e.Code, e.Message)
}

func (e *Error) Unwrap() error { return e.Transport }

// Unreachable reports a failure to connect at all: the request never reached the
// worker, so nothing was generated and nothing was charged.
func (e *Error) Unreachable() bool {
	var op *net.OpError
	return e.Transport != nil && errors.As(e.Transport, &op) && op.Op == "dial"
}

// BeforeWork reports a failure that happened before the worker started
// generating: it could not be reached, or it declined (saturated, draining,
// loading). Such a request can be placed on another replica without any risk of
// generating twice, which is the only kind of retry the router makes; general
// retries are Phase 10's.
func (e *Error) BeforeWork() bool {
	if e.MidStream {
		return false
	}
	return e.Unreachable() || e.Status == http.StatusServiceUnavailable || e.Status == http.StatusTooManyRequests
}

// ErrStreamTruncated means the stream ended without a final chunk or [DONE]: the
// worker died or the connection was cut mid-generation.
var ErrStreamTruncated = errors.New("worker stream ended without a final chunk")

// Client dispatches to workers.
type Client struct {
	http *http.Client
}

// Options configures the client.
type Options struct {
	// ConnectTimeout is the first of the three timeout layers
	// (docs/architecture.md §6.4). The other two, TTFT and total, are the request
	// deadline, which the worker also enforces.
	ConnectTimeout time.Duration
	// MaxConnsPerEndpoint bounds sockets to one worker.
	MaxConnsPerEndpoint int
}

// New builds a client with its own transport. Its own, not http.DefaultTransport,
// so worker connection pooling is tuned for many long-lived streams to few hosts
// and is not shared with anything else in the process.
func New(opts Options) *Client {
	if opts.ConnectTimeout <= 0 {
		opts.ConnectTimeout = 2 * time.Second
	}
	if opts.MaxConnsPerEndpoint <= 0 {
		opts.MaxConnsPerEndpoint = 512
	}
	tr := &http.Transport{
		Proxy:                 nil, // worker traffic never goes through an egress proxy
		DialContext:           (&net.Dialer{Timeout: opts.ConnectTimeout, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          1024,
		MaxIdleConnsPerHost:   opts.MaxConnsPerEndpoint,
		MaxConnsPerHost:       opts.MaxConnsPerEndpoint,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 0, // the deadline governs; a queued request legitimately waits
		ExpectContinueTimeout: 0,
		DisableCompression:    true, // compressing an SSE stream defeats incremental delivery
		ForceAttemptHTTP2:     false,
	}
	return &Client{http: &http.Client{Transport: tr}}
}

// NewWithHTTPClient wraps an existing client, for tests.
func NewWithHTTPClient(c *http.Client) *Client { return &Client{http: c} }

func (c *Client) request(ctx context.Context, call Call, path string) (*http.Request, error) {
	body, err := json.Marshal(call.Body)
	if err != nil {
		return nil, fmt.Errorf("encoding worker request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, call.Endpoint+path, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("building worker request: %w", err)
	}
	h := req.Header
	h.Set("Content-Type", "application/json")
	h.Set(HeaderRequestID, call.RequestID)
	h.Set(HeaderTraceparent, call.Traceparent)
	h.Set(HeaderDeadline, strconv.FormatInt(call.Deadline.UnixMilli(), 10))
	h.Set(HeaderPriority, call.Priority)
	h.Set(HeaderModelVersion, call.ModelVersion)
	h.Set(HeaderProtocol, ProtocolVersion)
	return req, nil
}

// Generate performs a non-streamed generation.
func (c *Client) Generate(ctx context.Context, call Call) (*Result, error) {
	req, err := c.request(ctx, call, "/internal/v1/generate")
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req) //nolint:bodyclose // closed by the deferred drainClose
	if err != nil {
		return nil, &Error{Transport: err}
	}
	defer drainClose(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, readError(resp)
	}
	var out Result
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResultBytes)).Decode(&out); err != nil {
		return nil, &Error{Status: resp.StatusCode, Type: "upstream_error", Code: "invalid_worker_response",
			Message: "the worker's response could not be decoded: " + err.Error()}
	}
	return &out, nil
}

// maxResultBytes bounds a non-streamed response. Generous — a long completion is
// megabytes at most — but finite, so a misbehaving worker cannot make the gateway
// allocate without limit.
const maxResultBytes = 64 << 20

// Stream opens a streamed generation. The status line has been read when it
// returns, so a worker that refuses the request (429, 409, 503) is reported here as
// an *Error with no bytes sent to the client yet.
func (c *Client) Stream(ctx context.Context, call Call) (*Stream, error) {
	req, err := c.request(ctx, call, "/internal/v1/generate/stream")
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/event-stream")
	// The body is handed to the Stream on success, which closes it in Close.
	resp, err := c.http.Do(req) //nolint:bodyclose // owned by the returned Stream
	if err != nil {
		return nil, &Error{Transport: err}
	}
	if resp.StatusCode != http.StatusOK {
		defer drainClose(resp.Body)
		return nil, readError(resp)
	}
	return &Stream{body: resp.Body, reader: bufio.NewReaderSize(resp.Body, 16<<10)}, nil
}

// Cancel asks a worker to stop a request. It is best-effort by design: the
// authoritative cancel is closing the stream, which the worker also watches. This
// call exists so the worker stops computing even while the gateway keeps reading
// the stream for its final usage frame.
func (c *Client) Cancel(ctx context.Context, endpoint, requestID, traceparent string) error {
	body, _ := json.Marshal(map[string]string{"request_id": requestID})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/internal/v1/cancel", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(HeaderRequestID, requestID)
	req.Header.Set(HeaderTraceparent, traceparent)
	req.Header.Set(HeaderProtocol, ProtocolVersion)
	resp, err := c.http.Do(req) //nolint:bodyclose // closed by drainClose below
	if err != nil {
		return err
	}
	drainClose(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("worker cancel answered %d", resp.StatusCode)
	}
	return nil
}

// Stream reads a worker SSE stream.
type Stream struct {
	body   io.ReadCloser
	reader *bufio.Reader
	// done is set by the reader goroutine at [DONE] and read by Close on the
	// handler goroutine, hence atomic.
	done atomic.Bool
}

// maxEventBytes bounds one SSE event. A single token chunk is tiny; the final
// chunk with usage and timing is under a kilobyte.
const maxEventBytes = 1 << 20

// Next returns the next chunk. It returns io.EOF after the terminal [DONE];
// ErrStreamTruncated if the body ends before a final chunk; and an *Error with
// MidStream set for an error event.
func (s *Stream) Next() (Chunk, error) {
	if s.done.Load() {
		return Chunk{}, io.EOF
	}
	for {
		data, err := s.readEvent()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return Chunk{}, ErrStreamTruncated
			}
			return Chunk{}, err
		}
		if data == nil {
			continue // comment or empty event
		}
		if string(data) == "[DONE]" {
			s.done.Store(true)
			return Chunk{}, io.EOF
		}
		// An error event is {"error": {...}}; a chunk never has that key.
		var probe struct {
			Error *struct {
				Message string `json:"message"`
				Type    string `json:"type"`
				Code    string `json:"code"`
			} `json:"error"`
		}
		if err := json.Unmarshal(data, &probe); err == nil && probe.Error != nil {
			return Chunk{}, &Error{Status: http.StatusOK, Type: probe.Error.Type, Code: probe.Error.Code,
				Message: probe.Error.Message, MidStream: true}
		}
		var ch Chunk
		if err := json.Unmarshal(data, &ch); err != nil {
			return Chunk{}, &Error{Status: http.StatusOK, Type: "upstream_error", Code: "invalid_worker_event",
				Message: "undecodable stream event: " + err.Error(), MidStream: true}
		}
		return ch, nil
	}
}

// readEvent reads one SSE event and returns its data, or nil for an event with no
// data lines (a comment keep-alive).
func (s *Stream) readEvent() ([]byte, error) {
	var data []byte
	total := 0
	for {
		line, err := s.reader.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			return nil, &Error{Status: http.StatusOK, Type: "upstream_error", Code: "worker_event_too_large",
				Message: "a stream event line exceeded the buffer", MidStream: true}
		}
		if err != nil {
			if len(line) == 0 {
				return nil, err
			}
			// A final line without a newline is still a line; the error surfaces on
			// the next read.
		}
		total += len(line)
		if total > maxEventBytes {
			return nil, &Error{Status: http.StatusOK, Type: "upstream_error", Code: "worker_event_too_large",
				Message: "a stream event exceeded 1 MiB", MidStream: true}
		}
		trimmed := strings.TrimRight(string(line), "\r\n")
		if trimmed == "" {
			return data, nil // end of event
		}
		if strings.HasPrefix(trimmed, ":") {
			continue
		}
		field, value, _ := strings.Cut(trimmed, ":")
		value = strings.TrimPrefix(value, " ")
		if field == "data" {
			if data != nil {
				data = append(data, '\n')
			}
			data = append(data, value...)
		}
		if err != nil {
			// A last line without a newline is still a line; the read error
			// surfaces on the next call, which has nothing left to return.
			return data, nil //nolint:nilerr // see above
		}
	}
}

// Close releases the connection. Closing before [DONE] aborts the request at the
// worker, which is how a stream is cancelled for certain.
func (s *Stream) Close() error {
	if s.done.Load() {
		drainClose(s.body)
		return nil
	}
	return s.body.Close()
}

// readError decodes a worker's error envelope.
func readError(resp *http.Response) *Error {
	e := &Error{Status: resp.StatusCode, Reason: resp.Header.Get(HeaderReason)}
	if ra := resp.Header.Get("Retry-After"); ra != "" {
		if secs, err := strconv.Atoi(ra); err == nil && secs >= 0 {
			e.RetryAfter = time.Duration(secs) * time.Second
		}
	}
	var env struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    string `json:"code"`
		} `json:"error"`
		// FastAPI's own validation errors use "detail"; a worker answering 422 means
		// the gateway sent something the protocol does not allow.
		Detail json.RawMessage `json:"detail"`
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err := json.Unmarshal(b, &env); err == nil {
		e.Type, e.Code, e.Message = env.Error.Type, env.Error.Code, env.Error.Message
		if e.Message == "" && len(env.Detail) > 0 {
			e.Code, e.Message = "worker_rejected_request", string(env.Detail)
		}
	}
	if e.Message == "" {
		e.Message = http.StatusText(resp.StatusCode)
	}
	return e
}

func drainClose(rc io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(rc, 64<<10))
	_ = rc.Close()
}
