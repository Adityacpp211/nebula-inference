package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/adityasatwar321/nebula/packages/httpx"
	"github.com/adityasatwar321/nebula/services/gateway/internal/dispatch"
	"github.com/adityasatwar321/nebula/services/gateway/internal/openai"
	"github.com/adityasatwar321/nebula/services/gateway/internal/router"
	"github.com/adityasatwar321/nebula/services/gateway/internal/usage"
)

// stream serves a streamed request: SSE frames relayed from the worker as they
// arrive, never buffered whole (docs/api.md §2).
//
// Frame order, which the tests pin:
//
//	: nebula.meta {...}     routing context as an SSE comment, which every parser
//	                        ignores (ADR-0029); also sent as X-Nebula-* headers
//	data: {role chunk}      chat only: delta {"role":"assistant","content":""}
//	data: {content chunk}…  one per worker token chunk
//	: keep-alive            after KeepAliveInterval of silence
//	data: {finish chunk}    delta {} with finish_reason
//	data: {usage chunk}     only with stream_options.include_usage
//	data: [DONE]
//
// A failure after the first frame cannot change the status line, so it is
// delivered as a terminal error frame followed by [DONE], and no finish_reason is
// ever sent: a client can always tell a completed response from an interrupted one.
//
// Concurrency: one goroutine reads the worker and hands chunks over a small
// buffered channel; the handler goroutine is the only writer to the client. The
// channel is the backpressure path — a client that reads slowly stops the reader,
// which stops reading the worker's socket, which fills TCP buffers back to the
// worker. Nothing grows without bound.
func (g *Gateway) stream(w http.ResponseWriter, r *http.Request, ex *exchange) {
	ctx := r.Context()
	wctx, wcancel := g.workerContext(ctx, ex)
	defer wcancel()

	var st *dispatch.Stream
	var err error
	for {
		st, err = g.d.Workers.Stream(wctx, ex.call)
		if err == nil || ctx.Err() != nil || !g.replace(ctx, ex, err) {
			break
		}
	}
	if err != nil {
		if ctx.Err() != nil {
			ex.record.StatusCode = StatusClientClosedRequest
			ex.record.Outcome = usage.OutcomeClientCancelled
			ex.verdict = router.Abandoned
			return
		}
		ex.verdict = verdictOf(err)
		apiErr := g.workerError(ctx, w, err)
		ex.record.StatusCode = apiErr.Status
		ex.record.ErrorClass = string(apiErr.Type)
		ex.record.Outcome = outcomeOf(apiErr)
		g.fail(w, r, apiErr)
		return
	}
	defer func() { _ = st.Close() }()

	events := make(chan streamItem, 8)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		defer close(events)
		for {
			ch, err := st.Next()
			select {
			case events <- streamItem{ch, err}:
			case <-stop:
				return
			}
			if err != nil {
				return
			}
		}
	}()

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	setRoutingHeaders(h, ex)
	w.WriteHeader(http.StatusOK)
	ex.record.StatusCode = http.StatusOK

	sw := &sseWriter{w: w, rc: http.NewResponseController(w), timeout: g.d.Config.Gateway.StreamWriteTimeout.Duration()}
	created := ex.started.Unix()

	clientGone := func() {
		g.drainAfterDisconnect(ctx, ex, events)
		ex.record.StatusCode = StatusClientClosedRequest
		ex.record.Outcome = usage.OutcomeClientCancelled
		ex.verdict = router.Abandoned
	}

	meta := openai.Meta{
		RequestID:      ex.call.RequestID,
		Route:          ex.route.Model,
		Deployment:     ex.target.Deployment,
		ModelVersion:   ex.target.ModelVersion,
		Variant:        ex.target.Label,
		Attempts:       ex.attempts,
		Degraded:       ex.degraded,
		GatewayQueueMS: gatewayQueueMS(ex),
	}
	if err := sw.metaComment(meta); err != nil {
		clientGone()
		return
	}
	if ex.kind == openai.KindChat {
		empty := ""
		if err := sw.data(ex.chatChunk(created, openai.Delta{Role: "assistant", Content: &empty}, nil)); err != nil {
			clientGone()
			return
		}
	}

	keepAlive := time.NewTimer(g.d.Config.Gateway.KeepAliveInterval.Duration())
	defer keepAlive.Stop()
	resetKeepAlive := func() {
		if !keepAlive.Stop() {
			select {
			case <-keepAlive.C:
			default:
			}
		}
		keepAlive.Reset(g.d.Config.Gateway.KeepAliveInterval.Duration())
	}

	for {
		select {
		case <-ctx.Done():
			clientGone()
			return

		case <-keepAlive.C:
			if err := sw.comment("keep-alive"); err != nil {
				clientGone()
				return
			}
			keepAlive.Reset(g.d.Config.Gateway.KeepAliveInterval.Duration())

		case it, ok := <-events:
			if !ok || it.err != nil {
				err := it.err
				if !ok || errors.Is(err, io.EOF) {
					// [DONE] with no final chunk before it: the worker broke its own
					// contract (obligation 2). Treated as an interruption.
					err = dispatch.ErrStreamTruncated
				}
				g.interrupted(ctx, sw, ex, err)
				return
			}
			ch := it.chunk
			if ch.Content != "" {
				content := ch.Content
				var werr error
				if ex.kind == openai.KindChat {
					werr = sw.data(ex.chatChunk(created, openai.Delta{Content: &content}, nil))
				} else {
					werr = sw.data(ex.textChunk(created, content, nil))
				}
				if werr != nil {
					clientGone()
					return
				}
				resetKeepAlive()
			}
			if !ch.Stop {
				continue
			}

			ex.record.FinishReason = ch.FinishReason
			ex.absorbFinal(ch.Usage, ch.Timing, ch.Runtime)
			switch ch.FinishReason {
			case "stop", "length":
				g.completeStream(sw, ex, created, ch.FinishReason)
			case "deadline":
				ex.record.Outcome = usage.OutcomeDeadlineExceeded
				ex.record.ErrorClass = string(httpx.TypeTimeout)
				_ = sw.data(openai.StreamError{Error: openai.StreamErrorBody{
					Message: "the request did not complete within its timeout", Type: string(httpx.TypeTimeout),
					Code: "deadline_exceeded", RequestID: ex.call.RequestID,
				}})
				_ = sw.done()
			default:
				g.interrupted(ctx, sw, ex, errors.New("worker finished with reason "+ch.FinishReason))
			}
			return
		}
	}
}

// completeStream writes the finish chunk, the optional usage chunk and [DONE].
func (g *Gateway) completeStream(sw *sseWriter, ex *exchange, created int64, reason string) {
	ex.record.Outcome = usage.OutcomeCompleted
	fr := reason
	var err error
	if ex.kind == openai.KindChat {
		err = sw.data(ex.chatChunk(created, openai.Delta{}, &fr))
	} else {
		err = sw.data(ex.textChunk(created, "", &fr))
	}
	if err == nil && ex.req.Usage && ex.record.PromptTokens != nil && ex.record.CompletionTokens != nil {
		u := openai.NewUsage(*ex.record.PromptTokens, *ex.record.CompletionTokens)
		if ex.kind == openai.KindChat {
			err = sw.data(openai.ChatChunk{ID: ex.id, Object: openai.ObjectChatChunk, Created: created,
				Model: ex.route.Model, Choices: []openai.ChunkChoice{}, Usage: u})
		} else {
			err = sw.data(openai.TextCompletion{ID: ex.id, Object: openai.ObjectTextCompletion, Created: created,
				Model: ex.route.Model, Choices: []openai.TextChoice{}, Usage: u})
		}
	}
	if err == nil {
		err = sw.done()
	}
	if err != nil {
		// The client left after the last token but before [DONE]. The generation is
		// complete and fully accounted; only the terminator was lost.
		ex.record.Outcome = usage.OutcomeClientCancelled
		ex.record.StatusCode = StatusClientClosedRequest
	}
}

// interrupted ends a stream that failed after it started: a terminal error frame,
// then [DONE], and no finish_reason.
func (g *Gateway) interrupted(ctx context.Context, sw *sseWriter, ex *exchange, cause error) {
	g.logger(ctx).WarnContext(ctx, "stream interrupted", slog.String("cause", cause.Error()))
	ex.verdict = router.Failed
	ex.record.Outcome = usage.OutcomeStreamInterrupted
	ex.record.ErrorClass = string(httpx.TypeUpstream)
	_ = sw.data(openai.StreamError{Error: openai.StreamErrorBody{
		Message:   "the model runtime stopped before the response was complete",
		Type:      string(httpx.TypeUpstream),
		Code:      "stream_interrupted",
		RequestID: ex.call.RequestID,
	}})
	_ = sw.done()
}

// drainAfterDisconnect runs when the client has gone: tell the worker to stop,
// then keep reading its stream for the final chunk, which carries the runtime's
// count of the tokens already generated. Bounded by CancelDrainTimeout; if the
// final chunk does not arrive the record says the counts are unavailable rather
// than guessing them.
func (g *Gateway) drainAfterDisconnect(ctx context.Context, ex *exchange, events <-chan streamItem) {
	g.cancelWorker(ctx, ex)
	deadline := time.NewTimer(g.d.Config.Gateway.CancelDrainTimeout.Duration())
	defer deadline.Stop()
	for {
		select {
		case it, ok := <-events:
			if !ok || it.err != nil {
				return
			}
			if it.chunk.Stop {
				ex.record.FinishReason = it.chunk.FinishReason
				ex.absorbFinal(it.chunk.Usage, it.chunk.Timing, it.chunk.Runtime)
				return
			}
		case <-deadline.C:
			g.logger(ctx).WarnContext(ctx, "no final frame from the cancelled worker; usage for this request is incomplete")
			return
		}
	}
}

// streamItem is one read from the worker stream, handed from the reader goroutine
// to the writer.
type streamItem struct {
	chunk dispatch.Chunk
	err   error
}

func (ex *exchange) chatChunk(created int64, d openai.Delta, finish *string) openai.ChatChunk {
	return openai.ChatChunk{
		ID: ex.id, Object: openai.ObjectChatChunk, Created: created, Model: ex.route.Model,
		Choices: []openai.ChunkChoice{{Index: 0, Delta: d, FinishReason: finish}},
	}
}

func (ex *exchange) textChunk(created int64, text string, finish *string) openai.TextCompletion {
	return openai.TextCompletion{
		ID: ex.id, Object: openai.ObjectTextCompletion, Created: created, Model: ex.route.Model,
		Choices: []openai.TextChoice{{Index: 0, Text: text, FinishReason: finish}},
	}
}

// sseWriter writes SSE frames with a deadline on every write. A client that stops
// reading hits the deadline, the write fails, and the request is treated as
// disconnected — its worker slot is freed instead of held for as long as the
// client cares to stall (docs/components.md §2.1, "slow client").
type sseWriter struct {
	w       http.ResponseWriter
	rc      *http.ResponseController
	timeout time.Duration
}

func (s *sseWriter) write(b []byte) error {
	_ = s.rc.SetWriteDeadline(time.Now().Add(s.timeout))
	if _, err := s.w.Write(b); err != nil {
		return err
	}
	return s.rc.Flush()
}

func (s *sseWriter) data(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	buf := make([]byte, 0, len(b)+8)
	buf = append(buf, "data: "...)
	buf = append(buf, b...)
	buf = append(buf, "\n\n"...)
	return s.write(buf)
}

// metaComment writes the routing context as an SSE comment line. A comment, not a
// named event: the SSE specification requires every parser to ignore comments,
// while current OpenAI SDKs yield named events as if they were chunks
// (ADR-0029). A human with curl still sees it on the first line.
func (s *sseWriter) metaComment(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return s.write([]byte(": nebula.meta " + string(b) + "\n\n"))
}

func (s *sseWriter) comment(text string) error { return s.write([]byte(": " + text + "\n\n")) }

func (s *sseWriter) done() error { return s.write([]byte("data: [DONE]\n\n")) }
