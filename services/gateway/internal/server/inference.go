package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/adityasatwar321/nebula/packages/auth"
	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/packages/httpx"
	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/services/gateway/internal/dispatch"
	"github.com/adityasatwar321/nebula/services/gateway/internal/openai"
	"github.com/adityasatwar321/nebula/services/gateway/internal/ratelimit"
	"github.com/adityasatwar321/nebula/services/gateway/internal/router"
	"github.com/adityasatwar321/nebula/services/gateway/internal/routes"
	"github.com/adityasatwar321/nebula/services/gateway/internal/usage"
)

// StatusClientClosedRequest is nginx's 499, used for a client that went away.
// Logged, never sent (there is nobody to send it to), and not an SLO violation.
const StatusClientClosedRequest = 499

// exchange is one inference request as it moves through the stages.
type exchange struct {
	kind    openai.Kind
	req     *openai.Request
	route   *routes.Route
	target  *routes.Target
	call    dispatch.Call
	lease   *ratelimit.Lease
	reserve int
	started time.Time
	ident   auth.Identity
	id      string // response id: chatcmpl-<request id> or cmpl-<request id>
	timing  *dispatch.Timing

	// sel is the endpoint of the current attempt; placement is what selected it.
	sel       *router.Selection
	placement router.Request
	attempts  int
	// verdict is what this request says about the endpoint's health, reported to
	// the router when the request finishes.
	verdict router.Outcome
	// degraded names a degradation the response carries (a failover route).
	degraded string

	// record is filled as the request progresses and emitted exactly once.
	record usage.Record
}

func (g *Gateway) chatCompletions(w http.ResponseWriter, r *http.Request) {
	g.serve(w, r, openai.KindChat)
}

func (g *Gateway) completions(w http.ResponseWriter, r *http.Request) {
	g.serve(w, r, openai.KindCompletion)
}

// serve runs stages VALIDATE through FINALIZE for one request.
func (g *Gateway) serve(w http.ResponseWriter, r *http.Request, kind openai.Kind) {
	ex, err := g.prepare(w, r, kind)
	if err != nil {
		g.fail(w, r, err)
		return
	}
	defer g.finish(r.Context(), ex)

	if ex.req.Stream {
		g.stream(w, r, ex)
		return
	}
	g.generate(w, r, ex)
}

// prepare validates, resolves, admits and builds the worker call. Nothing is
// charged to the caller's limits until every check that can fail without cost has
// passed.
func (g *Gateway) prepare(w http.ResponseWriter, r *http.Request, kind openai.Kind) (*exchange, error) {
	ctx := r.Context()
	started := g.d.Now()
	ident := auth.MustFromContext(ctx)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			return nil, httpx.ErrPayloadTooLarge(tooBig.Limit)
		}
		return nil, httpx.ErrInvalidRequest("the request body could not be read", "unreadable_body", "")
	}
	req, err := openai.Parse(kind, body)
	if err != nil {
		return nil, err
	}

	// RESOLVE. A route in another org is reported exactly as a missing one.
	route, ok := g.d.Router.Table().Lookup(ident.OrgSlug, req.Model)
	if !ok {
		return nil, &httpx.APIError{
			Status:  http.StatusNotFound,
			Message: fmt.Sprintf("the model %q does not exist or you do not have access to it", req.Model),
			Type:    httpx.TypeNotFound,
			Code:    "model_not_found",
			Param:   "model",
		}
	}
	if kind == openai.KindChat && route.Task != routes.TaskChat {
		return nil, httpx.ErrInvalidRequest(
			fmt.Sprintf("%s is a text-completion model; use /v1/completions", route.Model), "unsupported_endpoint", "model")
	}

	defaultMax := route.DefaultMaxTokens
	if defaultMax == 0 {
		defaultMax = g.d.Config.Gateway.DefaultMaxTokens
	}
	if err := req.Check(route.Model, openai.Limits{
		ContextWindow:    route.ContextWindow,
		DefaultMaxTokens: defaultMax,
		JSONMode:         route.Capabilities.JSONMode,
		Streaming:        route.Capabilities.Streaming,
	}); err != nil {
		return nil, err
	}

	if req.Nebula.DeploymentID != "" && !ident.Has(auth.ScopeInferencePin) {
		return nil, forbidden("pinning a deployment with nebula.deployment_id requires the inference:pin scope",
			"insufficient_scope")
	}

	// Render before admission, because the reservation is sized from the rendered
	// prompt: the template's own markup is prompt the model reads.
	prompt := req.Prompt
	stops := req.Stop
	if kind == openai.KindChat {
		rendered, err := openai.Render(string(route.ChatTemplate), req.Messages)
		if err != nil {
			return nil, httpx.ErrInternal(err)
		}
		prompt = rendered.Prompt
		stops = openai.MergeStops(req.Stop, rendered.Stops)
	}

	timeout := g.d.Config.Gateway.DefaultTimeout.Duration()
	if route.Timeout > 0 {
		timeout = route.Timeout
	}
	if t := req.Nebula.TimeoutMS; t != nil {
		timeout = time.Duration(*t) * time.Millisecond
	}
	if limit := g.d.Config.Gateway.MaxTimeout.Duration(); timeout > limit {
		timeout = limit
	}
	deadline := started.Add(timeout)

	// SELECT, before admission: a request nothing can serve is refused without
	// being charged to the caller's limits.
	requestID := telemetry.RequestID(ctx)
	placement := router.Request{
		Key:             requestID,
		Pin:             req.Nebula.DeploymentID,
		RequiredContext: (len(prompt)+openai.MaxBytesPerToken-1)/openai.MaxBytesPerToken + *req.MaxTokens,
		Exclude:         map[string]bool{},
	}
	sel, degraded, err := g.place(ident.OrgSlug, route, placement)
	if err != nil {
		if errors.Is(err, router.ErrNoTarget) {
			return nil, httpx.ErrInvalidRequest(
				fmt.Sprintf("nebula.deployment_id %q is not a deployment of %s", req.Nebula.DeploymentID, route.Model),
				"deployment_not_in_route", "nebula.deployment_id")
		}
		g.logger(ctx).WarnContext(ctx, "no endpoint can serve the request", slog.String("cause", err.Error()))
		w.Header().Set("Retry-After", "1")
		return nil, noHealthyEndpoint(err)
	}
	route, target := sel.Route, sel.Target

	// ADMIT. The reservation is an upper bound on what this request can consume:
	// every token covers at least one byte of prompt, plus the completion's ceiling,
	// plus a margin for the BOS/EOS tokens an engine adds.
	reserve := len(prompt) + *req.MaxTokens + 8
	lease, err := g.admit(w, r, reserve, timeout+time.Minute, false)
	if err != nil {
		sel.Done(router.Abandoned)
		return nil, err
	}

	priority := string(ident.Priority)
	if priority == "" {
		priority = string(models.PriorityNormal)
	}
	traceparent := ""
	if tc, ok := telemetry.Trace(ctx); ok {
		if child, err := tc.Child(); err == nil {
			traceparent = child.Header()
		}
	}

	ex := &exchange{
		kind:    kind,
		req:     req,
		route:   route,
		target:  target,
		lease:   lease,
		reserve: reserve,
		started: started,
		ident:   ident,

		sel:       sel,
		placement: placement,
		attempts:  1,
		verdict:   router.Succeeded,
		degraded:  degraded,
		call: dispatch.Call{
			Endpoint:     sel.URL,
			RequestID:    requestID,
			Traceparent:  traceparent,
			Deadline:     deadline,
			Priority:     priority,
			ModelVersion: target.ModelVersion,
			Body:         workerBody(req, prompt, stops),
		},
	}
	if kind == openai.KindChat {
		ex.id = "chatcmpl-" + requestID
	} else {
		ex.id = "cmpl-" + requestID
	}
	ex.record = usage.Record{
		TraceID:      traceID(ctx),
		OrgID:        ident.OrgID.String(),
		RequestID:    requestID,
		APIKeyID:     ident.ActorID.String(),
		RouteID:      route.ID,
		Route:        route.Model,
		Deployment:   target.Deployment,
		DeploymentID: target.DeploymentID,
		ModelVersion: target.ModelVersion,
		Variant:      target.Label,
		Endpoint:     kind.String(),
		Priority:     priority,
		Streamed:     req.Stream,
		StartedAt:    started.UTC(),
		Attempts:     1,
		User:         req.User,
	}
	return ex, nil
}

// workerBody maps OpenAI parameters onto the worker protocol. Parameters the
// worker body has no field for travel in extra, which the llama.cpp adapter
// forwards to the engine verbatim.
func workerBody(req *openai.Request, prompt string, stops []string) dispatch.Body {
	// OpenAI's defaults, not the worker's: a client that omits temperature expects
	// OpenAI behaviour from an OpenAI-compatible API.
	temperature, topP := 1.0, 1.0
	if req.Temperature != nil {
		temperature = *req.Temperature
	}
	if req.TopP != nil {
		topP = *req.TopP
	}
	b := dispatch.Body{
		Prompt:      prompt,
		MaxTokens:   *req.MaxTokens,
		Temperature: temperature,
		TopP:        topP,
		Stop:        stops,
		Seed:        req.Seed,
	}
	extra := map[string]any{}
	if req.TopK != nil {
		extra["top_k"] = *req.TopK
	}
	if req.PresencePenalty != nil {
		extra["presence_penalty"] = *req.PresencePenalty
	}
	if req.FrequencyPenalty != nil {
		extra["frequency_penalty"] = *req.FrequencyPenalty
	}
	if req.JSONMode {
		// llama-server constrains output to JSON with an empty schema object.
		extra["json_schema"] = map[string]any{}
	}
	if len(extra) > 0 {
		b.Extra = extra
	}
	return b
}

// workerContext is the context for the worker call. It is deliberately NOT the
// client's request context: when the client disconnects, the gateway still has to
// tell the worker to stop and read the final frame for the tokens already spent.
// It ends at the request deadline plus the drain allowance, so it cannot outlive
// the request by more than that.
func (g *Gateway) workerContext(ctx context.Context, ex *exchange) (context.Context, context.CancelFunc) {
	return context.WithDeadline(context.WithoutCancel(ctx), ex.call.Deadline.Add(g.d.Config.Gateway.CancelDrainTimeout.Duration()))
}

// cancelWorker asks the worker to stop. Best effort, bounded, never on the
// request's own context (which is already done when this is needed).
func (g *Gateway) cancelWorker(ctx context.Context, ex *exchange) {
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if err := g.d.Workers.Cancel(cctx, ex.call.Endpoint, ex.call.RequestID, ex.call.Traceparent); err != nil {
		g.logger(ctx).WarnContext(ctx, "cancelling the worker failed; it stops at the deadline or on disconnect",
			slog.String("cause", err.Error()))
	}
}

// generate serves a non-streamed request.
func (g *Gateway) generate(w http.ResponseWriter, r *http.Request, ex *exchange) {
	ctx := r.Context()
	wctx, wcancel := g.workerContext(ctx, ex)
	defer wcancel()

	type outcome struct {
		res *dispatch.Result
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		res, err := g.generateOnce(wctx, ex)
		done <- outcome{res, err}
	}()

	var out outcome
	select {
	case out = <-done:
	case <-ctx.Done():
		// The client left. Stop the worker, then wait a bounded time for its answer,
		// which carries the tokens that were spent.
		g.cancelWorker(ctx, ex)
		select {
		case out = <-done:
		case <-time.After(g.d.Config.Gateway.CancelDrainTimeout.Duration()):
			wcancel()
			out = <-done
		}
		ex.absorb(out.res)
		ex.record.StatusCode = StatusClientClosedRequest
		ex.record.Outcome = usage.OutcomeClientCancelled
		ex.verdict = router.Abandoned
		return
	}

	if out.err != nil {
		ex.verdict = verdictOf(out.err)
		apiErr := g.workerError(w, ctx, out.err)
		ex.record.StatusCode = apiErr.Status
		ex.record.ErrorClass = string(apiErr.Type)
		ex.record.Outcome = outcomeOf(apiErr)
		g.fail(w, r, apiErr)
		return
	}
	res := out.res
	ex.absorb(res)

	switch res.FinishReason {
	case "stop", "length":
	case "deadline":
		ex.record.Outcome = usage.OutcomeDeadlineExceeded
		ex.record.StatusCode = http.StatusGatewayTimeout
		ex.record.ErrorClass = string(httpx.TypeTimeout)
		g.fail(w, r, deadlineError())
		return
	default:
		ex.record.Outcome = usage.OutcomeFailed
		ex.record.StatusCode = http.StatusBadGateway
		ex.record.ErrorClass = string(httpx.TypeUpstream)
		ex.verdict = router.Failed
		g.fail(w, r, (&httpx.APIError{
			Status: http.StatusBadGateway, Type: httpx.TypeUpstream, Code: "generation_failed",
			Message: "the model runtime could not complete the generation",
		}).WithInternal(fmt.Errorf("worker finish_reason %q", res.FinishReason)))
		return
	}
	if res.Usage == nil {
		ex.record.Outcome = usage.OutcomeFailed
		g.fail(w, r, httpx.ErrInternal(errors.New("worker returned no usage")))
		return
	}

	meta := ex.meta(g.d.Now())
	u := openai.NewUsage(res.Usage.PromptTokens, res.Usage.CompletionTokens)
	created := ex.started.Unix()
	var body any
	if ex.kind == openai.KindChat {
		body = openai.ChatCompletion{
			ID: ex.id, Object: openai.ObjectChatCompletion, Created: created, Model: ex.route.Model,
			Choices: []openai.ChatChoice{{
				Index:        0,
				Message:      openai.ChatMessage{Role: "assistant", Content: res.Text},
				FinishReason: res.FinishReason,
			}},
			Usage:  u,
			Nebula: meta,
		}
	} else {
		fr := res.FinishReason
		body = openai.TextCompletion{
			ID: ex.id, Object: openai.ObjectTextCompletion, Created: created, Model: ex.route.Model,
			Choices: []openai.TextChoice{{Index: 0, Text: res.Text, FinishReason: &fr}},
			Usage:   u,
			Nebula:  meta,
		}
	}
	ex.record.Outcome = usage.OutcomeCompleted
	ex.record.StatusCode = http.StatusOK
	setRoutingHeaders(w.Header(), ex)
	if err := httpx.WriteJSON(w, http.StatusOK, body); err != nil {
		g.logger(ctx).WarnContext(ctx, "writing the response failed", slog.String("cause", err.Error()))
	}
}

// absorb copies what the runtime reported into the exchange's usage record.
func (ex *exchange) absorb(res *dispatch.Result) {
	if res == nil {
		return
	}
	ex.record.FinishReason = res.FinishReason
	ex.absorbFinal(res.Usage, res.Timing, res.Runtime)
}

func (ex *exchange) absorbFinal(u *dispatch.Usage, t *dispatch.Timing, rt *dispatch.Runtime) {
	if u != nil {
		p, c := u.PromptTokens, u.CompletionTokens
		ex.record.PromptTokens, ex.record.CompletionTokens = &p, &c
		ex.record.TokenSource = usage.TokenSourceRuntime
	}
	if t != nil {
		ex.record.QueueWait = t.QueueMS
		ex.record.TTFT = t.TTFTMS
		if t.TTFTMS != nil && ex.sel != nil {
			ex.sel.FirstToken(time.Duration(*t.TTFTMS) * time.Millisecond)
		}
		if t.PrefillMS != nil && t.DecodeMS != nil {
			c := *t.PrefillMS + *t.DecodeMS
			ex.record.ComputeMS = &c
		}
		ex.timing = t
	}
	if rt != nil {
		ex.record.Runtime = rt.Name
	}
}

// meta builds the nebula block from what was measured.
func (ex *exchange) meta(now time.Time) *openai.Meta {
	d := now.Sub(ex.started).Milliseconds()
	m := &openai.Meta{
		RequestID:    ex.call.RequestID,
		Route:        ex.route.Model,
		Deployment:   ex.target.Deployment,
		ModelVersion: ex.target.ModelVersion,
		Variant:      ex.target.Label,
		DurationMS:   &d,
		Attempts:     ex.attempts,
		Degraded:     ex.degraded,
		Runtime:      ex.record.Runtime,
		QueueWaitMS:  ex.record.QueueWait,
		TTFTMS:       ex.record.TTFT,
	}
	if ex.timing != nil && ex.timing.DecodeMS != nil && *ex.timing.DecodeMS > 0 && ex.record.CompletionTokens != nil {
		tps := float64(*ex.record.CompletionTokens) / (float64(*ex.timing.DecodeMS) / 1000)
		tps = float64(int64(tps*10+0.5)) / 10
		m.TokensPerSecond = &tps
	}
	return m
}

// Routing headers. They carry the same context as the nebula block and the stream's
// meta comment, in the one place every client can read without parsing the body —
// and, for a stream, before the first token.
const (
	HeaderRoute        = "X-Nebula-Route"
	HeaderDeployment   = "X-Nebula-Deployment"
	HeaderModelVersion = "X-Nebula-Model-Version"
	HeaderVariant      = "X-Nebula-Variant"
	// HeaderDegraded names a degradation that served this response, such as
	// "failover:<route>". Absent when nothing was degraded.
	HeaderDegraded = "X-Nebula-Degraded"
)

func setRoutingHeaders(h http.Header, ex *exchange) {
	h.Set(HeaderRoute, ex.route.Model)
	h.Set(HeaderDeployment, ex.target.Deployment)
	h.Set(HeaderModelVersion, ex.target.ModelVersion)
	if ex.target.Label != "" {
		h.Set(HeaderVariant, ex.target.Label)
	}
	if ex.degraded != "" {
		h.Set(HeaderDegraded, ex.degraded)
	}
}

// finish is FINALIZE: settle the rate-limit reservation against the runtime's
// count and emit the usage record. It runs for every request that passed
// admission, whatever happened after.
func (g *Gateway) finish(ctx context.Context, ex *exchange) {
	now := g.d.Now()
	ex.record.FinishedAt = now.UTC()
	ex.record.DurationMS = now.Sub(ex.started).Milliseconds()
	if ex.record.Outcome == "" {
		ex.record.Outcome = usage.OutcomeFailed
	}

	// Settle at the runtime's count when there is one. When there is not — a
	// stream cut before the final frame — the reservation stands: charging the
	// upper bound is the safe error, charging nothing would make a broken stream
	// free.
	actual := ex.reserve
	if ex.record.PromptTokens != nil && ex.record.CompletionTokens != nil {
		actual = *ex.record.PromptTokens + *ex.record.CompletionTokens
	}
	ex.lease.Release(ctx, actual)
	ex.record.Attempts = ex.attempts
	if ex.sel != nil {
		ex.sel.Done(ex.verdict)
	}
	g.d.Usage.Emit(context.WithoutCancel(ctx), ex.record)
}

// place selects an endpoint for a route, following a failover policy to its
// fallback route when the route itself has nothing eligible. It returns the
// degradation to report, if any.
func (g *Gateway) place(org string, route *routes.Route, req router.Request) (*router.Selection, string, error) {
	sel, err := g.d.Router.Select(route, req)
	if err == nil {
		return sel, "", nil
	}
	var none *router.NoEndpointError
	fallback := route.Policy.Fallback()
	if !errors.As(err, &none) || fallback == "" || req.Pin != "" {
		return nil, "", err
	}
	alt, ok := g.d.Router.Table().Lookup(org, fallback)
	if !ok || alt.Task != route.Task {
		return nil, "", err
	}
	sel, ferr := g.d.Router.Select(alt, req)
	if ferr != nil {
		return nil, "", err
	}
	return sel, "failover:" + alt.Model, nil
}

// maxAttempts bounds how many replicas one request is offered to. Only failures
// before any work started are retried (dispatch.Error.BeforeWork), so an attempt
// never generates twice.
const maxAttempts = 3

// replace moves the exchange to another endpoint after a before-work failure,
// reporting the failed one to the router. It returns false when there is none.
func (g *Gateway) replace(ex *exchange, cause error) bool {
	var we *dispatch.Error
	if !errors.As(cause, &we) || !we.BeforeWork() || ex.attempts >= maxAttempts || ex.placement.Pin != "" {
		return false
	}
	ex.placement.Exclude[ex.sel.EndpointID] = true
	sel, err := g.d.Router.Select(ex.route, ex.placement)
	if err != nil {
		// Nothing else is eligible: the original failure stands, and finish()
		// reports it against the endpoint that failed.
		return false
	}
	ex.sel.Done(verdictOf(cause))
	ex.sel, ex.target = sel, sel.Target
	ex.call.Endpoint, ex.call.ModelVersion = sel.URL, sel.Target.ModelVersion
	ex.attempts++
	ex.record.Deployment, ex.record.DeploymentID = sel.Target.Deployment, sel.Target.DeploymentID
	ex.record.ModelVersion, ex.record.Variant = sel.Target.ModelVersion, sel.Target.Label
	return true
}

// generateOnce performs a non-streamed generation, moving to another replica
// when the chosen one fails before starting work.
func (g *Gateway) generateOnce(ctx context.Context, ex *exchange) (*dispatch.Result, error) {
	for {
		res, err := g.d.Workers.Generate(ctx, ex.call)
		if err == nil || ctx.Err() != nil || !g.replace(ex, err) {
			return res, err
		}
	}
}

// verdictOf classifies a dispatch failure for the endpoint's breaker.
func verdictOf(err error) router.Outcome {
	var we *dispatch.Error
	switch {
	case !errors.As(err, &we):
		return router.Abandoned
	case we.Unreachable():
		return router.Unreachable
	case we.BeforeWork():
		return router.Refused
	case we.Status >= 500 || we.Transport != nil || we.MidStream:
		return router.Failed
	}
	return router.Abandoned
}

// noHealthyEndpoint is the 503 for a request nothing can serve right now.
func noHealthyEndpoint(cause error) *httpx.APIError {
	return (&httpx.APIError{
		Status: http.StatusServiceUnavailable, Type: httpx.TypeServiceUnavailable, Code: "no_healthy_endpoint",
		Message: "no replica of this model is ready right now; retry shortly",
		Reason:  "no_healthy_endpoint",
	}).WithInternal(cause)
}

// workerError maps a dispatch failure that happened before any byte reached the
// client onto the public error it becomes. Worker message text is never passed
// through: it is upstream detail, which never crosses B1 outward.
func (g *Gateway) workerError(w http.ResponseWriter, ctx context.Context, err error) *httpx.APIError {
	var we *dispatch.Error
	if !errors.As(err, &we) {
		return httpx.ErrInternal(err)
	}
	g.logger(ctx).WarnContext(ctx, "worker call failed",
		slog.Int("worker_status", we.Status), slog.String("worker_code", we.Code),
		slog.String("worker_reason", we.Reason), slog.String("cause", we.Error()))

	if we.Transport != nil {
		if errors.Is(we.Transport, context.DeadlineExceeded) {
			return deadlineError()
		}
		w.Header().Set("Retry-After", "1")
		return (&httpx.APIError{
			Status: http.StatusServiceUnavailable, Type: httpx.TypeServiceUnavailable, Code: "no_healthy_endpoint",
			Message: "no replica of this model is reachable right now; retry shortly",
			Reason:  "no_healthy_endpoint",
		}).WithInternal(we)
	}
	switch we.Status {
	case http.StatusTooManyRequests:
		// The worker knows when it expects capacity back; pass that on rather than
		// inventing a number.
		retry := we.RetryAfter
		if retry < time.Second {
			retry = time.Second
		}
		w.Header().Set("Retry-After", seconds(retry))
		return (&httpx.APIError{
			Status: http.StatusTooManyRequests, Type: httpx.TypeRateLimit, Code: "worker_saturated",
			Message: "this model is at capacity; retry after the interval in Retry-After",
			Reason:  "queue_full",
		}).WithInternal(we)
	case http.StatusConflict:
		// The route table and the worker disagree about which version it serves. The
		// worker refused rather than serve the wrong model, which is the guarantee;
		// the client sees an upstream error, not a model mix-up.
		return (&httpx.APIError{
			Status: http.StatusBadGateway, Type: httpx.TypeUpstream, Code: "model_version_mismatch",
			Message: "the replica selected for this request is serving a different model version",
		}).WithInternal(we)
	case http.StatusServiceUnavailable:
		w.Header().Set("Retry-After", "5")
		return (&httpx.APIError{
			Status: http.StatusServiceUnavailable, Type: httpx.TypeServiceUnavailable, Code: "model_loading",
			Message: "the model is not loaded yet; retry shortly", Reason: "model_loading",
		}).WithInternal(we)
	case http.StatusGatewayTimeout:
		return deadlineError()
	case http.StatusBadRequest:
		return (&httpx.APIError{
			Status: http.StatusBadRequest, Type: httpx.TypeInvalidRequest, Code: nonEmpty(we.Code, "unsupported_parameter"),
			Message: "the model runtime does not support a parameter of this request",
		}).WithInternal(we)
	}
	return (&httpx.APIError{
		Status: http.StatusBadGateway, Type: httpx.TypeUpstream, Code: "upstream_error",
		Message: "the model runtime returned an unusable response",
	}).WithInternal(we)
}

func deadlineError() *httpx.APIError {
	return &httpx.APIError{
		Status: http.StatusGatewayTimeout, Type: httpx.TypeTimeout, Code: "deadline_exceeded",
		Message: "the request did not complete within its timeout",
	}
}

func outcomeOf(e *httpx.APIError) string {
	switch {
	case e.Status == http.StatusGatewayTimeout:
		return usage.OutcomeDeadlineExceeded
	case e.Status == http.StatusTooManyRequests:
		return usage.OutcomeRejected
	}
	return usage.OutcomeFailed
}

func nonEmpty(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

func traceID(ctx context.Context) string {
	if tc, ok := telemetry.Trace(ctx); ok {
		return tc.TraceID
	}
	return ""
}

func itoa(n int) string { return strconv.Itoa(n) }

// seconds renders a Retry-After value, rounding up so a client never retries
// before the limit has actually cleared.
func seconds(d time.Duration) string {
	s := int64((d + time.Second - 1) / time.Second)
	if s < 1 {
		s = 1
	}
	return strconv.FormatInt(s, 10)
}
