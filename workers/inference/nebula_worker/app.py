"""The worker's HTTP surface: the internal worker API, probes and metrics.

This is gateway → worker only (docs/api.md §6). It is never exposed publicly, which
is why there is no authentication here and a NetworkPolicy is doing that job instead —
stated rather than assumed, because "internal" written in a comment protects nothing.

Two behaviours are worth reading the code for:

*Required headers are required.* In strict mode a request without a request id, a
trace context and a deadline is refused. The point is not tidiness: an untraced
request cannot be found again when it misbehaves, and a deadline-less one has nothing
that will ever stop it. Development keeps the defaults so ``curl`` still works.

*Every exit path releases its slot.* Success, stop sequence, cancel, deadline, engine
crash, or the client hanging up mid-stream all pass through the same ``finally``. A
leaked slot is a replica that quietly stops accepting work, which is the worst kind of
capacity bug because nothing looks broken.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, Literal

from fastapi import APIRouter, FastAPI, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from .cancel import CancelRegistry
from .config import WorkerConfig
from .deadline import DeadlineError, budget_for, parse_deadline
from .queue import AdmissionQueue, DeadlineUnreachable, QueueFull
from .runtimes import build as build_runtime
from .runtimes.base import (
    Cancelled,
    CapabilityUnsupported,
    DeadlineExceeded,
    FinishReason,
    GenerationRequest,
    InferenceRuntime,
    InferenceRuntimeError,
    ModelNotLoaded,
    ModelSpec,
    ModelVersionMismatch,
    Priority,
    TokenChunk,
)
from .telemetry import Metrics, configure_logging, request_id_var, trace_id_var

#: Bumped when this API changes incompatibly. Echoed on every response so a gateway
#: talking to a worker from a different release finds out immediately.
WORKER_PROTOCOL_VERSION = "1"

HEADER_REQUEST_ID = "X-Request-Id"
HEADER_TRACEPARENT = "traceparent"
HEADER_DEADLINE = "X-Nebula-Deadline"
HEADER_PRIORITY = "X-Nebula-Priority"
HEADER_MODEL_VERSION = "X-Nebula-Model-Version"
HEADER_PROTOCOL = "X-Nebula-Worker-Protocol"
HEADER_REASON = "X-Nebula-Reason"


# ---------------------------------------------------------------------------
# wire types
# ---------------------------------------------------------------------------


class GenerateBody(BaseModel):
    """A generation request body.

    ``extra="forbid"``: an unknown field is a 422 rather than a silently ignored
    parameter. A caller who misspells ``max_tokens`` should be told, not quietly given
    the default and left wondering why the response is short.
    """

    model_config = {"extra": "forbid"}

    prompt: str
    max_tokens: int = Field(default=128, ge=1, le=32768)
    temperature: float = Field(default=0.0, ge=0.0, le=2.0)
    top_p: float = Field(default=1.0, gt=0.0, le=1.0)
    stop: list[str] = Field(default_factory=list, max_length=8)
    seed: int | None = None
    extra: dict[str, Any] = Field(default_factory=dict)


class CancelBody(BaseModel):
    model_config = {"extra": "forbid"}
    request_id: str


class ErrorPayload(BaseModel):
    message: str
    type: str
    code: str = ""
    param: str = ""
    request_id: str = ""


def error_response(
    status: int,
    *,
    message: str,
    err_type: str,
    code: str = "",
    param: str = "",
    request_id: str = "",
    reason: str = "",
    retry_after_s: float | None = None,
) -> JSONResponse:
    """The same envelope the Go services return.

    One shape across the whole system, so a gateway does not need a second error
    parser for worker responses (docs/api.md §1).
    """
    headers = {HEADER_PROTOCOL: WORKER_PROTOCOL_VERSION}
    if request_id:
        headers[HEADER_REQUEST_ID] = request_id
    if reason:
        headers[HEADER_REASON] = reason
    if retry_after_s is not None:
        # Ceil to whole seconds: Retry-After is defined in seconds, and rounding down
        # would invite an immediate retry into the same saturation.
        headers["Retry-After"] = str(max(1, int(retry_after_s + 0.999)))
    return JSONResponse(
        status_code=status,
        headers=headers,
        content={
            "error": ErrorPayload(
                message=message,
                type=err_type,
                code=code,
                param=param,
                request_id=request_id,
            ).model_dump()
        },
    )


#: Typed runtime errors → HTTP status. Mapped in one table so two endpoints cannot
#: disagree about what a checksum mismatch means.
_ERROR_STATUS: dict[type[InferenceRuntimeError], int] = {
    ModelNotLoaded: 503,
    ModelVersionMismatch: 409,
    CapabilityUnsupported: 400,
}


# ---------------------------------------------------------------------------
# worker state
# ---------------------------------------------------------------------------


class Worker:
    """Everything one worker process owns."""

    def __init__(self, config: WorkerConfig, runtime: InferenceRuntime | None = None) -> None:
        self.config = config
        self.log = configure_logging(config.log_level)
        self.metrics = Metrics()
        self.cancels = CancelRegistry()
        self.runtime: InferenceRuntime = runtime or build_runtime(
            config.runtime, config.runtime_config, env=config.env
        )
        self.loaded: Any | None = None
        self.draining = False
        self.started_at = time.time()
        # Set once the model is loaded; ``slots`` is the smaller of what the operator
        # allowed and what the adapter declares, because either being exceeded is a
        # different kind of failure and neither is worth discovering under load.
        self.queue: AdmissionQueue | None = None

    # -- lifecycle ---------------------------------------------------------

    def spec(self) -> ModelSpec:
        return ModelSpec(
            model_version=self.config.model_version,
            artifact_path=self.config.model_path,
            artifact_format=self.config.model_format,
            context_window=self.config.context_window,
            checksum_sha256=self.config.checksum_sha256 or None,
            runtime_config=self.config.runtime_config,
        )

    async def load(self) -> None:
        started = time.perf_counter()
        self.loaded = await self.runtime.load(self.spec())
        self.metrics.load_duration.observe(time.perf_counter() - started)
        declared = self.runtime.capabilities().parallel_slots or 1
        slots = min(self.config.slots, declared) if self.config.slots else declared
        self.queue = AdmissionQueue(slots=slots, max_queue_depth=self.config.max_queue_depth)
        self.metrics.slots_total.set(slots)
        self.log.info(
            "model loaded",
            extra={
                "fields": {
                    "runtime": self.runtime.name,
                    "engine_version": self.runtime.version,
                    "model_version": self.loaded.model_version,
                    "load_duration_ms": self.loaded.load_duration_ms,
                    "resident_bytes": self.loaded.resident_bytes,
                    "slots": slots,
                    "already_loaded": self.loaded.already_loaded,
                }
            },
        )

    async def shutdown(self) -> None:
        """Drain, then stop the engine.

        Order matters: stop admitting, wake anything queued so it fails fast rather
        than waiting for a slot that will never come, cancel what is in flight, then
        unload. Unloading first would leave callers blocked on an iterator that has
        gone away.
        """
        self.draining = True
        self.metrics.draining.set(1)
        if self.queue is not None:
            woken = self.queue.drain_waiters()
            if woken:
                self.log.info("released queued requests", extra={"fields": {"count": woken}})
        cancelled = self.cancels.cancel_all()
        if cancelled:
            self.log.info("cancelled in-flight requests", extra={"fields": {"count": cancelled}})
        # Polled rather than event-driven: the condition is "the registry has emptied",
        # which several independent request tasks contribute to, and a shared event
        # would have to be reset correctly by whichever one finished last. A 50 ms poll
        # during shutdown costs nothing and cannot deadlock.
        deadline = time.monotonic() + self.config.shutdown_grace_s
        while self.cancels and time.monotonic() < deadline:  # noqa: ASYNC110
            await asyncio.sleep(0.05)
        await self.runtime.unload()
        self.log.info("engine unloaded")

    # -- observation -------------------------------------------------------

    async def refresh_gauges(self) -> None:
        """Pull adapter-owned numbers at scrape time.

        Pulled rather than pushed so a gauge cannot go stale: if the adapter stops
        answering, the scrape reflects that instead of showing the last good value
        forever.
        """
        health = await self.runtime.health()
        rm = self.runtime.metrics()
        self.metrics.process_alive.set(1 if health.process_alive else 0)
        self.metrics.model_ready.set(1 if health.model_ready else 0)
        self.metrics.slots_busy.set(rm.slots_busy)
        if rm.kv_cache_used_bytes is not None:
            self.metrics.kv_cache_used_bytes.set(rm.kv_cache_used_bytes)
        self.metrics.engine_restarts.set(rm.engine_restarts_total)
        if self.queue is not None:
            self.metrics.queue_depth.set(self.queue.depth)
            self.metrics.in_flight.set(self.queue.in_flight)
            self.metrics.slots_total.set(self.queue.slots)


# ---------------------------------------------------------------------------
# request plumbing
# ---------------------------------------------------------------------------


class RequestContext(BaseModel):
    model_config = {"arbitrary_types_allowed": True}

    request_id: str
    trace_id: str
    priority: Priority
    deadline_ms: int | None
    remaining_s: float
    asserted_model_version: str | None


def _trace_id_of(traceparent: str | None) -> str:
    """Pull the trace id out of a W3C traceparent, tolerating a malformed one.

    Parsed with string operations rather than an OTel SDK: the worker only needs to
    *correlate* in Phase 3, and taking the dependency now would mean shipping a
    tracing stack before the phase that configures it (Phase 8). A malformed value
    yields no trace id rather than an error, because losing correlation is not worth
    failing a request over.
    """
    if not traceparent:
        return ""
    parts = traceparent.split("-")
    if len(parts) >= 3 and len(parts[1]) == 32 and parts[1] != "0" * 32:
        try:
            int(parts[1], 16)
        except ValueError:
            return ""
        return parts[1]
    return ""


def read_context(request: Request, config: WorkerConfig) -> RequestContext:
    """Validate the headers this API requires and build the request's context."""
    strict = config.strict_headers
    request_id = (request.headers.get(HEADER_REQUEST_ID) or "").strip()
    traceparent = request.headers.get(HEADER_TRACEPARENT)
    trace_id = _trace_id_of(traceparent)

    if strict:
        missing = [
            name
            for name, value in (
                (HEADER_REQUEST_ID, request_id),
                (HEADER_TRACEPARENT, traceparent),
            )
            if not value
        ]
        if missing:
            raise DeadlineError(f"missing required header(s): {', '.join(missing)}")
    if not request_id:
        request_id = str(uuid.uuid4())

    raw_priority = (request.headers.get(HEADER_PRIORITY) or Priority.NORMAL.value).strip().upper()
    try:
        priority = Priority(raw_priority)
    except ValueError as exc:
        raise DeadlineError(
            f"{HEADER_PRIORITY} must be LOW, NORMAL or HIGH, got {raw_priority!r}"
        ) from exc

    deadline_ms = parse_deadline(request.headers.get(HEADER_DEADLINE))
    budget = budget_for(
        deadline_ms,
        default_timeout_s=config.default_timeout_s,
        strict=strict,
    )

    asserted = (request.headers.get(HEADER_MODEL_VERSION) or "").strip() or None
    return RequestContext(
        request_id=request_id,
        trace_id=trace_id,
        priority=priority,
        deadline_ms=budget.deadline_ms,
        remaining_s=budget.remaining_s,
        asserted_model_version=asserted,
    )


def to_generation_request(body: GenerateBody, ctx: RequestContext) -> GenerationRequest:
    return GenerationRequest(
        request_id=ctx.request_id,
        prompt=body.prompt,
        max_tokens=body.max_tokens,
        temperature=body.temperature,
        top_p=body.top_p,
        stop=tuple(body.stop),
        seed=body.seed,
        priority=ctx.priority,
        deadline_ms=ctx.deadline_ms,
        model_version=ctx.asserted_model_version,
        extra=body.extra,
    )


def _chunk_payload(chunk: TokenChunk) -> dict[str, Any]:
    payload: dict[str, Any] = {"content": chunk.text, "index": chunk.index, "stop": chunk.is_final}
    if chunk.finish_reason is not None:
        payload["finish_reason"] = chunk.finish_reason.value
    if chunk.usage is not None:
        payload["usage"] = {
            "prompt_tokens": chunk.usage.prompt_tokens,
            "completion_tokens": chunk.usage.completion_tokens,
        }
    if chunk.timing is not None:
        payload["timing"] = {
            "queue_ms": chunk.timing.queue_ms,
            "prefill_ms": chunk.timing.prefill_ms,
            "decode_ms": chunk.timing.decode_ms,
            "ttft_ms": chunk.timing.ttft_ms,
            "total_ms": chunk.timing.total_ms,
        }
    if chunk.runtime is not None:
        payload["runtime"] = {
            "name": chunk.runtime.name,
            "version": chunk.runtime.version,
            "model_version": chunk.runtime.model_version,
            "slot": chunk.runtime.slot,
            "kv_cache_used_bytes": chunk.runtime.kv_cache_used_bytes,
        }
    return payload


# ---------------------------------------------------------------------------
# the app
# ---------------------------------------------------------------------------


def create_app(config: WorkerConfig, runtime: InferenceRuntime | None = None) -> FastAPI:
    worker = Worker(config, runtime)
    router = APIRouter()

    @contextlib.asynccontextmanager
    async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
        # The model is loaded before the server reports ready, so Kubernetes never
        # routes traffic to a replica that would answer 503.
        await worker.load()
        try:
            yield
        finally:
            await worker.shutdown()

    app = FastAPI(
        title="NEBULA inference worker",
        version=WORKER_PROTOCOL_VERSION,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=_lifespan,
    )
    app.state.worker = worker

    @app.middleware("http")
    async def correlate(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        """Bind correlation ids for the duration of the request, and log the result."""
        rid = (request.headers.get(HEADER_REQUEST_ID) or "").strip() or str(uuid.uuid4())
        token_rid = request_id_var.set(rid)
        token_tid = trace_id_var.set(_trace_id_of(request.headers.get(HEADER_TRACEPARENT)))
        started = time.perf_counter()
        try:
            response = await call_next(request)
        finally:
            request_id_var.reset(token_rid)
            trace_id_var.reset(token_tid)
        response.headers[HEADER_PROTOCOL] = WORKER_PROTOCOL_VERSION
        response.headers[HEADER_REQUEST_ID] = rid
        if request.url.path not in {"/livez", "/readyz", "/healthz", "/metrics"}:
            worker.log.info(
                "request completed",
                extra={
                    "fields": {
                        "http_method": request.method,
                        "http_path": request.url.path,
                        "http_status": response.status_code,
                        "duration_ms": int((time.perf_counter() - started) * 1000),
                    }
                },
            )
        return response

    # -- probes ------------------------------------------------------------

    @router.get("/livez")
    async def livez() -> Response:
        """Is the process (and its engine) alive?

        Separate from readiness because they answer different questions: a failing
        livez gets the container killed, and killing a replica that is merely loading a
        model would make it never start (obligation 6).
        """
        health = await worker.runtime.health()
        status = 200 if health.process_alive else 503
        return JSONResponse(
            status_code=status,
            content={
                "status": "ok" if health.process_alive else "engine_down",
                "detail": health.detail,
            },
        )

    @router.get("/readyz")
    async def readyz() -> Response:
        health = await worker.runtime.health()
        ready = health.model_ready and not worker.draining
        reason = ""
        if worker.draining:
            reason = "draining"
        elif not health.model_ready:
            reason = "model_not_ready"
        return JSONResponse(
            status_code=200 if ready else 503,
            headers={HEADER_REASON: reason} if reason else None,
            content={
                "status": "ok" if ready else "not_ready",
                "reason": reason,
                "model_version": health.model_version,
                "detail": health.detail,
            },
        )

    @router.get("/healthz")
    async def healthz() -> Response:
        health = await worker.runtime.health()
        caps = worker.runtime.capabilities()
        rm = worker.runtime.metrics()
        return JSONResponse(
            content={
                "status": "ok" if health.model_ready and not worker.draining else "degraded",
                "runtime": {
                    "name": worker.runtime.name,
                    "version": worker.runtime.version,
                    "process_alive": health.process_alive,
                    "model_ready": health.model_ready,
                    "detail": health.detail,
                },
                "model": {
                    "version": health.model_version,
                    "context_window": caps.max_context,
                    "slots": caps.parallel_slots,
                },
                "capabilities": {
                    "streaming": caps.streaming,
                    "embeddings": caps.embeddings,
                    "tools": caps.tools,
                    "json_mode": caps.json_mode,
                    "supports_cancel": caps.supports_cancel,
                },
                "engine_restarts": rm.engine_restarts_total,
                "draining": worker.draining,
                "uptime_s": int(time.time() - worker.started_at),
                "protocol": WORKER_PROTOCOL_VERSION,
            }
        )

    @router.get("/metrics")
    async def metrics() -> Response:
        await worker.refresh_gauges()
        body, content_type = worker.metrics.render()
        return Response(content=body, media_type=content_type)

    # -- internal worker API ----------------------------------------------

    @router.get("/internal/v1/state")
    async def state() -> Response:
        """What a router and an autoscaler need in one call.

        Exists so neither has to infer load from latency. Everything here is observed:
        there is no field a caller could mistake for a prediction.
        """
        health = await worker.runtime.health()
        rm = worker.runtime.metrics()
        queue = worker.queue
        return JSONResponse(
            content={
                "model_version": health.model_version,
                "runtime": worker.runtime.name,
                "engine_version": worker.runtime.version,
                "ready": health.model_ready and not worker.draining,
                "draining": worker.draining,
                "slots_total": queue.slots if queue else 0,
                "slots_busy": rm.slots_busy,
                "in_flight": queue.in_flight if queue else 0,
                "queue_depth": queue.depth if queue else 0,
                "queue_capacity": worker.config.max_queue_depth,
                "ewma_service_time_ms": int((queue.service_time_s if queue else 0) * 1000),
                "wait_estimate_ms": int((queue.wait_estimate_s() if queue else 0) * 1000),
                "tokens_generated_total": rm.tokens_generated_total,
                "tokens_prompt_total": rm.tokens_prompt_total,
                "engine_restarts_total": rm.engine_restarts_total,
                "protocol": WORKER_PROTOCOL_VERSION,
            }
        )

    @router.post("/internal/v1/drain")
    async def drain() -> Response:
        """Stop accepting work; let in-flight requests finish.

        Called from a preStop hook. Idempotent, because a preStop hook that runs twice
        must not be a second kind of event.
        """
        was = worker.draining
        worker.draining = True
        worker.metrics.draining.set(1)
        if worker.queue is not None:
            worker.queue.drain_waiters()
        return JSONResponse(
            content={
                "draining": True,
                "already_draining": was,
                "in_flight": len(worker.cancels),
            }
        )

    @router.post("/internal/v1/cancel")
    async def cancel(body: CancelBody) -> Response:
        """Stop an in-flight or queued request.

        Both layers are asked: the worker's registry covers requests still waiting for
        a slot, and the adapter covers those already generating. Reporting whether
        anything was actually stopped matters, because the gateway uses it to decide
        whether capacity came back.
        """
        entry = worker.cancels.cancel(body.request_id)
        runtime_stopped = await worker.runtime.cancel(body.request_id)
        stopped = entry is not None or runtime_stopped
        if stopped:
            worker.metrics.cancellations_total.inc()
        return JSONResponse(
            content={
                "request_id": body.request_id,
                "cancelled": stopped,
                "was_queued": entry is not None and not entry.started,
                "runtime_stopped": runtime_stopped,
            }
        )

    @router.post("/internal/v1/generate")
    async def generate(request: Request, body: GenerateBody) -> Response:
        return await _handle(request, body, streaming=False)

    @router.post("/internal/v1/generate/stream")
    async def generate_stream(request: Request, body: GenerateBody) -> Response:
        return await _handle(request, body, streaming=True)

    # -- the shared request path -------------------------------------------

    async def _handle(request: Request, body: GenerateBody, *, streaming: bool) -> Response:
        endpoint = "stream" if streaming else "generate"
        try:
            ctx = read_context(request, config)
        except DeadlineError as exc:
            worker.metrics.admission_rejected_total.labels(reason="bad_headers").inc()
            return error_response(
                400,
                message=str(exc),
                err_type="invalid_request_error",
                code="invalid_headers",
            )

        if worker.draining:
            # 503 with a reason, not a silent hang: the gateway should try another
            # replica immediately rather than waiting out this one's shutdown.
            worker.metrics.admission_rejected_total.labels(reason="draining").inc()
            return error_response(
                503,
                message="this worker is draining and is not accepting new requests",
                err_type="service_unavailable",
                code="draining",
                request_id=ctx.request_id,
                reason="draining",
                retry_after_s=1,
            )

        queue = worker.queue
        if queue is None:
            worker.metrics.admission_rejected_total.labels(reason="model_not_loaded").inc()
            return error_response(
                503,
                message="no model is loaded",
                err_type="service_unavailable",
                code="model_not_loaded",
                request_id=ctx.request_id,
                reason="model_not_ready",
            )

        if ctx.remaining_s <= 0:
            worker.metrics.admission_rejected_total.labels(reason="deadline_passed").inc()
            return error_response(
                400,
                message="the request's deadline has already passed",
                err_type="invalid_request_error",
                code="deadline_passed",
                param=HEADER_DEADLINE,
                request_id=ctx.request_id,
            )

        caps = worker.runtime.capabilities()
        if streaming and not caps.streaming:
            return error_response(
                400,
                message=f"runtime {worker.runtime.name!r} does not support streaming",
                err_type="invalid_request_error",
                code="capability_unsupported",
                request_id=ctx.request_id,
            )
        if body.max_tokens > caps.max_context:
            return error_response(
                400,
                message=(
                    f"max_tokens ({body.max_tokens}) exceeds this model's context "
                    f"window ({caps.max_context})"
                ),
                err_type="invalid_request_error",
                code="context_length_exceeded",
                param="max_tokens",
                request_id=ctx.request_id,
            )

        try:
            queue.admit_or_raise(ctx.remaining_s)
        except QueueFull as exc:
            worker.metrics.admission_rejected_total.labels(reason="saturated").inc()
            return error_response(
                429,
                message="this worker's queue is full",
                err_type="rate_limit_error",
                code="worker_saturated",
                request_id=ctx.request_id,
                reason="worker_saturated",
                retry_after_s=exc.retry_after_s,
            )
        except DeadlineUnreachable as exc:
            worker.metrics.admission_rejected_total.labels(reason="deadline_unreachable").inc()
            return error_response(
                429,
                message=(
                    "this worker cannot start the request before its deadline "
                    f"(estimated wait {exc.wait_estimate_s:.2f}s, "
                    f"budget {exc.remaining_s:.2f}s)"
                ),
                err_type="rate_limit_error",
                code="deadline_unreachable",
                request_id=ctx.request_id,
                reason="worker_saturated",
                retry_after_s=exc.wait_estimate_s,
            )

        gen_req = to_generation_request(body, ctx)
        if streaming:
            return await _stream_response(gen_req, ctx, endpoint)
        return await _collect_response(gen_req, ctx, endpoint)

    async def _acquire(gen_req: GenerationRequest, ctx: RequestContext) -> tuple[Any, float]:
        registration = worker.cancels.register(ctx.request_id)
        queue = worker.queue
        assert queue is not None
        wait_s = await queue.acquire(ctx.priority, registration.cancelled)
        registration.started = True
        worker.metrics.queue_wait.observe(wait_s)
        return registration, wait_s

    def _record(endpoint: str, chunk: TokenChunk | None, outcome: str, duration_s: float) -> None:
        worker.metrics.requests_total.labels(endpoint=endpoint, outcome=outcome).inc()
        worker.metrics.request_duration.observe(duration_s)
        if chunk is not None and chunk.usage is not None:
            worker.metrics.tokens_generated_total.inc(chunk.usage.completion_tokens)
            worker.metrics.tokens_prompt_total.inc(chunk.usage.prompt_tokens)
        if chunk is not None and chunk.timing is not None and chunk.timing.ttft_ms:
            worker.metrics.ttft.observe(chunk.timing.ttft_ms / 1000)
        if outcome == "cancelled":
            worker.metrics.cancellations_total.inc()
        elif outcome == "deadline":
            worker.metrics.deadlines_exceeded_total.inc()

    async def _collect_response(
        gen_req: GenerationRequest, ctx: RequestContext, endpoint: str
    ) -> Response:
        started = time.perf_counter()
        registration, wait_s = await _acquire(gen_req, ctx)
        text: list[str] = []
        final: TokenChunk | None = None
        outcome = "ok"
        try:
            async for chunk in worker.runtime.stream(gen_req):
                if registration.cancelled.is_set() and not chunk.is_final:
                    # The adapter also watches the event; this is the worker's own
                    # backstop so a cancel is honoured even against an adapter that
                    # only checks between tokens.
                    await worker.runtime.cancel(ctx.request_id)
                text.append(chunk.text)
                if chunk.is_final:
                    final = chunk
            if final is None:
                raise RuntimeError("runtime produced no final chunk")
            outcome = _outcome_of(final.finish_reason)
            payload: dict[str, Any] = {
                "text": "".join(text),
                **_chunk_payload(final),
            }
            payload.pop("content", None)
            payload.pop("stop", None)
            payload.pop("index", None)
            if payload.get("timing") is not None:
                payload["timing"]["queue_ms"] = int(wait_s * 1000)
            return JSONResponse(
                headers={HEADER_REQUEST_ID: ctx.request_id},
                content=payload,
            )
        except InferenceRuntimeError as exc:
            outcome = "error"
            return _runtime_error_response(exc, ctx)
        finally:
            duration = time.perf_counter() - started
            _record(endpoint, final, outcome, duration)
            worker.cancels.release(ctx.request_id)
            assert worker.queue is not None
            worker.queue.release(max(0.0, duration - wait_s))

    async def _stream_response(
        gen_req: GenerationRequest, ctx: RequestContext, endpoint: str
    ) -> Response:
        started = time.perf_counter()
        registration, wait_s = await _acquire(gen_req, ctx)

        async def body() -> AsyncIterator[bytes]:
            final: TokenChunk | None = None
            outcome = "ok"
            try:
                async for chunk in worker.runtime.stream(gen_req):
                    if registration.cancelled.is_set() and not chunk.is_final:
                        await worker.runtime.cancel(ctx.request_id)
                    payload = _chunk_payload(chunk)
                    if chunk.is_final:
                        final = chunk
                        outcome = _outcome_of(chunk.finish_reason)
                        if payload.get("timing") is not None:
                            payload["timing"]["queue_ms"] = int(wait_s * 1000)
                    yield f"data: {json.dumps(payload)}\n\n".encode()
                # A terminal marker, so a consumer can tell a finished stream from a
                # connection that died mid-flight. Without it those look identical.
                yield b"data: [DONE]\n\n"
            except InferenceRuntimeError as exc:
                outcome = "error"
                # The status line is long gone by now, so an error mid-stream has to
                # be delivered as an event. Clients are told to treat an ``error``
                # event as terminal (docs/api.md §1).
                yield (
                    "data: "
                    + json.dumps(
                        {
                            "error": {
                                "message": exc.message,
                                "type": "upstream_error",
                                "code": exc.code,
                                "request_id": ctx.request_id,
                            }
                        }
                    )
                    + "\n\n"
                ).encode()
                yield b"data: [DONE]\n\n"
            finally:
                duration = time.perf_counter() - started
                _record(endpoint, final, outcome, duration)
                worker.cancels.release(ctx.request_id)
                assert worker.queue is not None
                worker.queue.release(max(0.0, duration - wait_s))

        return StreamingResponse(
            body(),
            media_type="text/event-stream",
            headers={
                HEADER_REQUEST_ID: ctx.request_id,
                # Proxies that buffer would defeat the entire point of streaming.
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )

    def _runtime_error_response(exc: InferenceRuntimeError, ctx: RequestContext) -> Response:
        status = _ERROR_STATUS.get(type(exc), 500)
        if isinstance(exc, Cancelled):
            status = 499  # nginx's "client closed request"; the work really did stop
        elif isinstance(exc, DeadlineExceeded):
            status = 504
        return error_response(
            status,
            message=exc.message,
            err_type="upstream_error" if status >= 500 else "invalid_request_error",
            code=exc.code,
            request_id=ctx.request_id,
        )

    app.include_router(router)
    return app


def _outcome_of(reason: FinishReason | None) -> Literal["ok", "cancelled", "deadline", "error"]:
    if reason is FinishReason.CANCEL:
        return "cancelled"
    if reason is FinishReason.DEADLINE:
        return "deadline"
    if reason is FinishReason.ERROR:
        return "error"
    return "ok"


__all__ = ["WORKER_PROTOCOL_VERSION", "GenerateBody", "Worker", "create_app"]
