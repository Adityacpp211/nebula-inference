"""Structured logging and Prometheus metrics for the worker.

Two rules carried over from the Go services, because an operator should not have to
learn two conventions to read one request's path through the system:

* Logs are JSON on stdout, and every line carries ``request_id`` and ``trace_id``
  when they exist. That is what makes "find this request" one query instead of a
  guess.
* Prompts and completions are never logged. Not at debug, not on error. A worker is
  the one place in NEBULA that holds user text in memory, and a log line is the
  easiest way for it to escape (docs/security-boundaries.md).
"""

from __future__ import annotations

import json
import logging
import sys
import time
from contextvars import ContextVar
from typing import Any

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

#: Correlation identifiers for the request being handled on this task.
request_id_var: ContextVar[str] = ContextVar("request_id", default="")
trace_id_var: ContextVar[str] = ContextVar("trace_id", default="")


class JSONFormatter(logging.Formatter):
    """Render a record as one JSON object, with correlation pulled from context."""

    def __init__(self, service: str, instance: str) -> None:
        super().__init__()
        self._service = service
        self._instance = instance

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname.lower(),
            "msg": record.getMessage(),
            "service": self._service,
            "instance": self._instance,
        }
        if rid := request_id_var.get():
            payload["request_id"] = rid
        if tid := trace_id_var.get():
            payload["trace_id"] = tid
        # Extras attached with logger.info("...", extra={"fields": {...}}). A dict
        # rather than loose kwargs so a field can never collide with a LogRecord
        # attribute and silently vanish.
        fields = getattr(record, "fields", None)
        if isinstance(fields, dict):
            payload.update(fields)
        if record.exc_info:
            payload["error"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warn": logging.WARNING,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}


def configure_logging(
    level: str, service: str = "nebula-worker", instance: str = "local"
) -> logging.Logger:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter(service, instance))
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(_LEVELS.get(level.lower(), logging.INFO))
    # uvicorn's own access log duplicates ours and is not JSON. One access log, ours.
    logging.getLogger("uvicorn.access").disabled = True
    logging.getLogger("uvicorn.error").handlers = [handler]
    return logging.getLogger(service)


class Metrics:
    """The worker's metric surface.

    Every metric here is measured by the worker itself. The adapter contributes only
    what the worker cannot see from outside — slot occupancy, the engine's token
    totals, restart count — and those are refreshed on scrape rather than pushed, so
    a stale adapter cannot leave a gauge lying.

    Latency histograms use buckets chosen for token generation, not for web requests:
    the interesting region for time-to-first-token is tens of milliseconds to a few
    seconds, and default buckets put almost everything in one bin.
    """

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        reg = self.registry

        self.requests_total = Counter(
            "nebula_worker_requests_total",
            "Generation requests, by how they finished.",
            ["endpoint", "outcome"],
            registry=reg,
        )
        self.admission_rejected_total = Counter(
            "nebula_worker_admission_rejected_total",
            "Requests refused before entering the runtime, by reason.",
            ["reason"],
            registry=reg,
        )
        self.request_duration = Histogram(
            "nebula_worker_request_duration_seconds",
            "End-to-end duration of a generation request.",
            buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120),
            registry=reg,
        )
        self.ttft = Histogram(
            "nebula_worker_ttft_seconds",
            "Time to first token, measured at the first token.",
            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
            registry=reg,
        )
        self.queue_wait = Histogram(
            "nebula_worker_queue_wait_seconds",
            "Time a request spent waiting for a slot.",
            buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 30),
            registry=reg,
        )
        self.tokens_generated_total = Counter(
            "nebula_worker_tokens_generated_total",
            "Completion tokens produced, counted by the engine's tokenizer.",
            registry=reg,
        )
        self.tokens_prompt_total = Counter(
            "nebula_worker_tokens_prompt_total",
            "Prompt tokens evaluated, counted by the engine's tokenizer.",
            registry=reg,
        )
        self.cancellations_total = Counter(
            "nebula_worker_cancellations_total",
            "Requests stopped by an explicit cancel.",
            registry=reg,
        )
        self.deadlines_exceeded_total = Counter(
            "nebula_worker_deadlines_exceeded_total",
            "Requests aborted because their deadline passed.",
            registry=reg,
        )
        self.load_duration = Histogram(
            "nebula_worker_model_load_duration_seconds",
            "How long loading the model took.",
            buckets=(0.1, 0.5, 1, 5, 15, 30, 60, 180, 600),
            registry=reg,
        )

        self.in_flight = Gauge(
            "nebula_worker_in_flight", "Generations currently running.", registry=reg
        )
        self.queue_depth = Gauge(
            "nebula_worker_queue_depth", "Requests waiting for a slot.", registry=reg
        )
        self.slots_total = Gauge(
            "nebula_worker_slots_total", "Slots the runtime declares.", registry=reg
        )
        self.slots_busy = Gauge(
            "nebula_worker_slots_busy", "Slots the runtime reports busy.", registry=reg
        )
        self.model_ready = Gauge(
            "nebula_worker_model_ready",
            "1 when a model is loaded and the engine answers.",
            registry=reg,
        )
        self.process_alive = Gauge(
            "nebula_worker_engine_process_alive",
            "1 when the engine process is running.",
            registry=reg,
        )
        self.draining = Gauge(
            "nebula_worker_draining", "1 once the worker has begun draining.", registry=reg
        )
        # A gauge, not a counter, because it mirrors a number the adapter owns rather
        # than one this process increments — so it must not carry the ``_total``
        # suffix that Prometheus reserves for counters.
        self.engine_restarts = Gauge(
            "nebula_worker_engine_restarts",
            "Times the adapter has restarted the engine.",
            registry=reg,
        )
        # Base units, per Prometheus convention. Only set when the adapter actually
        # reports a value: an absent series says "this engine does not tell us", which
        # is true, where a zero would say "there is KV cache to spare", which is not.
        self.kv_cache_used_bytes = Gauge(
            "nebula_worker_kv_cache_used_bytes",
            "KV cache in use, when the engine reports it.",
            registry=reg,
        )

    def render(self) -> tuple[bytes, str]:
        return generate_latest(self.registry), CONTENT_TYPE_LATEST


__all__ = ["JSONFormatter", "Metrics", "configure_logging", "request_id_var", "trace_id_var"]
