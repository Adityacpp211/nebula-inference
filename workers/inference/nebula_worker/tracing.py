"""Worker tracing (docs/observability.md §3.1).

One ``worker.generate`` server span per generation, continuing the gateway's
``traceparent`` so it sits under ``dispatch.attempt`` in the same trace, with
``worker.queue`` and ``runtime.stream`` as children.

Spans go to an OpenTelemetry Collector over OTLP/HTTP when ``NEBULA_OTLP_ENDPOINT``
is set; otherwise the API's no-op tracer makes every call here free. No span ever
carries prompt or completion text — counts and timings only.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from opentelemetry import trace
from opentelemetry.trace import Span, SpanKind, Status, StatusCode
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

_propagator = TraceContextTextMapPropagator()
tracer = trace.get_tracer("nebula/worker")


def setup(endpoint: str, service: str, instance: str, version: str) -> Any | None:
    """Install the SDK provider exporting to ``endpoint`` (host:port). Returns the
    provider, whose ``shutdown`` flushes; ``None`` when tracing is off."""
    if not endpoint:
        return None
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    provider = TracerProvider(
        resource=Resource.create(
            {
                "service.name": service,
                "service.instance.id": instance,
                "service.version": version,
            }
        )
    )
    # Bounded and asynchronous: a slow collector drops spans, never requests.
    provider.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(endpoint=f"http://{endpoint}/v1/traces"), max_queue_size=4096
        )
    )
    trace.set_tracer_provider(provider)
    return provider


def start_generate(headers: Mapping[str, str], attributes: Mapping[str, Any]) -> Span:
    """Start ``worker.generate`` under the caller's W3C trace context."""
    carrier = {k.lower(): v for k, v in headers.items()}
    ctx = _propagator.extract(carrier=carrier)
    return tracer.start_span(
        "worker.generate", context=ctx, kind=SpanKind.SERVER, attributes=dict(attributes)
    )


def child(parent: Span, name: str) -> Span:
    """Start a child of ``parent``."""
    return tracer.start_span(name, context=trace.set_span_in_context(parent))


def fail(span: Span, message: str) -> None:
    span.set_status(Status(StatusCode.ERROR, message))
