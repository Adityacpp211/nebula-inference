"""Worker spans: under the gateway's trace, the documented tree, no user text."""

from __future__ import annotations

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from tests.test_app import _serve, headers, worker_config

# The provider is process-global and can be set once; this module owns it.
exporter = InMemorySpanExporter()
_provider = TracerProvider()
_provider.add_span_processor(SimpleSpanProcessor(exporter))
trace.set_tracer_provider(_provider)

TRACE_ID = "0af7651916cd43dd8448eb211c80319c"
PARENT = "b7ad6b7169203331"
PROMPT = "tell me something secret-xyzzy"


def _spans() -> dict[str, object]:
    return {
        s.name: s
        for s in exporter.get_finished_spans()
        if format(s.context.trace_id, "032x") == TRACE_ID
    }


async def _check(streaming: bool) -> None:
    exporter.clear()
    path = "/internal/v1/generate/stream" if streaming else "/internal/v1/generate"
    async with _serve(worker_config()) as client:
        resp = await client.post(path, json={"prompt": PROMPT, "max_tokens": 4}, headers=headers())
        assert resp.status_code == 200
        await resp.aread()
    spans = _spans()
    gen = spans["worker.generate"]
    assert format(gen.parent.span_id, "016x") == PARENT, (
        "worker.generate must continue the caller's span"
    )
    for name in ("worker.queue", "runtime.stream"):
        assert spans[name].parent.span_id == gen.context.span_id, (
            f"{name} is not a child of worker.generate"
        )
    assert spans["runtime.stream"].attributes["tokens_out"] == 4
    for s in spans.values():
        for v in (s.attributes or {}).values():
            assert "xyzzy" not in str(v), f"{s.name} carries user text"


async def test_non_streamed_generation_is_traced() -> None:
    await _check(streaming=False)


async def test_streamed_generation_is_traced() -> None:
    await _check(streaming=True)


async def test_request_log_line_is_correlated_and_has_no_prompt() -> None:
    import io
    import json as _json
    import logging

    from nebula_worker.telemetry import JSONFormatter

    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(JSONFormatter("nebula-worker", "test"))
    log = logging.getLogger("nebula-worker")
    log.addHandler(handler)
    try:
        async with _serve(worker_config()) as client:
            resp = await client.post(
                "/internal/v1/generate",
                json={"prompt": PROMPT, "max_tokens": 4},
                headers=headers(),
            )
            assert resp.status_code == 200
    finally:
        log.removeHandler(handler)
    lines = [_json.loads(x) for x in buf.getvalue().splitlines() if x.strip()]
    done = [x for x in lines if x.get("msg") == "request completed"]
    assert done, lines
    assert done[0]["request_id"] == headers()["X-Request-Id"]
    assert done[0]["trace_id"] == TRACE_ID
    assert "xyzzy" not in buf.getvalue()
