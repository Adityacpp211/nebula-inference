"""The Phase 3 exit criterion: client → worker → real model → streamed response.

Everything here is real. A real upstream ``llama-server`` child process, a real GGUF
artifact, real tokenisation, real attention, real sampling, real Server-Sent Events
over a real socket. No mock adapter is imported in this file, and one test asserts
that the response really did come from ``llamacpp`` — because a suite that silently
fell back to the stub would pass while proving nothing.

The model is small (see ``tools/make_tiny_model.py``): about 0.4 M parameters, trained
to memorise one paragraph, because no model host is reachable from CI here. That is a
limit on what can be asserted about *output quality*, and the tests are written to
respect it — content assertions only cover the span the model was trained on. It is not
a limit on what can be asserted about the *pipeline*, which is what this phase is
about.

Skipped unless ``NEBULA_LLAMA_SERVER_BIN`` and ``NEBULA_TEST_MODEL_PATH`` are set.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import socket
import time
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest
import uvicorn

from nebula_worker.app import (
    HEADER_DEADLINE,
    HEADER_MODEL_VERSION,
    HEADER_REQUEST_ID,
    create_app,
)
from nebula_worker.config import WorkerConfig

pytestmark = pytest.mark.integration

#: The first tokens the fixture model was trained to produce after "nebula streams".
#: Asserting on real content is the whole point: a stub could satisfy every structural
#: assertion in this file, and only the text proves a trained model ran.
PROMPT = "nebula streams"
EXPECTED_PREFIX = " tokens from a supervised llama server"

N_CTX = 8192
N_PARALLEL = 2
#: Long enough that the engine is still generating when a test cancels or a deadline
#: fires. The fixture model produces roughly a thousand tokens a second.
LONG_TOKENS = 3000


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture
async def worker(
    llama_server_bin: Path, model_artifact: Path, model_version: str
) -> AsyncIterator[httpx.AsyncClient]:
    """The worker, serving the real engine, over a real socket."""
    config = WorkerConfig(
        env="dev",
        runtime="llamacpp",
        model_version=model_version,
        model_path=str(model_artifact),
        model_format="gguf",
        context_window=N_CTX,
        max_queue_depth=8,
        default_timeout_s=60.0,
        runtime_config={
            "server_bin": str(llama_server_bin),
            "n_ctx": N_CTX,
            "n_parallel": N_PARALLEL,
            "n_threads": 2,
            "startup_timeout_s": 180.0,
        },
    )
    app = create_app(config)
    port = _free_port()
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host="127.0.0.1",
            port=port,
            log_config=None,
            access_log=False,
            lifespan="on",
            ws="none",
        )
    )
    serving = asyncio.create_task(server.serve())
    try:
        deadline = time.monotonic() + 240
        while not server.started and time.monotonic() < deadline:
            if serving.done():
                await serving
            await asyncio.sleep(0.05)
        assert server.started, "the worker did not start"
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{port}", timeout=httpx.Timeout(120.0)
        ) as client:
            yield client
    finally:
        server.should_exit = True
        with contextlib.suppress(TimeoutError, asyncio.TimeoutError):
            await asyncio.wait_for(serving, timeout=60)


def headers(**extra: str) -> dict[str, str]:
    base = {
        HEADER_REQUEST_ID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        HEADER_DEADLINE: str(int((time.time() + 60) * 1000)),
    }
    base.update(extra)
    return base


async def read_sse(
    client: httpx.AsyncClient, body: dict[str, object], hdrs: dict[str, str] | None = None
) -> tuple[list[dict], list[float]]:
    """Consume an SSE generation, returning the events and when each arrived."""
    events: list[dict] = []
    arrivals: list[float] = []
    async with client.stream(
        "POST", "/internal/v1/generate/stream", json=body, headers=hdrs or headers()
    ) as resp:
        assert resp.status_code == 200, await resp.aread()
        assert resp.headers["content-type"].startswith("text/event-stream")
        async for line in resp.aiter_lines():
            if not line.startswith("data:"):
                continue
            payload = line[len("data:") :].strip()
            if payload == "[DONE]":
                break
            events.append(json.loads(payload))
            arrivals.append(time.perf_counter())
    return events, arrivals


class TestRealModelStreaming:
    """The proof: a real model's tokens, arriving one at a time, over HTTP."""

    async def test_streams_real_tokens_from_a_real_engine(
        self, worker: httpx.AsyncClient, model_version: str
    ) -> None:
        started = time.perf_counter()
        events, arrivals = await read_sse(
            worker, {"prompt": PROMPT, "max_tokens": 48, "temperature": 0.0}
        )

        assert len(events) >= 10, "a real generation should produce many events"
        final = events[-1]
        assert final["stop"] is True
        assert all(e.get("stop") is False for e in events[:-1])

        # 1. The tokens arrived incrementally rather than in one batch at the end.
        # This is the property streaming exists for, and the one a buffering bug
        # silently destroys.
        assert arrivals[0] < arrivals[-1]
        assert arrivals[0] - started < 10.0
        first_gap = arrivals[0] - started
        total = arrivals[-1] - started
        assert total > first_gap, "the last token arrived at the same time as the first"

        # 2. The text is what this model was trained to say. A stub adapter, a wrong
        # RoPE convention or a mismatched tokenizer would all fail here — and the
        # second and third of those produce confident nonsense, which nothing
        # structural would catch.
        text = "".join(e.get("content", "") for e in events)
        assert text.startswith(EXPECTED_PREFIX), (
            f"expected the trained continuation, got {text[:80]!r}"
        )

        # 3. Counts and timings come from the engine.
        assert final["usage"]["prompt_tokens"] > 0
        assert final["usage"]["completion_tokens"] > 0
        assert final["timing"]["ttft_ms"] >= 0
        assert final["timing"]["total_ms"] >= final["timing"]["ttft_ms"]
        assert final["finish_reason"] in {"stop", "length"}

        # 4. It really was the llama.cpp adapter, with a real engine build string.
        runtime = final["runtime"]
        assert runtime["name"] == "llamacpp", "this test must not run against the stub"
        assert runtime["model_version"] == model_version
        assert runtime["version"] and runtime["version"] != "unknown", (
            "the adapter must report the engine build it is talking to"
        )

    async def test_non_streaming_agrees_with_streaming(self, worker: httpx.AsyncClient) -> None:
        """Both endpoints must give the same answer at temperature zero.

        They share one implementation precisely so they cannot drift; this is the test
        that would notice if someone split them.
        """
        body = {"prompt": PROMPT, "max_tokens": 24, "temperature": 0.0}
        collected = await worker.post("/internal/v1/generate", json=body, headers=headers())
        assert collected.status_code == 200
        events, _ = await read_sse(worker, body)

        streamed_text = "".join(e.get("content", "") for e in events)
        assert collected.json()["text"] == streamed_text
        assert (
            collected.json()["usage"]["completion_tokens"]
            == events[-1]["usage"]["completion_tokens"]
        )

    async def test_deterministic_at_temperature_zero(self, worker: httpx.AsyncClient) -> None:
        body = {"prompt": PROMPT, "max_tokens": 20, "temperature": 0.0}
        first = await worker.post("/internal/v1/generate", json=body, headers=headers())
        second = await worker.post("/internal/v1/generate", json=body, headers=headers())
        assert first.json()["text"] == second.json()["text"]

    async def test_max_tokens_is_honoured_and_reported_as_length(
        self, worker: httpx.AsyncClient
    ) -> None:
        resp = await worker.post(
            "/internal/v1/generate",
            json={"prompt": PROMPT, "max_tokens": 5, "temperature": 0.0},
            headers=headers(),
        )
        body = resp.json()
        assert body["usage"]["completion_tokens"] == 5
        assert body["finish_reason"] == "length", (
            "running out of budget must be reported as 'length', not 'stop'"
        )

    async def test_stop_sequence_ends_generation(self, worker: httpx.AsyncClient) -> None:
        resp = await worker.post(
            "/internal/v1/generate",
            json={
                "prompt": PROMPT,
                "max_tokens": 200,
                "temperature": 0.0,
                "stop": ["worker"],
            },
            headers=headers(),
        )
        body = resp.json()
        assert body["finish_reason"] == "stop"
        assert body["usage"]["completion_tokens"] < 200


class TestRealModelLifecycle:
    async def test_readiness_gates_on_the_loaded_model(
        self, worker: httpx.AsyncClient, model_version: str
    ) -> None:
        """The worker reports ready only once the engine has the model.

        The fixture reaches this test after startup completed, so readiness must
        already be true: the lifespan loads the model before uvicorn accepts traffic,
        which is what stops Kubernetes routing to a replica that would answer 503.
        """
        ready = await worker.get("/readyz")
        assert ready.status_code == 200
        assert ready.json()["model_version"] == model_version

        health = (await worker.get("/healthz")).json()
        assert health["runtime"]["name"] == "llamacpp"
        assert health["runtime"]["process_alive"] is True
        assert health["runtime"]["model_ready"] is True
        assert health["capabilities"]["streaming"] is True
        assert health["capabilities"]["supports_cancel"] is True
        assert health["engine_restarts"] == 0, "the engine should not have restarted"

    async def test_state_reports_observed_capacity(self, worker: httpx.AsyncClient) -> None:
        state = (await worker.get("/internal/v1/state")).json()
        assert state["runtime"] == "llamacpp"
        assert state["ready"] is True
        assert state["slots_total"] == N_PARALLEL
        assert state["in_flight"] == 0
        assert state["engine_version"] != "unknown"

    async def test_model_version_assertion_is_enforced(self, worker: httpx.AsyncClient) -> None:
        """A stale router must not receive the wrong model from a real engine either."""
        resp = await worker.post(
            "/internal/v1/generate",
            json={"prompt": PROMPT, "max_tokens": 4},
            headers=headers(**{HEADER_MODEL_VERSION: "some-other-model:v9"}),
        )
        assert resp.status_code == 409
        assert resp.json()["error"]["code"] == "model_version_mismatch"

    async def test_metrics_reflect_real_generation(self, worker: httpx.AsyncClient) -> None:
        await worker.post(
            "/internal/v1/generate",
            json={"prompt": PROMPT, "max_tokens": 12, "temperature": 0.0},
            headers=headers(),
        )
        body = (await worker.get("/metrics")).text

        assert 'nebula_worker_requests_total{endpoint="generate",outcome="ok"}' in body
        assert "nebula_worker_engine_process_alive 1.0" in body
        assert "nebula_worker_model_ready 1.0" in body
        # The histogram must have observed something, or time-to-first-token is not
        # actually being measured on the real path.
        assert "nebula_worker_ttft_seconds_count" in body
        ttft_count = next(
            float(line.split()[-1])
            for line in body.splitlines()
            if line.startswith("nebula_worker_ttft_seconds_count")
        )
        assert ttft_count >= 1, "no TTFT was recorded for a real generation"
        tokens = next(
            float(line.split()[-1])
            for line in body.splitlines()
            if line.startswith("nebula_worker_tokens_generated_total ")
        )
        assert tokens >= 12


class TestRealModelCancellation:
    async def test_cancel_stops_the_real_engine(self, worker: httpx.AsyncClient) -> None:
        """Cancelling mid-stream must stop the engine, not just our reading of it.

        The evidence is the token count: far fewer than requested means the engine
        stopped generating. Obligation 3 calls returning success without stopping a
        contract violation, because the gateway's capacity model then believes a slot
        is busy that is free — or worse, the reverse.
        """
        rid = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
        seen: list[dict] = []

        async def read() -> None:
            async with worker.stream(
                "POST",
                "/internal/v1/generate/stream",
                json={"prompt": PROMPT, "max_tokens": LONG_TOKENS, "temperature": 0.0},
                headers=headers(**{HEADER_REQUEST_ID: rid}),
            ) as resp:
                async for line in resp.aiter_lines():
                    if not line.startswith("data:"):
                        continue
                    payload = line[len("data:") :].strip()
                    if payload == "[DONE]":
                        return
                    seen.append(json.loads(payload))

        reader = asyncio.create_task(read())
        # Cancel only once generation is genuinely under way, so this proves an
        # in-flight cancel rather than a race with admission.
        while not seen:
            await asyncio.sleep(0.01)

        cancel = await worker.post("/internal/v1/cancel", json={"request_id": rid})
        assert cancel.status_code == 200
        assert cancel.json()["cancelled"] is True
        assert cancel.json()["runtime_stopped"] is True

        await asyncio.wait_for(reader, timeout=60)
        final = seen[-1]
        assert final["finish_reason"] == "cancel"
        assert final["usage"]["completion_tokens"] < LONG_TOKENS, (
            "the engine kept generating after a cancel"
        )

        # The slot came back, which is the other half of obligation 3.
        state = (await worker.get("/internal/v1/state")).json()
        assert state["in_flight"] == 0
        after = await worker.post(
            "/internal/v1/generate",
            json={"prompt": PROMPT, "max_tokens": 4},
            headers=headers(),
        )
        assert after.status_code == 200

    async def test_deadline_aborts_a_real_generation(self, worker: httpx.AsyncClient) -> None:
        """A deadline must stop a real engine mid-generation."""
        budget_s = 0.4
        started = time.perf_counter()
        events, _ = await read_sse(
            worker,
            {"prompt": PROMPT, "max_tokens": LONG_TOKENS, "temperature": 0.0},
            headers(**{HEADER_DEADLINE: str(int((time.time() + budget_s) * 1000))}),
        )
        elapsed = time.perf_counter() - started

        final = events[-1]
        assert final["finish_reason"] == "deadline", (
            f"expected the deadline to abort generation, got {final['finish_reason']!r} "
            f"after {elapsed:.2f}s"
        )
        assert final["usage"]["completion_tokens"] < LONG_TOKENS
        assert elapsed < budget_s + 5.0, "the deadline was honoured far too late"

        # Aborting on a deadline must also free the slot.
        state = (await worker.get("/internal/v1/state")).json()
        assert state["in_flight"] == 0

    async def test_client_disconnect_stops_generation(self, worker: httpx.AsyncClient) -> None:
        """Hanging up must stop the engine, without an explicit cancel call.

        This is the common case in practice — a user closes a tab — and it is the one
        that wastes the most capacity if unhandled, because nothing ever tells the
        worker to stop.
        """
        rid = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
        async with worker.stream(
            "POST",
            "/internal/v1/generate/stream",
            json={"prompt": PROMPT, "max_tokens": LONG_TOKENS, "temperature": 0.0},
            headers=headers(**{HEADER_REQUEST_ID: rid}),
        ) as resp:
            async for line in resp.aiter_lines():
                if line.startswith("data:"):
                    break  # abandon the stream mid-generation
            # Closing here rather than letting the context manager unwind: an
            # abandoned body otherwise surfaces as an unretrieved task exception from
            # the HTTP client, which is noise in an otherwise clean test run.
            with contextlib.suppress(Exception):
                await resp.aclose()

        # The worker notices and releases the slot rather than generating into a void.
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            state = (await worker.get("/internal/v1/state")).json()
            if state["in_flight"] == 0:
                break
            await asyncio.sleep(0.1)
        assert state["in_flight"] == 0, "an abandoned stream leaked its slot"
