"""The worker's HTTP surface, exercised through the real ASGI app.

Driven with the mock runtime, because these tests are about the *worker*: header
contracts, admission control, the error envelope, the drain sequence, metrics. Using a
real engine here would make them slow and would test llama.cpp's behaviour twice.
The real engine is covered by ``test_llamacpp_conformance.py`` and end to end by
``test_real_model.py``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import socket
import time
from collections.abc import AsyncIterator

import httpx
import pytest
import uvicorn

from nebula_worker.app import (
    HEADER_DEADLINE,
    HEADER_MODEL_VERSION,
    HEADER_PRIORITY,
    HEADER_PROTOCOL,
    HEADER_REASON,
    HEADER_REQUEST_ID,
    WORKER_PROTOCOL_VERSION,
    create_app,
)
from nebula_worker.config import WorkerConfig
from nebula_worker.runtimes.mock import MockConfig, MockRuntime

MODEL_VERSION = "mock-model:v1"


def worker_config(**overrides: object) -> WorkerConfig:
    base: dict[str, object] = {
        "env": "dev",
        "runtime": "mock",
        "model_version": MODEL_VERSION,
        "model_path": "/dev/null",
        "model_format": "mock",
        "context_window": 4096,
        "max_queue_depth": 4,
        "default_timeout_s": 30.0,
    }
    base.update(overrides)
    return WorkerConfig(**base)  # type: ignore[arg-type]


def deadline_header(seconds: float) -> str:
    return str(int((time.time() + seconds) * 1000))


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@contextlib.asynccontextmanager
async def _serve(
    config: WorkerConfig, runtime: MockRuntime | None = None
) -> AsyncIterator[httpx.AsyncClient]:
    """Run the worker as a real HTTP server for the duration of a test.

    A real socket rather than ``httpx.ASGITransport``, and not for purity: the ASGI
    transport buffers a streaming response and hands it over complete, so a test that
    cancels mid-stream or fills a slot would pass against a worker that streams
    nothing. Three tests here failed exactly that way before this changed, and they
    were the three that mattered most. Anything asserting on *incremental* delivery has
    to cross a socket.
    """
    app = create_app(
        config,
        runtime or MockRuntime(MockConfig(tokens_per_second=150.0, parallel_slots=2)),
    )
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
        deadline = time.monotonic() + 30
        while not server.started and time.monotonic() < deadline:
            if serving.done():  # startup failed; surface the real exception
                await serving
            await asyncio.sleep(0.01)
        assert server.started, "the worker did not start"

        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{port}", timeout=httpx.Timeout(30.0)
        ) as client:
            yield client
    finally:
        server.should_exit = True
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(serving, timeout=30)


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    async with _serve(worker_config()) as c:
        yield c


@pytest.fixture
async def strict_client() -> AsyncIterator[httpx.AsyncClient]:
    async with _serve(worker_config(strict_headers=True)) as c:
        yield c


def headers(**extra: str) -> dict[str, str]:
    base = {
        HEADER_REQUEST_ID: "11111111-1111-4111-8111-111111111111",
        "traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        HEADER_DEADLINE: deadline_header(30),
    }
    base.update(extra)
    return base


# ---------------------------------------------------------------------------
# probes
# ---------------------------------------------------------------------------


class TestProbes:
    async def test_livez_and_readyz_answer_different_questions(
        self, client: httpx.AsyncClient
    ) -> None:
        live = await client.get("/livez")
        ready = await client.get("/readyz")
        assert live.status_code == 200
        assert ready.status_code == 200
        assert ready.json()["model_version"] == MODEL_VERSION

    async def test_livez_stays_up_when_the_model_is_not_ready(self) -> None:
        """Killing a container for a not-yet-ready model would mean it never starts.

        This is obligation 6 expressed as HTTP: the process being alive and the model
        being ready are different facts, so they are different probes.
        """
        runtime = MockRuntime(MockConfig())
        async with _serve(worker_config(), runtime) as c:
            await runtime.unload()  # alive, but no model
            live = await c.get("/livez")
            ready = await c.get("/readyz")
            assert live.status_code == 200, "livez must not fail merely because no model is loaded"
            assert ready.status_code == 503
            assert ready.headers[HEADER_REASON] == "model_not_ready"

    async def test_livez_fails_when_the_engine_is_dead(self) -> None:
        runtime = MockRuntime(MockConfig(process_dead=True))
        async with _serve(worker_config(), runtime) as c:
            assert (await c.get("/livez")).status_code == 503
            assert (await c.get("/readyz")).status_code == 503

    async def test_healthz_describes_the_runtime_and_capabilities(
        self, client: httpx.AsyncClient
    ) -> None:
        body = (await client.get("/healthz")).json()
        assert body["runtime"]["name"] == "mock"
        assert body["capabilities"]["streaming"] is True
        assert body["capabilities"]["supports_cancel"] is True
        assert body["model"]["version"] == MODEL_VERSION
        assert body["protocol"] == WORKER_PROTOCOL_VERSION

    async def test_metrics_are_prometheus_text(self, client: httpx.AsyncClient) -> None:
        resp = await client.get("/metrics")
        assert resp.status_code == 200
        assert "text/plain" in resp.headers["content-type"]
        body = resp.text
        for metric in (
            "nebula_worker_requests_total",
            "nebula_worker_ttft_seconds",
            "nebula_worker_queue_depth",
            "nebula_worker_model_ready",
            "nebula_worker_slots_total",
        ):
            assert metric in body, f"{metric} is missing from /metrics"

    async def test_every_response_carries_the_protocol_version(
        self, client: httpx.AsyncClient
    ) -> None:
        """A gateway talking to a worker from another release must find out at once."""
        for path in ("/livez", "/readyz", "/healthz", "/internal/v1/state"):
            resp = await client.get(path)
            assert resp.headers[HEADER_PROTOCOL] == WORKER_PROTOCOL_VERSION
            assert resp.headers[HEADER_REQUEST_ID]


# ---------------------------------------------------------------------------
# header contract
# ---------------------------------------------------------------------------


class TestHeaderContract:
    async def test_strict_mode_requires_request_id_and_traceparent(
        self, strict_client: httpx.AsyncClient
    ) -> None:
        resp = await strict_client.post(
            "/internal/v1/generate",
            json={"prompt": "hello", "max_tokens": 2},
            headers={HEADER_DEADLINE: deadline_header(5)},
        )
        assert resp.status_code == 400
        assert resp.json()["error"]["code"] == "invalid_headers"
        assert "X-Request-Id" in resp.json()["error"]["message"]

    async def test_strict_mode_requires_a_deadline(self, strict_client: httpx.AsyncClient) -> None:
        """A request with no deadline has nothing that will ever stop it."""
        h = headers()
        del h[HEADER_DEADLINE]
        resp = await strict_client.post(
            "/internal/v1/generate", json={"prompt": "hello", "max_tokens": 2}, headers=h
        )
        assert resp.status_code == 400
        assert "deadline" in resp.json()["error"]["message"].lower()

    async def test_development_mode_supplies_a_default_deadline(
        self, client: httpx.AsyncClient
    ) -> None:
        """curl has to keep working outside production."""
        resp = await client.post("/internal/v1/generate", json={"prompt": "hello", "max_tokens": 2})
        assert resp.status_code == 200

    async def test_malformed_deadline_is_rejected_not_defaulted(
        self, client: httpx.AsyncClient
    ) -> None:
        """The caller meant *some* bound; substituting ours would run longer than agreed."""
        resp = await client.post(
            "/internal/v1/generate",
            json={"prompt": "hello", "max_tokens": 2},
            headers=headers(**{HEADER_DEADLINE: "soon please"}),
        )
        assert resp.status_code == 400
        assert resp.json()["error"]["code"] == "invalid_headers"

    async def test_deadline_in_seconds_is_caught(self, client: httpx.AsyncClient) -> None:
        """Seconds mistaken for milliseconds would pin a slot for weeks."""
        resp = await client.post(
            "/internal/v1/generate",
            json={"prompt": "hello", "max_tokens": 2},
            headers=headers(**{HEADER_DEADLINE: str(int(time.time()) + 30)}),
        )
        assert resp.status_code == 400

    async def test_already_passed_deadline_is_refused(self, client: httpx.AsyncClient) -> None:
        resp = await client.post(
            "/internal/v1/generate",
            json={"prompt": "hello", "max_tokens": 2},
            headers=headers(**{HEADER_DEADLINE: deadline_header(-1)}),
        )
        assert resp.status_code == 400
        assert resp.json()["error"]["code"] == "deadline_passed"

    async def test_unknown_priority_is_refused(self, client: httpx.AsyncClient) -> None:
        resp = await client.post(
            "/internal/v1/generate",
            json={"prompt": "hello", "max_tokens": 2},
            headers=headers(**{HEADER_PRIORITY: "URGENT"}),
        )
        assert resp.status_code == 400

    async def test_model_version_mismatch_is_409(self, client: httpx.AsyncClient) -> None:
        """A stale router must never silently receive the wrong model."""
        resp = await client.post(
            "/internal/v1/generate",
            json={"prompt": "hello", "max_tokens": 2},
            headers=headers(**{HEADER_MODEL_VERSION: "some-other-model:v9"}),
        )
        assert resp.status_code == 409
        assert resp.json()["error"]["code"] == "model_version_mismatch"

    async def test_matching_model_version_is_served(self, client: httpx.AsyncClient) -> None:
        resp = await client.post(
            "/internal/v1/generate",
            json={"prompt": "hello", "max_tokens": 2},
            headers=headers(**{HEADER_MODEL_VERSION: MODEL_VERSION}),
        )
        assert resp.status_code == 200

    async def test_request_id_is_echoed(self, client: httpx.AsyncClient) -> None:
        rid = "22222222-2222-4222-8222-222222222222"
        resp = await client.post(
            "/internal/v1/generate",
            json={"prompt": "hello", "max_tokens": 2},
            headers=headers(**{HEADER_REQUEST_ID: rid}),
        )
        assert resp.headers[HEADER_REQUEST_ID] == rid

    async def test_unknown_body_field_is_refused(self, client: httpx.AsyncClient) -> None:
        """A misspelled parameter must be an error, not a silent default."""
        resp = await client.post(
            "/internal/v1/generate",
            json={"prompt": "hello", "maxTokens": 4},
            headers=headers(),
        )
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# generation
# ---------------------------------------------------------------------------


class TestGenerate:
    async def test_response_shape_matches_the_contract(self, client: httpx.AsyncClient) -> None:
        resp = await client.post(
            "/internal/v1/generate",
            json={"prompt": "nebula streams", "max_tokens": 5},
            headers=headers(),
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["text"]
        assert body["finish_reason"] in {"stop", "length"}
        assert body["usage"]["prompt_tokens"] >= 1
        assert body["usage"]["completion_tokens"] >= 1
        assert body["timing"]["ttft_ms"] >= 0
        assert body["timing"]["queue_ms"] >= 0
        assert body["runtime"]["name"] == "mock"
        assert body["runtime"]["model_version"] == MODEL_VERSION

    async def test_max_tokens_beyond_context_is_refused(self, client: httpx.AsyncClient) -> None:
        resp = await client.post(
            "/internal/v1/generate",
            json={"prompt": "hello", "max_tokens": 9999},
            headers=headers(),
        )
        assert resp.status_code == 400
        assert resp.json()["error"]["code"] == "context_length_exceeded"


class TestStream:
    async def test_stream_is_sse_and_incremental(self, client: httpx.AsyncClient) -> None:
        events: list[dict] = []
        arrivals: list[float] = []
        async with client.stream(
            "POST",
            "/internal/v1/generate/stream",
            json={"prompt": "nebula streams", "max_tokens": 10},
            headers=headers(),
        ) as resp:
            assert resp.status_code == 200
            assert resp.headers["content-type"].startswith("text/event-stream")
            # A buffering proxy would defeat the point of streaming entirely.
            assert resp.headers["X-Accel-Buffering"] == "no"
            async for line in resp.aiter_lines():
                if not line.startswith("data:"):
                    continue
                payload = line[len("data:") :].strip()
                if payload == "[DONE]":
                    break
                events.append(json.loads(payload))
                arrivals.append(time.perf_counter())

        assert len(events) >= 2
        assert arrivals[0] < arrivals[-1], "every event arrived at the same instant"
        finals = [e for e in events if e.get("stop")]
        assert len(finals) == 1
        final = finals[0]
        assert final is events[-1]
        assert final["usage"]["completion_tokens"] >= 1
        assert final["finish_reason"] in {"stop", "length"}
        assert final["timing"]["ttft_ms"] >= 0
        # Only the final event carries usage, so a consumer cannot double count.
        assert all("usage" not in e for e in events[:-1])

    async def test_stream_terminates_with_done(self, client: httpx.AsyncClient) -> None:
        """A finished stream and a dropped connection must not look identical."""
        saw_done = False
        async with client.stream(
            "POST",
            "/internal/v1/generate/stream",
            json={"prompt": "hi", "max_tokens": 3},
            headers=headers(),
        ) as resp:
            async for line in resp.aiter_lines():
                if line.strip() == "data: [DONE]":
                    saw_done = True
        assert saw_done

    async def test_text_matches_the_non_streaming_path(self, client: httpx.AsyncClient) -> None:
        body = {"prompt": "nebula streams", "max_tokens": 6, "seed": 5}
        collected = await client.post("/internal/v1/generate", json=body, headers=headers())

        parts: list[str] = []
        async with client.stream(
            "POST", "/internal/v1/generate/stream", json=body, headers=headers()
        ) as resp:
            async for line in resp.aiter_lines():
                if not line.startswith("data:"):
                    continue
                payload = line[len("data:") :].strip()
                if payload == "[DONE]":
                    break
                parts.append(json.loads(payload).get("content", ""))

        assert "".join(parts) == collected.json()["text"]


# ---------------------------------------------------------------------------
# cancellation, admission, draining
# ---------------------------------------------------------------------------


class TestCancellation:
    async def test_cancel_ends_an_in_flight_stream(self, client: httpx.AsyncClient) -> None:
        rid = "33333333-3333-4333-8333-333333333333"
        seen: list[dict] = []

        async def read() -> None:
            async with client.stream(
                "POST",
                "/internal/v1/generate/stream",
                json={"prompt": "nebula", "max_tokens": 400},
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
        # Wait for generation to be genuinely under way before cancelling, so the test
        # proves an in-flight cancel rather than a race with admission.
        while not seen:
            await asyncio.sleep(0.01)

        cancel = await client.post("/internal/v1/cancel", json={"request_id": rid})
        assert cancel.status_code == 200
        assert cancel.json()["cancelled"] is True
        await asyncio.wait_for(reader, timeout=10)

        final = seen[-1]
        assert final["finish_reason"] == "cancel"
        assert final["usage"]["completion_tokens"] < 400

    async def test_cancel_of_unknown_request_reports_false(self, client: httpx.AsyncClient) -> None:
        resp = await client.post("/internal/v1/cancel", json={"request_id": "nope"})
        assert resp.status_code == 200
        assert resp.json()["cancelled"] is False

    async def test_cancellation_frees_capacity(self, client: httpx.AsyncClient) -> None:
        rid = "44444444-4444-4444-8444-444444444444"
        seen: list[dict] = []

        async def read() -> None:
            async with client.stream(
                "POST",
                "/internal/v1/generate/stream",
                json={"prompt": "nebula", "max_tokens": 400},
                headers=headers(**{HEADER_REQUEST_ID: rid}),
            ) as resp:
                async for line in resp.aiter_lines():
                    if line.startswith("data:") and "[DONE]" not in line:
                        seen.append(json.loads(line[len("data:") :].strip()))

        reader = asyncio.create_task(read())
        while not seen:
            await asyncio.sleep(0.01)
        await client.post("/internal/v1/cancel", json={"request_id": rid})
        await asyncio.wait_for(reader, timeout=10)

        after = await client.post(
            "/internal/v1/generate", json={"prompt": "again", "max_tokens": 2}, headers=headers()
        )
        assert after.status_code == 200

        state = (await client.get("/internal/v1/state")).json()
        assert state["in_flight"] == 0, "a cancelled request leaked its slot"
        assert state["queue_depth"] == 0


class TestAdmission:
    async def test_saturation_is_429_with_retry_after(self) -> None:
        """A full queue must say so, with a hint, rather than timing out.

        A 429 with Retry-After is information the gateway can act on immediately. A
        timeout is the same outcome with the caller's budget spent.
        """
        # One slot, no queue: the second concurrent request has nowhere to go.
        runtime = MockRuntime(MockConfig(tokens_per_second=40.0, parallel_slots=1))
        async with _serve(worker_config(max_queue_depth=0), runtime) as c:
            started: list[dict] = []

            async def occupy() -> None:
                async with c.stream(
                    "POST",
                    "/internal/v1/generate/stream",
                    json={"prompt": "nebula", "max_tokens": 200},
                    headers=headers(**{HEADER_REQUEST_ID: "55555555-5555-4555-8555-555555555555"}),
                ) as resp:
                    async for line in resp.aiter_lines():
                        if line.startswith("data:") and "[DONE]" not in line:
                            started.append(json.loads(line[len("data:") :].strip()))

            holder = asyncio.create_task(occupy())
            while not started:
                await asyncio.sleep(0.01)

            resp = await c.post(
                "/internal/v1/generate",
                json={"prompt": "second", "max_tokens": 4},
                headers=headers(**{HEADER_REQUEST_ID: "66666666-6666-4666-8666-666666666666"}),
            )
            assert resp.status_code == 429
            assert resp.json()["error"]["code"] == "worker_saturated"
            assert resp.headers[HEADER_REASON] == "worker_saturated"
            assert int(resp.headers["Retry-After"]) >= 1

            await c.post(
                "/internal/v1/cancel",
                json={"request_id": "55555555-5555-4555-8555-555555555555"},
            )
            await asyncio.wait_for(holder, timeout=10)

    async def test_request_that_cannot_start_in_time_is_refused_up_front(self) -> None:
        """Never accept work that cannot start before its own deadline.

        Accepting it would spend the caller's remaining budget in a queue and then
        return a timeout, when the gateway could have spent it on a free replica.
        """
        runtime = MockRuntime(MockConfig(tokens_per_second=20.0, parallel_slots=1))
        async with _serve(worker_config(max_queue_depth=8), runtime) as c:
            started: list[dict] = []

            async def occupy() -> None:
                async with c.stream(
                    "POST",
                    "/internal/v1/generate/stream",
                    json={"prompt": "nebula", "max_tokens": 200},
                    headers=headers(**{HEADER_REQUEST_ID: "77777777-7777-4777-8777-777777777777"}),
                ) as resp:
                    async for line in resp.aiter_lines():
                        if line.startswith("data:") and "[DONE]" not in line:
                            started.append(json.loads(line[len("data:") :].strip()))

            holder = asyncio.create_task(occupy())
            while not started:
                await asyncio.sleep(0.01)

            resp = await c.post(
                "/internal/v1/generate",
                json={"prompt": "impatient", "max_tokens": 4},
                # 30 ms of budget against a busy slot: it cannot possibly start in time.
                headers=headers(**{HEADER_DEADLINE: deadline_header(0.03)}),
            )
            assert resp.status_code == 429
            assert resp.json()["error"]["code"] == "deadline_unreachable"

            await c.post(
                "/internal/v1/cancel",
                json={"request_id": "77777777-7777-4777-8777-777777777777"},
            )
            await asyncio.wait_for(holder, timeout=10)


class TestDraining:
    async def test_drain_fails_readiness_and_refuses_new_work(
        self, client: httpx.AsyncClient
    ) -> None:
        drain = await client.post("/internal/v1/drain")
        assert drain.status_code == 200
        assert drain.json()["draining"] is True

        ready = await client.get("/readyz")
        assert ready.status_code == 503
        assert ready.headers[HEADER_REASON] == "draining"

        resp = await client.post(
            "/internal/v1/generate", json={"prompt": "hi", "max_tokens": 2}, headers=headers()
        )
        assert resp.status_code == 503
        assert resp.json()["error"]["code"] == "draining"
        assert resp.headers[HEADER_REASON] == "draining"

        # livez stays up: the process is fine, it is just finishing its work. Failing
        # it here would have Kubernetes kill the pod mid-drain.
        assert (await client.get("/livez")).status_code == 200

    async def test_drain_is_idempotent(self, client: httpx.AsyncClient) -> None:
        """A preStop hook that runs twice must not be a second kind of event."""
        first = await client.post("/internal/v1/drain")
        second = await client.post("/internal/v1/drain")
        assert first.json()["already_draining"] is False
        assert second.json()["already_draining"] is True


class TestState:
    async def test_state_reports_observed_load(self, client: httpx.AsyncClient) -> None:
        body = (await client.get("/internal/v1/state")).json()
        assert body["model_version"] == MODEL_VERSION
        assert body["runtime"] == "mock"
        assert body["ready"] is True
        assert body["slots_total"] >= 1
        assert body["in_flight"] == 0
        assert body["queue_depth"] == 0
        assert body["queue_capacity"] == 4
        assert body["ewma_service_time_ms"] >= 0
        assert body["wait_estimate_ms"] >= 0

    async def test_metrics_count_requests_by_outcome(self, client: httpx.AsyncClient) -> None:
        await client.post(
            "/internal/v1/generate", json={"prompt": "hi", "max_tokens": 3}, headers=headers()
        )
        body = (await client.get("/metrics")).text
        assert 'nebula_worker_requests_total{endpoint="generate",outcome="ok"}' in body
        assert "nebula_worker_tokens_generated_total" in body
