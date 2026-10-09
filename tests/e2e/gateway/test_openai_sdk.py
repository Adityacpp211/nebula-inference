"""The Phase 4 exit criterion: the OpenAI Python SDK, unmodified, against NEBULA.

A hand-written curl proves less than an SDK does: the SDK deserializes responses
into typed objects, parses SSE itself, maps error statuses onto exception classes,
and fails on a missing required field. If these tests pass, existing OpenAI client
code works against NEBULA by changing ``base_url`` and ``api_key`` and nothing else.

Run through ``scripts/e2e-gateway.sh`` (``make e2e-gateway``), which starts the
stack and exports the environment below. The backend is the mock runtime: it
generates deterministic placeholder tokens, not language, and every response says
so in ``nebula.runtime``. The assertions are therefore about the protocol — shapes,
counts, ordering, errors — never about the text.
"""

from __future__ import annotations

import json
import os
import time
import uuid

import httpx
import openai
import pytest
from openai import OpenAI

GATEWAY = os.environ["NEBULA_GATEWAY_URL"]
WORKER = os.environ["NEBULA_WORKER_URL"]
API_KEY = os.environ["NEBULA_API_KEY"]
MODEL = "nebula-mock"


@pytest.fixture(scope="module")
def client() -> OpenAI:
    # max_retries=0: a retry would hide exactly the errors these tests look for.
    return OpenAI(base_url=f"{GATEWAY}/v1", api_key=API_KEY, max_retries=0)


def _messages() -> list[dict[str, str]]:
    return [
        {"role": "system", "content": "You are a test."},
        {"role": "user", "content": "Say something."},
    ]


def test_models_list_is_openai_shaped(client: OpenAI) -> None:
    models = client.models.list()
    ids = [m.id for m in models.data]
    assert MODEL in ids
    m = client.models.retrieve(MODEL)
    assert m.id == MODEL and m.object == "model" and m.owned_by == "dev"


def test_chat_completion(client: OpenAI) -> None:
    raw = client.chat.completions.with_raw_response.create(
        model=MODEL, messages=_messages(), max_tokens=12, temperature=0
    )
    resp = raw.parse()
    assert resp.object == "chat.completion"
    assert resp.model == MODEL
    choice = resp.choices[0]
    assert choice.message.role == "assistant"
    assert choice.message.content
    assert choice.finish_reason in ("stop", "length")
    assert resp.usage is not None
    assert resp.usage.completion_tokens > 0
    assert (
        resp.usage.total_tokens
        == resp.usage.prompt_tokens + resp.usage.completion_tokens
    )

    # The nebula block survives the SDK as an extra field, and names the runtime,
    # so a stub response can never pass for a real model's.
    extra = resp.model_extra or {}
    assert extra["nebula"]["runtime"] == "mock"
    assert extra["nebula"]["deployment"] == "mock-e2e"

    # Request id and rate-limit headers, as OpenAI clients expect them.
    assert raw.headers["x-request-id"]
    assert int(raw.headers["x-ratelimit-limit-requests"]) > 0
    assert int(raw.headers["x-ratelimit-remaining-tokens"]) >= 0


def test_chat_streaming_with_usage(client: OpenAI) -> None:
    """Every frame the SDK sees must be a real chunk. The routing context travels
    as headers and an SSE comment, which the SDK never surfaces (ADR-0029)."""
    raw = client.chat.completions.with_raw_response.create(
        model=MODEL,
        messages=_messages(),
        max_tokens=16,
        stream=True,
        stream_options={"include_usage": True},
    )
    assert raw.headers["x-nebula-deployment"] == "mock-e2e"
    assert raw.headers["x-nebula-model-version"] == "e2e:mock"
    stream = raw.parse()
    text: list[str] = []
    finish = None
    usage = None
    chunks = 0
    for chunk in stream:
        chunks += 1
        assert chunk.object == "chat.completion.chunk"
        if chunk.usage is not None:
            usage = chunk.usage
            assert chunk.choices == []
            continue
        delta = chunk.choices[0].delta
        if delta.content:
            text.append(delta.content)
        if chunk.choices[0].finish_reason:
            finish = chunk.choices[0].finish_reason
    assert text, "no content streamed"
    assert finish in ("stop", "length")
    assert usage is not None, "include_usage must produce a usage chunk"
    assert usage.completion_tokens > 0
    assert chunks >= 3


def test_streaming_and_non_streaming_agree_on_counts(client: OpenAI) -> None:
    # The mock is deterministic for a given prompt and seed, so both paths must
    # report the same runtime counts: usage comes from one place.
    kwargs = {"model": MODEL, "messages": _messages(), "max_tokens": 10, "seed": 7}
    plain = client.chat.completions.create(**kwargs)
    usage = None
    streamed: list[str] = []
    for chunk in client.chat.completions.create(
        **kwargs, stream=True, stream_options={"include_usage": True}
    ):
        if chunk.usage:
            usage = chunk.usage
        elif chunk.choices and chunk.choices[0].delta.content:
            streamed.append(chunk.choices[0].delta.content)
    assert usage is not None and plain.usage is not None
    assert usage.completion_tokens == plain.usage.completion_tokens
    assert usage.prompt_tokens == plain.usage.prompt_tokens
    assert "".join(streamed) == plain.choices[0].message.content


def test_text_completion(client: OpenAI) -> None:
    resp = client.completions.create(model=MODEL, prompt="once upon", max_tokens=8)
    assert resp.object == "text_completion"
    assert resp.choices[0].text
    assert resp.usage is not None and resp.usage.completion_tokens > 0

    streamed = list(
        client.completions.create(
            model=MODEL, prompt="once upon", max_tokens=8, stream=True
        )
    )
    assert any(c.choices and c.choices[0].text for c in streamed)


def test_unsupported_parameter_is_refused_by_name(client: OpenAI) -> None:
    with pytest.raises(openai.BadRequestError) as info:
        client.chat.completions.create(model=MODEL, messages=_messages(), n=2)
    body = info.value.body
    assert isinstance(body, dict)
    assert body["code"] == "unsupported_parameter"
    assert body["param"] == "n"

    with pytest.raises(openai.BadRequestError) as info:
        client.chat.completions.create(
            model=MODEL,
            messages=_messages(),
            tools=[{"type": "function", "function": {"name": "f", "parameters": {}}}],
        )
    assert info.value.body["param"] == "tools"


def test_unknown_model_is_not_found(client: OpenAI) -> None:
    with pytest.raises(openai.NotFoundError) as info:
        client.chat.completions.create(model="no-such-model", messages=_messages())
    assert info.value.body["code"] == "model_not_found"


def test_bad_key_is_an_authentication_error() -> None:
    bad = OpenAI(base_url=f"{GATEWAY}/v1", api_key="nbk_" + "A" * 50, max_retries=0)
    with pytest.raises(openai.AuthenticationError):
        bad.chat.completions.create(model=MODEL, messages=_messages())


def test_context_window_is_enforced(client: OpenAI) -> None:
    with pytest.raises(openai.BadRequestError) as info:
        client.chat.completions.create(
            model=MODEL, messages=_messages(), max_tokens=100_000
        )
    assert info.value.body["code"] == "context_length_exceeded"


def _admin() -> httpx.Client:
    return httpx.Client(
        base_url=GATEWAY, headers={"Authorization": f"Bearer {API_KEY}"}, timeout=10
    )


def test_admin_api_through_the_gateway() -> None:
    """B2 end to end: the gateway authenticates, signs, and the real control plane
    verifies the signature and answers for the right org."""
    with _admin() as admin:
        me = admin.get("/v1/me")
        assert me.status_code == 200, me.text
        assert me.json()["org_slug"] == "dev"
        assert (
            me.headers.get_list("x-request-id")
            and len(me.headers.get_list("x-request-id")) == 1
        )

        # The internal surface is unreachable through the public edge.
        internal = admin.get("/internal/v1/credentials/" + API_KEY[:11])
        assert internal.status_code == 404


def test_key_lifecycle_through_the_gateway() -> None:
    """Create a key through the admin proxy, use it for inference, revoke it, and
    watch it stop working at once — the revocation evicts the shared cache."""
    with _admin() as admin:
        created = admin.post(
            "/v1/api-keys",
            json={
                "name": f"e2e-{uuid.uuid4().hex[:8]}",
                "scopes": ["inference:invoke"],
            },
        )
        assert created.status_code == 201, created.text
        key = created.json()["key"]
        key_id = created.json()["id"]

        scoped = OpenAI(base_url=f"{GATEWAY}/v1", api_key=key, max_retries=0)
        resp = scoped.chat.completions.create(
            model=MODEL, messages=_messages(), max_tokens=4
        )
        assert resp.choices[0].message.content

        # This key carries inference:invoke only: the admin API refuses it.
        with httpx.Client(
            base_url=GATEWAY, headers={"Authorization": f"Bearer {key}"}
        ) as limited:
            assert limited.get("/v1/deployments").status_code == 403

        revoked = admin.delete(f"/v1/api-keys/{key_id}")
        assert revoked.status_code == 204
        assert "x-nebula-revoked-key-prefix" not in revoked.headers

        with pytest.raises(openai.AuthenticationError) as info:
            scoped.chat.completions.create(
                model=MODEL, messages=_messages(), max_tokens=4
            )
        assert info.value.body["code"] == "credential_revoked"


def _slots_busy() -> int:
    state = httpx.get(f"{WORKER}/internal/v1/state", timeout=5).json()
    return int(state["slots_busy"]) + int(state["in_flight"])


def test_client_disconnect_frees_the_worker_slot() -> None:
    """A client that walks away mid-stream must not keep a worker slot busy."""
    with (
        httpx.Client(timeout=30) as http,
        http.stream(
            "POST",
            f"{GATEWAY}/v1/chat/completions",
            headers={"Authorization": f"Bearer {API_KEY}"},
            json={
                "model": MODEL,
                "messages": _messages(),
                "max_tokens": 2000,
                "stream": True,
            },
        ) as resp,
    ):
        assert resp.status_code == 200
        seen = 0
        for line in resp.iter_lines():
            if line.startswith("data: {") and '"content"' in line:
                seen += 1
            if seen >= 3:
                break
    # The connection is closed now. The slot must come back promptly.
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if _slots_busy() == 0:
            break
        time.sleep(0.1)
    assert _slots_busy() == 0, (
        "the worker slot was not released after the client disconnected"
    )


def test_usage_record_for_a_cancelled_stream() -> None:
    """The partial usage of a cancelled stream is recorded from the runtime's count."""
    log_path = os.environ.get("NEBULA_E2E_GATEWAY_LOG")
    if not log_path:
        pytest.skip("gateway log not available")
    request_id = str(uuid.uuid4())
    with (
        httpx.Client(timeout=30) as http,
        http.stream(
            "POST",
            f"{GATEWAY}/v1/chat/completions",
            headers={"Authorization": f"Bearer {API_KEY}", "X-Request-Id": request_id},
            json={
                "model": MODEL,
                "messages": _messages(),
                "max_tokens": 2000,
                "stream": True,
            },
        ) as resp,
    ):
        for line in resp.iter_lines():
            if '"content"' in line:
                break
    deadline = time.monotonic() + 10
    record = None
    while time.monotonic() < deadline and record is None:
        with open(log_path, encoding="utf-8") as f:
            for line in f:
                if request_id in line and "usage.record" in line:
                    record = json.loads(line)["usage"]
        time.sleep(0.2)
    assert record is not None, "no usage record for the cancelled request"
    assert record["outcome"] == "client_cancelled"
    assert record["token_source"] == "runtime"
    assert record["completion_tokens"] >= 1
