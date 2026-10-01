# NEBULA — API Contracts

**Status:** Phase 0 design. The authoritative artifact will be `packages/api/openapi.yaml`, from
which Go server interfaces, the CLI client, and the dashboard's TypeScript types are generated
(`oapi-codegen` and `openapi-typescript`). Hand-written types on either side of the wire are a
divergence waiting to happen, so the spec is the source of truth and drift fails CI.

Four contracts are described here:

1. [Public inference API](#2-inference-api-openai-compatible) — OpenAI-compatible, used by SDKs.
2. [Control API](#4-control-api) — used by the CLI and dashboard.
3. [Internal worker API](#6-internal-worker-api) — gateway → worker. Never public.
4. [Runtime interface](#7-inferenceruntime-interface) — in-process, inside the worker.

---

## 1. Conventions

| Aspect | Decision |
|--------|----------|
| Base URL | `https://<host>/v1` for everything. The OpenAI-compatible subset is `/v1/{models, chat/completions, completions, embeddings}`; NEBULA's own resources share the same prefix. No collision exists because no OpenAI client calls `/v1/deployments`. |
| Versioning | Major version in the path (`/v1`). Additive changes ship without a version bump. Breaking changes get `/v2` and a `Deprecation` + `Sunset` header on `/v1` for at least one release. `X-Nebula-Api-Version` response header always names the exact build contract. |
| Auth | `Authorization: Bearer nbk_<key>` for API keys; `Authorization: Bearer <jwt>` for CLI/dashboard sessions. The token type is distinguished by prefix, not by guessing. |
| Content type | `application/json`. Streaming is `text/event-stream`. |
| Request ID | Client may send `X-Request-Id` (UUID). Always echoed. Generated (UUIDv7) when absent. Present on every response including errors. |
| Tracing | W3C `traceparent` / `tracestate` accepted and propagated. |
| Idempotency | `Idempotency-Key` honoured on all non-streaming POSTs; replay within 24 h returns the original response with `X-Nebula-Idempotent-Replay: true`. |
| Pagination | Cursor-based: `?limit=50&cursor=<opaque>`; response `{"data": [...], "next_cursor": "...", "has_more": true}`. No offset pagination — it skips and duplicates rows under concurrent writes. |
| Timestamps | RFC 3339 UTC strings. |
| Money | Integer micros plus a `currency` field. Never a float. |
| Partial failure | Never a 200 with an error body. |

### Error envelope

One shape everywhere, OpenAI-compatible so existing SDK error handling works:

```json
{
  "error": {
    "message": "max_tokens (8192) exceeds the remaining context window (4096) for qwen2.5:0.5b-q4",
    "type": "invalid_request_error",
    "code": "context_length_exceeded",
    "param": "max_tokens",
    "request_id": "0192f3c1-7b5a-7c3e-9a11-0e5d2b8f4a21"
  }
}
```

| HTTP | `type` | Retryable | Notes |
|------|--------|-----------|-------|
| 400 | `invalid_request_error` | no | Schema or semantic validation. |
| 401 | `authentication_error` | no | Missing, malformed, expired, or revoked credential. |
| 403 | `permission_error` | no | Valid credential, insufficient scope, or cross-org access. |
| 404 | `not_found_error` | no | Unknown route/resource *within the caller's org*. Never reveals existence in another org. |
| 409 | `conflict_error` | no | Version conflict, rollout already active, duplicate name. |
| 422 | `unprocessable_entity` | no | Well-formed but unsatisfiable (e.g. capacity request no node can ever meet). |
| 429 | `rate_limit_error` | **yes**, per `Retry-After` | Key/org limit or queue full. `X-Nebula-Reason` distinguishes them. |
| 499 | `client_closed_request` | n/a | Client disconnected. Logged, not an SLO violation. |
| 500 | `internal_error` | yes | Never includes internal detail; carries `request_id` for correlation. |
| 502 | `upstream_error` | yes | Worker returned an unusable response. |
| 503 | `service_unavailable` | yes | No healthy endpoint, or a hard dependency is down. `X-Nebula-Reason` gives the cause. |
| 504 | `timeout_error` | conditional | Retryable only if zero tokens were emitted. |

`X-Nebula-Reason` values are a closed, documented set (`no_healthy_endpoint`, `queue_full`,
`queue_timeout`, `capacity_exhausted`, `breaker_open`, `rate_limited_rpm`, `rate_limited_tpm`,
`concurrency_limit`, `model_loading`, `control_plane_degraded`) so clients and dashboards can branch
on them.

Since Phase 7 the gateway queues a request when its deployment has no free capacity, rather than
failing it ([ADR-0034](./architecture-decisions/0034-admission-queue-per-deployment.md)). The queue
answers in one of three ways when it cannot admit: `429 queue_full` with `Retry-After` (the queue is
at its bound), `429 capacity_exhausted` with `Retry-After` (the request set `nebula.queue` to
`"reject"` and there was no room now), or `504 queue_timeout` (its timeout passed while it waited;
nothing was dispatched). A shed request is not charged tokens. Priority comes from the API key and
ages while it waits, so `LOW` is never starved.

---

## 2. Inference API (OpenAI-compatible)

### `POST /v1/chat/completions`

Supported fields: `model` (required — a **route** name), `messages`, `max_tokens`, `temperature`,
`top_p`, `top_k`, `stop`, `seed`, `stream`, `stream_options.include_usage`, `presence_penalty`,
`frequency_penalty`, `user`, `response_format: {"type":"json_object"}` where the runtime supports it.

NEBULA extensions, all optional, all namespaced so they cannot collide with future OpenAI fields:

```json
{
  "nebula": {
    "timeout_ms": 30000,
    "deployment_id": "0192f3c1-...",   // pin a specific deployment; bypasses weighting. Requires scope.
    "queue": "allow" ,                  // allow | reject  — reject fails fast instead of queuing
    "trace": true                        // force-sample this request's trace
  }
}
```

Unsupported OpenAI fields (`logprobs`, `n > 1`, `tools` on runtimes without tool support, `logit_bias`)
return **400 with `code: "unsupported_parameter"`** naming the field. Silently ignoring a parameter
that changes output semantics is worse than refusing it.

**Non-streaming response** is the OpenAI shape, plus a `nebula` block:

```json
{
  "id": "chatcmpl-0192f3c1-7b5a-7c3e-9a11-0e5d2b8f4a21",
  "object": "chat.completion",
  "created": 1774195200,
  "model": "qwen2.5-chat",
  "choices": [{ "index": 0, "message": {"role":"assistant","content":"..."}, "finish_reason": "stop" }],
  "usage": { "prompt_tokens": 42, "completion_tokens": 128, "total_tokens": 170 },
  "nebula": {
    "request_id": "0192f3c1-7b5a-7c3e-9a11-0e5d2b8f4a21",
    "deployment": "qwen-prod",
    "model_version": "qwen2.5:0.5b-q4",
    "variant": "baseline",
    "queue_wait_ms": 0,
    "ttft_ms": 84,
    "duration_ms": 1320,
    "tokens_per_second": 97.0,
    "attempts": 1,
    "estimated_cost": { "micros": 41, "currency": "USD", "basis": "estimate" }
  }
}
```

`usage` comes from the runtime's own token counters. The gateway does not tokenize (axiom A2): a
second tokenizer would eventually disagree with the one that actually generated the text, and the
disagreement would show up in someone's cost report.

**Streaming** (`"stream": true`) — SSE, `text/event-stream`, `Cache-Control: no-cache`,
`X-Accel-Buffering: no`:

```
: nebula.meta {"request_id":"0192f3c1-...","route":"qwen2.5-chat","deployment":"qwen-prod","model_version":"qwen2.5:0.5b-q4","variant":"baseline","attempts":1}

data: {"id":"chatcmpl-...","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","content":""}}]}

data: {"id":"chatcmpl-...","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"Hel"}}]}

: keep-alive

data: {"id":"chatcmpl-...","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}

data: {"id":"chatcmpl-...","object":"chat.completion.chunk","usage":{"prompt_tokens":42,"completion_tokens":128,"total_tokens":170}}

data: [DONE]
```

- The routing context is the first line, as an SSE **comment** (`: nebula.meta {...}`), and in the
  `X-Nebula-Route`, `X-Nebula-Deployment`, `X-Nebula-Model-Version` and `X-Nebula-Variant` response
  headers, which non-streamed responses carry too. Phase 0 specified a named `event: nebula.meta`;
  the Phase 4 end-to-end test showed that the current OpenAI Python SDK yields named events as
  chunks, so it was replaced by a comment, which every SSE parser must ignore
  ([ADR-0029](./architecture-decisions/0029-stream-metadata-is-a-comment.md)). No NEBULA stream
  frame is ever a named event.
- Comment-line keep-alives (`: keep-alive`) every 15 s prevent idle proxy timeouts.
- The usage frame is emitted only when `stream_options.include_usage` is true, matching OpenAI.
- **Mid-stream failure** emits a terminal error frame, never a silent truncation:
  `data: {"error":{"message":"worker terminated","type":"upstream_error","code":"stream_interrupted","request_id":"..."}}`
  followed by `data: [DONE]`. `finish_reason` is absent, so a client can tell a completed response
  from an interrupted one.
- Client disconnect cancels the worker's generation and still records the tokens already produced.

`POST /v1/completions` is the legacy text-completion equivalent; `POST /v1/embeddings` returns the
OpenAI embeddings shape and is available for model versions whose `task` is `embedding`.

> **What is served today (Phase 6).** `POST /v1/chat/completions`, `POST /v1/completions` and
> `GET /v1/models` (plus `GET /v1/models/{route}`) are served by the gateway, exactly as above,
> including streaming, `stream_options.include_usage`, refusal of unsupported parameters by name, the
> `nebula` extension block, rate-limit headers (`x-ratelimit-{limit,remaining,reset}-{requests,tokens}`)
> and client-disconnect cancellation with partial usage. `POST /v1/embeddings` is **not** served: no
> Phase 3 runtime produces embeddings, and an endpoint with nothing behind it would be a 400 in
> disguise. Since Phase 6 routes come from the control plane and every response names the deployment that
> served it; `nebula.attempts` counts the replicas it was offered to, and `nebula.degraded` (and
> `X-Nebula-Degraded`) names a failover route when one served it. Chat messages are rendered
> into a prompt by the gateway with the route's declared template (`chatml`, `llama3` or `plain`);
> reading the template from GGUF metadata is still open (TODO(NEB-144)). `estimated_cost`
> is absent from the `nebula` block until the cost engine (Phase 13) can compute it — absent, not
> zero (axiom A6). `nebula.queue` is `"allow"` (default: wait in the admission queue within the timeout) or
> `"reject"` (fail at once with `429 capacity_exhausted` instead of waiting).
> `nebula.gateway_queue_ms` reports the time spent in the gateway's queue, absent when there was none.

### `GET /v1/models`

OpenAI-shaped list of **routes** the caller can invoke, with a NEBULA extension block:

```json
{ "object": "list",
  "data": [{
    "id": "qwen2.5-chat", "object": "model", "created": 1774195200, "owned_by": "acme",
    "nebula": {
      "route_id": "0192f3...", "context_window": 32768, "task": "chat",
      "targets": [
        {"deployment": "qwen-prod",   "model_version": "qwen2.5:0.5b-q4", "weight": 90, "label": "baseline", "state": "ready"},
        {"deployment": "qwen-canary", "model_version": "qwen2.5:1.5b-q4", "weight": 10, "label": "canary",   "state": "ready"}
      ],
      "streaming": true, "embeddings": false
    }
  }]
}
```

---

## 3. Health and metrics endpoints

Every service, identical shape — one of the "every service must have" requirements, enforced by a
shared `packages/telemetry` handler rather than reimplemented six times.

| Path | Purpose | Semantics |
|------|---------|-----------|
| `/livez` | liveness | Process is not deadlocked. Never checks dependencies — a failing dependency must not cause a restart loop. |
| `/readyz` | readiness | Can serve *now*: dependencies reachable, schema version matches, caches warm. Fails during drain. |
| `/healthz` | human/debug | JSON with per-dependency status, version, build SHA, uptime, schema version. Operator-only. |
| `/metrics` | Prometheus | Not exposed publicly; scraped in-cluster. |
| `/debug/pprof/*` | profiling | Go services, bound to localhost or gated by a debug flag. |

---

## 4. Control API

Used by the CLI and dashboard, served by `nebula-controlplane`, reached through the gateway. All
paths are org-scoped by the caller's credential; `org_id` is never a client-supplied parameter.

> **What is served today (Phase 2).** This section describes the finished control API.
> [`packages/api/openapi.yaml`](../packages/api/openapi.yaml) describes only what a handler actually
> answers, is embedded in the binary and served at `/openapi.yaml`, and a test fails if the two
> diverge from the mounted routes in either direction. A specification that documents unbuilt
> endpoints trains clients to call them and turns a 404 into a support ticket, so the two files have
> different jobs: this one is the design, that one is the contract.
>
> Implemented in Phase 2: models and versions (with `finalize`, `fail` and archive), deployments
> (create, patch, scale, rollback, stop, start, status, revisions, transitions, and an admin-only
> explicit transition), users, API keys, audit logs, `GET /v1/me`, and
> `GET /v1/lifecycle/deployment-states` — the state machine as data, so a CLI or dashboard renders the
> graph the database enforces instead of keeping its own copy.
>
> Since Phase 4 the control API is reached through the gateway's admin proxy, which authenticates
> the key and forwards a signed identity (§6a); the control plane still accepts API keys directly for
> development and tests.
>
> Since Phase 5, a version whose artifact is uploaded to NEBULA's own store gets a presigned
> upload target, and `finalize` answers `202` while the control plane hashes the stored bytes
> ([ADR-0031](./architecture-decisions/0031-artifacts-verified-asynchronously.md)); a version
> registered against an external URI keeps `verification: declared_checksum`.
>
> Since Phase 6: routes (`/v1/routes`, create, read, list, atomic weight and policy replacement,
> delete) and `GET /v1/policies/routing`. A deployment that a route targets cannot be deleted
> (`409 in_use`) until the route stops targeting it. The `route` shortcut on `POST /v1/deployments`
> shown below is not implemented; create the route separately.
>
> Not yet implemented: embeddings (§2), rollouts and experiments, usage, costs, nodes, rate-limit
> policies, pricing, and the `/replicas`, `/events`, `/metrics` and `/logs` sub-resources of a
> deployment. Session tokens are refused with `unsupported_credential`; API keys are the only
> accepted credential until the dashboard.

### Models and versions

```
GET    /v1/models/registry                       list registered models (NEBULA shape, not OpenAI's)
POST   /v1/models                                create a model
GET    /v1/models/{model_id}
DELETE /v1/models/{model_id}                     soft delete; 409 if any version is deployed
POST   /v1/models/{model_id}/versions            begin version creation → returns upload target
GET    /v1/models/{model_id}/versions
GET    /v1/model-versions/{id}
POST   /v1/model-versions/{id}/finalize          checksum verified → status ready → IMMUTABLE
DELETE /v1/model-versions/{id}                   archive; 409 if referenced by a live deployment
```

Artifact upload is a two-phase flow so multi-gigabyte weights never pass through the JSON API:

```
POST /v1/models/{id}/versions
  → 201 { "model_version_id": "...", "upload": {
            "method": "PUT", "url": "https://minio.../sha256/<hex>?X-Amz-...",
            "headers": {...}, "expires_at": "...", "max_bytes": 53687091200 } }
client PUTs bytes directly to object storage
POST /v1/model-versions/{id}/finalize { "checksum_sha256": "..." }
  → control plane verifies size + checksum + parses GGUF metadata
  → 200 { "status": "ready", "context_window": 32768, "quantization": "q4_k_m", ... }
```

The client-declared checksum is verified server-side, not trusted. A mismatch sets `failed` with the
computed value in `failure_reason`.

### Deployments

```
GET    /v1/deployments                           ?state=&model=&limit=&cursor=
POST   /v1/deployments
GET    /v1/deployments/{id}                      spec + observed status + conditions
PATCH  /v1/deployments/{id}                      spec update → new revision, generation bump
DELETE /v1/deployments/{id}                      409 if a route still targets it (unless ?force=true)
GET    /v1/deployments/{id}/revisions
GET    /v1/deployments/{id}/status                lightweight, for polling
GET    /v1/deployments/{id}/replicas              live pod view, from informer cache (not PostgreSQL)
GET    /v1/deployments/{id}/events                worker_events history
GET    /v1/deployments/{id}/metrics              ?window=1h&step=1m — Prometheus-backed
GET    /v1/deployments/{id}/logs                 ?follow=true&since=&container= — SSE
POST   /v1/deployments/{id}/scale                { "replicas": 3 }
POST   /v1/deployments/{id}/rollback             { "revision": 4 }  (or omit for previous)
```

`POST /v1/deployments` request:

```json
{
  "name": "qwen-prod",
  "model_version_id": "0192f3c1-...",
  "replicas": 2,
  "resources": { "cpu_milli": 4000, "memory_mib": 8192, "gpu_count": 0 },
  "autoscaling": { "enabled": true, "min_replicas": 1, "max_replicas": 6,
                   "target_queue_wait_ms": 500, "target_concurrency": 4 },
  "queue": { "max_depth": 128, "default_timeout_ms": 60000 },
  "runtime_overrides": { "n_ctx": 8192, "n_parallel": 4 },
  "routing_policy": "least_loaded",
  "route": { "model_name": "qwen2.5-chat", "weight": 100 }
}
```

Response is `202 Accepted` with the created deployment at `state: "pending"` and a `Location`
header. **Deliberate:** deployment is a reconciled intent, not a synchronous action. Returning 201
"created" while pods are still pulling a 4 GB artifact would be a lie about what happened. Clients
poll `/status` or watch `/events`.

The response includes the resolved placement decision, so a rejection is explainable *before*
anything is created:

```json
{ "state": "pending",
  "placement": { "admitted": true, "constraints": {
      "nodeSelector": {"nebula.dev/accelerator": "cpu"},
      "requests": {"cpu": "4", "memory": "8Gi"},
      "limits": {"memory": "10Gi"},
      "topologySpreadConstraints": [{"topologyKey":"kubernetes.io/hostname","maxSkew":1}] },
    "capacity_note": "2 of 3 nodes can host this pod; cluster headroom after placement: 6 CPU, 14 GiB" } }
```

An inadmissible request returns `422` with which constraint failed and the observed cluster capacity
— never a deployment that sits `Pending` forever with no explanation.

### Routes, rollouts, experiments

```
GET    /v1/routes
POST   /v1/routes                                { model_name, targets:[{deployment_id, weight}] }
GET    /v1/routes/{id}
PATCH  /v1/routes/{id}                           set weights atomically; rejects sums ≠ 100
DELETE /v1/routes/{id}

POST   /v1/rollouts                              start a canary
GET    /v1/rollouts                              ?state=
GET    /v1/rollouts/{id}                          current step, per-step analysis, per-arm observations
POST   /v1/rollouts/{id}/pause
POST   /v1/rollouts/{id}/resume
POST   /v1/rollouts/{id}/promote                 skip remaining steps → 100%
POST   /v1/rollouts/{id}/abort                   → weights back to baseline

POST   /v1/experiments
GET    /v1/experiments/{id}/results              per-arm comparison
POST   /v1/experiments/{id}/stop
```

`GET /v1/experiments/{id}/results` returns observations with their uncertainty, and says plainly when
the sample does not support a conclusion:

```json
{
  "arms": [
    {"label":"baseline","deployment":"qwen-prod","requests":1842,"error_rate":0.0049,
     "p50_ms":410,"p95_ms":980,"tokens_per_second":96.2,"cost_per_1k_tokens_micros":38},
    {"label":"variant-b","deployment":"qwen-canary","requests":1795,"error_rate":0.0061,
     "p50_ms":388,"p95_ms":1120,"tokens_per_second":101.4,"cost_per_1k_tokens_micros":44}
  ],
  "comparison": {
    "p50_ms": {"delta": -22, "relative": -0.054, "ci95": [-41, -3], "method": "bootstrap_10k"},
    "error_rate": {"delta": 0.0012, "ci95": [-0.0031, 0.0055],
                   "note": "interval spans zero; no difference detected at this sample size"}
  },
  "significance": {
    "claimed": false,
    "reason": "Bootstrap confidence intervals are reported. No multiple-comparison correction or sequential-testing correction is applied, so these intervals must not be read as a peeking-safe significance test."
  }
}
```

### Usage, cost, audit, infrastructure

```
GET /v1/usage        ?from=&to=&group_by=deployment|model|api_key|day&limit=&cursor=
GET /v1/costs        ?from=&to=&group_by=  → micros, currency, pricing_profile version, basis:"estimate"
GET /v1/audit-logs   ?actor=&action=&resource_type=&from=&to=
GET /v1/nodes        cluster inventory + utilization (admin scope)
GET /v1/nodes/{name}
GET /v1/api-keys  |  POST /v1/api-keys  |  DELETE /v1/api-keys/{id}
GET /v1/policies/routing  |  GET /v1/policies/rate-limits
GET /v1/pricing      |  POST /v1/pricing  (new immutable version)
```

Two endpoints in this group exist beyond what the sketch above shows, both added in Phase 2:
`POST /v1/model-versions/{id}/fail` records why a version could not be made ready, because "failed"
with no cause is the state that makes an incident unexplainable an hour later; and
`POST /v1/deployments/{id}/transition` performs an explicit state transition. The second is
admin-scoped and audited, and exists for two reasons: during an incident desired state has to be
correctable in a way that is *recorded* rather than a hand-written `UPDATE` that is not, and until the
Phase 5 controller runs, nothing else advances an in-flight state. The transition is attributed to the
calling credential in `deployment_state_transitions`, so a state a human set stays distinguishable
from one a controller observed.

`POST /v1/api-keys` returns the plaintext key exactly once:

```json
{ "id":"0192...", "name":"ci", "prefix":"nbk_7Kq2mZ4",
  "key":"nbk_7Kq2mZ4x…", "scopes":["inference:invoke"], "priority":"NORMAL",
  "warning":"Store this key now. It is not retrievable." }
```

Every cost response carries its assumptions inline, because a cost figure without its basis is
misinformation:

```json
{ "total": {"micros": 412900, "currency": "USD"},
  "basis": "estimate",
  "pricing_profile": {"name":"default","version":3,"effective_from":"2026-09-01T00:00:00Z"},
  "assumptions": [
    "GPU allocation is charged whole; fractional MIG accounting is not configured.",
    "Idle replica time is attributed to the deployment, not to individual requests.",
    "Request sampling rate 1.0; no extrapolation applied."
  ] }
```

### Scopes

| Scope | Grants |
|-------|--------|
| `inference:invoke` | `/v1/chat/completions`, `/v1/completions`, `/v1/embeddings`, `GET /v1/models` |
| `inference:pin` | the `nebula.deployment_id` override |
| `models:read` / `models:write` | registry |
| `deployments:read` / `deployments:write` | deployments, scale, rollback |
| `rollouts:write` | rollouts, experiments, route weights |
| `usage:read` | usage, costs |
| `audit:read` | audit logs |
| `admin` | keys, policies, pricing, nodes |

---

## 5. CLI contract

`nebula` is a thin, fully scriptable client over the Control API. It contains **no business logic** —
if the CLI can do something the API cannot, that is a bug.

| Command | Calls |
|---------|-------|
| `nebula login [--api-key \| --oidc]` | `POST /v1/auth/token`; stores in OS keyring, falls back to `~/.nebula/config.yaml` (0600) |
| `nebula whoami` | `GET /v1/auth/me` |
| `nebula models` | `GET /v1/models/registry` |
| `nebula model register ./m.gguf --name qwen2.5 --version 0.5b-q4` | create version → direct upload with resumable progress → finalize |
| `nebula deploy qwen2.5:0.5b-q4 [--replicas 2] [--cpu 4] [--memory 8Gi] [--gpu 1] [--route qwen2.5-chat] [--wait]` | `POST /v1/deployments`, then watch `/events` when `--wait` |
| `nebula deployment list \| get \| delete` | deployments CRUD |
| `nebula logs <deployment> [-f] [--since 10m]` | `GET /v1/deployments/{id}/logs` (SSE) |
| `nebula metrics <deployment> [--window 1h]` | `GET /v1/deployments/{id}/metrics`, sparkline output |
| `nebula scale <deployment> --replicas 3` | `POST .../scale` |
| `nebula rollback <deployment> [--revision 4]` | `POST .../rollback` |
| `nebula rollout start <route> --canary <deployment> [--steps 10,25,50,100]` | `POST /v1/rollouts` |
| `nebula rollout status \| promote \| abort` | rollout verbs |
| `nebula usage \| costs [--from --to --group-by]` | usage/cost reads |
| `nebula nodes` | `GET /v1/nodes` |
| `nebula keys create \| list \| revoke` | API keys |
| `nebula version` | build info; warns on client/server contract skew |

Conventions: `--output table|json|yaml` on every read command (`json` is stable and documented, for
scripting); non-zero exit codes distinguishing user error (2), auth (3), not found (4), conflict (5),
server error (10); `--wait` blocks with a live status line and exits non-zero if the operation ends
in a failed state; colour and progress disabled automatically when not a TTY; `NEBULA_API_URL`,
`NEBULA_API_KEY`, `NEBULA_ORG` environment overrides.

---

## 6. Internal worker API

Gateway → worker only. Never exposed through the gateway; protected by NetworkPolicy and
(optionally) mTLS. Versioned independently via `X-Nebula-Worker-Protocol: 1`.

```
POST /internal/v1/generate        non-streaming
POST /internal/v1/generate/stream SSE
POST /internal/v1/cancel          { "request_id": "..." }
GET  /internal/v1/state           loaded version, queue depth, in-flight, slots, EWMAs
POST /internal/v1/drain           stop accepting, finish in-flight (used by preStop)
GET  /livez /readyz /healthz /metrics
```

Required request headers — a worker **rejects** a request missing them, rather than inventing
defaults, so an untraced or deadline-less request can never enter the runtime:

| Header | Meaning |
|--------|---------|
| `X-Request-Id` | correlation |
| `traceparent` | trace context |
| `X-Nebula-Deadline` | absolute Unix milliseconds. The worker aborts at this instant regardless of progress. |
| `X-Nebula-Priority` | `LOW\|NORMAL\|HIGH` for the worker's local queue |
| `X-Nebula-Model-Version` | asserted version; mismatch → 409, so a stale router can never silently get the wrong model |

Worker response body (non-streaming):

```json
{ "text": "...", "finish_reason": "stop",
  "usage": {"prompt_tokens": 42, "completion_tokens": 128},
  "timing": {"queue_ms": 4, "prefill_ms": 61, "decode_ms": 1180, "ttft_ms": 84,
             "total_ms": 1245},
  "runtime": {"name": "llamacpp", "version": "b11157", "model_version": "qwen2.5:0.5b-q4",
              "slot": 2, "kv_cache_used_bytes": null}
}
```

`finish_reason` is one of `stop`, `length`, `cancel`, `deadline`, `error`. `cancel` and `deadline` are
deliberately distinct: one is a client changing its mind and the other is the system running out of
budget, and a router that cannot tell them apart cannot tell a healthy worker from an overloaded one.

`kv_cache_used_bytes` is `null` when the engine does not report it, which is the case for the pinned
llama.cpp build. Reporting zero would read as "there is KV cache to spare", which is a worse answer
than "unknown" — see [observability.md §2.3](./observability.md#23-worker-and-runtime).

Admission control: when the local queue is full the worker returns **429 with `Retry-After`** and
`X-Nebula-Reason: worker_saturated`. It never accepts work it cannot start before the deadline —
accepting and then timing out wastes the deadline budget that the gateway could have spent on
another replica.

---

## 6a. Internal control-plane API

Gateway → control plane only. Never proxied by the gateway (it answers 404 for any `/internal/`
path), absent from `openapi.yaml` by design, and a test fails if it ever appears there.

```
GET /internal/v1/credentials/{prefix}     a key's record: stored HMAC, org id and slug, scopes,
                                          priority, expiry, revocation, rate-limit policy
```

Every call carries `X-Nebula-Auth-Context`: a payload signed with HMAC-SHA256 under
`NEBULA_INTERNAL_AUTH_SECRET`, bound to the audience `nebula-controlplane`, valid for
`NEBULA_INTERNAL_AUTH_MAX_AGE` (default 10 s). Internal routes accept only `actor_type: service`; a
tenant identity is refused with 403 `service_only`, and a service identity is refused on every public
route with 403 `service_identity`. The same header carries the caller's verified identity on every
admin call the gateway proxies (docs/security-boundaries.md §2, B2), in place of the API key, which
the gateway strips. Why the record rather than a verification verdict:
[ADR-0030](./architecture-decisions/0030-gateway-credentials-through-the-control-plane.md).

A successful `DELETE /v1/api-keys/{id}` sets `X-Nebula-Revoked-Key-Prefix`, which the gateway consumes
to evict the key from the shared credential cache and removes before the response reaches the client.

---

## 7. `InferenceRuntime` interface

The abstraction that keeps NEBULA from marrying one engine. Python, inside the worker.

```python
class InferenceRuntime(Protocol):
    name: str
    version: str

    async def load(self, spec: ModelSpec) -> LoadResult: ...
    async def unload(self) -> None: ...
    async def generate(self, req: GenerationRequest) -> GenerationResult: ...
    def stream(self, req: GenerationRequest) -> AsyncIterator[TokenChunk]: ...
    async def cancel(self, request_id: str) -> bool: ...
    async def health(self) -> RuntimeHealth: ...
    def metrics(self) -> RuntimeMetrics: ...
    def capabilities(self) -> RuntimeCapabilities: ...
```

Contract obligations every adapter must satisfy — written down because these are exactly the places
where adapters usually diverge and the divergence surfaces as inconsistent user-visible behaviour:

1. `load` is idempotent, reports `load_duration_ms` and resident bytes, and raises a typed error
   (`ArtifactMissing`, `ChecksumMismatch`, `InsufficientMemory`, `UnsupportedQuantization`)
   distinguishable from a transient failure.
2. `stream` yields the **first** chunk as soon as the first token exists (TTFT is a measured
   quantity, not a batched artefact), and its final chunk always carries `usage` and `finish_reason`.
3. `cancel` actually stops computation and frees the slot. Returning `True` without stopping is a
   contract violation, because the gateway's capacity model would then be wrong.
4. Token counts come from the runtime's tokenizer. Adapters never estimate.
5. `capabilities()` declares `streaming`, `embeddings`, `tools`, `json_mode`, `max_context`,
   `parallel_slots`, `supports_cancel`. The gateway's `CapabilityBased` strategy and the validation
   layer both read this, so an unsupported parameter is a clean 400 rather than a surprise at
   generation time.
6. `health()` separates *process alive* from *model ready*, so Kubernetes probes map correctly.
7. Every adapter ships with the same conformance test suite (`tests/runtime_conformance.py`). The
   `mock` adapter is the reference implementation of correct behaviour; a new adapter is not
   considered integrated until it passes the same suite.

Shipping order: `mock` (Phase 3, CI and load-test workhorse), `llamacpp` (Phase 3, the real one),
`vllm` (post-v1, when GPU hardware exists — designed now so the interface is not shaped around a
single engine's quirks).

**Implemented in Phase 3**, as written above: `runtimes/base.py` is the protocol, `runtimes/mock.py`
and `runtimes/llamacpp.py` are the two adapters, and one conformance suite runs unchanged against both
with nothing skipped or overridden for either. Two notes on how the obligations turned out in practice:

- Obligation 3 is why the llama.cpp adapter supervises the engine over HTTP rather than calling into
  it: `llama-server` abandons a generation when its client disconnects, so cancelling really does stop
  computing. An in-process binding could stop *delivering* tokens and nothing more, which would satisfy
  a naive test and lie to the gateway's capacity model.
- Obligation 6 is why there is no single `up` gauge. `/livez` and `/readyz`, and
  `engine_process_alive` and `model_ready` beside them, are separate because "the engine died" and "the
  model is still loading" need different responses.

See [workers/inference/README.md](../workers/inference/README.md).

---

## 8. OpenAPI and generated code

- `packages/api/openapi.yaml` — one document covering the public and control APIs, with `x-internal`
  tags on operations the dashboard should not surface. Examples for every operation, and every error
  code enumerated.
- `packages/api/worker.openapi.yaml` — internal worker contract, versioned separately.
- Generated: Go server interfaces + typed client (`oapi-codegen`), TypeScript client and types
  (`openapi-typescript`), CLI request/response structs.
- CI gates: spec lints (`spectral`); generated code regenerated and `git diff --exit-code` must be
  clean; breaking-change detection against the previous tag (`oasdiff`) fails the build unless the
  PR carries an `api-breaking` label and a `/v2` plan. This is the mechanical enforcement of "never
  silently change APIs".
- The spec is served at `GET /openapi.yaml` and rendered at `/docs` by the gateway, so the running
  system documents itself and drift is visible to users, not just to CI.
