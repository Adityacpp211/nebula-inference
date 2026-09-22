# NEBULA — System Architecture

**Status:** Phase 0 design, awaiting approval. No implementation code exists yet.
**Audience:** engineers working on NEBULA.
**Companion documents:** [data-model.md](./data-model.md), [api.md](./api.md), [deployment-architecture.md](./deployment-architecture.md), [repository-structure.md](./repository-structure.md), [roadmap.md](./roadmap.md), [risk-register.md](./risk-register.md), [ADRs](./architecture-decisions/).

---

## 1. What NEBULA is

NEBULA is a self-hostable inference control plane. It takes model artifacts, materializes them as
inference workloads on Kubernetes, and exposes them behind a single OpenAI-compatible endpoint with
routing, queuing, autoscaling, progressive delivery, and full observability.

NEBULA is **not** an inference engine. It does not implement attention kernels, batching, or
quantization. Those belong to llama.cpp, vLLM, and TGI. NEBULA's engineering content is everything
*around* the engine: identity, placement, admission, routing, reliability, rollout, accounting.

NEBULA is **not** a Kubernetes replacement. It does not bind pods to nodes, does not implement a
scheduler plugin, and does not reimplement the Deployment controller. It expresses intent as
Kubernetes objects and constraints, and it owns the decisions Kubernetes has no opinion about:
*which model version should serve this request, right now, given queue depth and health*.

### The one-sentence test

> A developer runs `nebula deploy qwen2.5:0.5b-q4`, gets back a URL and an API key, points the
> OpenAI SDK at it, streams tokens, and can then answer — from real telemetry — how many tokens
> were generated, what p95 latency was, how much it cost, which pod served it, and what happened
> when that pod was killed.

Everything in this document exists to make that sentence true.

---

## 2. Design axioms

These are the rules that resolve arguments later. They are ordered; earlier axioms win.

| # | Axiom | Consequence |
|---|-------|-------------|
| A1 | **Desired state lives in PostgreSQL. Actual state lives in Kubernetes and Redis.** | The control plane never treats a pod list as truth to be persisted; it reconciles. Live replica state is never a column in `deployments`. |
| A2 | **Single source of truth per fact, and that source is named.** | Token counts come from the runtime, not a gateway-side tokenizer. Node capacity comes from the Kubernetes API, not from worker self-reports. |
| A3 | **The token path is HTTP. The control path is NATS.** | No broker hop between a client and generated tokens. See [ADR-0003](./architecture-decisions/0003-push-dispatch-and-nats-scope.md). |
| A4 | **Cross-cutting logic is a library, not a service.** | Routing, queuing, reliability, and placement are Go packages consumed by whichever binary needs them. A new process must justify a new failure domain. See [ADR-0004](./architecture-decisions/0004-router-as-library.md). |
| A5 | **Anything a request touches is observable by `request_id` and `trace_id`.** | Every hop propagates W3C `traceparent`. Every log line is structured and carries both IDs. |
| A6 | **Nothing is fabricated.** | No synthetic metrics, no placeholder dashboards, no invented costs. A number that cannot be measured yet is absent, not estimated silently. |
| A7 | **Model versions are immutable; deployments reference versions.** | Rollback is re-pointing traffic at an existing revision, never rebuilding an artifact. |
| A8 | **A degraded control plane must not break inference.** | Gateways serve from cached routing state when the control plane is down. Reconciliation stops; serving does not. |
| A9 | **Every stub is declared.** | A development stub is named `mock`/`dev`, gated by config, refuses to run when `NEBULA_ENV=production`, and has a `TODO(NEB-nnn)` pointing at its replacement. |

---

## 3. Component inventory

Six first-party services, four shared infrastructure dependencies, one CLI, one dashboard.

### 3.1 First-party services

| Service | Language | Replicas | State | Purpose |
|---------|----------|----------|-------|---------|
| `nebula-gateway` | Go | N (HPA) | stateless; caches in-process + Redis | Data plane. Auth, rate limit, validation, routing, admission queue, dispatch, streaming, usage emission. |
| `nebula-controlplane` | Go | N | stateless; PostgreSQL is truth | Admin API. Owns all writes to PostgreSQL. Registry, deployments, keys, policies, usage read models, audit. |
| `nebula-controller` | Go | 1 active (leader-elected) | none | Reconcilers: deployment → Kubernetes objects; rollout/canary state machine; node & capacity inventory; usage ingestion. |
| `nebula-autoscaler` | Go | 1 active (leader-elected) | none | Scaling control loop. Reads signals, writes `desired_replicas`, records decisions. |
| `nebula-worker` | Python | per deployment replica | model weights in node cache | Inference worker. Hosts a runtime adapter, bounded local queue, health, metrics, token accounting. |
| `nebula-dashboard` | React + TS | N | none | Operator UI. Talks only to the gateway. |

`nebula` (Go) is the CLI, distributed as a single static binary.

### 3.2 Infrastructure dependencies, and why each exists

| Dependency | Justification | What breaks without it |
|------------|---------------|------------------------|
| **PostgreSQL** | Relational, transactional desired state with foreign keys and constraints. Multi-tenant isolation via row-level security. | Writes to the admin API fail. Inference continues from cached routing state (A8). |
| **Redis** | Atomic rate-limit counters (Lua), API-key lookup cache, routing-state snapshot for gateway cold start, idempotency keys. All of it is transient and rebuildable. | Rate limiting fails **closed** to a conservative in-process limiter; key lookups fall through to PostgreSQL; gateway cold start is slower. |
| **NATS + JetStream** | Control-plane signalling and durable event streams. Core NATS for high-frequency, latest-wins worker heartbeats (ephemeral, no persistence wanted). JetStream for usage/audit/event streams that must survive a consumer restart because they feed billing and audit. | Heartbeats stop → router degrades to Kubernetes endpoint readiness. Usage records buffer on the worker/gateway, then spool to disk, then shed. See §8.4. |
| **Prometheus / OTel Collector / Tempo / Loki / Grafana** | Metrics, trace and log pipelines. See [ADR-0020](./architecture-decisions/README.md#adr-0020). | Observability degrades. Nothing in the request path depends on them. |

**Rejected dependencies and why:** Kafka (durability and replay we don't need, operational weight a
laptop shouldn't carry); a service mesh (mTLS and retries are handled in-process; a mesh would hide
the reliability engineering this project exists to demonstrate); etcd (we already have Kubernetes'
etcd via the API server, and PostgreSQL for the rest); a separate feature-flag service
(`routing_policies` rows already are the flags).

---

## 4. Service boundaries

The contract for each service is *what it owns*, *what it may call*, and — most importantly — *what
it must never do*. Violations of the "must never" column are architectural bugs, not style
preferences.

### 4.1 `nebula-gateway`

**Owns:** the public edge. Every external request enters here.

| Responsibility | Detail |
|----------------|--------|
| Authentication | API key (`Authorization: Bearer nbk_…`) or short-lived JWT for dashboard/CLI sessions. |
| Authorization | Scope check against the route's org. Deny cross-org access before routing. |
| Request validation | JSON schema + semantic limits (max tokens vs model context window, message count, payload size). |
| Rate limiting | Redis token bucket: requests/min, tokens/min, max concurrency — per API key and per org. |
| Identity propagation | Mint `X-Request-Id` (UUIDv7) if absent; create or continue `traceparent`. |
| Routing | In-process router library resolves `model` → route → weighted deployment → specific worker endpoint. |
| Admission queue | Bounded, priority-aware, deadline-aware queue used only when no endpoint has capacity. |
| Dispatch | HTTP to worker, with timeout, retry policy, circuit breaker, deadline header, cancellation. |
| Streaming | SSE (OpenAI-compatible `data:` frames) and WebSocket. Proxies frames with backpressure, no buffering of the full response. |
| Accounting | Emits one usage event per request to JetStream, including partial usage on client disconnect. |
| Admin proxy | Proxies `/v1/{models,deployments,…}` admin verbs to `nebula-controlplane` with a signed internal auth context. |

**Must never:** load a model, tokenize prompts for billing, hold unbounded per-request memory,
write to PostgreSQL, make routing decisions inline rather than through the router library, or
retry a request that has already emitted a token to the client.

### 4.2 `nebula-controlplane`

**Owns:** PostgreSQL. It is the only service with write credentials to the primary schema.

Registry (models, versions, artifacts, checksum verification), deployment specs and revisions,
organizations/users/API keys, routing policies, routes and traffic targets, rollout and experiment
definitions, usage read models, audit log, pricing profiles.

**Must never:** talk to the Kubernetes API (that is the controller's job), talk to a worker, sit in
the inference request path, or be reachable from outside the cluster.

### 4.3 `nebula-controller`

**Owns:** the loop between desired state and Kubernetes. Kubernetes-style, with a work queue,
informers, exponential backoff, and periodic full resync.

Reconcilers:

1. **Deployment reconciler** — `deployments` row + `deployment_revisions` spec → `Deployment`,
   `Service`, `ConfigMap`, `PodDisruptionBudget`, optional `PersistentVolumeClaim`. Computes
   placement constraints from the model version's hardware profile via `packages/scheduler`.
   Writes observed status back (`observed_generation`, conditions, `ready_replicas`).
2. **Rollout reconciler** — canary/AB state machine. Advances weights on `route_targets`, evaluates
   analysis windows against Prometheus, promotes or aborts.
3. **Inventory reconciler** — Node/Pod informers → `nodes` cache table and capacity inventory in
   Redis; emits `worker_events` on crash, OOM, eviction, model-load failure.
4. **Usage ingester** — durable JetStream consumer → batched `requests` inserts and hourly
   `usage_records` rollups. Idempotent on `request_id`.

**Must never:** serve public traffic, be the only writer of *desired* state (users write desired
state through the control plane API), or mutate Kubernetes objects it does not own (everything it
creates carries `nebula.dev/managed-by` and an owner reference).

### 4.4 `nebula-autoscaler`

**Owns:** `deployments.desired_replicas` within `[min_replicas, max_replicas]`, and
`autoscaling_events`.

Reads: queue wait time p95, in-flight concurrency per replica, request rate, GPU/CPU utilization,
worker-reported saturation. Writes a decision with the full signal vector attached so any scaling
event is explainable after the fact.

**Must never:** call the Kubernetes API directly (it changes the *desired* record and lets the
controller reconcile — one writer per Kubernetes object), or scale outside configured bounds.

Why a separate process rather than a loop inside the controller: blast radius and cadence. A wedged
or misconfigured autoscaler must be independently disableable (`replicas: 0`) without stopping
deployment reconciliation, and operators must be able to reason about "is NEBULA scaling right now"
by looking at one pod's logs. See [ADR-0021](./architecture-decisions/README.md#adr-0021).

### 4.5 `nebula-worker`

**Owns:** one model version, loaded, on one pod. Executes inference.

Structure: a thin FastAPI/uvicorn server exposing NEBULA's worker contract, wrapping a
**runtime adapter** that implements `InferenceRuntime` (`load`, `unload`, `generate`, `stream`,
`health`, `metrics`, `cancel`). The adapter is selected by the model version's `runtime` field.

Adapters:

- `llamacpp` — supervises the upstream `llama-server` process as a child, proxies to it over
  localhost, and owns lifecycle, readiness, and metric translation. Supervision rather than
  in-process bindings, so we inherit llama.cpp's continuous batching and avoid the Python GIL on
  the token path. See [ADR-0015](./architecture-decisions/README.md#adr-0015).
- `mock` — deterministic, seeded token generator with configurable latency, error injection, and
  fake VRAM accounting. Used by CI, load tests, and failure tests so they need no model weights.
  Declared stub per axiom A9: refuses to start when `NEBULA_ENV=production`.
- `vllm` — designed now, implemented when a GPU node exists. In-process `AsyncLLMEngine`.

**Must never:** talk to PostgreSQL, talk to another worker, make routing decisions, accept work
beyond its bounded queue, or trust a request that carries no deadline.

### 4.6 `nebula-dashboard`

Static React bundle. Calls the gateway only. Renders exclusively from API responses and Prometheus
queries proxied by the gateway. No client-side metric synthesis, no demo data path.

---

## 5. Architecture diagram

```
                     ┌──────────────┐   ┌───────────────┐   ┌─────────────┐
   external           │  nebula CLI  │   │ OpenAI SDK /  │   │  Dashboard  │
   clients            │   (Go bin)   │   │ curl / any    │   │  (React)    │
                     └──────┬───────┘   └───────┬───────┘   └──────┬──────┘
                            │                   │                  │
                            └─────────┬─────────┴──────────────────┘
                                      │  HTTPS  (API key | session JWT)
╔═════════════════════════════════════▼═══════════════════════════════════════════╗
║  DATA PLANE                     nebula-gateway  (N replicas, stateless)         ║
║  ┌───────────────────────────────────────────────────────────────────────────┐  ║
║  │ authn → authz → validate → ratelimit → requestID/trace → ROUTER →         │  ║
║  │ ADMISSION QUEUE (bounded, priority, deadline) → DISPATCH (retry, breaker, │  ║
║  │ timeout, cancel) → STREAM (SSE/WS) → USAGE EMIT                           │  ║
║  └───────┬─────────────────────────────┬──────────────────────┬──────────────┘  ║
╚══════════│═════════════════════════════│══════════════════════│═════════════════╝
           │ HTTP + SSE                  │ admin proxy          │ events/usage
           │ (token path, no broker)     │                      │
 ┌─────────▼──────────┐      ┌───────────▼──────────┐    ┌──────▼──────────────┐
 │  nebula-worker     │      │ nebula-controlplane  │    │  NATS + JetStream   │
 │  ┌──────────────┐  │      │  admin API, registry │    │  heartbeats (core)  │
 │  │ bounded queue│  │      │  ONLY writer of SQL  │    │  usage/events (JS)  │
 │  ├──────────────┤  │      └───────────┬──────────┘    └──────┬──────────────┘
 │  │InferenceRun- │  │                  │                      │
 │  │time adapter  │  │            ┌─────▼──────┐               │
 │  │ llamacpp│mock│  │            │ PostgreSQL │◄──────────────┤ usage ingester
 │  │ │vllm    │    │  │            │  DESIRED   │               │
 │  └──────┬───────┘  │            │   STATE    │               │
 │         │ child    │            └─────▲──────┘               │
 │  ┌──────▼───────┐  │                  │ read spec            │
 │  │ llama-server │  │      ┌───────────┴──────────┐           │
 │  └──────────────┘  │      │  nebula-controller   ├───────────┘
 └────────▲───────────┘      │  (leader-elected)    │
          │ creates          │  deployment/rollout/ │      ┌──────────────────┐
          │                  │  inventory/usage     ├─────►│   Kubernetes API │
          └──────────────────┤  reconcilers         │      │  ACTUAL STATE    │
                             └───────────┬──────────┘      └──────────────────┘
                                         │ desired_replicas
                             ┌───────────▼──────────┐      ┌──────────────────┐
                             │  nebula-autoscaler   │      │  Redis           │
                             │  (leader-elected)    │◄────►│  ratelimit,cache,│
                             └──────────────────────┘      │  capacity,idemp. │
                                                           └──────────────────┘
 OBSERVABILITY:  all services → OTel Collector → { Prometheus | Tempo | Loki } → Grafana
```

A rendered version of this diagram is in the project [README](../README.md).

---

## 6. The request path

This is the hottest, most correctness-sensitive path in the system. Named stages, named failure
behaviour.

```
 1. ACCEPT        gateway receives POST /v1/chat/completions
 2. IDENTIFY      X-Request-Id (UUIDv7) minted or adopted; traceparent created or continued
 3. AUTHENTICATE  key prefix → Redis cache → PostgreSQL; HMAC compare; org + scopes resolved
 4. AUTHORIZE     scope allows inference; requested model belongs to caller's org
 5. VALIDATE      schema, max_tokens ≤ context_window − prompt estimate, payload size, params
 6. ADMIT         Redis token bucket: RPM, TPM (optimistic reservation), concurrency
 7. RESOLVE       model name → route → weighted traffic targets → candidate deployment
 8. SELECT        strategy chain picks a worker endpoint from healthy candidates
 9. QUEUE         if no endpoint has capacity: enqueue with priority + absolute deadline
10. DISPATCH      HTTP POST to worker with X-Nebula-Deadline; breaker consulted; attempt recorded
11. GENERATE      worker admits or rejects (429 + Retry-After); runtime streams tokens
12. STREAM        gateway relays SSE frames; TTFT recorded on first token
13. FINALIZE      usage from the runtime's own counters; emit usage event; close trace
```

### 6.1 Routing: resolution then selection

Two distinct steps that are frequently conflated. Keeping them separate is what makes canary
deployments and A/B tests possible without touching the request code.

**Resolution** — `"model": "qwen2.5-7b"` is a **route**, not a deployment. A route is the stable
public identity of a capability. It fans out to one or more weighted `route_targets`, each pointing
at a deployment (which in turn pins one immutable model version). Weighted selection is
deterministic given a bucketing key: `hash(request_id)` by default, `hash(org_id + user_hint)` when
an experiment requires sticky assignment. See [ADR-0009](./architecture-decisions/README.md#adr-0009).

**Selection** — within the chosen deployment, pick an endpoint. Implemented as a strategy chain
over a shared `EndpointSnapshot`:

| Strategy | Signal | Use |
|----------|--------|-----|
| `Filter` (always first) | readiness, heartbeat freshness, breaker state, model-loaded, saturation flag | Removes ineligible endpoints. Not optional. |
| `LeastLoaded` (default) | in-flight requests + queue depth per endpoint | Best default for inference, where request cost varies by orders of magnitude. |
| `RoundRobin` | — | Baseline, and a control arm for benchmarking other strategies. |
| `LatencyAware` | EWMA of TTFT and tokens/sec per endpoint | Heterogeneous hardware. |
| `CostAware` | `pricing_profiles` × node class | Prefer CPU nodes under a latency budget, spill to GPU. |
| `CapabilityBased` | required context window, tools, embeddings, quantization floor | Route long-context requests only to versions that can serve them. |
| `Failover` (wrapper) | primary set empty or all-breaker-open | Falls back to a declared secondary route; records degradation. |

Strategies are `packages/routing` implementations of one interface, composed by configuration from
a `routing_policies` row. No routing decision is written anywhere outside this package.

### 6.2 Router state: how the gateway knows

The router needs data that changes per second. Fetching it per request would be absurd; trusting
stale data would be wrong. Three overlapping sources, in priority order:

1. **Kubernetes EndpointSlice informer** (authoritative for existence and readiness).
2. **Worker heartbeats over core NATS**, ~1 Hz per pod: in-flight count, queue depth, queue wait
   EWMA, tokens/sec EWMA, loaded model version, VRAM/RAM used. Latest-wins, no persistence.
   Heartbeats older than 3 intervals mark an endpoint `stale` and it drops out of selection.
3. **Local observations** — every dispatch updates the in-process breaker and latency EWMA for that
   endpoint. Locally observed failure is believed immediately; it does not wait for a heartbeat.

On cold start a gateway replica loads a Redis snapshot of routes and endpoints (written by the
controller) so its first request is not served blind, then converges via informer + heartbeats.

### 6.3 Admission queue

The queue exists to convert "we are momentarily over capacity" into *latency* instead of *errors* —
but only within a bounded, deadline-respecting budget.

- Bounded per deployment (`max_queue_depth`). Full → HTTP 429 with `Retry-After`, immediately. No
  unbounded growth, ever.
- Three priorities: `LOW`, `NORMAL`, `HIGH`. Priority comes from the API key's policy, not the
  request body, so callers cannot promote themselves.
- Every entry carries an **absolute deadline** derived from the client's timeout, the policy
  default, and the remaining request budget. Entries past deadline are dropped at dequeue *and* by
  a sweeper, and counted as `queue_timeout`, not as an inference error.
- Aging: an entry's effective priority increases with age, bounded, so `LOW` cannot starve forever.
- Cancellation: client disconnect cancels the context, which removes the entry and records
  `client_cancelled`.
- Exported per deployment: depth, oldest-entry age, wait p50/p95/p99, drop counts by reason.

A second bounded queue lives in the worker, in front of the runtime, for exactly the same reasons at
a different scale. Two tiers are intentional: the gateway queue is about *fairness across tenants*,
the worker queue is about *keeping the runtime's batch window full*.

### 6.4 Reliability rules

| Mechanism | Rule |
|-----------|------|
| Timeouts | Three layers: connect, TTFT (time to first token), and total. TTFT timeout is the one that matters — a model that has not produced a token in 20 s is wedged. |
| Retries | Only for classified-retryable errors, only if **no token has reached the client**, capped attempts, budget-limited (retries may not exceed 10% of traffic to a deployment). |
| Retryable | connection refused/reset, 502/503/504, worker 429 with `Retry-After`, TTFT timeout with zero tokens, pod disappeared mid-dial. |
| Non-retryable | all 4xx validation/auth/quota, context-length exceeded, model-load failure (retry the *route*, not the same pod), any error after first token, non-idempotent replay without an `Idempotency-Key`. |
| Backoff | exponential, base 50 ms, factor 2, cap 2 s, **full jitter**, and never past the request deadline. |
| Circuit breaker | per (gateway replica × endpoint), sliding window, half-open single-probe recovery. Deliberately *local*, not distributed — see [ADR-0014](./architecture-decisions/README.md#adr-0014). |
| Hedging | designed, default **off**. Hedged requests double GPU cost; enabled only per-policy with a p95 trigger. |
| Graceful shutdown | SIGTERM → fail readiness → drain in-flight up to `terminationGracePeriodSeconds` → stop. `preStop` sleep covers EndpointSlice propagation. |

---

## 7. Control loops

Every control loop in NEBULA has the same shape, deliberately: observe → diff → act → record.

### 7.1 Deployment reconciliation

```
desired (PostgreSQL)                      actual (Kubernetes)
  deployments.generation = 7                Deployment.spec.replicas = 2
  desired_replicas = 3                      2 Ready, 1 CrashLoopBackOff
  model_version = qwen2.5:0.5b-q4           pod-c: model load failed (checksum)
  resources = {cpu:2, mem:6Gi}
        │                                            │
        └──────────────► controller: diff ◄──────────┘
                              │
        ┌─────────────────────┼──────────────────────────────┐
        ▼                     ▼                              ▼
  patch replicas        emit worker_event                set condition
  to 3                  (model_load_failed)              Degraded=True
                        + audit entry                    reason=ArtifactChecksum
```

Reconcile is triggered by: a `nebula.control.reconcile` NATS nudge on spec change (fast path),
informer events on owned objects, and a periodic full resync (default 60 s) that catches everything
the fast paths missed. Errors requeue with exponential backoff and are surfaced as deployment
conditions, never swallowed.

### 7.2 Autoscaling

Signals are normalized to a ratio against target, and the maximum across signals wins — the same
shape as HPA, so behaviour is predictable to anyone who knows Kubernetes.

```
desired = ceil( current × max_over_signals( observed / target ) )
```

| Signal | Target (default) | Notes |
|--------|------------------|-------|
| queue wait p95 | 500 ms | Primary. Directly reflects user-visible pain. |
| in-flight concurrency per replica | runtime's declared parallel slots | Primary for GPU saturation. |
| requests/sec per replica | policy | Secondary; steadier than latency, slower to react. |
| GPU utilization | 70% | GPU deployments only, from DCGM. |
| CPU utilization | 70% | CPU deployments. |

Guards, all mandatory: scale-up stabilization 30 s, scale-down stabilization 300 s, cooldown after
any change, max step (double up, 10%-or-1 down), `[min,max]` clamp, and a hard "no scaling while a
rollout is progressing" interlock. Every decision writes an `autoscaling_events` row containing the
full signal vector, so oscillation is diagnosable rather than mysterious.

Scale-to-zero is designed (gateway holds the request in an *activation* queue with an extended
deadline while replicas come up) but is a Phase 9 stretch goal, off by default, because a cold model
load is seconds-to-minutes and that must be a deliberate operator choice.

### 7.3 Progressive delivery

```
 promote v2:   [ 0% ] → 10% → 25% → 50% → 100%
                  │      │      │      │
                  └──────┴──────┴──────┴── after each step: hold, then analyse
                                             error_rate  ≤ baseline + 1pp ?
                                             p95 latency ≤ baseline × 1.2 ?
                                             min_requests reached ?
                        ┌────────────────────┴───────────────────┐
                    all pass                                any fail
                        │                                        │
                 advance to next step              abort → weights to 100% baseline
                                                   → rollout state = rolled_back
                                                   → audit + event + CLI/dashboard reason
```

Analysis queries Prometheus over the hold window for both arms. A step with fewer than
`min_requests` observations does **not** pass; it either extends the window or, at the operator's
configured choice, pauses for manual promotion. NEBULA reports observed differences with
counts and confidence intervals where the sample supports it, and explicitly declines to claim
statistical significance otherwise — per axiom A6 and the A/B testing requirement.

A/B tests use the same weighting machinery with no promotion intent: two or more targets, fixed
weights, sticky bucketing, and a comparison read model (latency, error rate, tokens/sec, tokens per
request, cost per request per arm).

**Rollback** is not a special case of rollout: it is "set `route_targets` to the previous
revision's deployment, weight 100%". Because model versions and `deployment_revisions` are
immutable, the target always still exists.

---

## 8. Event flows

NATS subject namespace, with delivery semantics stated per subject.

| Subject | Transport | Producer | Consumers | Semantics |
|---------|-----------|----------|-----------|-----------|
| `nebula.worker.heartbeat.<deployment>.<pod>` | core NATS | worker | gateways, autoscaler | at-most-once, latest-wins, ~1 Hz. Loss is tolerable and expected. |
| `nebula.usage.records` | JetStream (WorkQueue, 7 d, dedup 2 m on `request_id`) | gateway | controller usage ingester | at-least-once, idempotent insert. Must not lose data — feeds cost. |
| `nebula.deployment.events` | JetStream (Limits, 30 d) | controller | dashboard live feed, audit sink | at-least-once, ordered per deployment. |
| `nebula.autoscale.events` | JetStream (Limits, 30 d) | autoscaler | dashboard, controller (audit) | at-least-once. |
| `nebula.control.reconcile` | core NATS | control plane | controller | best-effort nudge; periodic resync is the safety net, so loss only costs latency. |
| `nebula.worker.lifecycle` | JetStream | worker, controller | controller, dashboard | model load start/ok/fail, drain, crash. |
| `nebula.batch.<deployment>` | JetStream (WorkQueue) | gateway | workers | reserved for the async batch API (post-v1). The only place NEBULA puts inference work on a broker. |

### 8.1 Deploy flow

```
CLI: nebula model register ./qwen2.5-0.5b-q4.gguf
  → control plane: create model + model_version (status=uploading), issue upload target
  → CLI streams artifact to object store (content-addressed by sha256)
  → control plane: verify checksum + parse GGUF header (context window, quant, params)
      status → ready ;  version becomes IMMUTABLE
  → audit_log: model_version.created

CLI: nebula deploy qwen2.5:0.5b-q4 --replicas 2
  → control plane: TX { deployments row (generation=1), deployment_revisions rev=1,
                        route + route_targets(100% → this deployment) }
  → publish nebula.control.reconcile
  → controller: placement constraints from hardware_profile (packages/scheduler)
                capacity admission against inventory  → fits? else Deployment
                condition Pending/InsufficientCapacity with a real explanation
  → controller: apply Deployment + Service + ConfigMap + PDB
  → kubelet: initContainer pulls artifact into node cache (verify sha256) → worker starts
  → worker: runtime.load() → readyz → heartbeat begins → EndpointSlice ready
  → controller: observed_generation=1, ready_replicas=2, condition Ready=True
  → gateway routers converge; endpoint enters selection
  → CLI prints endpoint URL + example curl
```

### 8.2 Streaming inference flow

```
client ──POST /v1/chat/completions {stream:true}──► gateway
  span gateway.request
    ├─ authn/authz/validate/ratelimit
    ├─ span router.resolve   (route → target, weight bucket, experiment variant)
    ├─ span router.select    (strategy chain, candidates considered, chosen pod)
    ├─ span queue.wait       (only if enqueued; records wait_ms, priority, deadline)
    └─ span dispatch.attempt(1)
         └─ worker: span worker.generate
              ├─ span worker.queue        (bounded, deadline honoured)
              └─ span runtime.stream      (TTFT, tokens/sec, stop reason)
       ◄── SSE frames relayed to client as they arrive (no full-response buffering)
    └─ finalize: usage from runtime counters → nebula.usage.records → JetStream
client disconnect mid-stream
    → context cancel → worker cancel → partial usage still emitted (tokens were produced)
```

### 8.3 Worker crash and recovery

```
pod-b OOMKilled
  t+0.0s  in-flight requests to pod-b fail (connection reset)
  t+0.0s  gateway classifies retryable; requests with zero tokens emitted are retried on pod-a/c
          requests already streaming are terminated with an error frame + incomplete usage
  t+0.1s  local breaker for pod-b opens after threshold
  t+~1s   heartbeat gap detected → pod-b marked stale, removed from selection
  t+~2s   EndpointSlice removes pod-b (authoritative)
  t+~2s   controller Pod informer → worker_events(type=oom) + deployment condition Degraded
  t+…     kubelet restarts container; startupProbe covers model load
  t+…     readyz passes → heartbeats resume → breaker half-open probe succeeds → back in rotation
  autoscaler: sees the capacity dip in its signals; scales up only if the guards allow
```

### 8.4 Dependency failure expectations

Documented now because a design that has not decided its degradation behaviour has not been
designed. Each of these gets a failure test in Phase 16.

| Failure | Expected behaviour |
|---------|--------------------|
| PostgreSQL unavailable | Admin API returns 503 with `Retry-After`. Inference **continues**: gateways serve from cached routes; auth serves from Redis cache with extended TTL and a hard cap, then fails closed. Controller stops reconciling and reports it; it does not delete Kubernetes objects it cannot verify. |
| Redis unavailable | Rate limiting falls back to a conservative in-process limiter (documented as approximate); key lookups hit PostgreSQL; capacity snapshot rebuilt from informers. No request is dropped solely because Redis is down. |
| NATS unavailable | Heartbeats stop → routing degrades to EndpointSlice readiness + local observations (still correct, less optimal). Usage events buffer in memory (bounded), then spool to a local file, then shed with a counter and a loud log. Usage loss is measured and reported, never silent. |
| All workers for a route unhealthy | 503 with `X-Nebula-Reason: no_healthy_endpoint`, plus optional `Failover` strategy to a declared secondary route. |
| Model fails to load | Pod never becomes ready; startupProbe fails; `worker_events` + deployment condition carry the actual cause (checksum, OOM, unsupported quant). Rollout aborts rather than shifting traffic to a deployment that never became ready. |
| Kubernetes API unavailable | Controller and autoscaler stop making changes and say so. Gateways keep serving from last-known endpoints. |
| Node lost | Kubernetes reschedules; NEBULA's job is to not send traffic there (heartbeat staleness catches it before the node is formally NotReady) and to record it. |

---

## 9. Multi-tenancy and security posture

- **Tenant scoping is enforced twice.** Every query is org-scoped in code, *and* PostgreSQL
  row-level security policies filter on a per-transaction `app.current_org`. Defence in depth: an
  application bug becomes an empty result set, not a data leak.
- **No shared workers across orgs in v1.** A deployment belongs to exactly one org. Sharing a
  process that holds KV cache across tenants is a side-channel and a noisy-neighbour problem we are
  not solving yet; it is documented as a non-goal rather than pretended.
- **API keys** are shown once. Stored as HMAC-SHA256 with a server-side pepper, plus an indexed
  8-character prefix for O(1) lookup and constant-time comparison. Not bcrypt/argon2 — those are
  correct for user passwords and wrong for a hot path hit on every request; the entropy of a
  generated 256-bit key makes a slow KDF unnecessary. See
  [ADR-0011](./architecture-decisions/README.md#adr-0011).
- **Internal trust.** The control plane is never internet-reachable. It accepts an internal auth
  context only from the gateway's ServiceAccount, enforced by NetworkPolicy and a signed header;
  architecture is mTLS-ready (cert-manager) without requiring a mesh.
- **Audit log** is append-only at the database-role level (no UPDATE/DELETE grant) and records
  actor, action, before/after, request ID, and IP for every state change.
- **No plaintext secrets.** Dev uses Kubernetes Secrets rendered by Helm from values; production
  path is External Secrets / Sealed Secrets. Nothing secret enters Git, logs, traces, or the
  dashboard.
- **Error hygiene.** Public errors carry a stable `type`/`code` and a request ID. Pod names, node
  names, internal hostnames, and SQL detail never cross the gateway boundary — they go to logs and
  traces, which are operator-only.

---

## 10. Observability model

Three pillars, one correlation key (`request_id`), one trace ID that appears in every log line.

**Metrics** (Prometheus, RED + USE + inference-specific):
`nebula_requests_total{route,deployment,model_version,status,error_class}`,
`nebula_request_duration_seconds` (histogram), `nebula_ttft_seconds`,
`nebula_tokens_total{direction}`, `nebula_tokens_per_second`,
`nebula_queue_depth`, `nebula_queue_wait_seconds`, `nebula_queue_drops_total{reason}`,
`nebula_inflight_requests`, `nebula_retries_total{reason}`,
`nebula_breaker_state`, `nebula_ratelimit_rejections_total`,
`nebula_model_load_duration_seconds`, `nebula_worker_up`,
`nebula_deployment_replicas{state}`, `nebula_reconcile_duration_seconds`,
`nebula_reconcile_errors_total`, `nebula_autoscale_decisions_total{direction,reason}`,
`nebula_rollout_step`, `nebula_usage_events_dropped_total`, `nebula_estimated_cost_micros_total`.
GPU metrics come from DCGM, CPU/memory from cAdvisor/node-exporter — NEBULA does not invent its own
copies of numbers Kubernetes already publishes (axiom A2).

**Traces** (OpenTelemetry → Collector → Tempo): the span tree in §8.2, with head sampling
(configurable, 100% in dev) plus always-sample on error and on latency above a threshold.

**Logs** (`log/slog` JSON in Go, structlog in Python → stdout → Promtail → Loki): mandatory fields
`ts, level, msg, service, version, request_id, trace_id, span_id, org_id, route, deployment,
model_version, error_class`. Prompt and completion content is **never** logged by default; an
opt-in per-org debug flag with retention limits exists for support, and is audited when used.

**Dashboards** shipped as Grafana JSON in the repo: Fleet Overview, Deployment Detail, Request
Path, Queue & Autoscaling, Rollouts, Cost. Alert rules for error-rate SLO burn, queue saturation,
breaker-open, reconcile failure, usage-event drop, and rollout abort.

---

## 11. Cost engine

Cost is computed, reproducible, and labelled as an estimate everywhere it appears.

```
compute_cost   = Σ over resource classes ( allocated_units × seconds × unit_price_micros / 3600 )
attributed     = compute_cost × ( request_compute_seconds / replica_busy_seconds )
storage_cost   = artifact_bytes × price_per_gib_month × fraction_of_month
request_cost   = attributed + per_token_markup(prompt_tokens, completion_tokens)
```

- Integer **micros** throughout. No floating-point money.
- `pricing_profiles` rows are immutable and versioned with `effective_from`; every `usage_records`
  row stores the pricing profile version it was computed with, so a historical invoice is
  reproducible after prices change.
- Attribution is by measured compute-seconds, and the model's assumptions (idle time is charged to
  the deployment, not the request; GPU allocation is charged whole, not fractionally, unless MIG is
  configured) are stated in the docs and in the API response, because an unexplained cost number is
  worse than none.

---

## 12. Explicit non-goals for v1

Stated so that "missing" is never confused with "broken":

1. No custom pod scheduler or Kubernetes scheduler plugin ([ADR-0005](./architecture-decisions/0005-cooperate-with-kubernetes.md)).
2. No multi-cluster or multi-region federation.
3. No training, fine-tuning, or LoRA hot-swapping.
4. No cross-org worker sharing.
5. No billing, invoicing, or payment — cost *estimation* only.
6. No distributed circuit breaking ([ADR-0014](./architecture-decisions/README.md#adr-0014)).
7. No service mesh requirement.
8. No prompt caching / semantic cache layer.
9. No synchronous inference over a message broker (batch API only, post-v1).
10. No statistical-significance claims beyond what the implemented analysis supports.
