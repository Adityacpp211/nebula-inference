# NEBULA — Component Specification

**Status:** Phase 0 design. No implementation code exists.
**Scope:** every component — deployable service, shared library, and infrastructure dependency —
specified as responsibility, inputs, outputs, dependencies, failure modes, and scaling strategy.
Plus [§5](#5-what-must-not-be-a-separate-microservice), which names the things that look like
services and must not be.

Companions: [architecture.md](./architecture.md) (why the system is shaped this way),
[diagrams.md](./diagrams.md) (the pictures), [events.md](./events.md) (message contracts),
[api.md](./api.md) (HTTP contracts).

---

## 1. How to read this document

Each card has six fields, in the same order, with the same meaning:

| Field | Means |
|-------|-------|
| **Responsibility** | The one sentence that justifies the component's existence. If two components' sentences overlap, one of them is wrong. |
| **Inputs** | Everything it reads, with the source named. A component that reads something not listed here is a bug. |
| **Outputs** | Everything it writes or emits, with the destination named. |
| **Dependencies** | `hard` = cannot serve its purpose without it. `soft` = degrades, keeps working. The hard/soft split is the availability design. |
| **Failure modes** | What breaks, how it is detected, what the system does, and the blast radius. Not a list of things that could go wrong in principle — the ones with a designed response. |
| **Scaling strategy** | The axis it scales on, the bound that stops it, and what breaks first. |

### 1.1 The process-justification test

A component becomes a separate **process** only if it satisfies at least one of these. Nothing else
counts — not conceptual cleanliness, not diagram symmetry, not "it's a different concern".

1. **Distinct failure domain** — it must be able to fail, or be turned off, without taking something
   else down. *(autoscaler, controller)*
2. **Distinct scaling axis** — its load is uncorrelated with the others'. *(gateway scales with request
   rate; workers scale with model demand)*
3. **Single-writer requirement** — correctness needs exactly one active instance. *(controller,
   autoscaler, via leader election)*
4. **Different language or runtime** — it cannot live in the same binary. *(the Python worker)*
5. **Different trust or isolation boundary** — it runs tenant-controlled or untrusted work. *(workers
   execute model inference on tenant prompts)*

A component that satisfies none of these is a **library**. §5 applies the test to eleven tempting
candidates and rejects all of them.

---

## 2. Services (deployable processes)

### 2.1 `nebula-gateway` — data plane

**Responsibility.** Be the only public entry point: authenticate, admit, route, dispatch, stream, and
account for every inference request — and contain no model-serving or persistence logic.

**Inputs**
- HTTP requests from OpenAI-compatible clients, the CLI, and the dashboard (public).
- API keys and session JWTs → verified against Redis cache, falling through to the control plane.
- Route table and endpoint set → `EndpointSlice` informer (authoritative for existence/readiness) +
  Redis snapshot (cold start) + control plane API (route definitions).
- Worker load state → `nebula.worker.heartbeat.*` on core NATS (~1 Hz per pod).
- Rate-limit counters → Redis (Lua token bucket).
- Routing and rate-limit policies → control plane, cached with TTL.
- Its own dispatch observations (latency EWMA, failures) → in-process, not shared.

**Outputs**
- HTTP responses, including SSE token streams, to clients.
- Worker requests on `POST /internal/v1/{generate,generate/stream,cancel}` with `X-Request-Id`,
  `traceparent`, `X-Nebula-Deadline`, `X-Nebula-Priority`, `X-Nebula-Model-Version`.
- Usage events → `nebula.usage.records` (JetStream), one per request including partial usage on
  client disconnect.
- Proxied admin calls → control plane, carrying a signed internal auth context.
- Metrics (`/metrics`), OTLP spans, structured logs.

**Dependencies.** `hard`: none beyond the workers it dispatches to. `soft`: Redis (rate limiting falls
back to a conservative in-process limiter; key lookup falls through to the control plane), control
plane (serves from cached routes — axiom A8), NATS (routing degrades to EndpointSlice readiness +
local observation), Kubernetes API (keeps last-known endpoints), OTel collector.

This row is the single most important availability claim in the system: **the gateway has no hard
dependency that can take inference down except the workers themselves.** Every soft dependency has a
tested degradation path (Phase 16).

**Failure modes**

| Failure | Detection | Behaviour | Blast radius |
|---------|-----------|-----------|--------------|
| One replica crashes | `/livez`, EndpointSlice | In-flight requests on that replica fail (retryable if no token emitted); LB removes it | requests in flight on one replica |
| All replicas saturated | `nebula_inflight_requests`, admission queue depth | Queue to bound, then 429 + `Retry-After` and `X-Nebula-Reason: queue_full`. Never unbounded growth | new requests, as latency then shedding |
| Stale routing state | heartbeat age metric, dispatch failures | Stale endpoints filtered out; wrong-version dispatch rejected by the worker with 409; local failure believed immediately | brief mis-dispatch, absorbed by retry (R-04) |
| Redis down | Redis client errors, `/healthz` | In-process conservative limiter, documented as approximate; PostgreSQL fallback for key verification | rate-limit precision only |
| Memory growth under load | RSS, queue depth, in-flight gauge | Admission queue and concurrency limiter are the bound; OOM is a bug, not a capacity outcome | one replica, restarted |
| Slow client (backpressure) | write timeouts, stream duration | SSE writes are bounded by a write deadline; a client that stops reading is cancelled and its worker slot freed | one request and one worker slot |

**Scaling strategy.** Horizontal, stateless, HPA on CPU and `nebula_inflight_requests`. Each replica
holds one connection per in-flight request, so **file descriptors and memory bound concurrency, not
CPU** — the concurrency limiter is set below the FD limit deliberately. Every replica maintains its own
informer cache and NATS subscription, so watch connections grow linearly with replicas: fine at tens,
and the number to watch if it ever reaches hundreds. Routing state is per-replica, so weighted splits
are exact only in aggregate ([ADR-0004](./architecture-decisions/0004-router-as-library.md)).

---

### 2.2 `nebula-controlplane` — admin API and system of record

**Responsibility.** Own PostgreSQL: be the only component that writes desired state, and serve the
admin API that creates and mutates it.

**Inputs**
- Admin HTTP requests, proxied from the gateway with a signed internal auth context.
- PostgreSQL rows (its own store).
- Artifact metadata — checksum, size, parsed GGUF header — from the object store during version
  finalization.
- Prometheus query results, for the metrics and usage endpoints it serves.

**Outputs**
- PostgreSQL writes: organizations, users, api_keys, models, model_versions, deployments,
  deployment_revisions, routes, route_targets, policies, rollouts, experiments, pricing, audit_logs.
- `nebula.control.reconcile` nudges on any spec change (best-effort; resync is the safety net).
- Presigned upload targets for artifacts.
- Redis: route/endpoint snapshot for gateway cold start; auth cache invalidations on key revocation.
- Audit entries for every mutation, with before/after, redacted by a shared helper.

**Dependencies.** `hard`: PostgreSQL. `soft`: Redis (cache warming), NATS (nudges — losing one costs
seconds of reconcile latency), object store (only for registry operations), Prometheus (metrics
endpoints return 503 for that sub-resource, not for the API).

**Failure modes**

| Failure | Detection | Behaviour | Blast radius |
|---------|-----------|-----------|--------------|
| PostgreSQL unreachable | pool errors, `/readyz` fails | Admin API returns 503 + `Retry-After`. **Inference is unaffected** — gateways serve cached routes | all state changes; no serving impact |
| PostgreSQL slow / connection exhaustion | pool wait time, query duration | Bounded pool with per-query timeouts; requests fail fast rather than pile up | admin API latency |
| Migration mismatch after a partial upgrade | startup schema-version assertion | Refuses to serve, exits non-zero, `/healthz` names the expected and actual version | that pod; rollout halts |
| Artifact checksum mismatch on finalize | server-side verification | Version → `failed` with the computed checksum in `failure_reason`; never marked `ready` | one model version |
| Concurrent conflicting spec edits | `generation` / optimistic concurrency | 409 with the current generation; client re-reads | one request |
| Reachable from outside the cluster | NetworkPolicy test in CI | Structurally prevented; a regression fails the test, not production | — |

**Scaling strategy.** Horizontal and stateless — PostgreSQL is the state. Scaling is bounded by the
database connection pool long before CPU, so replica count is chosen against `max_connections`, and
PgBouncer is the documented next step rather than more replicas. Read-heavy endpoints (usage, audit)
can move to a read replica; writes stay on the primary because desired-state transactions must be
serializable.

---

### 2.3 `nebula-controller` — reconciliation

**Responsibility.** Make the cluster match the desired state in PostgreSQL, and record what actually
happened.

Four reconcilers in one process, sharing one informer cache and one leader election:

| Reconciler | Reconciles |
|------------|------------|
| Deployment | `deployments` + current revision → `Deployment`, `Service`, `ConfigMap`, `PDB`, PVC |
| Rollout | `canary_rollouts` state machine → `route_targets` weights, with Prometheus-based analysis |
| Inventory | Node/Pod informers → `nodes` cache, Redis capacity inventory, `worker_events` |
| Usage ingester | `nebula.usage.records` → batched `requests` inserts, hourly `usage_records` rollups |

**Inputs**
- Desired state: `deployments`, `deployment_revisions`, `routes`, `canary_rollouts` (PostgreSQL, read).
- Actual state: Deployment, Pod, Node, EndpointSlice informers (Kubernetes watch).
- Triggers: `nebula.control.reconcile` (fast path), informer events, periodic resync (60 s default).
- Usage events: `nebula.usage.records` durable JetStream consumer.
- Rollout analysis: Prometheus range queries per arm.
- Placement constraints: `packages/scheduler` (pure function over the model version's hardware profile).

**Outputs**
- Kubernetes objects, applied server-side, labelled `nebula.dev/*` with owner references to a
  per-deployment root ConfigMap.
- Status write-back: `observed_generation`, `state`, `ready_replicas`, `conditions`, `last_error`.
- `worker_events`, `nodes`, `requests`, `usage_records` rows.
- `nebula.deployment.events`, `nebula.worker.lifecycle` (JetStream).
- Route weight changes during a rollout; abort/rollback transitions with a recorded reason.

**Dependencies.** `hard`: PostgreSQL (read desired, write observed), Kubernetes API. `soft`: NATS
(resync covers lost nudges; usage ingest pauses and resumes from the durable consumer), Prometheus
(rollout analysis stalls at the current step and says so rather than promoting blind), Redis (capacity
snapshot rebuildable from informers).

**Failure modes**

| Failure | Detection | Behaviour | Blast radius |
|---------|-----------|-----------|--------------|
| Leader lost / pod crash | Lease expiry | Standby acquires the Lease in seconds; reconcile resumes from current state — it is level-triggered, so nothing is lost | seconds of reconcile latency |
| Kubernetes API unavailable | client errors, `/readyz` | Stops making changes and reports it. **Does not delete objects it cannot verify** | no new deployments; serving continues |
| Reconcile of one deployment wedges | `nebula_reconcile_duration_seconds`, queue depth | Per-item exponential backoff; one bad item cannot block the queue (work queue with per-key rate limiting) | one deployment |
| Two writers (split brain) | Lease conflict | Prevented by leader election; a Lease conflict is logged and the loser stops immediately | — |
| Pod unschedulable | Pod informer, scheduler's own condition | Surfaces the scheduler's message as a deployment condition within seconds instead of silent `Pending` | one deployment, with an explanation |
| Usage ingest lag | consumer pending count, ingest lag metric | Batches grow; JetStream retains for 7 days; sustained lag alerts. At-least-once + idempotent insert means duplicates are safe | cost data freshness |
| Status write-back storm | write rate | Status writes are coalesced and rate-limited per deployment; only changes are written | database load |

**Scaling strategy.** **Does not scale horizontally by design** — single active writer via Lease
leader election, 2 replicas for failover only. Scales vertically and by work-queue concurrency
(parallel workers over distinct keys). The bound is informer cache memory, proportional to objects in
`nebula-workloads`, and the usage ingester's insert throughput. If one process ever becomes the
bottleneck, the split is *by reconciler* (each keeping its own leader election), not by sharding one
reconciler — and that would be a new ADR.

---

### 2.4 `nebula-autoscaler` — scaling control loop

**Responsibility.** Decide the replica count for each deployment from demand signals, and make every
decision — including the decision not to act — explainable afterwards.

**Inputs**
- Signals: queue wait p95 and in-flight concurrency (worker heartbeats over NATS + Prometheus), request
  rate, CPU utilization (cAdvisor), GPU utilization (DCGM, when present).
- Policy: `deployments.autoscaling` (targets, windows, enabled), `min_replicas`, `max_replicas`.
- Interlocks: active rollout state; last change time per deployment.

**Outputs**
- `deployments.desired_replicas` (PostgreSQL) — **never a Kubernetes write**, so there is exactly one
  writer per Kubernetes object.
- `autoscaling_events` rows with the full signal vector and the decision, including
  `suppressed_cooldown`, `suppressed_stabilization`, `suppressed_rollout`, `clamped_max`.
- `nebula.autoscale.events` (JetStream).

**Dependencies.** `hard`: PostgreSQL (read policy, write desired). `soft`: NATS and Prometheus
(missing signals → **hold**, never scale to a default); the controller (it actuates, but the autoscaler
does not need it to record a decision).

**Failure modes**

| Failure | Detection | Behaviour | Blast radius |
|---------|-----------|-----------|--------------|
| All signals missing | signal staleness check | **Holds** the current replica count and records `decision=suppressed_no_signal`. Scaling to min on missing data would be an outage | no scaling |
| Oscillation | change count per deployment per hour | Asymmetric stabilization (30 s up / 300 s down), cooldown, max step, and the recorded signal vector make it diagnosable (R-05) | replica churn on one deployment |
| Scaling fights a rollout | rollout state interlock | Suppressed while a rollout is progressing | — |
| Wedged loop | loop iteration metric, `/livez` | Operator sets `replicas: 0` to disable autoscaling cluster-wide without touching reconciliation | scaling only |
| Runaway scale-up | `max_replicas` clamp, org quota | Hard clamp; quota rejection recorded | bounded by policy |

**Scaling strategy.** Single active instance (leader-elected), 2 replicas for failover. Loop cost is
O(deployments) per interval, so it scales vertically and by interval tuning. Independently
scale-to-zero-able — that escape hatch is what makes shipping an autoscaler safe.

---

### 2.5 `nebula-worker` — inference execution

**Responsibility.** Hold exactly one model version, loaded, and turn admitted requests into tokens —
while reporting its own load honestly and refusing work it cannot finish.

**Inputs**
- `POST /internal/v1/generate{,/stream}` from gateways, with required deadline, priority, request ID,
  trace context, and asserted model version.
- Model artifact at `/models/sha256/<hex>`, verified by the initContainer before the container starts.
- Configuration: runtime name, model version ID, queue depth, parallel slots, NATS URL, OTLP endpoint.
- `POST /internal/v1/cancel`, `POST /internal/v1/drain` (the latter from the `preStop` hook).

**Outputs**
- Generated tokens: JSON, or SSE chunks with `usage` and `finish_reason` on the final frame.
- Runtime-sourced token counts and timings (`queue_ms`, `prefill_ms`, `decode_ms`, `ttft_ms`) —
  **the authoritative source for billing** (axiom A2).
- Heartbeats → `nebula.worker.heartbeat.<deployment>.<pod>` (core NATS, ~1 Hz).
- Lifecycle events → `nebula.worker.lifecycle` (JetStream): model load start/ok/fail, drain, shutdown.
- 429 + `Retry-After` + `X-Nebula-Reason: worker_saturated` when it cannot start work before the
  deadline.
- Metrics, spans, logs. Never prompt content by default.

**Dependencies.** `hard`: the artifact on local disk; the runtime adapter's engine (`llama-server` child
process, or in-process vLLM). `soft`: NATS (heartbeats stop; the gateway falls back to EndpointSlice
readiness), OTel collector. **Deliberately none**: PostgreSQL, Redis, the control plane, other workers
— all four are denied by NetworkPolicy, not merely unused.

**Failure modes**

| Failure | Detection | Behaviour | Blast radius |
|---------|-----------|-----------|--------------|
| Model fails to load (checksum, OOM, unsupported quant) | typed error from `load()`; `startupProbe` fails | Pod never becomes ready; typed reason in `worker_events` and the deployment condition. A rollout to this version aborts rather than shifting traffic | one replica; rollout halted, not traffic |
| OOMKilled | Pod informer, restart count | In-flight requests fail (retryable only if no token emitted); heartbeat gap removes it from selection in ~1 s, EndpointSlice in ~2 s; kubelet restarts; `startupProbe` covers reload | in-flight requests on that pod |
| Engine child process crashes | supervisor detects exit | Adapter marks unready (`/readyz` fails) and restarts the child; does not hang | that pod's in-flight work |
| Engine hangs (no tokens) | TTFT timeout at the gateway; `/livez` on the worker | Gateway aborts at the TTFT deadline; a wedged process fails liveness and is restarted | requests on that pod |
| Local queue full | queue depth | 429 + `Retry-After`; never accepts work it cannot start before the deadline | shifts load to peers |
| Deadline exceeded mid-generation | absolute `X-Nebula-Deadline` | Aborts, frees the slot, reports partial usage for tokens actually produced | one request |
| Slow artifact pull | initContainer duration, `nebula_model_load_duration_seconds` | `startupProbe` tolerates ~5 min; the node cache makes the second pod on that node free | first-pod startup latency |

**Scaling strategy.** Horizontal, per deployment, driven by the autoscaler through the controller. The
real limits are **memory and VRAM, not CPU** — a replica's ceiling is `parallel_slots` from the
runtime's capability declaration, and concurrency beyond that queues rather than parallelizes. Vertical
scaling (more slots, bigger `n_ctx`) trades KV-cache memory for throughput. Scale-out is bounded by
node capacity, which is exactly what `packages/scheduler` admits against. Cold start is dominated by
model load, which is why scale-to-zero is opt-in and off by default.

---

### 2.6 `nebula-dashboard` — operator UI

**Responsibility.** Render NEBULA's real state for operators, and render nothing it cannot get from an
API.

**Inputs.** Only the gateway: control API reads, SSE streams for logs and events, and a bounded
metrics-query endpoint with server-injected tenant label matchers.

**Outputs.** Rendered UI; API mutations on operator action (scale, rollback, promote, abort, key
creation).

**Dependencies.** `hard`: the gateway. Nothing else — not the control plane, not Prometheus, not a
worker ([ADR-0024](./architecture-decisions/README.md#adr-0024)).

**Failure modes.** Gateway unreachable → explicit error states per panel, never a blank chart implying
zero; partial API failure → that panel degrades while the page works; stale data → every panel shows
its data's age, because a stale dashboard that looks live is worse than a visibly broken one. There is
no mock-data path in the production bundle, and the build fails if the fixtures module is imported
outside tests.

**Scaling strategy.** Static assets behind nginx; horizontal and trivially cacheable. Load lands on the
gateway, not here. Expensive panels are bounded by query cost limits at the gateway, not by client-side
politeness.

---

### 2.7 `nebula` CLI

**Responsibility.** Be a complete, scriptable client of the control API — and contain no logic the API
does not have.

**Inputs.** Command-line arguments; `~/.nebula/config.yaml` (0600) and the OS keyring; `NEBULA_API_URL`,
`NEBULA_API_KEY`, `NEBULA_ORG`; local model files for upload.

**Outputs.** Control API calls; artifact bytes streamed directly to object storage with resumable
progress; table, JSON, or YAML on stdout; exit codes distinguishing user error (2), auth (3), not found
(4), conflict (5), server error (10).

**Dependencies.** `hard`: the gateway. `soft`: object store (only for upload).

**Failure modes.** Network failure → retry with backoff on idempotent reads, never silent partial state;
interrupted upload → resumable, and an unfinalized version stays `uploading` rather than becoming a
half-registered model; contract skew → `nebula version` warns, and a breaking mismatch refuses to run
rather than sending a request whose semantics it guesses; expired credentials → clear re-login
instruction, not a raw 401.

**Scaling strategy.** Not applicable — a client binary. Its one scaling concern is `--wait`, which polls
with backoff and a ceiling rather than tight-looping against the API.

---

## 3. Libraries (in-process, not services)

Each is I/O-free unless noted, which is what makes it unit-testable under an injected clock with no
network. Enforced by an import-boundary linter
([repository-structure.md §4](./repository-structure.md#4-dependency-rules)).

| Library | Responsibility | Inputs | Outputs | Dependencies | Failure modes | Scaling |
|---------|---------------|--------|---------|--------------|---------------|---------|
| `routing` | Resolve a model name to a route, then select an endpoint via a strategy chain | `EndpointSnapshot`, policy, bucketing key | chosen endpoint, or "none eligible" with a reason | none (pure) | empty candidate set → caller returns 503 `no_healthy_endpoint`; stale snapshot → filtered by heartbeat age | scales with the gateway; O(endpoints) per decision |
| `queue` | Bounded, priority-aware, deadline-aware admission queue with aging | enqueue requests, injected clock | dequeued item or a typed rejection (`full`, `timeout`, `cancelled`) | none (pure) | full → reject immediately; expired → dropped at dequeue and by a sweeper; starvation → prevented by bounded aging | memory-bounded by `max_depth` × deployments |
| `reliability` | Error classification, retry with budget and jitter, timeouts, circuit breaker, drain | attempt outcomes, injected clock | retry decision, breaker state, backoff duration | none (pure) | breaker stuck open → half-open probe recovers; budget exhausted → fast-fail, counted | per-replica state; no coordination |
| `scheduler` | Model hardware profile → Kubernetes constraints; capacity admission | hardware profile, deployment resources, node inventory | constraints; admit/reject with an explanation | none (pure; inventory passed in) | optimistic admission → pod may still be `Pending`, surfaced as a real condition | O(nodes) per admission |
| `costing` | Integer-micro cost arithmetic, attribution, pricing profile resolution | usage records, pricing profile version | cost in micros + stated assumptions | none (pure) | missing pricing → zero cost, explicitly flagged, never fabricated | O(rows) in the rollup job |
| `auth` | Key generation and HMAC verification, JWT issue/verify, scope and role checks | credential, scopes, pepper | auth context or a typed denial | pepper from config | pepper rotation → dual-verify window; cache staleness → bounded 30 s TTL + revocation invalidation | verification is O(1); cached |
| `events` | NATS/JetStream subjects, publishers, durable consumers, schema versioning | NATS connection, typed payloads | published messages; consumer callbacks | NATS (soft for publishers, buffered) | broker down → bounded buffer → file spool → shed with a counter (never silent, R-12) | consumer concurrency per subject |
| `artifact` | Content-addressed store: presign, stream, verify checksum | store config, artifact refs | presigned URLs, verified bytes, metadata | object store | checksum mismatch → typed error, version `failed`; store down → registry ops fail, serving unaffected | bounded by object-store throughput |
| `db` | pgx pool, transactions, RLS session, sqlc queries, schema assertion | connection config | typed query results | PostgreSQL | pool exhaustion → bounded wait then fail fast; schema mismatch → refuse to start | pool size; PgBouncer beyond that |
| `telemetry` | slog setup, OTel init, metric registry, shared probe handlers | config | configured loggers, tracers, meters, `/livez` `/readyz` `/healthz` `/metrics` | OTel collector (soft) | exporter down → bounded in-memory buffer, then drop with a counter; never blocks the request path | negligible |
| `api` | OpenAPI specs and generated server/client types | spec files | generated Go and TypeScript code | codegen toolchain | drift → CI fails on a non-empty `git diff` | build-time only |
| `config` | Layered loading, validation, redaction, `NEBULA_ENV` rules | defaults, file, env, flags | validated config struct | none | invalid → exit non-zero with **all** problems listed, not the first | startup only |
| `k8s` | Typed clients, informers, server-side apply, owner references, label helpers | kubeconfig / in-cluster | applied objects, informer caches | Kubernetes API | watch disconnect → informer resync; apply conflict → retried with backoff | cache memory ∝ object count |
| `version` | Build info and contract-skew detection | ldflags | version strings, skew warnings | none | — | — |

---

## 4. Infrastructure dependencies

| Component | Responsibility | Consumed by | Failure behaviour (NEBULA's response) | Scaling |
|-----------|---------------|-------------|----------------------------------------|---------|
| **PostgreSQL** | Desired state, history, audit, usage rollups | controlplane (rw), controller (rw-observed), autoscaler (rw-desired) | Admin API 503; **inference continues** on cached routes; controller stops changing things and says so | Vertical first; read replicas for usage/audit; connection pooling before more replicas. Partitioning keeps `requests` and `audit_logs` maintainable |
| **Redis** | Rate-limit buckets, auth cache, capacity + route snapshot, idempotency keys | gateway, controlplane, controller | Conservative in-process limiter; PostgreSQL fallback for keys; snapshots rebuilt from informers. **No request is dropped solely because Redis is down** | Single instance is adequate at this scale; Sentinel/cluster if it ever becomes hard. Everything in it is rebuildable by design |
| **NATS + JetStream** | Heartbeats (core), usage/events/lifecycle (durable) | all services | Routing degrades to EndpointSlice readiness; usage buffers → spools → sheds with a counter and an alert. Inference never depends on it ([ADR-0003](./architecture-decisions/0003-push-dispatch-and-nats-scope.md)) | Cluster of 3 with JetStream replicas in production; single node in dev. Retention bounds storage |
| **Object store** (MinIO dev / S3 prod) | Immutable content-addressed model artifacts | CLI (upload), initContainer (pull), controlplane (verify) | Registry and new-pod startup fail; running pods unaffected because artifacts are cached on the node | Object-store native; node cache absorbs most reads |
| **Kubernetes API** | Actual state, workload lifecycle | controller (rw), gateway (EndpointSlice read) | Controller and autoscaler stop making changes; gateways keep serving last-known endpoints | Cluster-managed |
| **Prometheus** | Metrics, rollout analysis, autoscaler signals | autoscaler, controller, dashboard | Rollout analysis stalls at the current step rather than promoting blind; autoscaler holds | Vertical; remote-write for long retention |
| **OTel Collector** | Telemetry fan-out and backend decoupling | all services | Spans and metrics dropped with a counter; never blocks a request | DaemonSet or deployment; horizontal |
| **Tempo / Loki** | Traces / logs | Grafana, operators | Observability degrades only | Object-storage backed |
| **Grafana** | Dashboards and alert routing | operators | Alerting continues via Alertmanager; dashboards unavailable | Stateless with provisioned dashboards |

---

## 5. What must NOT be a separate microservice

Eleven candidates that look like services in a diagram. Each is rejected against
[the process test](#11-the-process-justification-test), with the condition that would change the answer
— because "no, and here is what would change my mind" is a decision, while "no" is an opinion.

| Candidate | Why it looks like a service | Why it is not | Where it lives | What would change the decision |
|-----------|----------------------------|---------------|----------------|-------------------------------|
| **Model router** | The brief lists it; it is conceptually central | A per-request hop adds latency and a failure domain without adding capability, and it would lose the caller's own failure observations — the most valuable signal it has | `packages/routing`, in the gateway ([ADR-0004](./architecture-decisions/0004-router-as-library.md)) | A requirement for **global fairness ordering** across tenants, which genuinely needs one arbiter |
| **Scheduler** | The brief lists it; "scheduling" sounds like a daemon | Constraint generation is a pure function; capacity admission is a cache lookup. Kubernetes does the actual placement | `packages/scheduler` + the inventory reconciler ([ADR-0005](./architecture-decisions/0005-cooperate-with-kubernetes.md)) | Fractional GPU packing (MIG, time-slicing), which needs a scheduler **extender** — a webhook, not a service |
| **Request queue** | Queues are infrastructure; a broker is right there | A queue as a service is a broker, and a broker on the token path costs TTFT and makes NATS a hard dependency of inference | `packages/queue`, in the gateway and the worker | An async **batch** inference API, where no client waits — already reserved as `nebula.batch.*` |
| **Rate limiter** | Shared counters imply a shared service | The shared state is Redis; the logic is 40 lines of Lua plus a policy lookup. A limiter service would add a hop in front of a hop | `packages/auth` + Redis, in the gateway | Nothing plausible. Redis *is* the shared component |
| **Auth service** | Auth is a classic first microservice | Verification is one HMAC and a cached lookup. An auth service would be a hard dependency on the hottest path in the system | `packages/auth`, in the gateway | External IdP federation with token introspection — and even then the IdP is the service, not ours |
| **Cost engine** | "Engine" implies a process | Pure integer arithmetic over rows already being written. It has no state and no independent load | `packages/costing`, in the controller's rollup path | Real billing with invoicing, dunning, and payment — a different product, and a different security boundary |
| **Usage ingester** | It has its own throughput profile and could back up | It already is a distinct concern with distinct backpressure — but it shares the controller's informer cache, leader election, and database pool, and its backlog is durable in JetStream, so a backup does not need process isolation to be safe | a reconciler inside `nebula-controller` | Ingest throughput becoming the controller's bottleneck. The split is then **by reconciler**, with its own leader election |
| **Model registry** | Registries are usually services (think Harbor, MLflow) | It is CRUD over four tables plus checksum verification. The *artifact store* is the heavy component, and that is already separate — it is S3 | `nebula-controlplane` | Cross-cluster artifact replication or registry federation |
| **Canary / analysis service** | Progressive delivery products ship one (Flagger, Argo Rollouts) | It is a state machine over rows plus Prometheus range queries, level-triggered on the same resync as everything else. A separate process would duplicate the informer cache and the leader election for one table | rollout reconciler in `nebula-controller` | Pluggable third-party analysis providers with untrusted evaluation code — an isolation boundary, which *is* a valid reason |
| **Tokenizer service** | Token counts are needed for quotas and cost | Re-tokenizing outside the runtime guarantees eventual disagreement with the model that generated the text, and the disagreement surfaces in someone's invoice. The runtime's counters are authoritative (axiom A2) | nowhere; the worker reports counts | Pre-flight exact context-window validation for very long prompts, where an estimate is not good enough. Even then: a library in the worker, not a service |
| **Dashboard BFF** | SPAs commonly get a backend-for-frontend | The gateway already does authentication, rate limiting, proxying, and tenant scoping. A BFF would be a second edge with a second auth path — two places to get tenant isolation wrong | `nebula-gateway` ([ADR-0024](./architecture-decisions/README.md#adr-0024)) | A public third-party API with different auth semantics from the operator UI |

### 5.1 The anti-pattern this rules out

The version of NEBULA that would look most impressive in a diagram is also the one that would work
worst:

```
gateway → auth-svc → ratelimit-svc → router-svc → queue-svc → scheduler-svc → worker
```

Six network hops before a single token, six availability multipliers on the request path, six
deployments to operate, and six places for tenant scoping to be implemented slightly differently — in
exchange for no capability the library design lacks. Each hop's p99 adds; a 99.9% service in front of a
99.9% service is 99.8%. The brief's instruction not to create microservices "simply for appearance" is
the same conclusion reached from the other direction.

### 5.2 Why the six that exist, exist

| Process | Test it passes |
|---------|---------------|
| `nebula-gateway` | Distinct scaling axis (request rate); the only component in the public trust zone |
| `nebula-controlplane` | Distinct scaling axis (admin traffic, connection-pool bound); sole database writer |
| `nebula-controller` | Distinct failure domain (must not wedge the API); **single-writer** requirement for Kubernetes objects |
| `nebula-autoscaler` | Distinct failure domain (must be independently disableable); **single-writer** for `desired_replicas`; different cadence |
| `nebula-worker` | Different runtime (Python); different trust boundary (executes tenant prompts); scales per model, not per request rate |
| `nebula-dashboard` | Different runtime (static assets); no server-side logic at all |

Every one passes at least one test. None passes zero.

---

## 6. Scaling summary

| Component | Axis | Trigger | Hard bound | What breaks first |
|-----------|------|---------|------------|-------------------|
| gateway | horizontal | CPU + `nebula_inflight_requests` | FDs and memory per replica; informer/NATS connections × replicas | file descriptors, before CPU |
| controlplane | horizontal | request rate | PostgreSQL `max_connections` | the connection pool |
| controller | **vertical + queue concurrency** | reconcile latency | one active writer by design | informer cache memory; usage-ingest throughput |
| autoscaler | **vertical** | loop duration | one active instance by design | O(deployments) loop cost |
| worker | horizontal per deployment | queue wait p95, concurrency | node CPU/RAM/VRAM via capacity admission | memory and VRAM, before CPU; cold start dominates scale-up |
| dashboard | horizontal | trivial | — | the gateway behind it |
| PostgreSQL | vertical, then read replicas | connections, IOPS | write primary is single | write throughput on `requests` ingest |
| Redis | vertical | ops/sec | single instance in v1 | nothing critical — all contents rebuildable |
| NATS | 3-node cluster | JetStream storage | retention policy | JetStream disk |

Two asymmetries worth stating because they are deliberate and look like mistakes: the gateway has
**no CPU limit** (limits cause throttling that shows up as tail latency on a request path) but does have
a memory limit; and the controller and autoscaler **do not scale horizontally at all**, because
correctness requires one writer and adding replicas would be a bug disguised as capacity.

---

## 7. Failure-mode summary

Cross-cutting view: what each failure costs, ordered by blast radius. Each row gets a test in Phase 16.

| Failure | Inference | State changes | Detection | Recovery |
|---------|-----------|---------------|-----------|----------|
| One worker pod dies | brief errors on that pod's in-flight work | none | heartbeat gap ~1 s, EndpointSlice ~2 s | kubelet restart + model reload; peers absorb load |
| All workers for a route die | 503 `no_healthy_endpoint`, or `Failover` to a secondary route | none | immediate | controller recreates; autoscaler may scale up |
| One gateway replica dies | in-flight on that replica | none | `/livez`, LB | restart; other replicas unaffected |
| PostgreSQL down | **unaffected** (cached routes) | all blocked | pool errors, `/readyz` | automatic on recovery; controller resyncs |
| Redis down | unaffected; rate limits approximate | unaffected | client errors | automatic; state rebuilt |
| NATS down | unaffected; routing less optimal | usage buffers then sheds (counted) | connection state, consumer lag | durable consumer resumes from its sequence |
| Kubernetes API down | unaffected (last-known endpoints) | no new deployments | client errors | reconcile resumes; level-triggered, nothing lost |
| Object store down | unaffected (node cache) | no new versions or first-pod starts | request errors | automatic |
| Node lost | requests on that node's pods | none | heartbeat staleness precedes `NotReady` | Kubernetes reschedules; capacity admission re-evaluated |
| Model load failure | traffic stays on the healthy version | rollout aborts | typed error, `startupProbe`, condition | operator fixes the version; old version never stopped serving |
| Rollout analysis breach | none — that is the point | weights restored to baseline | Prometheus analysis at step boundary | automatic rollback with a recorded reason |
| Prometheus down | unaffected | rollouts pause; autoscaler holds | scrape failure | automatic |
