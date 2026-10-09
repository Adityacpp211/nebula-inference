# NEBULA — Implementation Roadmap

**Status:** Phase 0 complete (this document set), awaiting approval to begin Phase 1.

Phase numbering follows the project brief. Two additions: **Phase 0.5** (toolchain bootstrap, because
the target machine has none of the required tools yet) and an explicit gate definition.

---

## How a phase ends

A phase is complete when `make verify-phase` is green and the seven checks below pass. Not before.
The brief's rule — *do not continue if the current phase is broken* — is enforced mechanically rather
than by judgement.

| # | Gate | Mechanism |
|---|------|-----------|
| 1 | Tests pass | `go test -race ./...`, `pytest`, `vitest`; new code covered by new tests |
| 2 | Build clean | all binaries build; all images build; lints pass with no suppressions added |
| 3 | Database sound | `migrate up` → `down` → `up` from empty **and** from a seeded snapshot |
| 4 | API contracts honoured | OpenAPI lints; generated code produces no diff; `oasdiff` reports no unapproved breaking change |
| 5 | Documentation current | the phase's docs written or updated; `make docs-check` passes; every ADR the phase decided is committed |
| 6 | Working state committed | conventional commits, PR merged, `main` deployable, tag `phase-N` |
| 7 | Technical debt recorded | debt from this phase logged as `TODO(NEB-nnn)` **and** an issue, and listed in the phase's closing note |

**Demo obligation:** from Phase 4 onward every phase ends with a reproducible command sequence, run
from a clean checkout, that shows the new capability working. If it cannot be demonstrated, it is not
done.

---

## Phase 0 — Architecture and repository design ✅

This document set: architecture, service boundaries, data model, API contracts, event flows,
deployment architecture, repository structure, roadmap, risk register, ADR list.

**Exit:** approved by the reviewer. No implementation code written before approval.

---

## Phase 0.5 — Toolchain bootstrap

The development machine (Windows 11 + Docker Desktop/WSL2) currently has no Go, kubectl, kind, Helm,
or k6, and the cloud sandbox used for authoring has no Kubernetes. Fixing this first, explicitly, is
cheaper than discovering it in Phase 5.

**Deliverables:** `docs/development.md` with exact install steps (Go 1.24+, Node 22+, Python 3.11+,
kubectl, kind, Helm, k6, golangci-lint, sqlc, oapi-codegen, migrate) for Windows/WSL2, macOS, and
Linux; `scripts/preflight.sh` + `preflight.ps1` that verify versions and print what is missing;
`.tool-versions`; a `devcontainer.json` so a reviewer can skip all of it.

**Exit:** `make preflight` passes on the target laptop, and `kind create cluster` succeeds.

---

## Phase 1 — Repository, configuration, database, migrations ✅

**Status: complete.** 8 migrations, 5 Go packages, 2 binaries, 94 tests (68 unit, 26 integration).
Two decisions taken during implementation are recorded as
[ADR-0025](./architecture-decisions/0025-own-the-migration-runner.md) (own the migration runner) and
[ADR-0026](./architecture-decisions/README.md#adr-0026) (`packages/httpx`).

**Deliverables**

- Repository skeleton exactly as [repository-structure.md](./repository-structure.md); `go.mod`;
  Makefile; `.golangci.yml`; `.gitattributes`; CI workflow running build + lint + test.
- `packages/config` — layered loader, validation with full error lists, redaction, `NEBULA_ENV` rules.
- `packages/telemetry` — `slog` JSON setup with the documented field schema, correlation context
  (request ID, W3C trace context, org), and shared `/livez`, `/readyz`, `/healthz` handlers with a
  dependency-checker registry. Every later service gets these for free, which is how "every service
  must have a health endpoint" stops depending on discipline. `/metrics` and OTel tracing: Phase 8.
- `packages/httpx` — error envelope, middleware chain, graceful server with the drain sequence.
- `packages/db` — pgx pool with session settings, transaction helpers, RLS session setter,
  schema-version assertion, health checker, the migration runner, and row types for the whole schema.
- `migrations/000001…` — the full schema from [data-model.md](./data-model.md), including enums,
  constraints, partitions for `requests`/`audit_logs`, the immutability triggers, RLS policies, the
  three roles, and seed data.
- `packages/version` — build stamping and contract-skew detection.

**Tests:** config precedence and validation; migration up/down/up; RLS actually isolates two orgs
(the test that matters — a policy nobody verified is a policy that does not work); constraint tests
proving a ready `model_version` cannot be mutated and route weights cannot sum to anything but 100.

**Exit (met).** Empty database → migrated schema with seed data; `make migrate-cycle` walks up, down to
empty and up again; the integration suite verifies RLS isolation, the model-version immutability
trigger, the deferred route-weight constraint, append-only grants, partition routing and concurrent
migration locking against a real PostgreSQL 16.

**Deferred from this phase, with reasons.** Prometheus metrics and the `/metrics` endpoint move to
Phase 8 alongside the metric catalogue and cardinality budget, rather than shipping a registry with
nothing registered; OpenTelemetry tracing likewise, though the log schema already carries `trace_id`
and `span_id` parsed from W3C `traceparent` with the standard library, so correlation works today and
the SDK slots in without changing the contract. Development seeding (`NEBULA_DEV_SEED`) is validated
and gated but does nothing yet: it creates an API key, and key generation arrives with
`packages/auth` in Phase 2.

---

## Phase 2 — Control plane and model registry ✅

**Status: complete**, with one deliverable moved to Phase 3 (see below).

**Delivered**

- `services/controlplane`: HTTP server with the route table in one place, API-key authentication,
  scope enforcement per endpoint, request IDs, structured logging, cursor pagination, the error
  envelope, and an audit record written in the same transaction as every mutation.
- `packages/auth`: key generation (`nbk_` + base62), HMAC-SHA256 hashing with a server-side pepper,
  indexed prefix lookup, argon2id password hashing, the scope vocabulary and the role→scope map.
- `packages/lifecycle`: the deployment state machine as a pure package — eight states, 24 edges, the
  reason vocabulary, and the predicates that depend on more than one state.
- `migrations/000009_deployment_lifecycle`: replaces the Phase 0 enum with the eight states, adds
  `state_entered_at` / `state_reason` / `state_message`, the `deployment_state_edges` table, the
  `deployment_state_transitions` history table, and the trigger that validates an edge, stamps the
  timer and writes history — in the caller's transaction
  ([ADR-0027](./architecture-decisions/0027-deployment-state-machine.md)).
- `services/controlplane/internal/store`: repositories over pgx in the shape sqlc produces, with
  compare-and-set state transitions and generation preconditions on spec changes.
- Registry endpoints: models, immutable model versions, `finalize`, `fail`, archive.
- Deployment endpoints: create (`202 Accepted`), patch with a generation precondition, scale,
  rollback, stop, start, an admin-only explicit transition, status, revisions and transitions.
- Deployment records written with their first revision transactionally. No Kubernetes — the rows are
  the contract.
- `packages/api/openapi.yaml`, embedded and served at `/openapi.yaml`, with a test asserting in both
  directions that every mounted route is documented and every documented path is mounted.
- `NEBULA_DEV_SEED`: an idempotent development organization, owner and API key, refused in production
  twice over.

**Moved to Phase 3: `packages/artifact`**

Presigned upload, streaming checksum verification and GGUF header parsing were planned here. They are
moved to Phase 3, where the inference worker introduces object storage, because all three need
something to talk to: a presigned URL with no MinIO behind it, or a checksum verifier with no bytes to
read, would be scaffolding shaped like a feature. The registry therefore records the checksum the
client declares and `finalize` compares it with the one presented, reporting
`verification: declared_checksum` in both the response and the audit record — so a version that
became ready without its bytes being read stays identifiable once the real verifier exists.
`upload` is present in the contract and `null` in every response, with a note saying why.

Session-token (JWT) authentication also moves out: it exists to serve the dashboard, which is
Phase 15, so it will land with the surface that needs it. A non-`nbk_` bearer token is refused with
code `unsupported_credential` rather than a generic 401.

**Tests:** unit tests for key generation, verification and prefix parsing (including a wrong secret
against a real prefix), argon2id, the scope and role tables, the state machine, and the canonical
spec hash; a test that parses migration 000009 and fails if the SQL and Go transition graphs differ in
either direction; integration tests against a real PostgreSQL for the immutability trigger, row-level
security under `SET ROLE nebula_app`, append-only grants, the atomicity of a deployment and its first
revision, `state_entered_at` not moving on a same-state write, the database refusing every edge the Go
machine rejects, concurrent transitions having exactly one winner, and stale-generation refusal; API
tests over HTTP for every documented error, cross-org access returning 404 rather than 403, scope
enforcement per endpoint, scope escalation, and revocation taking effect immediately.

**Exit:** `curl` can register a model, create a version, finalize it, create a deployment, scale it,
roll it back, drive it through the state machine and read its history; every mutation appears in
`audit_logs` with the calling key and request id.

---

## Phase 3 — Runtime abstraction and the first runtime ✅

**Status: complete**, with the artifact store still outstanding (see below).

**Delivered**

- `workers/inference/nebula_worker`: the FastAPI worker serving the internal API from
  [api.md §6](./api.md#6-internal-worker-api) — synchronous generation, SSE streaming, cancel by
  request id, `state`, `drain`, `livez`, `readyz`, `healthz`, `metrics`. Every response echoes the
  worker protocol version, and the request context (request id, traceparent, deadline, priority,
  asserted model version) travels in headers rather than the body.
- `runtimes/base.py`: the `InferenceRuntime` protocol — eight methods, frozen request and result
  dataclasses, and typed errors that each carry a `code` and a `retryable` flag so a caller decides
  what to do without matching on a message. A `Protocol`, not a base class, so an out-of-tree engine
  conforms by shape and imports nothing from NEBULA.
- `runtimes/llamacpp.py`: supervises upstream `llama-server` as a child process and proxies over
  loopback ([ADR-0015](./architecture-decisions/README.md#adr-0015)). Verifies the artifact checksum
  *before* starting the engine, binds to `127.0.0.1` on an OS-assigned port, maps engine startup
  failures onto the typed errors, drains the child's pipes, reaps it, and restarts a crashed child a
  bounded number of times. Cancellation disconnects from the engine, which is what makes it stop
  computing rather than merely stop delivering.
- `runtimes/mock.py`: the declared stub and the reference implementation of the contract. Configurable
  tokens/sec, load duration, slots and resident bytes; injectable load errors, load stalls, pre-first-
  token stalls, mid-stream failure and a dead engine process. Reports `declared_stub: true` and
  `generates_real_tokens: false`, and is refused in production three independent ways: configuration
  validation, the adapter's own entry point, and a container image that ships no engine.
- `queue.py`: the admission queue — bounded, priority-ordered, deadline-aware. A request whose
  remaining budget cannot cover the current EWMA service time is refused at admission rather than
  queued to die, and saturation answers `429` with `Retry-After` and
  `X-Nebula-Reason: worker_saturated`.
- `deadline.py`: `X-Nebula-Deadline` as absolute Unix **milliseconds**. A duration is re-derived at
  every hop and silently grants each hop a fresh budget; an absolute deadline cannot be renewed by
  accident. Malformed values are rejected rather than defaulted, including both units mistakes — a
  value that looks like seconds, and one more than 24 hours out.
- `telemetry.py`: JSON logging with request-id and trace-id correlation, and the Prometheus metric set
  — requests, admission rejections, request duration, TTFT, queue wait, prompt and generated tokens,
  cancellations, deadlines exceeded, load duration, plus adapter-owned gauges (slots total and busy,
  queue depth, in-flight, model ready, engine process alive, draining, engine restarts, KV cache used)
  pulled from the adapter at scrape time rather than cached.
- `main.py`: the drain sequence — SIGTERM, fail readiness, wait `drain_delay`, stop accepting, finish
  in-flight work, unload the model.
- `deploy/docker/Dockerfile.worker`: two targets from one file, `mock` and `llamacpp`. The mock image
  contains no engine binary at all, so selecting the stub is a different image name rather than a
  different environment variable. The engine is pinned to a llama.cpp tag.
- `tools/make_tiny_model.py`: the fixture generator. Trains a 2-layer, 128-dimension
  LLaMA-architecture model (401,280 parameters, 1.6 MB, 285-token vocabulary) and writes a GGUF that
  upstream `llama-server` loads unmodified
  ([ADR-0028](./architecture-decisions/0028-locally-trained-test-fixture-model.md)).

**Still outstanding: `packages/artifact`**

Presigned upload, streaming checksum verification and GGUF header parsing were moved here from Phase 2
and have **not** landed. They need MinIO and the deployment path that pulls from it, which arrives with
the controller in Phase 5, so they move there rather than being written against nothing. The worker
verifies a checksum it is given by configuration, which is real verification of the file it loads; it
is not the registry verifying bytes it received. `finalize` still reports
`verification: declared_checksum`, and the capability table still says so.

Session-token (JWT) authentication remains deferred to Phase 15, with the dashboard that needs it.

**Tests** — 145, all green; 103 need nothing installed, 42 need a real engine and a real model.

- `tests/runtime_conformance.py`: one suite, run unchanged against both adapters. A subclass supplies
  a `runtime` fixture and a `spec` and nothing else; no test is skipped or overridden for either
  engine. It enforces the seven obligations from [api.md §7](./api.md#7-inferenceruntime-interface):
  cancellation frees the slot (asserted through `/internal/v1/state`), TTFT is measured at the first
  token rather than derived at the end, a deadline aborts and reports `deadline` distinctly from
  `cancel`, token counts come from the engine's tokenizer, load failures are typed and a non-retryable
  one is never retried, `livez` and `readyz` answer different questions, and shutdown drains.
- `tests/test_real_model.py`: the exit criterion. Asserts on the *content* of real streamed output and
  separately that the runtime is named `llamacpp`, so a silent fallback to the stub fails the suite
  instead of passing it. Also: streaming and non-streaming parity, determinism at temperature 0,
  `length` versus `stop`, stop sequences, readiness gating, a model-version assertion mismatch
  answering `409`, cancellation stopping the real engine, a deadline aborting it, and a client
  disconnect releasing the slot.
- `tests/test_app.py`: 31 tests against a **real uvicorn server on an ephemeral port**, not
  `httpx.ASGITransport` — the ASGI transport buffers a streaming response, so a test that occupies a
  slot by reading slowly occupies nothing. Three tests here passed vacuously until this changed, and
  they were the three about saturation and slot accounting.
- `tests/test_llamacpp_conformance.py`: supervision — a missing artifact, a checksum mismatch refused
  before the engine starts, unload reaping the child (verified with `os.kill(pid, 0)`), loopback-only
  binding.
- `tests/test_worker_units.py`: deadline parsing, priority overtaking and FIFO within a class, cancel
  while queued, the EWMA, drain, duplicate request ids, configuration validation, the registry.

**Exit:** met for the runtime path — `make worker-test-integration` streams real tokens from a real
GGUF through a supervised `llama-server` and asserts on their content. **Not** met for the container
images: no Docker daemon was available in the environment this phase was built in, so
`Dockerfile.worker` is unbuilt and unrun here. CI builds both targets, and the llamacpp image's final
step runs `llama-server --version` so a missing shared object fails the build rather than someone's
first request. Until that job has gone green, treat the images as written-but-unverified.

---

## Phase 4 — API gateway and OpenAI-compatible API ✅

**Status: complete**, with the items below marked as not yet verified here.

**Delivered**

- `services/gateway`: API-key authentication with a three-tier credential lookup (in-process →
  Redis → the control plane's internal API), verification against the pepper on every request,
  scope enforcement, tenant-scoped route resolution (another org's model is a 404), validation that
  refuses unsupported parameters by name, and the OpenAI-compatible `/v1/chat/completions`,
  `/v1/completions` and `/v1/models`. The gateway imports no database package
  ([ADR-0030](./architecture-decisions/0030-gateway-credentials-through-the-control-plane.md)).
- Rate limiting: one atomic Redis Lua script for RPM, TPM (optimistic reservation at admission,
  settled against the runtime's count at release, overdraft carried forward) and concurrency leases
  that expire, per key and per org, with OpenAI's `x-ratelimit-*` headers. When Redis fails, an
  in-process implementation of the same algorithm takes over with limits scaled down, every decision
  is marked degraded, and Redis is re-probed after a one-second cooldown rather than paying its
  timeout on every request.
- Streaming: SSE relayed frame by frame with backpressure through a bounded channel, keep-alive
  comments, a per-write deadline that cuts off a client that stops reading, terminal error frames
  with no `finish_reason`, and client-disconnect cancellation that tells the worker to stop and still
  reads its final frame for the runtime's token count. Routing context moved from a named SSE event
  to headers plus an SSE comment when the real SDK proved the Phase 0 assumption wrong
  ([ADR-0029](./architecture-decisions/0029-stream-metadata-is-a-comment.md)).
- Usage records in the `nebula.usage.record.v1` shape, one per request including partial usage, with
  `token_source` saying where counts came from. Written as a structured log line until NATS exists
  (TODO(NEB-140)).
- The admin proxy: the caller's key is stripped, a signed `X-Nebula-Auth-Context` is attached, the
  control plane verifies it (`packages/auth/internal.go`), `/internal/` is never proxied, and
  revocations evict the shared credential cache through a response header the gateway consumes.
- Static routing from a route file — deterministic weighted bucketing by request id, pinning by
  deployment name or id (scope `inference:pin`), round-robin endpoints — replaced by dynamic
  routing in Phase 6.
- The spec covers the inference surface; operations the gateway serves carry
  `x-nebula-served-by: gateway`, and a drift test per service checks each side.
- `scripts/e2e-gateway.sh` (`make e2e-gateway`) and `scripts/load-gateway.sh`
  (`make load-gateway`), both over `scripts/lib/stack.sh`: a throwaway stack from a clean checkout.

**`packages/api` client: moved to Phase 14.** The roadmap listed a Go client here "for the CLI and
dashboard later". Nothing in Phase 4 consumes one, the dashboard's client is generated TypeScript,
and a client written ahead of its first caller is scaffolding; it lands with the CLI.

**Tests.** Go unit tests for parsing (every refusal names its parameter), templates, the route
table (weighted split within tolerance over 10 000 keys and stable per key), the limiter (every
behaviour run against both the Lua script and the in-process fallback, plus all-or-nothing
admission and no overshoot under 200 concurrent admissions), credentials (tiers, negative caching,
single-flight, revocation, expiry, degraded grace, fail-closed), the worker client, and the handler
end to end against a scriptable fake worker: frame order, keep-alives, interruption, disconnect with
partial usage, the error mapping with no worker detail leaking, rate-limit refusal, tenancy, pinning,
and the proxy's signed identity. Control-plane integration tests against PostgreSQL for signed
contexts, their refusals, the internal lookup and revocation. The end-to-end suite drives the
**unmodified OpenAI Python SDK** (3.19.2): list and retrieve, chat, streaming with usage, streaming
and non-streaming agreeing on counts, text completion, refusal by name, 404, 401, context window, the
admin API through the gateway, a key created, used and revoked through the gateway, a disconnect
releasing the worker slot, and the usage record of a cancelled stream. 13/13 green.

**Baseline** (`make load-gateway`, 60 s, mock worker at 400 tokens/s with 32 slots, 16 completion
tokens per request, everything on one laptop; `tests/load/results/phase4-baseline.json`):

| Scenario | Rate | Errors | p50 | p95 | p99 |
|----------|------|--------|-----|-----|-----|
| chat, non-streamed | 40 rps | 0 / 2401 | 130 ms | 155 ms | 162 ms |
| chat, streamed | 20 rps | 0 / 1200 | 144 ms | 168 ms | 178 ms |

About 40 ms of each request is the mock generating 16 tokens; the rest is the gateway, Redis, the
Python worker and the loopback network together. Gateway-only latency needs the Phase 8 histograms.

**Exit:** met — `OpenAI(base_url=..., api_key=...).chat.completions.create(stream=True)` streams
tokens from a worker through the gateway, in `make e2e-gateway`. Against the **mock** runtime: no
`llama-server` binary was available where this phase was built, so the real-engine path is the
Phase 3 suite's, not re-run through the gateway here.

**Not verified here, with reasons.** The gateway container image is written and unbuilt — Docker
Desktop could not start on the development machine, so CI builds it. The race detector did not run
locally (no C toolchain on Windows); CI runs every Go suite with `-race`. PostgreSQL and Redis for the
local runs came from a WSL install rather than compose, which is why the e2e script accepts
`NEBULA_E2E_DEPS=external`.

**Debt recorded:** TODO(NEB-140) usage records to JetStream (Phase 6/13); TODO(NEB-141) gateway
metrics (Phase 8); TODO(NEB-142) `Idempotency-Key` (Phase 10); TODO(NEB-143) cross-replica
revocation broadcast (Phase 6); TODO(NEB-144) chat template from GGUF metadata (Phase 5).

**Original plan**

- `services/gateway`: authn/authz, validation, Redis rate limiting (RPM, TPM with optimistic
  reservation, concurrency), request IDs, trace propagation, admin proxy to the control plane,
  OpenAI-compatible `/v1/chat/completions`, `/v1/completions`, `/v1/models`, SSE streaming with
  keep-alives and terminal error frames, client-disconnect cancellation, usage emission.
- Static routing for this phase only: endpoints from configuration. Dynamic routing is Phase 6, and
  saying so here prevents a temporary shortcut from quietly becoming the design.
- `packages/api` client used by the dashboard and CLI later.

**Tests:** OpenAI Python SDK works unmodified against NEBULA (the real compatibility test — a
hand-written curl proves less than an SDK does); streaming frame order and `[DONE]`; usage correct
non-streamed and streamed; rate-limit headers and 429 behaviour; unsupported parameters rejected by
name; disconnect mid-stream still records partial usage; load test establishes the phase's baseline
p50/p95/p99.

**Exit:** `openai.OpenAI(base_url=..., api_key=...).chat.completions.create(stream=True)` streams
tokens from a worker through the gateway.

---

## Phase 5 — Deployment controller and Kubernetes integration ✅

**Status: complete**, with the deviations below.

**Delivered**

- `services/controller`: deployments listed from PostgreSQL as the `nebula_controller` role and
  watched through informers, a rate-limited work queue with exponential backoff, poll + periodic
  resync, orphan GC, and leader election on a Lease
  ([ADR-0032](./architecture-decisions/0032-controller-reconciles-from-postgres.md)). The reconciler
  server-side-applies a root ConfigMap that owns the Deployment, Service and PDB, reverts drift,
  recreates deleted objects, writes status back (`observed_generation`, conditions, ready replicas),
  and moves the state machine only with reasons from `packages/lifecycle`.
- The inventory reconciler writes `nodes` (capacity, allocatable, requested) and `worker_events`.
- `packages/scheduler`: hardware profile → node selector, tolerations and requests; capacity
  admission that refuses with `422 insufficient_capacity` naming the resource before any row exists,
  treats stale inventory as "could not check" rather than as evidence, and returns the placement
  decision on `202`.
- `packages/artifact`: S3 (minio-go) and directory stores, presigned upload with expiry and a size
  limit, streaming SHA-256, GGUF header parsing, and the node-local content-addressed cache. `finalize`
  on a store-minted URI answers `202 verifying`; a verifier loop reads the stored bytes and moves the
  version to `ready` or `failed`, retrying transient store errors instead of deciding on them
  ([ADR-0031](./architecture-decisions/0031-artifacts-verified-asynchronously.md)).
- `cmd/nebula-artifact-puller`: the initContainer that re-hashes while filling the cache and reports
  failures through the termination message, which the controller turns into named failures
  (`ArtifactChecksumMismatch`).
- `deploy/helm/nebula` (RBAC, NetworkPolicies, migration Job hook, in-cluster PostgreSQL / Redis / S3
  for development), `deploy/kind/cluster.yaml`, `scripts/dev-up.sh` / `dev-down.sh`, container images
  for the gateway, controller and puller. Migration 000011 lets a deleted deployment's name be reused.

**Tests.** Controller decisions table-tested and the reconciler run against the fake clientset and a
real PostgreSQL; scheduler, artifact store (including an S3 integration test), puller and verifier
unit and integration tests; admission and name reuse through the API. `tests/e2e/kind/phase5_demo.py`
(`make e2e-kind`) on a fresh kind cluster, 12/12 green: presigned upload verified from the bytes;
a deployment created through the gateway reaches `ready` 2/2 with its placement; inference through
the gateway is served by those pods; a killed pod is replaced; **the Deployment deleted out from
under NEBULA is recreated in under a second**; hand-edited replicas are reverted; scaling through the
API is followed; an impossible request is a 422 with nothing created; 12 `kubectl auth can-i`
assertions (7 negative) plus none for the control plane and gateway; a workload pod cannot reach
PostgreSQL but can reach the store; a tampered artifact fails the deployment with
`ArtifactChecksumMismatch`; stop and delete remove pods, then objects. The attributed state history
is printed from the database at the end.

**Exit:** met — `curl` through the gateway creates real pods on kind, `kubectl get deploy -n
nebula-workloads` shows them, a killed pod is replaced and a deleted Deployment comes back.

**Deviations.**

- `envtest` was not used: decisions and object shapes are tested against the fake clientset, and
  the real API server, kubelet and garbage collector are exercised by the kind demo instead.
- The controller reads `pods` cluster-wide (capacity must count everyone's pods); see
  [deployment-architecture.md §3](./deployment-architecture.md#3-rbac).
- SeaweedFS replaced MinIO for development (images unavailable); Pod Security `restricted` is
  `warn` on the workload namespace because the dev cache is a hostPath.
- `scripts/dev-up.ps1` was not written: the kind path runs in WSL, which `dev-up.sh` covers.
- Chat templates from GGUF metadata (TODO(NEB-144)) remain open.

**Original plan**

**Deliverables**

- `services/controller`: informer-driven work queue, exponential backoff, periodic resync, leader
  election via Lease.
- Deployment reconciler: spec → `Deployment`/`Service`/`ConfigMap`/`PDB`, owner references, labels
  and annotations from [deployment-architecture.md](./deployment-architecture.md), status write-back
  (`observed_generation`, conditions, `ready_replicas`), artifact-puller initContainer.
- `packages/scheduler`: `hardware_profile` → constraints; node inventory from informers; capacity
  admission returning a real explanation on rejection.
- Inventory reconciler: `nodes` cache and `worker_events`.
- `packages/artifact`, arriving here rather than in Phase 2 or 3: MinIO, presigned upload with expiry
  and a size limit, streaming SHA-256 verification of the bytes actually received, GGUF header parsing,
  and the node-local content-addressed cache
  ([ADR-0016](./architecture-decisions/README.md#adr-0016)). It lands with the artifact-puller
  initContainer above, because a presigned URL with nothing behind it and a verifier with no bytes to
  read would both be scaffolding shaped like a feature. Until then `finalize` keeps reporting
  `verification: declared_checksum`, so a version that became ready without its bytes being read stays
  identifiable.
- `deploy/helm/nebula` + `deploy/kind/cluster.yaml`; RBAC; NetworkPolicies; migration Job hook;
  `scripts/dev-up.sh` / `.ps1`.

**Tests:** controller tests with `envtest` (real API server, fake kubelet) for create/update/scale/
delete/adopt-orphan/drift-correction; the deliberate one — delete the Deployment out from under
NEBULA and assert it is recreated; inadmissible capacity request returns 422 before any object is
created; RBAC verified by `kubectl auth can-i` assertions in CI, including the negatives; an upload
whose bytes hash to something other than the declared checksum is refused at `finalize`, and the
version stays unusable.

**Exit:** `nebula deploy` equivalent via `curl` creates real pods on kind; `kubectl get deploy -n
nebula-workloads` shows them; killing a pod brings a replacement; deleting the Deployment object
brings it back.

---

## Phase 6 — Router and health-aware routing ✅

**Status: complete**, with the deviations below.

**Delivered**

- `packages/routing`: the endpoint snapshot, the mandatory `Filter` (readiness, breaker, heartbeat
  staleness while heartbeats flow, acceptance, loaded model version, context window), the
  `RoundRobin`, `LeastLoaded` (default), `LatencyAware` and `CapabilityBased` strategies, `Failover`
  as a policy that wraps a primary strategy and names a fallback route, per-endpoint breakers and
  EWMAs, and deterministic weighted resolution that gives an unusable target's share to the others
  in proportion — the same key always lands on the same target, and keys on healthy targets never
  move. Pure functions over an injected clock.
- Control plane: `/v1/routes` (create, read, list, atomic replacement of weights and policy,
  delete; weights must total 100, all targets one task) and `GET /v1/policies/routing`, audited;
  the internal routing table (`GET /internal/v1/routing-table`, ETag-versioned, 304 when unchanged).
- Gateway router (`services/gateway/internal/router`): routes polled from the control plane every 2 s,
  endpoints from an EndpointSlice informer (RBAC: `endpointslices` get/list/watch in the workload
  namespace only), heartbeats from core NATS, local observation per dispatch, and a Redis snapshot
  written by every replica and loaded on a cold start when the control plane does not answer
  ([ADR-0033](./architecture-decisions/0033-router-state-and-before-work-retries.md)). A request
  whose endpoint fails *before any work started* (unreachable, draining, loading, saturated) is
  re-placed on another endpoint, at most three attempts; `nebula.attempts` reports it. `/v1/models`
  shows each target's state and eligible endpoints; `/healthz` shows each source.
- Worker heartbeat publisher (`nats-py`), once a second and immediately on drain, with an `instance`
  id so a container restart is not mistaken for a stale sequence; the controller gives every worker
  its pod name, node, deployment and version ids through the downward API.
- NATS in the development chart; NetworkPolicies for workers → NATS and gateway → NATS.
- The revocation broadcast (TODO(NEB-143)): a revocation proxied by one gateway replica evicts the
  key from every replica's in-process cache over NATS.
- A static route file remains for running without Kubernetes (`make run-gateway`,
  `make e2e-gateway`); the chart uses the control plane unless values supply routes.

**Tests.** Strategy and filter tables with an injected clock and no sleeps; the staleness window
exact to the millisecond (eligible at 3 s, gone at 3.001 s, back on the next heartbeat); weighted
resolution within ±2 % over 10 000 keys, stable per key, and a dead target's share redistributed
60/40 by weight with no healthy key moving; breakers (connection failure opens at once, one probe
after the cooldown, doubling on a failed probe); heartbeat ordering across restarts; EndpointSlice
merging (a terminating endpoint takes no new work); heartbeats and the revocation broadcast over an
embedded NATS server; **a cold replica with the control plane down routing from the snapshot
another replica left in Redis**; the gateway skipping an unreachable replica, answering 503
`no_healthy_endpoint` with `Retry-After` when nothing is eligible (and charging nothing), and failing
over to a fallback route with `X-Nebula-Degraded`. Control-plane integration tests for the route API,
its refusals, auditing, the routed-deployment delete guard, and the internal table with its 304.
Worker tests for the heartbeat payload, the immediate drain heartbeat and the final one. The OpenAI
SDK suite (`make e2e-gateway`, SDK 3.22.1) still passes 13/13 against the static-file path.

`tests/e2e/kind/phase6_demo.py` (`make e2e-kind`) on kind: two deployments behind one route created
through the API; the gateway converges in about 2 s with 2 + 2 endpoints; routing table, endpoint
discovery and heartbeats all live; 400 requests split 47.8 % / 52.2 %; every pod of one target
deleted under load from four clients — **0 errors in 488 requests**, the last request placed on the
old pods 0.25 s after the delete, traffic back on the replacements at 6.1 s; then the control plane
scaled to zero and the gateway restarted — 20/20 requests served from the Redis snapshot, `/healthz`
saying so, and back on the live table once the control plane returns.

**Exit:** met — 50/50 over two deployments, observed split matching, and killing every pod of one
target shifts its traffic within a second with no client errors.

**Deviations.**

- The snapshot is written by gateways, not the controller (ADR-0033).
- `CostAware` resolves to `LeastLoaded` with a logged note until the cost engine (Phase 13).
- `LatencyAware` uses the worker's own time-to-first-token as the local sample; gateway-side TTFT
  arrives with the Phase 8 histograms.
- The `route` shortcut on `POST /v1/deployments` (docs/api.md) is not implemented.
- The controller still polls PostgreSQL rather than reacting to `nebula.control.reconcile`; the poll
  is 2 s and nothing in this phase needed it sooner.

**Original plan**

**Deliverables**

- `packages/routing`: `EndpointSnapshot`, the mandatory `Filter` stage, `RoundRobin`, `LeastLoaded`,
  `LatencyAware`, `CapabilityBased`, `Failover`; policy composition from `routing_policies`;
  deterministic weighted bucketing for route targets.
- Gateway router state: EndpointSlice informer, NATS heartbeat subscription, local EWMA/breaker
  observations, Redis snapshot for cold start.
- Worker heartbeat publisher.
- Route resolution: `model` → route → weighted target → deployment.

**Tests:** strategy unit tests with table-driven fixtures and an injected clock (no sleeps); stale
heartbeat removes an endpoint within the documented window; weighted distribution is within
tolerance over 10 000 simulated requests and *stable* for a fixed bucketing key; a gateway started
cold with the control plane down still routes from the Redis snapshot (axiom A8, tested).

**Exit:** two deployments behind one route with 50/50 weights; observed split matches; killing all
pods of one target shifts traffic within seconds without client errors.

---

## Phase 7 — Request queue and concurrency control ✅

**Status: complete**, with the deviations below.

**Delivered**

- `packages/queue`: a concurrency gate with a bounded waiting room — three priorities with aging
  (one level per `NEBULA_GATEWAY_QUEUE_AGING`, default 5 s), absolute deadlines enforced by a
  sweeper and by each waiter, cancellation that removes the entry at once, a newcomer never
  overtaking a waiter, a `Retry-After` estimate from the queue ahead and observed service time, and
  per-gate stats (depth by priority, oldest age, wait p50/p95/p99, drops by reason). Injected clock.
- Gateway admission (`services/gateway/internal/admission`): one gate per deployment, sized on every
  decision by the router — each eligible endpoint's slots (heartbeat, route, or default) times an
  overcommit factor ([ADR-0034](./architecture-decisions/0034-admission-queue-per-deployment.md)).
  The order is resolve → rate limit → queue → place → dispatch; a request that waited is placed
  again on its deployment, because endpoints may have changed while it waited.
- Backpressure end to end: a worker's 429 marks the endpoint saturated until its `Retry-After`; it
  contributes no capacity and strategies avoid it; when every replica refuses, the request returns
  to its queue (at most twice) instead of failing.
- `nebula.queue: "reject"` fast-fail (`429 capacity_exhausted`), `429 queue_full` with
  `Retry-After`, `504 queue_timeout` (not an inference error), and `nebula.gateway_queue_ms`.
- `/debug/queues` (development only) with queue stats, heap and goroutines; `make load-overload`.
- CI made green: golangci-lint v2 now actually runs and reports 0 issues (67 findings fixed or
  annotated with their reason), and the whole Go suite passes under `-race` on Linux.

**Tests.** Deterministic queue tests on a simulated clock: immediate admission and FIFO, a full
queue shedding with `Retry-After` and never growing, deadline expiry by the sweeper counted as
`queue_timeout`, cancellation removing the entry, strict priority order, **LOW served despite a
sustained stream of HIGH** (aging), reject mode never waiting, capacity growth admitting waiters, and
64 clients hammering a gate of capacity 4 and depth 16 — depth never above 16, heap flat across 4×
the requests. Gateway tests: a saturated worker waited out and served on the second attempt, error
mapping under `reject`. The OpenAI SDK suite still passes 13/13.

**Load test** (`make load-overload`, `tests/load/results/phase7-overload.json`): one mock worker with
4 slots, gateway admitting 4 at a time with a queue of 16, 75 requests/s offered for 60 s — three times
what the stack serves. 1 052 served (17/s), 3 449 shed, **every shed a 429 with `Retry-After`, 0
unexpected statuses**, queue depth at most 16, gateway heap 2.5–7.5 MiB with growth after warm-up of
about 1 MiB, served-request latency p99 1.16 s.

**Exit:** met — at 3× capacity memory is stable, queue depth bounded, and shedding correct with
`Retry-After`.

**Deviations.**

- Queue metrics are in-process and at `/debug/queues`; the Prometheus export is Phase 8.
- The per-deployment `queue_config.max_depth` sizes the *worker's* queue; the gateway queue depth is
  a gateway setting (`NEBULA_GATEWAY_QUEUE_MAX_DEPTH`) for now.
- Accounting is per gateway replica (ADR-0034); several replicas can overshoot one deployment, which
  worker 429s and saturation correct.

**Original plan**

**Deliverables**

- `packages/queue`: bounded, three priorities, absolute deadlines, aging to prevent starvation,
  cancellation, sweeper, full metric set.
- Gateway admission queue per deployment; concurrency limiter; `nebula.queue: reject` fast-fail.
- Backpressure contract end to end: worker 429 → endpoint marked saturated → queue or shed.

**Tests:** deterministic queue tests under a simulated clock; queue-full sheds rather than grows
(memory asserted flat under sustained overload — the property the brief asks for); deadline expiry
counts as `queue_timeout`, not as an inference error; `LOW` priority is not starved by sustained
`HIGH` load; cancellation removes the entry immediately. Load test: sustained overload produces
bounded latency and a bounded error rate, not a crash.

**Exit:** a load test at 3× capacity shows stable memory, bounded queue depth, and correct shedding
with `Retry-After`.

---

## Phase 8 — Metrics, logs, tracing ✅

**Status: complete**, with the deviations below.

**Delivered**

- Metrics: `telemetry.Catalog` declares every Go metric of observability.md §2.1, §2.2 and §2.4 that
  exists today — request RED, TTFT, tokens, in-flight, attempts, interrupted streams, client
  cancellations, routing decisions, endpoints by state, heartbeat staleness, queue depth / wait /
  oldest age / drops, breaker state, rate-limit refusals, reconcile duration / errors / queue depth,
  generation lag, replicas, deployment state, capacity admission, schema version. An undeclared
  metric panics at startup; served on a separate port (`NEBULA_METRICS_ADDR`) so the gateway's
  public listener never exposes them. Histograms carry the trace id as an exemplar.
- Tracing: OpenTelemetry (API, SDK, OTLP/HTTP) in every Go service and the worker — `gateway.request`
  → `gateway.authenticate`, `gateway.validate`, `router.resolve`, `router.select`,
  `gateway.ratelimit`, `queue.wait` (only when the request waited), `dispatch.attempt` →
  `worker.generate` → `worker.queue`, `runtime.stream`; `controller.reconcile` as its own trace.
  Logs carry the span's trace id, so the trace and the log lines share one id.
- The observability stack in the chart (`observability.enabled`, on in development): OpenTelemetry
  Collector, Tempo, Loki with Promtail, Prometheus with exemplar storage and the alert rules, Grafana
  with cross-linked datasources (exemplar → trace, trace → logs, log line → trace) and four
  generated dashboards — Fleet Overview, Deployment Detail, Request Path, Queue & Autoscaling.
- Alert rules for every §7.2 alert whose metrics exist (11 rules), each with promtool unit tests
  that it fires and that it stays quiet (`make alerts-test`).
- CI: every action on its Node 24 major, runners pinned to `ubuntu-24.04`, an observability job
  (alert tests, dashboards match their generator), the Phase 8 demo in the kind job.

**Tests.** The metrics contract test parses observability.md's tables and compares them with the
catalogue in both directions; the gateway span-tree test asserts the documented tree for one request
and that the worker's parent is the `dispatch.attempt` span; the worker's tracing tests do the same
for `worker.generate` in both modes; the log tests assert request_id and trace_id on every request
line, in the gateway and the worker, and that prompt text appears in no line, span or attribute,
including on failure; a gateway test asserts the documented series exist after one request; promtool
tests every alert rule. `tests/e2e/kind/phase8_demo.py` on a fresh kind cluster: the stack up with
three linked datasources and four dashboards; Prometheus scraping the gateway, control plane,
controller and both worker pods; **all 28 dashboard panels returning series**; a histogram exemplar's
trace opening in Tempo; the known request as **one trace of 13 spans across gateway and worker**; and
Loki holding its lines from both services, all with that trace id, and the prompt nowhere.

Building it found three real defects, now fixed: the controller's spec-hash annotation replaced the
pod template's annotations (so workers were never scraped), the worker logged its request line
after unbinding the request id, and the gateway's usage record was logged without the request's
correlation fields.

**Exit:** met — one request is followed from a dashboard histogram's exemplar to its trace in Tempo to
its log lines in Loki, and every dashboard panel is backed by a real series.

**Deviations** ([ADR-0035](./architecture-decisions/0035-observability-wiring.md)).

- Annotation-based scraping instead of `ServiceMonitor`s: kind has no Prometheus Operator.
- OTLP over HTTP (4318) rather than gRPC.
- Rollouts & Experiments, Cost, and Nodes & GPU dashboards are not shipped until their metrics exist
  (Phases 12, 13, and a GPU exporter); their §7.2 alerts likewise.
- No tail sampling or Alertmanager routing in the development stack (sampling is 100% there).
- The cardinality test of observability.md §8 is covered by the catalogue's label-budget check
  rather than a synthetic multi-org workload.

**Original plan**

**Deliverables**

- Full metric set from [architecture.md](./architecture.md) §10 across all services.
- OTel Collector, Tempo, Loki, Promtail, Prometheus, Grafana in the observability subchart;
  `ServiceMonitor`s.
- End-to-end trace: gateway → router → queue → dispatch → worker → runtime, with the documented span
  names and attributes.
- Grafana dashboards as committed JSON (Fleet Overview, Deployment Detail, Request Path, Queue &
  Autoscaling, Rollouts & Experiments, Cost, Nodes & GPU) and the alert rules from
  [observability.md §7](./observability.md#7-slos-and-alerts).
- Exemplars wired on histograms, so a Prometheus bucket links to a Tempo trace.
- `docs/observability.md` extended with runbook links; the architecture, metric catalogue, span model,
  and cardinality budget it already specifies are the contract this phase implements.

**Tests:** a metrics contract test asserting every documented metric exists with the documented labels
(so a rename cannot silently break a dashboard); a trace test asserting the full span tree for one
request; a log test asserting mandatory fields and that prompt content is absent by default.

**Exit:** one request ID can be followed from a Grafana panel to its trace in Tempo to its logs in
Loki, and every dashboard panel is backed by a real series.

---

## Phase 9 — Autoscaling

**Deliverables**

- `services/autoscaler`: signal collection (queue wait p95, concurrency, RPS, CPU/GPU utilization),
  ratio-based decision, stabilization windows, cooldown, max step, clamps, rollout interlock,
  `autoscaling_events` with the full signal vector including suppressed decisions.
- HPA for the gateway itself (a different problem: stateless CPU-bound scaling).
- Optional scale-to-zero with an activation queue, **off by default**.

**Tests:** simulated-clock scenarios — ramp up, plateau, ramp down, spike, flapping input (must not
oscillate: assert the number of changes over a synthetic hour), all-signals-missing (must hold, not
scale to min), max/min clamping, no scaling during a rollout. A real kind test: drive load, watch
replicas rise, remove load, watch them fall after the stabilization window.

**Exit:** queue depth growth causes replicas to rise and idle causes them to fall, with every decision
explainable from a database row.

---

## Phase 10 — Reliability mechanisms

**Deliverables**

- `packages/reliability`: error classification table, retry with budget and attempt cap, exponential
  backoff with full jitter, three-layer timeouts, per-endpoint circuit breaker with half-open probing,
  graceful shutdown/drain, request cancellation propagation.
- `preStop` drain wired for gateway and workers; `PodDisruptionBudget`s honoured.
- `docs/reliability.md` with the failure-mode table and each mode's expected behaviour.

**Tests:** classification table tests (one case per row); the critical negative test — a request that
has emitted a token is **never** retried; retry budget caps amplification under a total outage;
breaker opens, rejects fast, half-opens, recovers; drain completes in-flight requests with zero
dropped during a rolling restart (asserted under live load).

**Exit:** `make test-failure` passes: rolling restart under load with zero client errors; killed pod
recovers; dependency outages behave exactly as documented in architecture.md §8.4.

---

## Phase 11 — Model versioning and rollback

**Deliverables**

- Version lifecycle enforcement end to end (immutability verified at the database and the API).
- `deployment_revisions` on every spec change; `POST /v1/deployments/{id}/rollback`.
- Model-version change as a rolling update with `maxUnavailable: 0`, gated on the new version's pods
  becoming ready before old ones are removed.

**Tests:** mutating a ready version fails at both layers; rollback restores the exact prior spec and
creates a new revision rather than rewriting history; a rollout to a version that cannot load leaves
the old version serving 100% of traffic and reports the real reason.

**Exit:** deploy v1 → v2 → rollback to v1, with no failed client request at any point and a full audit
trail.

---

## Phase 12 — Canary deployments and A/B testing

**Deliverables**

- Rollout reconciler: state machine, step advance, Prometheus-backed analysis per arm, abort and
  auto-rollback, pause/resume/promote.
- Experiments: fixed weights, sticky bucketing, comparison read model with bootstrap confidence
  intervals and an explicit significance disclaimer.
- CLI and dashboard surfaces for both.

**Tests:** step advance on healthy metrics; abort on error-rate breach; abort on latency breach;
`on_insufficient_data` behaviour for each of `extend`/`pause`/`fail`; sticky bucketing stable across
gateway replicas and restarts; concurrent rollout on one route rejected by the database constraint.

**Exit:** a canary walks 10→25→50→100 on healthy metrics; an injected error rate on the canary aborts
it and restores baseline weights automatically, with the reason recorded.

---

## Phase 13 — Cost accounting

**Deliverables**

- `packages/costing`: integer-micro arithmetic, versioned pricing profiles, attribution model,
  stated assumptions.
- Usage ingester: JetStream → `requests` (batched `COPY`) → hourly `usage_records` rollups,
  idempotent.
- `GET /v1/usage`, `GET /v1/costs`, pricing profile management, CLI commands.

**Tests:** golden-value cost tests (known inputs → exact micros, no float drift); rollup idempotency
under duplicate delivery; sampling correction arithmetic; a historical cost recomputed after a price
change is unchanged (the reproducibility property the immutable pricing design exists to give).

**Exit:** run 1 000 requests, then reconcile: token totals match the sum of `requests` rows, which
match the rollups, which match the reported cost, and `nebula costs` explains its assumptions.

---

## Phase 14 — CLI

**Deliverables:** the full `nebula` command set from [api.md](./api.md) §5, `--output table|json|yaml`,
`--wait` with live status, meaningful exit codes, keyring credential storage, shell completions,
resumable artifact upload with progress, client/server skew warning, cross-platform binaries.

**Tests:** golden-file tests for human output; a stability test for `--output json` (the scripting
contract); exit codes per error class; an end-to-end script that performs the entire Definition of
Done using only the CLI.

**Exit:** the brief's developer-experience sequence works verbatim, from `nebula login` to
`nebula metrics qwen`.

---

## Phase 15 — Dashboard

**Deliverables:** React + TypeScript + Vite; generated API client; dark, dense, restrained
infrastructure aesthetic per the brief (no gradients, no glassmorphism, no decorative imagery);
pages: Overview, Models, Deployments, Deployment Detail, Inference (a real playground that streams),
Nodes, GPU, Requests, Metrics, Costs, Experiments, Rollouts, Logs, API Keys, Settings. Time-series
rendering via `uPlot` for density; virtualized tables; SSE for live logs and events.

**Explicit rule:** every number rendered traces to an API field or a Prometheus series. There is no
mock data path in the production bundle, and the build fails if the fixtures module is imported
outside tests.

**Tests:** component tests; an MSW-backed integration test per page; an accessibility pass (keyboard
navigation, contrast ≥ 4.5:1 on the dark theme); a visual check that no page renders a value the API
cannot supply.

**Exit:** every page renders live backend data on the kind cluster, including a streaming playground
response and a rollout advancing in real time.

---

## Phase 16 — Load testing and failure testing

**Deliverables**

- `tests/load`: k6 scenarios — steady state, ramp, spike, sustained overload, streaming-heavy,
  long-context, mixed priorities. Thresholds committed as SLOs so a regression fails CI.
- `tests/failure`: worker crash, pod delete, node drain/loss, model-load failure, Postgres down,
  Redis down, NATS partition, inference timeout, rollout failure, slow-loris client, mid-stream
  disconnect — each asserting the documented expected behaviour from architecture.md §8.4.
- A performance report: requests/sec, p50/p95/p99, TTFT, tokens/sec, error rate, resource
  utilization, and the point at which queueing begins — measured with the mock runtime for
  repeatability and re-measured with the real runtime for realism.

**Exit:** `make load` and `make test-failure` both green in CI on kind; the report is committed with
the measurement method, so the numbers can be reproduced and challenged.

---

## Phase 17 — Security hardening

**Deliverables:** key rotation and expiry; scope enforcement audit across every endpoint; RLS
verification tests for every table; audit-log coverage assertion (every mutating endpoint writes one);
secret handling review; TLS/mTLS wiring; `NetworkPolicy` completeness test (a pod that should not
reach PostgreSQL is *proved* unable to); input-size and prompt-size limits; dependency and image
scanning gates; `docs/security.md` — the operational threat model and hardening checklist extending
[security-boundaries.md](./security-boundaries.md), whose boundary contracts, identity model, and
threat table are the design this phase verifies.

**Tests:** authz matrix test (every endpoint × every role × in-org/out-of-org); a negative RBAC suite;
a secret-leak scan over logs, traces, and API responses in the e2e run; rate-limit bypass attempts.

**Exit:** the authz matrix is exhaustive and green; no secret appears in any log, trace, or response;
`trivy` clean at High+.

---

## Phase 18 — Production documentation

**Deliverables:** README with the architecture diagram and an honest capability/limitation table;
`docs/development.md`, `deployment.md`, `model-runtime.md`, `observability.md`, `security.md`,
`reliability.md`; all ADRs; runbooks for the top ten alerts; an upgrade guide and backup/restore
procedure; the reproducibility test — a reviewer who has never seen the repository follows
`development.md` on a clean machine and reaches a working inference request.

**Exit:** the twenty items in the brief's Definition of Done are each demonstrable, and each is linked
from the README to the command that demonstrates it.

---

## Sequencing notes

- **Phases 1–5 are strictly sequential.** Each depends on the previous one's contracts.
- **Phase 6 and 7 must land together in practice.** Routing without admission control produces a
  system that routes cheerfully into an overloaded worker; they are separated for reviewability, not
  because either is useful alone.
- **Phase 8 could be deferred but must not be.** Building autoscaling (9) and reliability (10)
  without observability means tuning blind, and the load tests in 16 would have nothing to read.
- **Phases 13, 14, 15 are parallelizable** once 8 is done — they consume stable APIs and touch
  disjoint code.
- **GPU work is not a phase.** It is designed throughout and enabled by configuration; it cannot be
  verified on the current hardware (risk R-07).
- **The `mock` runtime is load-bearing for velocity.** It lets Phases 6–10 and 16 run in CI with no
  model weights, which is the difference between a nightly e2e suite that works and one that is
  disabled after a week of timeouts.
