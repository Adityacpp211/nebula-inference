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

## Phase 1 — Repository, configuration, database, migrations

**Deliverables**

- Repository skeleton exactly as [repository-structure.md](./repository-structure.md); `go.mod`;
  Makefile; `.golangci.yml`; `.gitattributes`; CI workflow running build + lint + test.
- `packages/config` — layered loader, validation with full error lists, redaction, `NEBULA_ENV` rules.
- `packages/telemetry` — `slog` JSON setup, OTel init, Prometheus registry, shared `/livez`,
  `/readyz`, `/healthz`, `/metrics` handlers. Every later service gets these for free, which is how
  "every service must have a health endpoint" stops depending on discipline.
- `packages/db` — pgx pool, transaction helpers, RLS session setter, schema-version assertion.
- `migrations/000001…` — the full schema from [data-model.md](./data-model.md), including enums,
  constraints, partitions for `requests`/`audit_logs`, the immutability triggers, RLS policies, the
  three roles, and seed data.
- `packages/version` — build stamping and contract-skew detection.

**Tests:** config precedence and validation; migration up/down/up; RLS actually isolates two orgs
(the test that matters — a policy nobody verified is a policy that does not work); constraint tests
proving a ready `model_version` cannot be mutated and route weights cannot sum to anything but 100.

**Exit:** empty database → migrated schema with seed data; `psql` inspection matches the documented
model; CI green.

---

## Phase 2 — Control plane and model registry

**Deliverables**

- `services/controlplane`: HTTP server, auth middleware, request IDs, structured logging, the
  registry and deployment-spec endpoints from [api.md](./api.md) §4, audit logging on every mutation,
  cursor pagination, the error envelope.
- `packages/auth`: key generation, HMAC hashing with pepper, prefix lookup, scope checks, JWT issue
  and verify, RBAC by role.
- `packages/artifact`: `ArtifactStore` with `s3` (MinIO in dev) and `file` backends; presigned
  upload; streaming checksum verification; GGUF header parsing to extract context window,
  quantization, and parameter count rather than trusting client-declared metadata.
- `packages/api/openapi.yaml` v0.1 with codegen wired into `make gen`.
- Deployment spec CRUD writing `deployments` + `deployment_revisions` + `routes` transactionally.
  No Kubernetes yet — the rows are the contract.

**Tests:** API tests per endpoint including every documented error; upload/finalize with a corrupted
artifact (must fail with the computed checksum reported); revision creation on update; audit entries
present and redacted; cross-org access returns 404 not 403 (no existence leak).

**Exit:** `curl` can register a model, upload an artifact, create a version, and create a deployment
spec; every mutation appears in `audit_logs`.

---

## Phase 3 — Runtime abstraction and the first runtime

**Deliverables**

- `workers/inference`: FastAPI app, worker API from [api.md](./api.md) §6, bounded priority queue
  with deadlines, cancellation, drain, probes, Prometheus metrics, OTel tracing.
- `runtimes/base.py` — the protocol, typed errors, capability declaration.
- `runtimes/mock.py` — declared stub: seeded deterministic output, configurable tokens/sec, injectable
  errors and stalls, fake VRAM accounting. Refuses to start when `NEBULA_ENV=production`.
- `runtimes/llamacpp.py` — supervises `llama-server`, proxies generation, translates metrics, maps
  load failures to typed errors, handles child crash and restart.
- `tests/runtime_conformance.py` — the suite both adapters must pass.
- Dockerfiles for both worker variants; a small real model (Qwen2.5-0.5B-Instruct-Q4_K_M or
  TinyLlama-1.1B-Q4) documented as the reference dev model.

**Tests:** conformance suite on both adapters; TTFT is measured on the first token, not the last;
cancel frees the slot (asserted via `/internal/v1/state`); deadline exceeded aborts and reports;
queue-full returns 429 with `Retry-After`; child-process crash surfaces as unready, not as a hang.

**Exit:** `docker run` the worker with the mock runtime and with the real GGUF; stream tokens from
both via `curl`; identical externally observable behaviour apart from content.

---

## Phase 4 — API gateway and OpenAI-compatible API

**Deliverables**

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

## Phase 5 — Deployment controller and Kubernetes integration

**Deliverables**

- `services/controller`: informer-driven work queue, exponential backoff, periodic resync, leader
  election via Lease.
- Deployment reconciler: spec → `Deployment`/`Service`/`ConfigMap`/`PDB`, owner references, labels
  and annotations from [deployment-architecture.md](./deployment-architecture.md), status write-back
  (`observed_generation`, conditions, `ready_replicas`), artifact-puller initContainer.
- `packages/scheduler`: `hardware_profile` → constraints; node inventory from informers; capacity
  admission returning a real explanation on rejection.
- Inventory reconciler: `nodes` cache and `worker_events`.
- `deploy/helm/nebula` + `deploy/kind/cluster.yaml`; RBAC; NetworkPolicies; migration Job hook;
  `scripts/dev-up.sh` / `.ps1`.

**Tests:** controller tests with `envtest` (real API server, fake kubelet) for create/update/scale/
delete/adopt-orphan/drift-correction; the deliberate one — delete the Deployment out from under
NEBULA and assert it is recreated; inadmissible capacity request returns 422 before any object is
created; RBAC verified by `kubectl auth can-i` assertions in CI, including the negatives.

**Exit:** `nebula deploy` equivalent via `curl` creates real pods on kind; `kubectl get deploy -n
nebula-workloads` shows them; killing a pod brings a replacement; deleting the Deployment object
brings it back.

---

## Phase 6 — Router and health-aware routing

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

## Phase 7 — Request queue and concurrency control

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

## Phase 8 — Metrics, logs, tracing

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
