# Architecture Decision Records

An ADR records a decision that was *not* obvious, the alternatives that were rejected, and the
consequences accepted. It is not documentation of how the code works — that is what the other docs
are for. An ADR exists so that six months later nobody re-litigates a settled question from scratch,
and so a reviewer can disagree with the reasoning rather than guess at it.

Format: [MADR](https://adr.github.io/madr/)-style, one file per decision, immutable once
**Accepted**. A decision that changes gets a **new** ADR that supersedes the old one; the old file
stays, marked `Superseded by ADR-nnnn`. Same reasoning as immutable model versions.

Status values: `Proposed` · `Accepted` · `Rejected` · `Superseded` · `Deprecated`.

## Index

| ADR | Title | Status | Phase |
|-----|-------|--------|-------|
| [0001](./0001-record-architecture-decisions.md) | Record architecture decisions | Accepted | 0 |
| [0002](#adr-0002) | Go for the control plane, Python only where model libraries require it | Accepted | 0 |
| [0003](./0003-push-dispatch-and-nats-scope.md) | Synchronous push dispatch; NATS for control and telemetry, not the token path | Accepted | 0 |
| [0004](./0004-router-as-library.md) | The router is an in-process library, not a network service | Accepted | 0 |
| [0005](./0005-cooperate-with-kubernetes.md) | Cooperate with Kubernetes: constraint generation and capacity admission, not a scheduler | Accepted | 0 |
| [0006](./0006-staged-orchestration.md) | Staged orchestration: native objects via client-go first, a CRD later | Accepted | 0 |
| [0007](#adr-0007) | JetStream for durable event streams, core NATS for heartbeats | Accepted | 0 |
| [0008](#adr-0008) | PostgreSQL owns desired state; Kubernetes and Redis own actual state | Accepted | 0 |
| [0009](#adr-0009) | Routes, not deployments, are the public inference identity | Accepted | 0 |
| [0010](#adr-0010) | Immutable model versions; rollback by revision, not rebuild | Accepted | 0 |
| [0011](#adr-0011) | API keys hashed with HMAC-SHA256 and a prefix index, not a slow KDF | Accepted | 0 |
| [0012](#adr-0012) | OpenAI-compatible subset under `/v1`, with NEBULA resources sharing the prefix | Accepted | 0 |
| [0013](#adr-0013) | Error classification, and never retrying after the first token | Accepted | 0 |
| [0014](#adr-0014) | Circuit breakers are per-replica and local, not distributed | Accepted | 0 |
| [0015](#adr-0015) | `InferenceRuntime` adapters; llama.cpp by supervision, not bindings | Accepted | 0 |
| [0016](#adr-0016) | Content-addressed artifact store with a node-local cache | Accepted | 0 |
| [0017](#adr-0017) | `requests` partitioned and sampled; `usage_records` as the billing read model | Accepted | 0 |
| [0018](#adr-0018) | Immutable versioned pricing profiles; integer micros | Accepted | 0 |
| [0019](#adr-0019) | Single Go module monorepo with OpenAPI and sqlc codegen | Accepted | 0 |
| [0020](#adr-0020) | Prometheus + OTel Collector + Tempo + Loki, no vendor SDK in service code | Accepted | 0 |
| [0021](#adr-0021) | Reconcilers and the autoscaler are separate, leader-elected processes | Accepted | 0 |
| [0022](#adr-0022) | Tenant isolation enforced twice: query scoping and row-level security | Accepted | 0 |
| [0023](#adr-0023) | UUIDv7 primary keys, generated in the application | Accepted | 0 |
| [0024](#adr-0024) | The dashboard talks only to the gateway | Accepted | 0 |
| 0025+ | reserved — written in the phase that makes the decision | — | — |

ADRs 0001, 0003, 0004, 0005, and 0006 have full files because their reasoning is long and they are the
ones a reviewer is most likely to challenge. The rest are recorded below in full-enough form to be
auditable, and are promoted to their own file if a phase revisits them.

---

<a id="adr-0002"></a>
### ADR-0002 — Go for the control plane, Python only where model libraries require it

**Decision.** All control-plane, data-plane, and CLI code is Go. Python is confined to the inference
worker, where the model-serving ecosystem lives.

**Why.** Go gives static binaries in distroless images, `client-go` and `controller-runtime` as
first-party Kubernetes tooling, predictable latency without GC tuning drama on a request path, and
`goroutine`-per-request concurrency that suits a proxy. Python owns the runtime layer because
`llama-cpp`, vLLM, and TGI are Python-first and reimplementing their bindings would be the opposite of
this project's point. The boundary is an HTTP contract, so neither language leaks into the other.

**Rejected.** All-Python (unacceptable for a latency-sensitive proxy and for Kubernetes controllers);
all-Go with CGO bindings to llama.cpp (locks out vLLM and TGI, and makes the runtime abstraction a
lie); Rust (excellent fit, but no `client-go` equivalent maturity and a smaller pool of reviewers for
a project meant to be read).

**Consequences.** Two toolchains, two test frameworks, two dependency ecosystems in CI. The worker's
Python image is fatter and needs more supply-chain attention (R-20).

---

<a id="adr-0007"></a>
### ADR-0007 — JetStream for durable event streams, core NATS for heartbeats

**Decision.** `nebula.usage.records`, `nebula.deployment.events`, `nebula.autoscale.events`, and
`nebula.worker.lifecycle` use JetStream with durable consumers. `nebula.worker.heartbeat.*` and
`nebula.control.reconcile` use core NATS with no persistence.

**Why.** The two classes of message have opposite requirements. Usage events feed cost accounting, so
losing one is a correctness bug and at-least-once with a dedup window on `request_id` is worth the
overhead. Heartbeats are ~1 Hz per pod and only the latest matters — persisting them would store
thousands of instantly worthless messages and add latency to the signal the router depends on. A
reconcile nudge is best-effort because the periodic resync is the actual safety net; delivering it
twice costs a no-op reconcile and losing it costs a few seconds.

**Rejected.** JetStream for everything (pointless write amplification for heartbeats); core NATS for
everything (silent usage loss on any consumer restart, which is exactly the "fake metrics" failure the
brief forbids); Kafka (durability and replay we do not need, at operational weight a laptop should not
carry); Redis Streams (already have a broker; using Redis as one would make Redis load-bearing for
correctness when it is deliberately only a cache).

**Consequences.** Two delivery semantics to reason about, and every consumer must be idempotent.
JetStream needs storage and a retention policy, configured in the chart.

---

<a id="adr-0008"></a>
### ADR-0008 — PostgreSQL owns desired state; Kubernetes and Redis own actual state

**Decision.** Desired state — what the user asked for — is exclusively in PostgreSQL. Actual state is
read from the Kubernetes API (existence, readiness, placement) and Redis (capacity snapshot, cached
routing view). Observed values persisted in PostgreSQL are labelled OBSERVED, carry `synced_at`, and
are treated as a rebuildable cache for history and UI, never as a decision input.

**Why.** Control planes fail in a characteristic way: someone persists live state for query
convenience and the system starts acting on a stale copy. Naming a single owner per fact makes that a
reviewable rule instead of a judgement call. It also makes the reconciler's job definable — diff two
clearly separated things — and makes "delete the database and rebuild the cache" a recovery procedure
rather than a catastrophe.

**Rejected.** Kubernetes CRDs as the only store (loses relational integrity, multi-tenant queries,
cost aggregation, and audit history; etcd is not a reporting database); a single store for both
(indistinguishable desired and observed values, which is how a controller ends up fighting itself).

**Consequences.** Some questions need two reads (spec from PostgreSQL, status from an informer). Status
write-back is eventually consistent, so the API exposes `observed_generation` and clients are told to
poll rather than assume.

---

<a id="adr-0009"></a>
### ADR-0009 — Routes, not deployments, are the public inference identity

**Decision.** The `model` field in an inference request names a **route**. A route fans out to weighted
`route_targets`, each pointing at a deployment that pins one immutable model version. Clients never
name a deployment (except with an explicit, scoped override).

**Why.** This single indirection is what makes canary deployments, A/B tests, blue-green swaps, and
rollback possible without clients changing anything or the request path gaining special cases. Without
it, every one of those features becomes a hack in the dispatch code. With it, they are all the same
operation: adjust weights in a transaction.

**Rejected.** `model` naming a deployment directly (canary would require clients to change, which
defeats the purpose); version-suffixed model names like `qwen:v2` (pushes rollout mechanics onto every
caller); a header-based override as the primary mechanism (untestable in an SDK that does not send it).

**Consequences.** One more resource to explain and one more join on the hot path — mitigated by caching
the resolved route table in the gateway. Weights must sum to 100, enforced in the database because a
route summing to 90 silently drops traffic.

---

<a id="adr-0010"></a>
### ADR-0010 — Immutable model versions; rollback by revision, not rebuild

**Decision.** A `model_versions` row is immutable once `ready`, enforced by a database trigger, not
only by application code. Deployments reference a version. Every spec change writes an immutable
`deployment_revisions` snapshot. Rollback writes a *new* revision whose spec equals an old one.

**Why.** Reproducibility: "what exactly was serving at 14:32 last Tuesday" must be answerable, and it
cannot be if an artifact or a spec can be edited in place. Rollback becomes a normal write instead of a
special code path, and because the target artifact is still cached by checksum, rollback is fast. The
database-level trigger matters because application-level immutability survives exactly until the first
migration script or admin console that bypasses it.

**Rejected.** Mutable versions with an audit trail (audit tells you it changed, not what the old bytes
were); tag-style mutable pointers as the only identity (the Docker `latest` problem); rollback by
rewriting the current revision (destroys the history that makes an incident reviewable).

**Consequences.** More rows. A typo in a version's metadata requires a new version. Retention and
archival policy for old versions and their artifacts becomes a real concern.

---

<a id="adr-0011"></a>
### ADR-0011 — API keys hashed with HMAC-SHA256 and a prefix index, not a slow KDF

**Decision.** Keys are `nbk_` + 32 random bytes, base62. Stored as HMAC-SHA256(server pepper, key)
alongside an indexed 11-character prefix. Lookup is by prefix, then constant-time comparison. The
plaintext is shown once. Verified results are cached in Redis with a short TTL.

**Why.** Argon2id and bcrypt are correct for *user passwords*, where the secret is low-entropy and
attacker-guessable, and where the verification cost is paid once per login. An API key is 256 bits of
CSPRNG output: brute force is not a threat model, so the KDF buys nothing and costs tens of
milliseconds on **every inference request**. The pepper (held only by the application) means a database
dump alone does not yield usable hashes. The prefix makes lookup O(1) and is safe to display in the
UI, in logs, and in audit records.

**Rejected.** Argon2id per request (latency on the hot path for no threat-model benefit); plain SHA-256
without a pepper (a stolen dump becomes directly verifiable offline); storing keys encrypted rather
than hashed (creates a decryption path and a key-management problem where none is needed); JWT-only
auth (revocation requires a blocklist, which is the thing JWTs were meant to avoid).

**Consequences.** Pepper rotation needs a documented dual-verify migration. Redis caching means a
revoked key stays valid for up to the TTL, so revocation additionally publishes an invalidation event
and the TTL is kept short (30 s).

---

<a id="adr-0012"></a>
### ADR-0012 — OpenAI-compatible subset under `/v1`, with NEBULA resources sharing the prefix

**Decision.** `/v1/chat/completions`, `/v1/completions`, `/v1/embeddings`, and `/v1/models` follow
OpenAI's shapes exactly. NEBULA's own resources (`/v1/deployments`, `/v1/usage`, …) live under the same
`/v1` prefix, as the brief specifies. NEBULA extensions to OpenAI payloads are namespaced under a
`nebula` object; unsupported OpenAI parameters are rejected with a named 400.

**Why.** Compatibility is worth real constraint: every OpenAI SDK, LangChain, and evaluation harness
works unmodified, which is a large part of the product value. No collision exists with the admin
resources because no OpenAI client requests `/v1/deployments`. Namespacing extensions keeps NEBULA
forward-compatible with new OpenAI fields. Rejecting unsupported parameters instead of ignoring them
matters because silently dropping `logit_bias` changes the output while the caller believes it was
applied.

**Rejected.** `/api/v1` for admin (cleaner in the abstract, contradicts the brief, and adds a second
prefix for no functional gain); top-level extension fields (collision risk with future OpenAI
additions); accepting and ignoring unsupported parameters (silent wrongness).

**Consequences.** NEBULA inherits some OpenAI awkwardness (`stream_options.include_usage`,
`finish_reason` values). Any future OpenAI addition must be triaged as support-or-reject.

---

<a id="adr-0013"></a>
### ADR-0013 — Error classification, and never retrying after the first token

**Decision.** A single classification table maps every failure to retryable or not (see
[architecture.md §6.4](../architecture.md#64-reliability-rules)). Retries are attempt-capped,
budget-limited to 10% of a deployment's traffic, jittered, and never extend past the request deadline.
**No request is retried once a token has been delivered to the client.**

**Why.** Blind retry is how a partial outage becomes a total one: capacity drops, every client retries,
load triples, the remaining replicas fall over. A budget bounds amplification independently of any
single client's behaviour. The first-token rule is not a limitation but a correctness requirement — an
HTTP response body cannot be un-sent, so a "retry" would concatenate two generations into one stream
and produce text no model actually produced.

**Rejected.** Retrying everything idempotent-looking (inference is not idempotent without a seed, and
even with one, sampling state differs); client-side-only retries (no global budget, no shared health
view); buffering the whole stream so it *could* be replayed (destroys time-to-first-token, which is the
main reason streaming exists).

**Consequences.** Mid-stream failures are user-visible and clients must handle a terminal error frame.
The retry budget can reject a legitimately retryable request during a widespread outage; this is
deliberate and metered (`nebula_retries_rejected_budget_total`).

---

<a id="adr-0014"></a>
### ADR-0014 — Circuit breakers are per-replica and local, not distributed

**Decision.** Each gateway replica keeps its own breaker state per worker endpoint, from its own
observations. No shared breaker state in Redis. Cross-replica health information travels as *signals*
(heartbeats, readiness) that each replica interprets itself.

**Why.** A distributed breaker adds a network read to the hot path, creates a shared-fate dependency on
Redis for a mechanism whose entire purpose is graceful degradation, and is prone to correlated
tripping — one replica's network problem opening the breaker for everyone. Local breakers converge
anyway, because all replicas observe the same failing backend, and they degrade independently, which is
the behaviour you want. Small clusters may have one gateway replica, where "distributed" is pure cost.

**Rejected.** Redis-backed shared breakers (hot-path dependency, correlated failure, cache-outage
coupling); no breakers at all (queues fill with doomed requests and the deadline budget is burned on a
known-bad endpoint).

**Consequences.** A new or restarted gateway replica must learn a bad endpoint itself, costing a few
failed requests. Breaker state is per-replica in metrics, so dashboards aggregate across replicas and
`nebula_breaker_state` is labelled by pod.

---

<a id="adr-0015"></a>
### ADR-0015 — `InferenceRuntime` adapters; llama.cpp by supervision, not bindings

**Decision.** Runtimes sit behind the `InferenceRuntime` protocol
([api.md §7](../api.md#7-inferenceruntime-interface)). The llama.cpp adapter **supervises the upstream
`llama-server` binary** as a child process and proxies to it over localhost. vLLM will use its
in-process async engine. A `mock` adapter is a declared stub for CI and load tests. All adapters pass
one shared conformance suite.

**Why.** Supervision inherits llama.cpp's continuous batching, slot management, and KV-cache handling —
thousands of engineering hours we should not reimplement — and keeps generation off the Python GIL, so
the worker's own HTTP server, metrics, and cancellation stay responsive under load. It also makes the
engine independently upgradable, pinned by digest. In-process bindings would couple the worker's
lifecycle to the engine's and make an engine crash a worker crash. vLLM's in-process engine is the
right choice for vLLM because that *is* its supported interface — which is exactly why the abstraction
must not assume a subprocess.

**Rejected.** `llama-cpp-python` bindings in-process (GIL contention on the token path, engine crash
kills the worker, no free continuous batching); one runtime with no abstraction (locks the project to a
single engine and makes GPU support a rewrite); a generic "OpenAI-compatible backend" adapter only
(hides load state, queue depth, slots, and cancellation — precisely the signals routing and autoscaling
need).

**Consequences.** The worker image contains a shell and a supervised binary, which weakens the
hardening story (noted in deployment-architecture.md §10). Child lifecycle, zombie reaping, and crash
restart are the adapter's problem. Two IPC hops (gateway → worker → engine) add ~1 ms, which is
negligible next to token generation.

---

<a id="adr-0016"></a>
### ADR-0016 — Content-addressed artifact store with a node-local cache

**Decision.** Artifacts live in S3-compatible object storage at `sha256/<hex>`, are pulled by an
initContainer into a node-local cache keyed by checksum, verified before use, and mounted read-only.
MinIO provides the S3 API in development.

**Why.** Content addressing makes a cache hit provably correct, deduplicates identical artifacts across
deployments and versions, and makes rollback free because the old artifact is still cached. Object
storage is the only realistic home for multi-gigabyte immutable blobs, and keeping the same S3 interface
in development means the production path is the tested path.

**Rejected.** Baking weights into container images (every deploy becomes a multi-gigabyte registry push
and the model lifecycle gets welded to the image build); one ReadWriteMany PVC (most storage classes
cannot, and it becomes a throughput bottleneck exactly when many pods start together); downloading from
Hugging Face at pod start (an external dependency in the startup path, and no integrity guarantee).

**Consequences.** MinIO is another dev dependency. Cache eviction needs a policy (LRU by bytes, never
evicting a referenced artifact). Presigned upload URLs need expiry and size limits.

---

<a id="adr-0017"></a>
### ADR-0017 — `requests` partitioned and sampled; `usage_records` as the billing read model

**Decision.** One row per request in a monthly-partitioned, foreign-key-free, retention-bounded
`requests` table, written by batched `COPY` from a single durable consumer, with configurable sampling
(errors always kept). Hourly `usage_records` rollups are what the UI and cost APIs read, and they are
retained indefinitely.

**Why.** Per-request rows are genuinely valuable for debugging and analysis, and genuinely dangerous as
an unbounded table with synchronous writes on the request path. Separating raw from rollup lets each
have the right retention, the right write pattern, and the right index set. Storing the sample rate with
the rollup means counts can be corrected rather than silently understated — the alternative is a
dashboard that under-reports traffic and nobody knows why.

**Rejected.** No `requests` table at all, traces only (Tempo retention is short and sampled; "list this
key's failed requests yesterday" becomes unanswerable); synchronous per-request inserts (a database
write on the inference path, coupling inference availability to PostgreSQL); unpartitioned with DELETE
cleanup (vacuum pressure and a table that eventually cannot be maintained).

**Consequences.** Referential integrity for `requests` is eventual by design, and the columns are
documented as historical snapshots. Partition creation and retention need a maintenance job. Ingest lag
is a metric with an alert.

---

<a id="adr-0018"></a>
### ADR-0018 — Immutable versioned pricing profiles; integer micros

**Decision.** Prices live in immutable `pricing_profiles` rows versioned by `(name, version)` with
`effective_from`. Every `usage_records` row names the profile version used. All money is `bigint`
micros with an explicit currency. Every cost response carries `basis: "estimate"` and its assumptions.

**Why.** A cost figure that cannot be recomputed is not auditable, and prices change — so the price must
be part of the historical record, not a mutable lookup. Integer micros eliminate float accumulation
error, which in cost arithmetic over millions of requests is not theoretical. Returning the assumptions
inline is the difference between an estimate and a misleading number, and the default profile prices
everything at zero so NEBULA can never invent a cost before an operator configures one.

**Rejected.** Mutable pricing rows (retroactively changes history); `numeric` money (correct but
invites float conversion at every boundary and in JSON); prices in configuration files only (not
queryable, not auditable, not per-tenant).

**Consequences.** A price change is a new row and a small migration-like operation. Multi-currency
comparison is out of scope; each profile declares one currency.

---

<a id="adr-0019"></a>
### ADR-0019 — Single Go module monorepo with OpenAPI and sqlc codegen

**Decision.** One Go module for all services and packages. `packages/api/openapi.yaml` is the source of
truth for HTTP contracts, generating Go server interfaces, the Go client, and TypeScript types. `sqlc`
generates database access from reviewed `.sql` files. Generated code is committed, and CI fails if
regeneration produces a diff.

**Why.** The contract-breaking changes worth catching are cross-cutting: an endpoint, its handler, its
client, and its migration. One module and one commit make those reviewable together, and codegen makes
drift mechanically impossible rather than a review responsibility. Committing generated code keeps the
build hermetic and makes diffs visible in review, which is where an accidental breaking change is
actually caught.

**Rejected.** Multi-module Go (version pinning and `replace` directives between components that always
ship together); polyrepo (cross-cutting changes become coordinated releases); hand-written types
(guaranteed eventual divergence); an ORM (hides the SQL this project should be demonstrating, and
generates queries nobody reviewed).

**Consequences.** CI must run codegen and diff. Contributors need the generator toolchain, pinned in
`.tool-versions` and checked by `make preflight`.

---

<a id="adr-0020"></a>
### ADR-0020 — Prometheus + OTel Collector + Tempo + Loki, no vendor SDK in service code

**Decision.** Services expose Prometheus metrics and emit OTLP traces to an OTel Collector, which fans
out to Prometheus, Tempo, and Loki. Service code imports only the OpenTelemetry API and
`prometheus/client_golang`. No vendor-specific exporter is linked into a binary.

**Why.** The Collector is the seam that makes the backend swappable: an operator with Datadog,
Honeycomb, or Grafana Cloud changes Collector configuration, not application code. Prometheus is the
Kubernetes-native metrics standard and what HPAs and the autoscaler already speak. Tempo and Loki share
Grafana's label model, so a trace, a metric, and a log line correlate in one UI — which is the actual
requirement behind "distributed tracing", not merely having spans.

**Rejected.** Jaeger (fine for traces, but a second UI next to Grafana for no gain); ELK (heavy for a
laptop, and a different label model from the metrics); direct vendor SDKs (couples the code to a
backend, which is the opposite of the point); metrics-only (no way to answer "what happened to *this*
request").

**Consequences.** The Collector is another component to run and to reason about when telemetry is
missing. Loki's label discipline must be respected — high-cardinality labels like `request_id` go in
the log body, not in labels, and the linter checks this.

---

<a id="adr-0021"></a>
### ADR-0021 — Reconcilers and the autoscaler are separate, leader-elected processes

**Decision.** Reconciliation lives in `nebula-controller` and scaling decisions in
`nebula-autoscaler`, both separate from the admin API in `nebula-controlplane`, each with two replicas
and Lease-based leader election so exactly one instance acts.

**Why.** Different failure modes and different cadences. A reconcile storm or a wedged informer must not
make the admin API unresponsive, and an operator must be able to disable autoscaling cluster-wide
(`replicas: 0`) without stopping deployment reconciliation. Leader election gives single-writer
semantics — essential when the work is "make the world match this record" — while keeping failover in
seconds. And "is NEBULA scaling right now?" becomes one pod's logs rather than a filter over a
multiplexed service.

**Rejected.** One process for everything (the brief's suggested shape; conflates a request-serving
latency profile with a control-loop one, and makes blast radius unnecessarily wide); a separate process
per reconciler (three leader elections and three informer caches of the same objects, for no benefit);
active-active without leader election (two writers racing on the same Kubernetes objects, which is a
correctness bug, not a performance choice).

**Consequences.** Two more Deployments and two more Leases. Shared informer caches are per-process, so
memory is duplicated — acceptable at this scale. The standby replica is idle capacity, which is the
price of fast failover.

---

<a id="adr-0022"></a>
### ADR-0022 — Tenant isolation enforced twice: query scoping and row-level security

**Decision.** Every org-scoped query filters on `org_id`, **and** PostgreSQL row-level security
policies filter on a per-transaction `app.current_org`. Background jobs use a distinct role that
bypasses RLS explicitly. Out-of-org resources return 404, not 403.

**Why.** Cross-tenant data exposure is the single highest-impact bug this system could have, and a
forgotten `WHERE` clause is an ordinary mistake that code review catches most of the time. RLS turns
"most of the time" into a database-enforced guarantee: the mistake yields an empty result set instead of
another tenant's rows. The cost is one `SET LOCAL` per transaction. Returning 404 rather than 403
prevents existence disclosure — 403 tells an attacker the resource is real.

**Rejected.** Application-level filtering only (one missed clause is a breach); a database per tenant
(operationally heavy and defeats cross-tenant operator views); schema per tenant (migration pain
multiplied by tenant count).

**Consequences.** Every connection path must set the session variable, including tests; a forgotten
setter surfaces as an empty result, so the RLS test suite must be per-table rather than sampled.
Explicit bypass for admin and background work is a reviewed, named code path.

---

<a id="adr-0023"></a>
### ADR-0023 — UUIDv7 primary keys, generated in the application

**Decision.** All primary keys are `uuid` columns holding UUIDv7, generated in Go, not by the database.

**Why.** UUIDv7 is time-ordered, so inserts land at the right edge of the B-tree instead of scattering
across it — which matters enormously for `requests` and `audit_logs` at volume, and costs nothing
elsewhere. Application-side generation means an ID exists before the insert, so a request ID can be
logged and traced before any database round-trip and multi-table inserts need no returning clauses.
Non-sequential values prevent cross-tenant enumeration.

**Rejected.** UUIDv4 (random insert distribution, index fragmentation, worse cache locality);
`bigserial` (enumerable, and requires a round-trip before the ID exists); ULID as text (16 bytes become
26, and PostgreSQL's native `uuid` type and operators are lost); database-side generation (the ID does
not exist early enough to correlate).

**Consequences.** PostgreSQL 16 has no built-in v7 generator, so the application owns generation via a
library, and tests must assert monotonicity. Timestamps are embedded in IDs, which is a minor
information leak (creation time) — acceptable, and noted.

---

<a id="adr-0024"></a>
### ADR-0024 — The dashboard talks only to the gateway

**Decision.** The dashboard is a static bundle that calls the gateway exclusively. It never calls the
control plane, a worker, Prometheus, or Loki directly. Metric and log queries are proxied by the
gateway with tenant scoping applied.

**Why.** The brief requires the dashboard not be coupled to inference workers, and the same reasoning
extends further: one auth path, one rate limiter, one audit trail, one place where tenant scoping is
enforced. A dashboard that queries Prometheus directly has no tenant boundary at all — PromQL has no
notion of an org, so any user could query any label. Proxying means the gateway can restrict the query
surface and inject label matchers.

**Rejected.** Direct control-plane access (exposes an internal service publicly and duplicates auth);
direct Prometheus/Loki access from the browser (no tenant isolation, and it exposes the whole
observability stack); a dedicated BFF service (another service to justify, when the gateway already
does auth, rate limiting, and proxying).

**Consequences.** The gateway needs a bounded metrics-query endpoint with injected label matchers,
query cost limits, and a small allowlist of query shapes — deliberately not an arbitrary PromQL
passthrough, which would be an injection surface.
