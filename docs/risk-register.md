# NEBULA — Risk Register

Reviewed at the end of every phase. A risk whose mitigation has not been implemented by the phase
named in "Mitigated by" is escalated in that phase's closing note rather than quietly carried
forward.

Scoring: **Likelihood** and **Impact** each Low/Medium/High. Severity is the combination, with
priority given to risks that would invalidate the project's claims rather than merely delay it.

---

## Environmental and process risks

### R-01 — No Kubernetes in the authoring sandbox · L: High · I: High

The cloud sandbox where code is authored has Go, Docker, and Postgres clients, but **no kubectl, kind,
or cluster**, and the Linux VM bridged to the laptop has no Go, Docker, or kubectl either. Everything
from Phase 5 onward that touches Kubernetes can be written and unit-tested here but can only be
*verified* on the laptop.

**Consequence if unmanaged:** manifests and controller logic that look right and fail on first
contact, discovered late, in bulk.

**Mitigation:** (a) controller logic tested with `envtest`, which runs a real API server as a
subprocess and needs no cluster — this covers most reconciler correctness in CI; (b) `helm template` +
`kubeconform` + `conftest` catch manifest errors without a cluster; (c) every Kubernetes-touching
phase ends with an explicit verification handoff — a short, copy-pasteable command list the reviewer
runs on the laptop, with expected output; (d) the nightly `e2e` workflow runs kind in GitHub Actions,
which is the real cluster verification and does not depend on either local machine.
**Mitigated by:** Phase 0.5 (preflight) and Phase 5 (envtest + CI kind).
**Residual:** timing and resource behaviour on a 2-node kind cluster on a laptop differs from CI.
Accepted; performance claims are always reported with the environment they were measured on.

### R-02 — Scope is larger than a single project can finish well · L: High · I: High

Twenty-eight product capabilities and eighteen phases. The failure mode is not running out of time;
it is producing eighteen half-features and calling it a platform.

**Mitigation:** the phase gate in [roadmap.md](./roadmap.md) — nothing advances while broken. Phases
1–10 are the spine and must be genuinely complete; 12–15 degrade gracefully in scope (a canary with
two steps and one metric is still a real canary; a dashboard with six pages of real data beats
fifteen of placeholders). Any reduction is recorded in the README's capability table, never left for
the reader to discover.
**Mitigated by:** every phase gate. **Residual:** accepted and made visible.

### R-03 — "Demo-only" logic silently becoming the implementation · L: Medium · I: High

The specific way infrastructure projects lie: a stub that works well enough, loses its comment, and
becomes load-bearing.

**Mitigation:** axiom A9 — every stub is named `mock`/`dev`, gated by `NEBULA_ENV`, refuses to run in
production, and carries `TODO(NEB-nnn)`. `make docs-check` fails if a `TODO(NEB-*)` has no tracking
issue. The `mock` runtime is the one intentional exception and is documented as a first-class test
tool, not a fallback.
**Mitigated by:** Phase 1 (config gating), Phase 3 (mock runtime), CI. **Residual:** Low.

---

## Architectural risks

### R-04 — Router state staleness causes wrong routing · L: Medium · I: High

The router decides from data that is up to a second old (heartbeats) plus an informer cache that lags
the API server. A request can be dispatched to a pod that is already gone, or to one whose queue
filled since its last heartbeat.

**Mitigation:** three overlapping sources with clear precedence (informer authoritative for existence,
heartbeats for load, local observation for failure); locally observed failure is believed immediately
rather than waiting for a heartbeat; workers reject what they cannot serve (429 + `Retry-After`) so a
stale decision costs a fast retry rather than a timeout; `X-Nebula-Model-Version` assertion makes a
stale route to the wrong model a clean 409 instead of wrong output; staleness windows are metrics with
alerts, so drift is observable.
**Mitigated by:** Phase 6, Phase 10. **Residual:** brief mis-dispatch on pod churn, absorbed by
retries. Measured, not assumed.

### R-05 — Autoscaler oscillation · L: Medium · I: Medium

Queue-depth-driven scaling with a slow actuator (model load takes seconds to minutes) is a classic
unstable control loop: scale up, load finishes, queue drains, scale down, queue grows.

**Mitigation:** asymmetric stabilization windows (30 s up, 300 s down), cooldown after every change,
max step limits, a rollout interlock, and `autoscaling_events` recording suppressed decisions so
oscillation is diagnosable. The dedicated test is a synthetic flapping signal with an assertion on the
*number* of replica changes per simulated hour.
**Mitigated by:** Phase 9. **Residual:** Low with defaults; tunable, and the tuning is visible.

### R-06 — Cost figures that look authoritative but are not · L: Medium · I: High

A number labelled "cost" gets screenshotted and believed. Attribution of shared idle capacity to
individual requests is genuinely ambiguous, and any single answer is a modelling choice.

**Mitigation:** `basis: "estimate"` on every cost field; assumptions returned inline in the API
response and rendered in the dashboard; integer micros so arithmetic is exact; immutable versioned
pricing so any figure is reproducible; the default global pricing profile has **zero** prices, so a
fabricated cost is impossible before an operator configures real ones.
**Mitigated by:** Phase 13. **Residual:** the attribution model is one defensible choice among
several, and it says so.

### R-07 — GPU paths cannot be verified on the available hardware · L: High · I: Medium

The development laptop has no NVIDIA GPU. GPU-aware scheduling, VRAM admission, DCGM-driven
autoscaling, and the vLLM adapter are therefore designed and coded but never run against real
hardware.

**Mitigation:** keep GPU support entirely configuration-driven so no code path is GPU-only; unit-test
the placement and admission logic against a fake capacity source that reports GPU nodes; ship the
device-plugin and DCGM wiring in `values-gpu.yaml` with a documented manual verification checklist;
label GPU support in the README as **designed, not verified**, with exactly what remains untested.
**Mitigated by:** Phase 5 (constraint generation), Phase 9 (signal plumbing), Phase 18 (honest
labelling). **Residual:** High and disclosed. Disclosure is the mitigation.

### R-08 — Streaming retries are impossible after the first token · L: High · I: Medium

Once a token reaches the client, the response cannot be replayed. A worker dying mid-stream is
therefore a user-visible error no retry policy can hide.

**Mitigation:** treat it as a designed behaviour, not a gap — the classification table forbids retry
after first token, the stream terminates with an explicit error frame and no `finish_reason` so
clients can detect truncation, partial usage is still recorded, and the metric
`nebula_streams_interrupted_total` makes the rate visible. Documented in `docs/reliability.md` so
users know what to handle.
**Mitigated by:** Phase 4, Phase 10. **Residual:** inherent to streaming. Accepted and documented.

### R-09 — The `requests` table becomes the bottleneck · L: Medium · I: Medium

One row per inference request, on a laptop-sized Postgres, with a load test pointed at it.

**Mitigation:** monthly partitions with automated creation and retention-based drop; no foreign keys
on the hot path; batched `COPY` inserts from a single durable consumer rather than per-request writes;
configurable sampling with errors always kept and the sample rate stored so counts are correctable;
`usage_records` rollups are what the UI reads, not the raw table. The load test explicitly asserts
that ingest lag stays bounded.
**Mitigated by:** Phase 1 (partitioning), Phase 13 (ingest), Phase 16 (verification). **Residual:**
Low.

### R-10 — Two sources of truth for live state · L: Medium · I: High

The most likely architectural regression: someone adds `pod_ips` or `live_replicas` to a Postgres
table because it is convenient for a dashboard query, and NEBULA starts making decisions from stale
copies of Kubernetes' state.

**Mitigation:** axiom A1 stated as a rule; every table labelled AUTHORITATIVE / OBSERVED /
APPEND-ONLY in [data-model.md](./data-model.md); OBSERVED tables carry `synced_at` and are documented
as rebuildable; database grants stop the control plane from writing controller-owned columns; the
`/replicas` endpoint reads the informer cache, not the database, and the API docs say so.
**Mitigated by:** Phase 1, Phase 5, and code review against the axiom. **Residual:** Low, but this is
the one to re-check every phase.

### R-11 — NATS becoming an undeclared dependency of the request path · L: Low · I: High

NATS is for control and telemetry ([ADR-0003](./architecture-decisions/0003-push-dispatch-and-nats-scope.md)).
The drift risk is that some future convenience — a cancellation broadcast, a capacity lookup — makes
inference *require* it.

**Mitigation:** an explicit test that runs the full inference path with NATS unreachable and asserts
success with degraded routing; the import-boundary linter keeps `packages/events` out of the
gateway's dispatch package; the degradation behaviour is written into architecture.md §8.4 as a
contract.
**Mitigated by:** Phase 6, Phase 16. **Residual:** Low.

### R-12 — Usage event loss under NATS outage · L: Low · I: Medium

Usage feeds cost. At-least-once delivery protects against consumer restarts, but a long publisher-side
outage eventually exhausts any buffer.

**Mitigation:** bounded in-memory buffer → local file spool → shed with `nebula_usage_events_dropped_total`
and a loud error log and an alert. Loss is **measured and reported**, never silent, and the API can
state that a period is incomplete rather than under-reporting it as fact.
**Mitigated by:** Phase 13. **Residual:** Medium under extended outage, and visible.

### R-13 — Rollout analysis drawing conclusions the data cannot support · L: Medium · I: Medium

A canary at 10% traffic for five minutes may see forty requests. Promoting on that is theatre; so is
claiming statistical significance.

**Mitigation:** `min_requests` gating with an explicit `on_insufficient_data` policy (extend, pause,
or fail — the operator chooses, the system does not guess); bootstrap confidence intervals reported
with the interval, not just the point estimate; an explicit `significance.claimed: false` field
naming what corrections are *not* applied.
**Mitigated by:** Phase 12. **Residual:** Low; the honesty is the feature.

---

## Operational and security risks

### R-14 — Model artifacts are large and slow · L: High · I: Medium

A 400 MB dev model is tolerable; a 4–15 GB real model is a cold start of minutes, a CI download nobody
wants, and a laptop disk filling quietly.

**Mitigation:** content-addressed artifacts with a node-local cache keyed by checksum (a rollback and
a second replica on the same node cost nothing); `startupProbe` tolerant of a five-minute load;
`nebula_model_load_duration_seconds` so cold start is a measured quantity; CI uses the `mock` runtime
and never downloads weights; the reference dev model is deliberately small; the cache has an LRU cap
and the host-mounted directory survives `kind delete cluster`.
**Mitigated by:** Phase 3, Phase 5. **Residual:** first-pull latency, inherent.

### R-15 — Multi-tenant isolation failure · L: Low · I: High

The highest-impact possible bug: one org's data visible to another.

**Mitigation:** enforcement twice — org-scoped queries *and* PostgreSQL RLS, so a missing `WHERE`
returns nothing instead of leaking; 404 rather than 403 for out-of-org resources so existence is not
disclosed; no cross-org worker sharing in v1 (stated as a non-goal rather than pretended); an
exhaustive authz matrix test over every endpoint × role × in/out-of-org in Phase 17; RLS tested per
table, because an unverified policy is decoration.
**Mitigated by:** Phase 1, Phase 2, Phase 17. **Residual:** Low.

### R-16 — Prompt content leaking into logs or traces · L: Medium · I: High

Prompts are the most sensitive data in an inference platform, and the easiest thing to accidentally
log while debugging.

**Mitigation:** content is never logged by default; a shared redaction helper is the only path for
writing request bodies anywhere; the opt-in per-org debug flag is time-bounded, audited when used, and
retention-limited; a secret-and-content scan runs over logs, traces, and API responses in the e2e
suite so a regression fails CI rather than reaching a Loki index.
**Mitigated by:** Phase 8, Phase 17. **Residual:** Low.

### R-17 — Dev conveniences reaching a production install · L: Medium · I: High

Default credentials, the mock runtime, seeded orgs, debug endpoints, NodePort with no TLS.

**Mitigation:** `NEBULA_ENV=production` makes each of these a startup failure rather than a warning;
no secret in the chart has a default value, so an insecure install cannot succeed silently; the
`conftest` policy suite fails a rendered chart that omits TLS, probes, resource requests, or non-root.
**Mitigated by:** Phase 1, Phase 5, Phase 17. **Residual:** Low.

### R-18 — Windows/Linux development friction · L: Medium · I: Low

The repository is developed on Windows, runs on Linux, and is authored partly in a Linux sandbox. Line
endings, path separators, file permissions, and shell script shebangs all bite.

**Mitigation:** `.gitattributes` normalizing to LF with an explicit CRLF exception for `.ps1`; every
script shipped as a `.sh`/`.ps1` pair with identical behaviour; a devcontainer as the escape hatch;
CI runs Linux, and a Windows job runs the CLI build and unit tests so Windows breakage is caught by a
machine rather than by a person.
**Mitigated by:** Phase 0.5, Phase 1. **Residual:** Low.

### R-19 — Helm chart and code drifting apart · L: Medium · I: Medium

New configuration is added to a service and the chart is updated a week later, or never, so the only
working configuration is the developer's laptop.

**Mitigation:** services validate their full configuration at startup and exit with a complete list of
problems, so a missing chart value fails loudly and immediately; `helm template` for all three values
files runs in CI; the nightly kind e2e installs *from the chart*, which is the only test that proves
the chart and the code agree.
**Mitigated by:** Phase 5, CI. **Residual:** Low.

### R-20 — Dependency and supply-chain exposure · L: Medium · I: Medium

llama.cpp moves fast, Python ML dependencies are heavy and CVE-rich, and a worker image with a
compiler in it is a liability.

**Mitigation:** pinned versions with lockfiles and hashes; Dependabot; `trivy` gating at High+ with
allowlist entries requiring a written justification; distroless static images for all Go services;
multi-stage Python build so no toolchain ships; SBOM and `cosign` signature per release; the
`llama-server` version pinned by digest and upgraded deliberately, with the runtime conformance suite
as the regression gate.
**Mitigated by:** Phase 1 (CI), Phase 17. **Residual:** Medium — inherent to the ML dependency tree,
and tracked rather than solved.

---

## Watch list

Not scored, but re-examined each phase, because these are the things most likely to be *discovered*
rather than predicted:

- Queue wait p95 as the primary autoscaling signal may prove too noisy at low request rates; the
  fallback is concurrency-per-replica, and the decision should be data-driven once Phase 16 has
  numbers.
- `llama-server`'s metrics surface may not expose everything the worker needs to report; if so, some
  metrics come from timing the adapter's own calls, and which ones must be documented rather than
  presented as engine-reported.
- Bootstrap confidence intervals over per-request latency assume independence, which continuous
  batching violates (requests in one batch share fate). If this matters at the sample sizes seen, the
  analysis needs a block bootstrap — and until it does, the disclaimer must say so.
- Two-node kind clusters cannot demonstrate topology spread under real node failure without cordoning
  tricks; the e2e node-loss test may need a third node, at a real memory cost on a laptop.
