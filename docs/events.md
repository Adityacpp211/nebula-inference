# NEBULA — Event and Message Definitions

**Status:** Phase 0 design. The authoritative artifacts will be Go structs in `packages/events` plus
JSON Schema files under `packages/events/schema/`, with a CI check that every published subject has a
schema and every schema has a round-trip test.

Scope: every asynchronous message in NEBULA. Synchronous HTTP contracts are in [api.md](./api.md).
The boundary between them is deliberate and load-bearing: **inference requests and generated tokens
never travel over NATS** ([ADR-0003](./architecture-decisions/0003-push-dispatch-and-nats-scope.md)).

---

## 1. Design rules

| Rule | Reason |
|------|--------|
| Subjects are hierarchical: `nebula.<domain>.<entity>[.<qualifier>]` | Wildcard subscription (`nebula.worker.heartbeat.qwen-prod.*`) without a registry. |
| Every message carries an **envelope**: `event_id`, `event_type`, `schema_version`, `occurred_at`, `producer`, `trace_id`, `org_id` | A consumer can route, deduplicate, and correlate without parsing the payload. |
| `event_id` is a **UUIDv7** and is the deduplication key | Time-ordered, and the natural idempotency key for at-least-once delivery. |
| Payloads are JSON | Debuggable with `nats sub`, which matters more here than the bytes saved by a binary codec. Protobuf is reconsidered only if heartbeat volume becomes a measured problem. |
| Every consumer is **idempotent** | At-least-once delivery means every consumer *will* see a duplicate eventually. Not "should be" — is. |
| No message is required for correctness of the request path | Losing every message in this document degrades quality; it never produces a wrong inference response. |
| Additive schema evolution only within a `schema_version` | New optional fields are safe; anything else is a version bump with dual-read. |
| Payloads carry **no prompt or completion content** | Logs, traces, and events are operator-visible; prompts are the most sensitive data in the system. |

### 1.1 Envelope

```json
{
  "event_id":       "0192f3c1-7b5a-7c3e-9a11-0e5d2b8f4a21",
  "event_type":     "nebula.usage.record.v1",
  "schema_version": 1,
  "occurred_at":    "2026-09-22T14:32:07.412Z",
  "producer":       { "service": "nebula-gateway", "version": "0.4.1+a1b2c3d", "instance": "gateway-7d9f-x2k" },
  "trace_id":       "4bf92f3577b34da6a3ce929d0e0e4736",
  "org_id":         "0192f3b0-1111-7000-8000-000000000001",
  "payload":        { }
}
```

`producer.instance` is included because "which replica emitted this" is the first question during an
incident, and reconstructing it from timestamps afterwards is guesswork.

### 1.2 Transport selection

Two classes, chosen per subject, never mixed
([ADR-0007](./architecture-decisions/README.md#adr-0007)):

| Class | Transport | When | Loss behaviour |
|-------|-----------|------|----------------|
| **Signal** | core NATS, no persistence | high frequency, latest-wins, only the current value matters | tolerated and expected; staleness is detected and handled |
| **Record** | JetStream, durable consumer | must survive a consumer restart because it feeds cost, audit, or a state machine | at-least-once with a dedup window; drops are counted and alerted, never silent |

Persisting signals would store thousands of instantly worthless messages and add latency to the very
signal routing depends on. Treating records as signals would silently lose billing data — which is the
"fake metrics" failure the brief forbids, arrived at by accident.

---

## 2. Subject catalogue

| Subject | Class | Stream config | Producer | Consumers | Rate |
|---------|-------|---------------|----------|-----------|------|
| `nebula.worker.heartbeat.<deployment>.<pod>` | Signal | — | worker | gateways (all replicas), autoscaler | 1 Hz × pods |
| `nebula.worker.lifecycle` | Record | Limits, 30 d, replicas 3 | worker, controller | controller, dashboard | low |
| `nebula.usage.records` | Record | **WorkQueue**, 7 d, dedup 2 m | gateway | controller usage ingester (single durable consumer) | 1 per request |
| `nebula.deployment.events` | Record | Limits, 30 d | controller | dashboard live feed, audit sink | low |
| `nebula.autoscale.events` | Record | Limits, 30 d | autoscaler | dashboard, controller | low |
| `nebula.control.reconcile` | Signal | — | controlplane | controller | on spec change |
| `nebula.control.cache-invalidate` | Signal | — | controlplane | gateways | on key revocation or policy change |
| `nebula.batch.<deployment>` | Record | WorkQueue, 24 h | gateway | workers (queue group) | **reserved, post-v1** |

`nebula.usage.records` is a **WorkQueue** stream specifically: exactly one logical consumer, and a
message is removed once acknowledged. A Limits stream with multiple consumers would invite a second
consumer that double-counts cost.

---

## 3. Signal messages

### 3.1 `nebula.worker.heartbeat.<deployment>.<pod>`

The highest-volume message in the system and the one routing quality depends on. Envelope is omitted
for this subject — at 1 Hz per pod, the envelope would be most of the bytes, and the subject already
carries identity.

```json
{
  "pod":                  "nebula-acme-qwen-prod-7d9f-x2k",
  "node":                 "nebula-dev-worker2",
  "deployment_id":        "0192f3c1-...",
  "model_version_id":     "0192f3c0-...",
  "model_version":        "qwen2.5:0.5b-q4",
  "runtime":              "llamacpp",
  "state":                "ready",
  "instance":             "5f0c2e9a41d34c0e9b7d6f1a2c3b4d5e",
  "sequence":             48213,
  "emitted_at_ms":        1774189927412,
  "inflight":             3,
  "queue_depth":          1,
  "queue_wait_ms_ewma":   84,
  "parallel_slots":       4,
  "slots_busy":           3,
  "ttft_ms_ewma":         96,
  "tokens_per_second_ewma": 97.4,
  "accepting":            true,
  "kv_cache_used_mib":    512,
  "kv_cache_total_mib":   2048,
  "rss_mib":              6140,
  "vram_used_mib":        0,
  "model_loaded_at":      "2026-09-22T14:02:11Z"
}
```

| Field group | Consumed by | For |
|-------------|-------------|-----|
| `inflight`, `queue_depth`, `slots_busy` | gateway `LeastLoaded` | endpoint selection |
| `ttft_ms_ewma`, `tokens_per_second_ewma` | gateway `LatencyAware` | heterogeneous hardware |
| `accepting`, `state` | gateway `Filter` | eligibility |
| `queue_wait_ms_ewma`, `inflight` | autoscaler | primary scaling signals |
| `kv_cache_used_mib`, `vram_used_mib`, `rss_mib` | autoscaler, dashboard | saturation and capacity |
| `model_version` | gateway | detect a stale route before dispatching |

Semantics:

- **Latest-wins per pod.** `sequence` is monotonic per worker process, which `instance` names; a
  consumer discards an out-of-order message from the same instance rather than applying it, and a new
  instance (a container restart in the same pod) replaces the old one.
- **Draining is published at once**, not at the next tick, with `accepting: false`, so gateways stop
  sending to a terminating pod before its EndpointSlice changes. A worker's last heartbeat on
  shutdown also says `accepting: false`.
- Optional fields: `context_window` (the loaded version's), `kv_cache_used_mib` when the engine
  reports it, and `advertise_url` for a worker outside Kubernetes, where no EndpointSlice ties the
  heartbeat to an endpoint.
- **Staleness is the failure detector.** Older than 3 intervals → endpoint marked `stale` and dropped
  from selection. This is usually *faster* than EndpointSlice removal, which is why it exists.
- `emitted_at_ms` is the producer's clock, so consumers compare against their own receive time and
  never subtract two clocks. Clock skew changes staleness precision, not correctness.
- Loss of individual heartbeats is normal and harmless. Loss of all of them degrades routing to
  EndpointSlice readiness plus local observation — correct, less optimal.

### 3.2 `nebula.control.reconcile`

```json
{ "payload": { "resource_type": "deployment", "resource_id": "0192f3c1-...",
               "generation": 7, "reason": "spec_updated" } }
```

A pure latency optimization. The controller's periodic resync (60 s) is the safety net, so losing this
message costs seconds, never correctness — which is exactly why it may be a fire-and-forget signal.

### 3.3 `nebula.control.cache-invalidate`

```json
{ "payload": { "kind": "api_key", "key_prefix": "nbk_7Kq2mZ4", "action": "revoked" } }
```

Gateways cache verified API keys in Redis for 30 s. Revocation publishes this so the window closes
immediately. The short TTL is the guarantee; this message is the optimization. A revoked key must never
depend on a broker to stop working.

---

## 4. Record messages

### 4.1 `nebula.usage.records`

The correctness-critical event. One per request, emitted after the response completes or aborts.

```json
{
  "event_id":       "0192f3c1-7b5a-7c3e-9a11-0e5d2b8f4a21",
  "event_type":     "nebula.usage.record.v1",
  "schema_version": 1,
  "occurred_at":    "2026-09-22T14:32:08.732Z",
  "producer":       { "service": "nebula-gateway", "version": "0.4.1", "instance": "gateway-7d9f-x2k" },
  "trace_id":       "4bf92f3577b34da6a3ce929d0e0e4736",
  "org_id":         "0192f3b0-1111-7000-8000-000000000001",
  "payload": {
    "request_id":        "0192f3c1-7b5a-7c3e-9a11-0e5d2b8f4a21",
    "api_key_id":        "0192f3b1-...",
    "route_id":          "0192f3b2-...",
    "deployment_id":     "0192f3c1-...",
    "model_version_id":  "0192f3c0-...",
    "experiment_id":     null,
    "variant":           "baseline",
    "endpoint":          "chat.completions",
    "priority":          "NORMAL",
    "streamed":          true,
    "status_code":       200,
    "error_class":       null,
    "outcome":           "completed",
    "started_at":        "2026-09-22T14:32:07.412Z",
    "finished_at":       "2026-09-22T14:32:08.732Z",
    "queue_wait_ms":     12,
    "ttft_ms":           84,
    "duration_ms":       1320,
    "compute_ms":        1236,
    "attempts":          1,
    "prompt_tokens":     42,
    "completion_tokens": 128,
    "token_source":      "runtime",
    "finish_reason":     "stop",
    "worker_pod":        "nebula-acme-qwen-prod-7d9f-x2k",
    "node_name":         "nebula-dev-worker2",
    "resource_profile":  { "cpu_milli": 4000, "memory_mib": 8192, "gpu_count": 0, "gpu_type": null },
    "sample_rate":       1.0
  }
}
```

Decisions embedded in this shape:

- **`token_source: "runtime"`** is recorded explicitly. If a future path ever produced an estimate, it
  would have to say so here, and the cost API could surface the distinction rather than mixing measured
  and guessed numbers (axiom A2).
- **`outcome`** is separate from `status_code`, with values `completed`, `client_cancelled`,
  `stream_interrupted`, `deadline_exceeded`, `queue_timeout`, `rejected`. A cancelled stream that
  produced 60 tokens is billable work and an availability non-event; one field cannot carry both facts.
- **Partial usage is always emitted.** Client disconnect, mid-stream worker death, and deadline
  expiry all produce a record with the tokens actually generated — the compute happened.
- **`resource_profile` is denormalized** into the event because cost attribution must be reproducible
  even after the deployment is resized or deleted.
- **`sample_rate`** travels with the event so the rollup can correct counts instead of silently
  under-reporting.

Delivery: JetStream WorkQueue, at-least-once, 2-minute dedup window on `event_id`, 7-day retention. The
ingester inserts with `ON CONFLICT (id, created_at) DO NOTHING`, so a duplicate is a no-op rather than
double billing.

Loss path when NATS is unreachable, in order: bounded in-memory buffer (10 000 events) → local file
spool → shed with `nebula_usage_events_dropped_total` incremented, an error log, and an alert. The usage
API then reports the affected period as **incomplete** rather than under-reporting it as fact
([risk R-12](./risk-register.md)).

### 4.2 `nebula.deployment.events`

```json
{
  "event_type": "nebula.deployment.event.v1",
  "payload": {
    "deployment_id": "0192f3c1-...",
    "deployment":    "qwen-prod",
    "generation":    7,
    "revision":      4,
    "type":          "rollout_aborted",
    "previous_state":"progressing",
    "state":         "ready",
    "reason":        "CanaryAnalysisFailed",
    "message":       "error rate delta 2.7pp exceeds threshold 1.0pp over 300s window",
    "detail": {
      "rollout_id": "0192f3d0-...",
      "step": 1, "weight": 10,
      "arms": [
        {"label":"baseline","requests":1842,"error_rate":0.0041,"p95_ms":980},
        {"label":"canary","requests":198,"error_rate":0.0311,"p95_ms":1240}
      ]
    },
    "actor": { "type": "system", "id": "nebula-controller" }
  }
}
```

`type` is a closed set: `created`, `applied`, `progressing`, `ready`, `degraded`, `failed`, `scaled`,
`spec_updated`, `rolled_back`, `rollout_started`, `rollout_step_advanced`, `rollout_paused`,
`rollout_promoted`, `rollout_aborted`, `deleting`, `deleted`. Closed because the dashboard and the
alerting rules branch on it, and an open set means a new value silently renders as nothing.

`reason` uses Kubernetes' CamelCase condition-reason convention (`InsufficientCapacity`,
`ArtifactChecksumMismatch`, `ModelLoadFailed`, `CanaryAnalysisFailed`, `QuotaExceeded`) so it is
machine-matchable, while `message` is the human sentence. Ordered per deployment; no global ordering is
assumed or needed.

### 4.3 `nebula.autoscale.events`

```json
{
  "event_type": "nebula.autoscale.event.v1",
  "payload": {
    "deployment_id": "0192f3c1-...",
    "from_replicas": 2,
    "to_replicas":   4,
    "direction":     "up",
    "decision":      "scaled",
    "reason":        "queue wait p95 1180ms exceeds target 500ms, ratio 2.36",
    "signals": [
      {"name":"queue_wait_p95_ms","observed":1180,"target":500,"ratio":2.36,"source":"heartbeat","stale":false,"selected":true},
      {"name":"concurrency_per_replica","observed":3.8,"target":4,"ratio":0.95,"source":"heartbeat","stale":false,"selected":false},
      {"name":"requests_per_second","observed":24.1,"target":20,"ratio":1.21,"source":"prometheus","stale":false,"selected":false},
      {"name":"cpu_utilization","observed":0.71,"target":0.70,"ratio":1.01,"source":"cadvisor","stale":false,"selected":false}
    ],
    "guards": {
      "stabilization_window_s": 30, "cooldown_remaining_s": 0,
      "max_step_applied": false, "clamped": false,
      "rollout_active": false, "min_replicas": 1, "max_replicas": 6
    }
  }
}
```

**Suppressed decisions are published too**, with `direction: "hold"` and `decision` one of
`suppressed_cooldown`, `suppressed_stabilization`, `suppressed_rollout`, `suppressed_no_signal`,
`clamped_max`, `clamped_min`. This is what turns "why didn't it scale?" from an argument into a query,
and it is the diagnostic that makes oscillation ([risk R-05](./risk-register.md)) tractable.

Every signal is reported, not just the winner, with `selected` marking which one drove the decision.

### 4.4 `nebula.worker.lifecycle`

```json
{
  "event_type": "nebula.worker.lifecycle.v1",
  "payload": {
    "deployment_id": "0192f3c1-...",
    "pod": "nebula-acme-qwen-prod-7d9f-x2k",
    "node": "nebula-dev-worker2",
    "model_version_id": "0192f3c0-...",
    "type": "model_load_failed",
    "detail": {
      "error_class": "ChecksumMismatch",
      "expected_sha256": "9f2c...",
      "computed_sha256": "31ab...",
      "duration_ms": 8412,
      "artifact_uri": "s3://nebula-models/sha256/9f2c..."
    }
  }
}
```

`type`: `starting`, `artifact_pulling`, `artifact_cached`, `model_loading`, `model_loaded`,
`model_load_failed`, `ready`, `unhealthy`, `engine_crashed`, `draining`, `terminated`. Typed
`error_class` values come from the runtime adapter contract (`ArtifactMissing`, `ChecksumMismatch`,
`InsufficientMemory`, `UnsupportedQuantization`, `EngineStartFailed`) so the controller can distinguish
a transient failure from a permanent one, and abort a rollout only for the latter.

This stream is also why a deployment's history survives: Kubernetes Events expire in an hour by
default, so "why was this degraded yesterday" is unanswerable without it.

### 4.5 `nebula.batch.<deployment>` — reserved

The one place NEBULA will put inference work on a broker: an asynchronous batch API where no client is
waiting, so durable queuing is exactly right and TTFT is irrelevant. Post-v1, named here so the
subject namespace is designed rather than retrofitted, and so the synchronous path's exclusion of NATS
stays an explicit boundary rather than an accident.

---

## 5. Consumer contracts

| Consumer | Subject | Durability | Ack | On failure |
|----------|---------|------------|-----|------------|
| gateway router state | heartbeat | ephemeral, per replica | n/a | staleness detection takes over |
| gateway auth cache | cache-invalidate | ephemeral | n/a | 30 s TTL is the guarantee |
| autoscaler | heartbeat | ephemeral | n/a | holds on missing signals |
| controller reconcile trigger | control.reconcile | ephemeral | n/a | periodic resync |
| controller usage ingester | usage.records | **durable, WorkQueue** | explicit, after DB commit | `nak` with backoff; redelivery is safe because the insert is idempotent |
| controller audit sink | deployment.events, autoscale.events | durable, per-consumer | explicit | retry; duplicates deduped on `event_id` |
| dashboard live feed | deployment.events, autoscale.events, worker.lifecycle | ephemeral push | n/a | reconnect and backfill from the API, not the stream |

The usage ingester's ack-after-commit ordering is the detail that matters: acknowledging before the
database commit would turn a crash into permanent data loss, which is precisely what JetStream was
chosen to prevent.

---

## 6. Schema evolution

1. **Within a version**, only additive optional fields. Consumers ignore unknown fields — tested
   explicitly, because a consumer that rejects unknown fields makes every producer change a breaking
   change.
2. **Breaking changes** bump `schema_version` and the `event_type` suffix (`...v1` → `...v2`).
   Producers dual-publish for at least one release; consumers handle both; the old version is removed
   only once stream retention has expired every v1 message.
3. `packages/events/schema/*.json` holds JSON Schema per version; CI asserts every published subject
   has a schema, every schema has a Go struct, and round-trip serialization is lossless.
4. Retention is the migration clock: a 7-day stream means a v1 consumer cannot be retired sooner than
   7 days after the last v1 publish. Written down here because it is the constraint everyone forgets.

## 7. Testing

- **Contract tests** — every event serializes, round-trips, and validates against its schema.
- **Idempotency tests** — every durable consumer processes the same message twice with identical
  resulting state. Not "should"; asserted.
- **Ordering tests** — heartbeats delivered out of order leave the endpoint snapshot correct
  (`sequence` discard path).
- **Loss tests** (Phase 16) — NATS unreachable during inference: requests still succeed
  ([risk R-11](./risk-register.md)); usage events buffer, spool, then shed with the counter
  incrementing and the period reported incomplete.
- **Backlog tests** — 100 000 buffered usage events drain without exhausting memory and without
  duplicate rows.
- **No-content assertion** — a scan over every event schema and a live capture proves no field can
  carry prompt or completion text ([risk R-16](./risk-register.md)).
