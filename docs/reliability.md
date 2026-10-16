# NEBULA reliability

How NEBULA behaves when something fails: which failures are retried, how much retrying the system
can afford, and what each failure mode looks like from the outside. The dependency outages of
[architecture.md §8.4](./architecture.md#84-dependency-failure-expectations) are the contract this
document refines.

Status: Phase 10 in progress. Sections marked *(Phase 10, pending)* describe behaviour that is
designed and not yet verified by a test.

## 1. The rule that matters most

**A request that has emitted output is never retried.** Once headers are committed or a token has
been streamed, a second attempt would duplicate tokens the client already has, and a generation is
not idempotent. Every other rule here is subordinate to this one, and `TestStartedIsNeverRetried`
asserts it for every failure that could otherwise be retryable.

## 2. Failure classification

`packages/reliability.Classify` maps what is known about a failed attempt onto one row. The first
matching row wins; the order is the table's order.

| Class | When | Retried on another replica | Counts against the breaker | Why |
|-------|------|----------------------------|----------------------------|-----|
| `client_gone` | the client cancelled or disconnected | no | no | finishing or retrying serves no one |
| `started` | output reached the client | **never** | yes | a retry would duplicate tokens |
| `connect` | refused, reset before a response, DNS, dial timeout | yes | yes | nothing reached the replica, so nothing ran |
| `saturated` | 429 before work | yes, or back to the admission queue | no | the replica is healthy and full (ADR-0034) |
| `unavailable` | 503 before work: draining, not ready, model not loaded | yes | yes | the replica refused before doing anything |
| `timeout` | the attempt's own deadline passed | no | yes | work may have started |
| `client_error` | 4xx | no | no | the request is wrong and will be wrong everywhere |
| `server_error` | 5xx after the replica accepted the request | no | yes | it may have run |
| `ok` | not a failure | — | — | — |

The gateway's dispatch path maps its own error type onto this table (`failureOf`), and the class
name is the `reason` label of `nebula_retries_total`.

## 3. Retry budget

Retries are bounded twice:

- **An attempt cap** of 3 replicas per request (the gateway's `maxAttempts`), so one request cannot
  walk the whole fleet.
- **A retry budget** shared by every request through a gateway replica: failovers may total at most
  `NEBULA_GATEWAY_RETRY_BUDGET_RATIO` (default 0.2) of the requests seen in the last 10 seconds,
  plus `NEBULA_GATEWAY_RETRY_BUDGET_MIN_PER_SECOND` (default 3) so a quiet gateway can still retry.

The cap alone is not enough. Under a total outage every request fails, and a cap of 3 would triple
the load on a fleet that is already failing. The budget is what turns that into a bounded
`1 + ratio`: in `TestRetryBudgetCapsAmplificationUnderOutage`, 60 requests against three replicas
that all answer 503 cause exactly 6 failovers, an amplification of 1.10. A failover the budget
refuses leaves the original failure standing and increments
`nebula_retries_rejected_budget_total`.

The budget is spent only when there is somewhere to go: a request with no other eligible replica
does not consume it.

## 4. Backoff

A failover to a *different* replica happens at once: the failure was that replica's, and waiting
helps no one. A wait before retrying the *same* target uses exponential backoff with full jitter,
uniform in `[0, min(cap, base·2^(n-1))]` (`reliability.Backoff`). Full jitter rather than equal
jitter because it spreads synchronized clients furthest for the same mean wait. Its first user is
the admission queue's requeue path *(Phase 10, pending)*.

## 5. Circuit breakers

Per endpoint, per gateway replica (ADR-0014), in `packages/routing`: consecutive failures of the
classes marked "counts against the breaker" open it; an open breaker rejects at once and the router
avoids the endpoint; after its cool-off one probe request half-opens it, and a success closes it.
Saturation never opens a breaker — a full replica is healthy. Exported as `nebula_breaker_state`.

## 6. Shutdown and drain

| Component | On SIGTERM | Bound |
|-----------|------------|-------|
| gateway | `preStop` sleep 5 s so endpoints are removed before it stops accepting; `/readyz` fails; in-flight streams finish | the pod's grace period |
| worker | `preStop` drain: stops accepting (503 `draining`, retried elsewhere as `unavailable`), finishes in-flight generations, then exits | `terminationGracePeriodSeconds: 60` |
| control plane | `preStop` sleep 5 s, then drains | — |
| autoscaler | stops deciding at once; a tick cut short records nothing; releases its advisory lock | — |

The drain-under-rolling-restart test (zero client errors while every worker pod is replaced under
live load) is *(Phase 10, pending)*.

## 7. Failure modes

| Failure | What a client sees | What an operator sees |
|---------|--------------------|-----------------------|
| One replica unreachable | nothing: the request fails over before work | `nebula_retries_total{reason="connect"}`, the endpoint's breaker opening |
| One replica draining | nothing: 503 `draining` is retried elsewhere | `nebula_retries_total{reason="unavailable"}` |
| A replica dies mid-stream | the stream ends with an error event; never a duplicated token | `nebula_streams_interrupted_total` |
| Every replica of a route down | 503 `no_healthy_endpoint` with `Retry-After` | retries bounded by the budget; `nebula_retries_rejected_budget_total` rising |
| Every replica full | waits in the admission queue within its deadline, then 429 or 504 `queue_timeout` | `nebula_queue_depth`, `nebula_queue_drops_total`, and the autoscaler scaling up |
| Prometheus down | nothing | every autoscaler decision `suppressed_no_signal`: replicas hold |
| PostgreSQL, Redis, NATS, Kubernetes API down | as [architecture.md §8.4](./architecture.md#84-dependency-failure-expectations) | as §8.4 |
