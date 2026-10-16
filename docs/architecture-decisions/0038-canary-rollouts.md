# 38. Canary rollouts: the controller moves weights, Prometheus judges, a breach wins

Date: 2026-10-16

## Status

Accepted. Implemented in Phase 12 (`packages/rollout`, `services/controller/internal/rolloutctrl`,
the control plane's `/v1/rollouts`, the gateway's bucketing).

## Context

docs/architecture.md §7.3 fixes a canary's shape: weights step 10 → 25 → 50 → 100, each step held
and then analysed against the baseline (error rate within one point, p95 within ×1.2, enough
requests), and any breach puts every request back on the baseline. It leaves open who moves the
weights, where the observations come from, what happens when an operator and the automation act at
once, and how a step with too little data or no data at all is judged.

## Decision

1. **The controller's rollout reconciler moves the weights; the control plane applies an operator's
   verbs.** The control plane creates the rollout (the canary joins the route at weight 0, the
   current target is marked baseline) and implements pause, resume, promote and abort. The
   reconciler, on the elected controller only, starts pending rollouts and advances progressing
   ones. Both write the rollout's state, the route's weights and an audit row in one transaction,
   as a **compare-and-set on the state and step the rollout was read in**: an operator's pause or
   abort always wins over a tick in flight, and a refused write changes nothing. One active rollout
   per route is the database's partial unique index, not a check that could race.
2. **Each arm is measured by deployment id.** The gateway's `nebula_requests_total` and
   `nebula_request_duration_seconds` carry `deployment_id` (a name is unique only within an
   organization). Over the time since the step began, the reconciler reads each arm's requests, its
   client-visible errors — 5xx, plus a stream that broke after its 200 — and its p95.
3. **A breach is judged before sufficiency.** A canary failing every request it got is aborted at
   its first analysis, not after it has served enough requests to be "significant". A step that
   passes the checks without `min_requests` is insufficient: `extend` waits further hold windows
   (then pauses), `pause` stops for an operator, `fail` restores the baseline.
4. **Blind is never healthy.** Metrics that cannot be read make a step insufficient, never a pass.
5. **Promotion makes the canary the baseline.** It serves every request and becomes the route's
   baseline target, so the next rollout on the route starts from it.
6. **Sticky bucketing for weighted splits.** The gateway buckets a request on the OpenAI `user`
   field (scoped to the organization) when present, so one end user stays on one arm on every
   gateway replica and across restarts; without it, each request is bucketed alone.

## Consequences

- In the Phase 12 demo a healthy canary walks 10 → 25 → 50 → 100 with zero client errors; a canary
  failing every generation is rolled back 40 s after it starts, having cost ten requests at 10%; a
  canary six times slower is rolled back on its p95. Each reason is on the rollout row.
- The experiments read model of docs/api.md (per-arm comparison with bootstrap intervals) needs
  per-request records, which arrived with the durable usage stream in Phase 13, where it was built
  (`packages/abtest`, `/v1/experiments`).
- Analysis compares arms only while the baseline still serves traffic: reaching 100 is promotion,
  with nothing left to compare against.
