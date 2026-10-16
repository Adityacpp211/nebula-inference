# 34. One admission queue per deployment, sized by the router's view of free capacity

Date: 2026-10-01

## Status

Accepted. Implemented in Phase 7 (`packages/queue`, `services/gateway/internal/admission`).

## Context

docs/architecture.md §6.3 specifies the gateway's queue — bounded, three priorities with aging,
absolute deadlines, a sweeper, cancellation — but not what it is a queue *for*: what it counts,
how big the room behind it is, and how a worker's own "I am full" reaches it. Three questions:

1. **Granularity.** One queue per gateway, per route, or per deployment?
2. **Capacity.** The gateway does not know a worker's real concurrency unless the worker says so.
3. **Backpressure.** A worker answering 429 means its own queue is full. Bouncing the request
   between replicas that are all full is wasted work; failing it while the queue still has room
   and the deadline still has time defeats the purpose of having a queue.

## Decision

1. **A queue per deployment** (per route target). A route split 90/10 queues each arm
   separately, so a saturated canary does not hold up the baseline, and a request waits for the
   deployment it was resolved to rather than drifting between targets while it waits.
2. **Capacity is re-read on every decision** from the router: each eligible endpoint contributes
   its slots — from its heartbeat's `parallel_slots`, else the route's `slots`, else
   `NEBULA_GATEWAY_DEFAULT_SLOTS` — times `NEBULA_GATEWAY_SLOT_OVERCOMMIT` (default 2, so the
   worker's own queue still has work to batch). Replicas that appear while requests wait admit them
   at the next sweep (50 ms).
3. **Saturation becomes waiting.** A worker's 429 marks that endpoint saturated until its
   `Retry-After` (clamped to 1–10 s). A saturated endpoint contributes no capacity and every
   strategy avoids it. If every replica of the deployment refuses, the request returns to the
   deployment's queue — at most twice — and is placed again when room returns, within its own
   deadline. Only `nebula.queue: "reject"` opts out.
4. **Refusals are distinct and honest.** Queue full: `429 queue_full` with a `Retry-After` estimated
   from the queue ahead and the observed service time (1–30 s). Caller asked not to wait:
   `429 capacity_exhausted`. Deadline passed while queued: `504 queue_timeout`, which is not an
   inference error because nothing was dispatched. A shed request is charged nothing (its token
   reservation is refunded).
5. **Accounting is local** to the gateway replica, like the router's (ADR-0004, ADR-0033). Several
   replicas can overshoot a deployment together; the workers' 429s and the saturation rule above
   correct it without a distributed counter.

## Consequences

- Under sustained overload the gateway's memory is bounded by queue depth, not offered load: the
  Phase 7 load test at three times capacity held depth at its bound of 16 and the heap within about
  1 MiB of its warm-up value, with every refusal a 429 carrying `Retry-After`.
- `nebula.gateway_queue_ms` reports the wait in the gateway queue, separately from the worker's own
  `queue_wait_ms`.
- Per-deployment queue depth, oldest age, wait quantiles and drops by reason exist in-process
  (`/debug/queues` in development); exporting them as metrics is Phase 8.
