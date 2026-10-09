# 33. Router state: gateways snapshot it themselves, and re-place only before work starts

Date: 2026-10-01

## Status

Accepted. Implemented in Phase 6 (`packages/routing`, `services/gateway/internal/router`,
`workers/inference/nebula_worker/heartbeat.py`).

## Context

docs/architecture.md §6.2 sets out three sources of routing state — EndpointSlices,
worker heartbeats over core NATS, and local observation — plus a Redis snapshot "written by
the controller" so a gateway that starts cold does not route blind. Building it raised four
questions the design left open.

1. **Who writes the snapshot.** The controller has no Redis connection and no reason to hold
   one; it would also have to reproduce the gateway's view (routes from the control plane,
   endpoints from EndpointSlices) to write it.
2. **How silence is interpreted.** "Older than three intervals is stale" is right while
   heartbeats flow, and wrong when NATS is down or a worker was never configured for it:
   every endpoint would drop out at once.
3. **Ordering across restarts.** `sequence` is monotonic per worker process; a container
   restart in the same pod starts again at 1, and a strict "discard lower sequences" rule would
   ignore that pod for good.
4. **What happens to a request whose endpoint fails.** General retries are Phase 10. But the
   Phase 6 exit criterion — kill every pod of a target with no client errors — cannot hold if a
   request placed on a pod in the instant before its EndpointSlice update is simply failed.

## Decision

1. **Every gateway replica writes the snapshot** (routing table as received plus discovered
   endpoints) to Redis when it changes, and reads it at startup only if the control plane does
   not answer. A replica serving from the snapshot never writes it back, so old state is never
   re-dated as new. `/healthz` reports `routing_table` as degraded while serving from it.
2. **Staleness counts only while the channel is live**: connected to NATS for at least the
   staleness window. An endpoint that has never sent a heartbeat is judged by readiness alone.
   With NATS down, routing degrades to readiness plus local observation, as docs/events.md §3.1
   says it should — rather than to nothing.
3. **Heartbeats carry an `instance`** (a random id per worker process). Ordering is per
   `(pod, instance)`; a new instance replaces the old one.
4. **The router re-places a request only when the failure happened before any work started**:
   the connection could not be made, or the worker declined with 503 (draining, loading) or 429
   (saturated). Nothing was generated, so nothing can be generated twice. At most three
   attempts, never across a pin, each on an endpoint not yet tried. A worker that answered and
   then failed is not retried here — that is Phase 10's decision, with idempotency keys.

Draining is also published at once rather than at the next tick, so gateways stop sending to a
terminating pod before Kubernetes updates the EndpointSlice.

## Consequences

- A cold gateway routes during a control-plane outage, with endpoints that may be up to one
  snapshot old; its breakers catch the ones that are gone.
- Weighted splits and load figures are per replica, exact only in aggregate (ADR-0004).
- `nebula.attempts` in the response and usage record says how many endpoints a request was
  offered to.
- The Phase 6 kind demo kills every pod of one target under load and asserts zero client
  errors; decision 4 is why that holds.
