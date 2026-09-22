# ADR-0004 — The router is an in-process library, not a network service

- **Status:** Accepted
- **Date:** 2026-09-22
- **Phase:** 0
- **Deviates from:** the project brief's suggested `services/router/`
- **Related:** [ADR-0003](./0003-push-dispatch-and-nats-scope.md), [ADR-0005](./0005-cooperate-with-kubernetes.md), [ADR-0021](./README.md#adr-0021)

## Context

The brief lists the model router as a component with its own directory under `services/`, alongside
the gateway, control plane, scheduler, and autoscaler. It also states, in the same document, that the
architecture must not "create unnecessary microservices simply for appearance", and invites a better
structure if one emerges, provided the reasoning is recorded.

The router's job: given an inference request, decide which deployment should serve it and which
worker endpoint within that deployment. To do that it needs the route table (changes rarely), endpoint
existence and readiness (changes on pod churn), per-endpoint load and latency (changes every second),
and local failure observations (changes per request).

The question is whether that work belongs in its own process.

## Decision

Routing is **`packages/routing`**, a library with no I/O, consumed in-process by the gateway. There is
no `nebula-router` deployment.

The library has three parts:

1. **`EndpointSnapshot`** — an immutable view of routes, targets, endpoints, and their observed state,
   swapped atomically by the gateway's state maintainer.
2. **Strategies** — one interface, several implementations (`Filter`, `RoundRobin`, `LeastLoaded`,
   `LatencyAware`, `CostAware`, `CapabilityBased`, `Failover`), composed from a `routing_policies` row.
3. **Bucketing** — deterministic weighted selection across route targets, used for canary and A/B
   splits.

The state that feeds it is maintained by the gateway from three sources with defined precedence
(EndpointSlice informer, NATS heartbeats, local observations), as described in
[architecture.md §6.2](../architecture.md#62-router-state-how-the-gateway-knows).

Routing *control* — creating routes, setting weights, choosing a policy — stays in the control plane,
where it is a database write with an audit entry.

## Rationale

**A network hop per request buys nothing here.** Every inference request would pay an extra RPC to ask
a question whose inputs are already available to the gateway. On a request whose total latency is
dominated by generation, a millisecond is not the main cost — the main cost is the new failure domain:
a router service that is down means no inference, and it would need its own replication, its own
health checks, its own breaker, and its own capacity planning, all to answer a question the caller can
answer itself.

**The usual reason to centralize routing does not apply.** A separate router is justified when it owns
state that replicas cannot each hold — a global queue with strict fairness ordering, or a single
scheduler that must serialize decisions. NEBULA's design has neither: admission queues are per-gateway
and bounded, fairness is per-tenant via rate limits rather than a global ordering, and worker-side
admission control is the actual arbiter of capacity. Each gateway replica converges on the same
endpoint view within a heartbeat interval, and disagreement between replicas is self-correcting
because it shows up as load that the next `LeastLoaded` decision absorbs.

**Routing needs the caller's own observations.** The most valuable signal — this endpoint just refused
my connection — is local. A separate router would either not have it, or would need every gateway to
report its failures back, which is a synchronization problem invented to justify the split.

**"Component" and "process" are different things.** The brief asks for routing to be a clear component
with replaceable strategies and no hardcoded decisions scattered through the codebase. A library with
one interface, one composition point, and a dependency rule forbidding routing logic elsewhere
satisfies all of that. It is *more* testable than a service: strategies are pure functions over a
snapshot, so 10 000-request distribution tests run in milliseconds with an injected clock and no
network.

**Separating for the sake of appearance is the failure mode the brief names.** A router service would
make the architecture diagram look busier and the system worse.

## Alternatives rejected

**A `nebula-router` HTTP service called per request.** Adds latency, a failure domain, and a
deployment to operate; requires its own copy of the same state; loses local failure observations.
Rejected.

**A router sidecar in every gateway pod.** Removes the network partition risk but keeps the process
boundary, the serialization cost, and a second container to build, version, and monitor — in exchange
for nothing, since it is the same machine and the same lifetime. Rejected.

**Routing decided by the control plane at deployment time (static endpoint lists).** Cannot react to
load, health, or queue depth, which is most of what routing is for. Rejected.

**Kubernetes Service / kube-proxy as the load balancer.** Zero code, and no knowledge of queue depth,
model readiness, breaker state, or capability requirements. Round-robin over TCP connections is not a
routing policy for requests whose cost varies by orders of magnitude. Rejected — though the Service
still exists for DNS and as a fallback path.

**A service mesh (Envoy/Istio) doing weighted routing.** Could express traffic splits, but not
least-loaded-by-queue-depth or capability matching, and it would hide the reliability engineering this
project exists to demonstrate behind a sidecar's configuration. It would also make the mesh a hard
dependency of a self-hostable product. Rejected.

## Consequences

**Accepted costs.**

- Every gateway replica maintains its own routing state: an informer cache and a NATS subscription per
  replica, so memory and connection count scale with gateway replicas. Small at this scale, and
  measured.
- Replicas can transiently disagree, so weighted splits are only exactly correct in aggregate. The
  Phase 6 test asserts distribution within tolerance over a large sample rather than exact per-replica
  weights, and sticky bucketing uses a hash so it is stable across replicas regardless.
- A new replica starts with a cold view. Mitigated by the Redis snapshot, then convergence within a
  heartbeat interval.
- Changing routing behaviour requires a gateway rollout rather than restarting one small service.
  Acceptable: policies themselves are database rows and change without a deploy; only new *strategy
  code* needs a rollout.

**Benefits.**

- No added latency and no added failure domain on the request path.
- Local failure observations are used immediately.
- Strategies are pure and unit-testable under a simulated clock; no test needs a cluster.
- One fewer service to build, secure, monitor, document, and explain.

**Enforcement.** The import-boundary linter forbids routing decisions outside `packages/routing`: no
other package may select an endpoint. If a future requirement genuinely needs centralized routing
state — global fairness ordering across tenants is the plausible one — this ADR is superseded, and the
library becomes the core of the new service rather than being rewritten.
