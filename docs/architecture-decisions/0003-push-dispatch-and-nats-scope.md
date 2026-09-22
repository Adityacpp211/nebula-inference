# ADR-0003 — Synchronous push dispatch; NATS for control and telemetry, not the token path

- **Status:** Accepted
- **Date:** 2026-09-22
- **Phase:** 0
- **Supersedes:** —
- **Related:** [ADR-0004](./0004-router-as-library.md), [ADR-0007](./README.md#adr-0007), [ADR-0013](./README.md#adr-0013)

## Context

NEBULA has a message broker (NATS + JetStream) and an inference request path. The obvious-looking
design is to use the broker for both: the gateway publishes an inference job, workers consume from a
queue group, responses come back over a reply subject. It gives queuing, fair distribution, priority,
and decoupling essentially for free, and it looks like a distributed system.

The competing design is direct push: the gateway chooses a worker and makes an HTTP request to it,
with queuing handled explicitly in the gateway and the worker.

This decision shapes the router, the queue, the reliability layer, the tracing model, and what
happens when NATS is down — so it has to be made first and stated clearly.

## Decision

**Synchronous and streaming inference is dispatched by direct HTTP push** from gateway to worker.
NATS carries control-plane signalling and telemetry only:

| Uses NATS | Does not use NATS |
|-----------|-------------------|
| worker heartbeats and load state | inference requests |
| usage records | generated tokens |
| deployment, rollout, autoscaling events | cancellation of an in-flight request |
| reconcile nudges | routing decisions |
| worker lifecycle events | anything between a client and its response |

Queuing is explicit and owned: a bounded, priority-aware, deadline-aware admission queue in the
gateway, and a bounded local queue in the worker in front of the runtime.

One narrow exception is reserved: a future asynchronous **batch** inference API (`nebula.batch.*`),
where there is no connected client waiting and durable queuing is exactly right. It is not part of
v1, and it does not affect the synchronous path.

## Rationale

**Streaming is the product, and a broker is a bad fit for it.** Time to first token is the metric
users feel. A broker hop adds latency to every token or forces the response to be reassembled, and
streaming tokens through a request-reply subject means either one message per token (enormous
overhead on a broker built for small control messages) or buffering, which destroys TTFT.
Backpressure through a broker is also indirect: a full queue group does not tell a gateway anything
useful in time to make a better decision.

**Cancellation and deadlines are first-class here, and awkward through a broker.** With HTTP, a
client disconnect cancels a context, which closes the connection, which the worker observes
immediately and aborts generation — freeing a slot that costs real GPU memory. Through a broker you
need an out-of-band cancel message, a correlation table, and a worker that polls for it. The simple
mechanism is also the correct one.

**Routing needs to be a decision, not a side effect.** A queue group distributes work by whoever grabs
it first. That is a fine load-balancing policy and a terrible routing policy — it cannot express
least-loaded, latency-aware, capability-based, or cost-aware selection, which is the component the
brief calls one of NEBULA's most important. Push dispatch makes the choice explicit, observable, and
testable, and it makes the routing strategy a replaceable object rather than a broker configuration.

**Availability coupling.** With push, NATS being unavailable degrades routing quality (stale load
data) and buffers telemetry. With broker dispatch, NATS being unavailable means *no inference at all*.
That makes a component we chose for convenience into a hard dependency of the product's core function,
which contradicts axiom A8.

**This is what production inference platforms do.** vLLM's router, Ray Serve, KServe, and
Triton-based stacks all push to replicas with a load-aware proxy; brokers appear for batch and
asynchronous workloads. Following that is not conservatism — it is the shape the problem has.

**Explicit queues are more valuable than free queues.** The brief asks for bounded queues with depth,
age, priority, timeout, and cancellation. A broker gives a queue whose internals are the broker's;
writing the queue ourselves is roughly 300 lines of well-tested Go and produces exactly the metrics,
priorities, aging behaviour, and deadline semantics the requirements name — and it is deterministically
testable under a simulated clock, which a broker is not.

## Alternatives rejected

**Broker dispatch for everything (NATS request-reply with queue groups).** Rejected for the latency,
cancellation, routing, and availability-coupling reasons above. It would also make tracing harder to
read — a span tree across a broker requires manual context propagation through message headers and
loses the natural parent-child relationship of an HTTP call.

**Hybrid: broker for non-streaming, push for streaming.** Rejected as the worst outcome: two dispatch
paths, two queue implementations, two failure models, two sets of metrics — and the non-streaming path
would be the less-tested one. A single path that handles both is simpler and better exercised.

**gRPC streaming instead of HTTP/SSE between gateway and worker.** A genuinely reasonable alternative
with better framing and native flow control. Rejected for v1 because the gateway must terminate SSE
toward the client anyway, so gRPC would add a translation layer; because HTTP is debuggable with
`curl` against a worker, which matters a great deal during development; and because the Python worker
side is simpler and faster to get right. The worker contract is versioned
(`X-Nebula-Worker-Protocol`), so moving to gRPC later is a contract change rather than a redesign.

**Kubernetes Service load balancing instead of endpoint selection.** Rejected because kube-proxy
balances per connection with no knowledge of queue depth, model load state, or breaker state — which
is precisely the knowledge that makes inference routing worth doing.

## Consequences

**Accepted costs.**

- Queuing, backpressure, retry, and breaker logic are ours to write and test rather than inherited
  from a broker. Mitigated by keeping them in I/O-free libraries (`packages/queue`,
  `packages/reliability`) that are unit-testable under a simulated clock.
- The gateway holds a connection per in-flight request, so its memory and file-descriptor limits scale
  with concurrency. Bounded by the admission queue and the concurrency limiter, and load-tested in
  Phase 7.
- The router must know worker endpoints and their load, which is the whole of the state problem in
  [ADR-0004](./0004-router-as-library.md) and risk R-04.
- A worker cannot pull work at its own pace; it must reject what it cannot serve. Hence the mandatory
  429 + `Retry-After` admission contract.

**Benefits.**

- Token latency is one network hop from the worker to the gateway to the client.
- Cancellation and deadlines work by the natural mechanism of the transport.
- NATS can fail without inference failing, which is tested explicitly in Phase 16 (risk R-11).
- The trace is a clean parent-child tree, readable in Tempo without manual stitching.
- Routing strategy is a replaceable, testable object rather than broker configuration.

**Enforcement.** The import-boundary linter forbids `packages/events` in the gateway's dispatch
package, and a Phase 16 failure test runs the full inference path with NATS unreachable and asserts
success. Without those, this decision would erode the first time a broker-shaped shortcut looked
convenient.
