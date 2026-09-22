# ADR-0005 — Cooperate with Kubernetes: constraint generation and capacity admission, not a scheduler

- **Status:** Accepted
- **Date:** 2026-09-22
- **Phase:** 0
- **Deviates from:** the project brief's suggested `services/scheduler/`
- **Related:** [ADR-0006](./0006-staged-orchestration.md), [ADR-0008](./README.md#adr-0008)

## Context

The brief asks NEBULA to "maintain knowledge of available compute resources", track CPU, memory, GPU
count, GPU memory, model requirements, node capacity and utilization, and "decide where workloads
should run" — while also saying, explicitly, not to recreate the Kubernetes scheduler, and to
cooperate with Kubernetes rather than replace it.

Those two instructions are compatible, but only if the boundary is drawn precisely. Drawn wrongly in
one direction, NEBULA builds a second scheduler that fights kube-scheduler over the same pods. Drawn
wrongly in the other, NEBULA "schedules" by creating a Deployment and hoping, which is not scheduling
at all and produces the experience the brief is trying to avoid: a deployment stuck `Pending` with no
explanation.

## Decision

NEBULA does not place pods. It does three things, all of which Kubernetes cannot do because they
require knowledge of models:

1. **Constraint generation** — translate a model version's `hardware_profile` and a deployment's
   `resources` into Kubernetes primitives: resource requests and limits, `nodeSelector`, tolerations,
   `runtimeClassName`, topology spread constraints, affinity. A pure function, in
   `packages/scheduler`, with table-driven tests.
2. **Capacity admission** — before creating any object, answer "can this cluster actually run N
   replicas of this shape?" against a node inventory built from informers, and reject with a real
   explanation if not.
3. **Capacity inventory** — maintain and expose the cluster's node capacity, allocatable resources,
   GPU models and memory, and current utilization, for admission, for the dashboard, for `CostAware`
   routing, and for historical questions after the nodes are gone.

kube-scheduler does the actual placement, using the constraints NEBULA generated. There is no
`nebula-scheduler` service: constraint generation is a library function, and the inventory is a
reconciler inside `nebula-controller`.

Explicitly not built: a scheduler plugin, a scheduler extender, a second `schedulerName`, bin-packing,
preemption, gang scheduling, or pod binding.

## Rationale

**kube-scheduler is very good, and its problems are not our problems.** Node affinity, taints and
tolerations, topology spread, resource fit, extended resources (`nvidia.com/gpu`), preemption and
priority, and the whole informer-backed cache that makes placement fast are already solved, hardened,
and known to operators. Reimplementing any of it would produce something worse that also has to be
explained.

**The part Kubernetes cannot do is model-aware.** Kubernetes has no idea that a `qwen2.5-7b-q4_k_m`
GGUF needs roughly 6 GiB of RAM and a context window of 32 768, or that this model version cannot run
on a runtime that lacks a quantization kernel. Only NEBULA knows that, and the translation from model
metadata to scheduling constraints is exactly the value NEBULA adds. That translation is a pure
function — it does not need a process.

**Admission before creation is the part most systems skip, and it is where the user experience is.**
Creating a Deployment that can never schedule is trivially easy and produces `Pending` pods, an
`Insufficient memory` event buried in `kubectl describe`, and a NEBULA deployment that sits in
`pending` forever. Checking first lets the API return `422` with "this cluster's largest allocatable
node has 8 GiB; this deployment requests 12 GiB" — an answer, at the moment of the request. That check
is cheap because the inventory is already maintained for other reasons.

**Two schedulers over one pod is a correctness bug.** Any design where NEBULA picks a node and then
tells Kubernetes about it — a custom `schedulerName`, direct pod binding, or a nodeName override —
means NEBULA now owns preemption, drains, node failure, and every race kube-scheduler handles. That is
the "recreate the Kubernetes scheduler" outcome the brief rules out, and it would be wrong even if the
brief were silent.

**A scheduler extender was considered seriously and rejected on cost, not principle.** An extender
lets NEBULA filter and score nodes during real scheduling, which is genuinely the right mechanism for
something like VRAM-aware GPU packing. It requires a webhook in the scheduling hot path, a
`KubeSchedulerConfiguration` change on the cluster (impossible on many managed clusters, awkward on
kind), and a new latency-sensitive availability dependency for *all* scheduling, not just NEBULA's.
The value it would add over node labels plus extended resources is small until fractional GPU sharing
exists, which is out of scope. Recorded here as the natural successor if that changes.

## Alternatives rejected

**A full NEBULA scheduler that binds pods.** Duplicates a mature component, owns every failure mode
kube-scheduler already handles, and breaks cooperation with cluster autoscalers and node drains.
Rejected.

**A scheduler extender or scheduling framework plugin.** Right mechanism, wrong phase: cluster
configuration requirements and hot-path availability cost outweigh the benefit at CPU-only and
single-GPU-per-pod granularity. Documented as the successor decision if fractional GPU or bin-packing
requirements appear.

**No placement logic at all — just set resource requests.** The simplest option, and it produces
unexplained `Pending` deployments, no GPU-model targeting, no capacity answers for the dashboard, and
nothing to say when a deployment cannot fit. Rejected: it also removes most of what the brief asks the
scheduler to track.

**A separate `nebula-scheduler` service.** The inventory is one informer-backed cache that the
controller already maintains, and constraint generation is a pure function. A service would add a
process, an API, and a failure domain around a function call. Rejected, for the same reason as
[ADR-0004](./0004-router-as-library.md).

## Consequences

**Accepted costs.**

- NEBULA's capacity view is eventually consistent with the scheduler's, so admission can be
  optimistic: a deployment admitted a moment before another workload consumes the headroom will still
  end up `Pending`. Handled, not hidden — the controller detects unschedulable pods and surfaces the
  scheduler's own message as a deployment condition, so the user sees the real reason within seconds.
- NEBULA cannot express preferences kube-scheduler has no primitive for (for example "pack these two
  replicas onto the same node to share an artifact cache" beyond what affinity can say).
- GPU accounting is at whole-device granularity. Fractional sharing (MIG, time-slicing) would need
  either the device plugin's own support or the extender path.

**Benefits.**

- Works on any conformant cluster, including managed ones, with no cluster-level configuration and no
  privileged scheduler component.
- Cluster autoscalers, drains, priority, and preemption all keep working, because NEBULA's pods are
  ordinary pods.
- The interesting logic — model metadata to constraints — is a pure function with exhaustive unit
  tests and no cluster required to test it, which matters given risk R-01.
- Capacity rejection is explainable at request time, which is a visible product quality, not just an
  architectural nicety.
