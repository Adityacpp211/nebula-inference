# ADR-0006 — Staged orchestration: native objects via client-go first, a CRD later

- **Status:** Accepted
- **Date:** 2026-09-22
- **Phase:** 0
- **Related:** [ADR-0005](./0005-cooperate-with-kubernetes.md), [ADR-0008](./README.md#adr-0008)

## Context

NEBULA materializes a deployment record as Kubernetes objects. There are two idiomatic ways to do it.

**Native objects via client-go.** The controller creates and reconciles ordinary `Deployment`,
`Service`, `ConfigMap`, and `PodDisruptionBudget` objects, driven from PostgreSQL rows.

**A Custom Resource Definition.** Define `NebulaDeployment`, write a `controller-runtime` operator,
and let Kubernetes' own machinery store and watch the desired state. This is the more idiomatic
pattern and the more impressive one, and it is how most mature platform products end up.

The choice affects Phase 5 heavily, and the wrong order could cost weeks: writing an operator first
means debugging CRD lifecycle, webhook certificates, status subresources, and conversion webhooks
before a single pod has ever served a token.

## Decision

**Stage it.**

**Stage 1 (Phase 5).** The controller reconciles native objects directly with `client-go` and typed
informers, driven from PostgreSQL. All objects carry `nebula.dev/*` labels and an owner reference to a
per-deployment root ConfigMap. The reconciler is written with a clean separation between *deriving the
desired object set* (pure function: spec → `[]client.Object`) and *applying it* (server-side apply,
diff, status write-back).

**Stage 2 (post-v1, its own ADR).** Introduce a `NebulaDeployment` CRD with `controller-runtime`. The
control plane writes the CR in the same transaction as the PostgreSQL row; the operator reconciles the
CR into the same object set. Because stage 1's derivation function is pure and already separated from
its trigger, the operator reuses it verbatim — the change is where desired state is read from, not what
is produced.

The migration path is written down now, in stage 1, so stage 1 is built in a shape that permits it.
That is the entire point of staging rather than deferring.

## Rationale

**The hard parts of reconciliation are not CRD-specific.** Drift detection, adoption of orphaned
objects, status conditions, backoff, resync, leader election, server-side apply conflicts, and
graceful deletion are the same problems either way. Stage 1 forces those to be solved and tested with
`envtest` while the surface area is small. A CRD added on top of a reconciler that already handles
them correctly is a mechanical change; a CRD added *first* means learning both at once and being
unable to tell which layer a bug is in.

**PostgreSQL remains the source of truth regardless.** NEBULA needs relational queries, multi-tenant
row-level security, usage aggregation, audit history, and cost reporting. etcd is not a reporting
database, so the CRD would never be the *only* store — it would be a projection. That makes the CRD a
convenience and an integration surface, not a foundation, and reorders its priority accordingly.

**Time to a working system matters.** Phase 5 is the first point where a real pod serves a real token,
and everything after it depends on that working. Adding CRD registration, RBAC for custom resources,
a status subresource, defaulting and validating webhooks, and cert-manager-issued webhook certificates
to that phase delays every subsequent phase for benefits that are real but not yet needed.

**The CRD's benefits are real and are mostly about integration, which is a later concern.**
`kubectl get nebuladeployments`, GitOps with Argo or Flux, `kubectl describe` showing conditions,
admission webhooks validating specs at the API server, and other controllers watching NEBULA's
resources — all valuable, all about NEBULA fitting into someone else's ecosystem, which matters once
NEBULA works.

**Deciding the endpoint now prevents the usual trap.** The common failure is building stage 1 in a way
that makes stage 2 a rewrite: reconciliation logic tangled with database reads, object derivation
inseparable from applying, status write-back hardcoded to SQL. Naming stage 2 as the plan makes the
seam a design requirement in stage 1 — `derive(spec) → objects` is pure, and both the trigger and the
status sink are interfaces.

## Alternatives rejected

**CRD-first.** Most idiomatic; slowest path to a working system; forces two unfamiliar problem domains
into one phase. Rejected on sequencing, not on merit — it is the destination.

**Native objects forever, no CRD.** Simplest, and genuinely sufficient for the brief's Definition of
Done. Rejected as a permanent answer because it leaves NEBULA unable to participate in GitOps
workflows and invisible to `kubectl`, which is a real limitation for a Kubernetes-native product.

**CRD as the only source of truth, no PostgreSQL desired state.** Rejected: loses relational integrity,
cross-tenant queries, RLS, usage rollups, and audit history. etcd is the wrong store for a product that
must answer "what did this org spend last month".

**A Helm release per deployment, rendered and applied by the controller.** Uses Helm as an API, which
it is not: no drift detection, poor status reporting, and release-state corruption becomes NEBULA's
problem. Rejected.

## Consequences

**Accepted costs.**

- Users cannot `kubectl get nebuladeployments` in v1. The CLI and dashboard are the interfaces, and the
  README says so plainly rather than implying Kubernetes-native ergonomics that do not exist yet.
- No GitOps flow in v1.
- Some reconciler code will be refactored in stage 2 — bounded deliberately by keeping derivation pure.
- Two mechanisms will briefly coexist during the stage-2 migration, needing a documented cutover
  (dual-write, verify, switch the trigger, remove the old path).

**Benefits.**

- Phase 5 delivers a working, reconciling system without CRD machinery in the way.
- Reconciliation correctness is proven with `envtest` before any CRD complexity exists.
- The eventual CRD is a projection over already-correct logic.
- No webhook certificates in the critical path of the first working deployment, which on a laptop is a
  meaningful simplification.

**Commitment.** Stage 1 must keep `derive(spec) → []client.Object` pure and free of database access,
and status write-back behind an interface. A Phase 5 review checks exactly this, because it is the
only thing that makes stage 2 cheap — and an ADR whose migration path is never protected is a promise,
not a plan.
