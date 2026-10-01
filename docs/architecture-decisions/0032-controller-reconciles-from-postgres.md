# 32. The controller reconciles from PostgreSQL with server-side apply

Date: 2026-09-30

## Status

Accepted. Implemented in Phase 5 (`services/controller`).

## Context

A NEBULA deployment is a database row with a state machine
([ADR-0027](./0027-deployment-state-machine.md)); the objects that run it are
Kubernetes objects. The controller has to converge the second on the first, survive
edits and deletions made behind its back, and report what it saw without inventing
vocabulary the API cannot explain.

## Decision

- **Source of truth is the row.** The controller lists deployments from PostgreSQL
  (as the `nebula_controller` role, set with a startup parameter, so its grants are
  its own), and watches its objects through informers. A change on either side, or a
  periodic resync, queues the deployment. No CRD: the API already owns the spec.
- **Server-side apply** with a fixed field manager. Drift in a field the controller
  owns (a hand-edited replica count) is reverted on the next pass; fields it does not
  own are left alone.
- **One root owner.** A per-deployment ConfigMap owns the Deployment, Service and PDB,
  so deleting that one object garbage-collects the rest, and orphan GC only has to find
  root ConfigMaps whose row is gone or deleted.
- **Decisions are pure.** `decide` maps (row, observation) → (next state, reason,
  conditions) with no I/O, so every transition is table-tested; the reconciler applies
  it through a conditional `UPDATE ... WHERE state = $from`, which makes a racing API
  write win rather than be overwritten.
- **Reasons come from `packages/lifecycle`.** The controller writes only reasons in the
  vocabulary the API validates and documents (`Claimed`, `ObjectsApplied`,
  `ReplicasPartiallyReady`, `StartTimeout`, `Drained`, `ArtifactChecksumMismatch`, …).
  Condition reasons may be finer-grained; state reasons may not.
- **Leader election** on a Lease; a replica that loses the lease cancels its work
  context, and a work error cancels the election rather than wedging it.

## Consequences

- Reconciliation works while the control plane is down (the controller needs only
  PostgreSQL and the API server).
- Tests use the fake clientset for decisions and object shapes; the real API server,
  kubelet and garbage collector are exercised by the kind demo rather than envtest.
