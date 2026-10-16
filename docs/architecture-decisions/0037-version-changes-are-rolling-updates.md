# 37. A version change is a rolling update: two versions serve, and a failed load is named

Date: 2026-10-16

## Status

Accepted. Implemented in Phase 11 (gateway router, controller, worker).

## Context

Changing a deployment's model version is a spec change, and the controller applies it as a
Kubernetes rolling update with `maxUnavailable: 0`: new pods start beside the old ones, and an old
pod is removed only once a new one is ready. Building the Phase 11 demo showed three places where
the rest of NEBULA did not agree with that:

1. **The gateway excluded the old version.** The router dropped any endpoint whose heartbeat
   reported a version other than the route target's. The moment the routing table named v2, every
   v1 pod became ineligible: requests failed until the first v2 pod was ready, and a v2 that could
   not load turned the change into a total outage — exactly when Kubernetes was keeping the old
   pods so they would keep serving.
2. **The controller called a stuck rollout ready.** Kubernetes counts the old pods in
   `readyReplicas`. A rollout whose new pods never loaded reported three ready replicas, and the
   deployment went `ready`.
3. **The cause of a failed load was invisible.** A worker that cannot load exits; the pod
   crash-loops, and the reason is in a log line. The controller saw only `CrashLoopBackOff` and,
   with old pods ready, no reason to fail at all.

## Decision

1. **A discovered endpoint is never excluded for its version.** It belongs to the deployment by
   construction (its EndpointSlice is selected by the deployment id), so during a rollout it may
   serve either version. The gateway asserts the version the endpoint's own heartbeat reports
   (`X-Nebula-Model-Version`), records that version in the usage record and the response, and
   asserts nothing when it has no heartbeat yet rather than a guess that would earn a 409. An
   endpoint written into a static route is still pinned to its target's version, because nothing
   else vouches for what it serves.
2. **Ready means rolled out.** A starting deployment is `ready` only when every pod runs the current
   template (`updatedReplicas >= desired` and no pod of an older revision left), not when enough
   pods of any revision are ready.
3. **A worker names its load failure.** On a load error the worker writes
   `{"status":"load_failed","error_code":…,"error":…}` to its termination-message file
   (`NEBULA_WORKER_TERMINATION_LOG`, set by the controller) before exiting. The controller reads it
   from a not-ready worker container of a **live pod of the current template** and fails the
   rollout at once with `ModelLoadFailed` (or `ArtifactChecksumMismatch`) and that sentence, adding
   "the previous version keeps serving" when old pods remain. Only the current template counts: the
   abandoned revision's pods linger while they terminate after a rollback, still carrying the report
   that failed them, and must not fail the revision that replaced them.

## Consequences

- A version change, a failed version and a rollback cost no requests: in the Phase 11 demo, v1 → v2,
  v2 → an unloadable v3 (failed three seconds after the change, v2 serving all traffic) and two
  rollbacks run under continuous load with zero client errors.
- Rollback needs no special path: it is a new revision whose template is an earlier one, and the
  old ReplicaSet scales back up.
- During a rollout the `model_version` a client sees can be either version, and the usage record
  says which one served it.
