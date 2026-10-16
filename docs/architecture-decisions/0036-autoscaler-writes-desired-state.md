# 36. The autoscaler writes desired state in PostgreSQL, reads Prometheus, and holds when blind

Date: 2026-10-16

## Status

Accepted. Implemented in Phase 9 (`packages/autoscale`, `services/autoscaler`, migration 000012).

## Context

docs/architecture.md §4.4 and §7.2 fix what the autoscaler decides (the HPA ratio with
stabilization windows, cooldown, a maximum step, `[min, max]`, a rollout interlock) and that every
decision is an `autoscaling_events` row. They leave open how its decisions reach Kubernetes, where
its signals come from, and how one replica is chosen to act. Four questions:

1. **The write path.** The autoscaler must not call the Kubernetes API (§4.4: one writer per
   Kubernetes object). Its database role, `nebula_controller`, may write `desired_replicas` and not
   `generation` (migration 000007) — but the controller only reconciles when
   `generation <> observed_generation`.
2. **Signals.** The gateway's metrics label deployments by name, which is unique only within an
   organization; the worker's metrics carry no deployment at all (Prometheus adds it from the pod
   label). Under load most waiting happens in the gateway's admission queue (ADR-0034), not at the
   worker, so worker metrics alone can never show a ratio far above 1.
3. **Missing data.** A Prometheus outage, a deployment with no scraped worker, or a quantile over no
   requests (NaN) must not look like "idle".
4. **Leadership.** The design says "1 active (leader-elected)". The controller uses a Kubernetes
   Lease; the autoscaler has no Kubernetes access.

## Decision

1. **A trigger makes a replica change a spec change.** Migration 000012 advances `generation` when
   `desired_replicas` changes and the statement did not already advance it. The autoscaler updates
   `desired_replicas` with a compare-and-set on the count it decided from (and on the bounds), in the
   same transaction as the event row, so a change without its explanation — or an explanation of a
   change that did not happen — is never visible. A user's scale the autoscaler has not seen wins.
   The control plane validates `autoscaling` with the autoscaler's own parser, so a typo is a 400.
2. **Signals are joined on `deployment_id`.** The gateway's `nebula_queue_depth` and
   `nebula_queue_wait_seconds` gain a `deployment_id` label (cardinality unchanged: one per
   deployment), Prometheus copies the worker pod's `nebula.dev/deployment-id` label onto worker
   series, and `nebula_queue_wait_seconds` now observes every admitted request, including those that
   waited zero, so its p95 is the user-visible one. Two signals, four instant queries per tick for
   every deployment at once:
   - `concurrency_per_replica` = (running + queued at the workers + waiting at the gateways) ÷ the
     desired replica count, against `target_concurrency` or the workers' declared slots. Dividing by
     the desired count, not the ready count, makes a replica that is still starting count as one
     carrying nothing: a scale-up looks smaller, never larger.
   - `queue_wait_p95_ms` from the gateway histogram, against `target_queue_wait_ms` (500).
3. **Missing is never zero.** A failed Prometheus query fails the whole pass; a deployment absent from
   a result, or a NaN, is a stale signal, carried in the event's signal vector and excluded from the
   decision. With every signal stale the decision is `suppressed_no_signal`: hold, never fall back to
   the minimum. The one documented exception: an absent gateway queue depth is an empty queue, since
   the gateway reports only deployments it has queued for.
4. **Leadership is a PostgreSQL advisory lock**, held on one dedicated connection and verified every
   tick. The autoscaler already cannot decide without PostgreSQL, so the lock adds no dependency, and
   a crashed or partitioned leader loses the lock with its connection. A replica that loses or never
   had the lock forgets its windows: when it leads, it observes them afresh before acting.

## Consequences

- A scale by the autoscaler moves the deployment through `provisioning` → `starting` → `ready` like a
  user's scale does. Routing is unaffected: the gateway routes on endpoints, not deployment state.
- A held decision is one row, not one per tick: suppressions are recorded when they change, steady
  state is not recorded at all, and the counter `nebula_autoscale_decisions_total` counts every
  evaluation for dashboards.
- RPS, CPU and GPU utilization (the remaining §7.2 signals) are not read yet: queue wait and
  concurrency are the primary signals, and the engine takes any number of signals.
- Scale-to-zero (a Phase 9 stretch goal, off by default) is not implemented: `min_replicas` 0 is
  accepted by the schema but the gateway has no activation queue, so a deployment at zero would
  refuse requests rather than hold them.
- The gateway itself scales with an ordinary CPU HPA (`gateway.autoscaling.enabled`, off by default
  because kind ships no metrics-server).
