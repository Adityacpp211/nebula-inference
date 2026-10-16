# 39. Usage flows through JetStream into priced request rows; rollups are recomputed, not incremented

Date: 2026-10-16

## Status

Accepted. Implemented in Phase 13 (`packages/costing`, the gateway's usage sink, the controller's
usage ingester, the control plane's `/v1/usage`, `/v1/costs` and `/v1/pricing-profiles`).

## Context

docs/events.md §4.1 fixes the usage record and its stream (a JetStream WorkQueue, 7 days, 2-minute
de-duplication) and ADR-0018 fixes money (immutable, versioned pricing profiles; integer micros).
Phase 13 had to decide what a request costs, where that number is computed, how a rollup survives
at-least-once delivery, and how a price change interacts with history.

## Decision

1. **Attribution by compute time.** A request is charged its share of one replica — the replica's
   CPU, memory and GPU requests divided by its parallel slots — for the time it generated, at the
   pricing profile version in force when it ran, plus the markup. Idle capacity is attributed to no
   one: a tenant's cost must not depend on other tenants' traffic. Arithmetic is exact rationals in
   integer micros, rounded once per request, half up. `costing.Assumptions` states all of this and
   `GET /v1/costs` returns it with every figure.
2. **The cost is computed once, at ingestion, and stored on the request row.** The controller's
   ingester prices each request when it stores it. A rollup sums stored costs, so recomputing one
   — which every redelivered batch does — can never reprice history.
3. **Rollups are recomputed from the rows, never incremented.** Inserting a request is idempotent
   (`ON CONFLICT (id, created_at) DO NOTHING`), and each touched hour is rebuilt from its rows with
   an upsert, so a batch delivered twice changes nothing. The batch is acknowledged only after its
   transaction commits.
4. **A price change is a new version, effective at an instant.** Requests after `effective_from`
   are priced under it; earlier ones keep their price. A bucket records the newest version any of
   its requests used. A request older than its organization's first version — a late record — is
   priced by the global default in force then, so no batch can block on a missing price. The
   seeded default prices everything at zero: no cost is invented before an operator sets real ones.
5. **The ingester runs on the controller leader only**, so two replicas never rebuild a rollup at
   once. The gateway publishes from a bounded buffer (10 000 records); a full buffer sheds, counted
   in `nebula_usage_events_dropped_total` and logged — never silently.
6. **Partitions are kept ahead by the migrator.** `nebula-migrate up` (every migrate init container
   and a daily CronJob) creates the next three months of `requests` and `audit_logs` partitions.

## Consequences

- In the Phase 13 demo, 1 000 requests reconcile exactly: the client's token totals equal the
  request rows, which equal the rollups and `/v1/usage`, and the rows' cost equals the rollups' and
  `/v1/costs`; after a price change the first requests' cost is unchanged.
- Deviations from docs/events.md: the record does not yet carry a denormalised `resource_profile`
  (the ingester reads the deployment's resources at ingestion, which is near-real-time, and a
  deleted deployment's row remains); and there is no local-file spool behind the gateway's buffer
  (its root filesystem is read-only) — loss beyond the buffer is shed and counted.
- Development NATS stores JetStream on an emptyDir: records not yet ingested are lost if the NATS
  pod restarts. A production NATS needs a persistent volume.
