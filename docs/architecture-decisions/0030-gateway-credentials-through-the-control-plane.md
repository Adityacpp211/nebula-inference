# 30. The gateway resolves credentials through the control plane, and verifies them itself

Date: 2026-09-25

## Status

Accepted. Implemented in Phase 4 (`services/gateway/internal/credentials`,
`services/controlplane/internal/api/internal.go`, `packages/auth/internal.go`).

## Context

Two documented rules pull against each other.

- docs/security-boundaries.md §2 (B1, step 4) puts API-key verification in the
  gateway, on every request: prefix lookup, then a constant-time HMAC compare against
  a pepper held in memory.
- docs/repository-structure.md §4 (rule 3) says the gateway imports no database
  package at all. The data plane holds no database credential.

The key records live in PostgreSQL. Something has to bridge them.

Three options were considered:

1. **Give the gateway read-only database access.** Violates rule 3, and puts a
   database credential on the most exposed component.
2. **Have the control plane verify every key** (the gateway forwards the presented
   key). Puts the control plane in the request path, which docs/architecture.md §4.2
   forbids, and sends a live plaintext key over one more hop.
3. **Have the control plane return the key's record, and the gateway verify it.**

## Decision

Option 3. The control plane exposes `GET /internal/v1/credentials/{prefix}`, which
returns the key's record — including its stored HMAC, org, scopes, priority, expiry,
revocation and rate-limit policy — to a caller presenting a signed service identity.
The gateway caches the record in two tiers (in-process, a few seconds; Redis, the key
cache TTL) and verifies the presented key against the cached hash on every request.

The internal surface is protected three ways: every route requires
`X-Nebula-Auth-Context` signed with the internal secret and carrying
`actor_type: service`; the gateway never proxies `/internal/`; and NetworkPolicy
(Phase 5) admits only the gateway. The same signed-context mechanism carries the
caller's identity on proxied admin calls (B2), so the control plane never sees an API
key it did not issue.

## Consequences

- The gateway holds the pepper, as B1 always required. The stored hash crossing the
  internal network is useless without it, and the plaintext key never leaves the
  gateway.
- A key the gateway has seen keeps working through a control-plane outage for a
  bounded grace (`NEBULA_GATEWAY_STALE_KEY_GRACE`, default 5 minutes), marked
  degraded in the logs; a key it has never seen fails closed with 503. That is axiom
  A8 with a hard cap.
- Revocation is immediate on the replica that proxied it (the control plane names the
  revoked prefix in a response header the gateway consumes and strips) and on the
  shared Redis tier. Other replicas' in-process caches honour their few-second TTL.
  Phase 6's NATS invalidation broadcast closes that window.
- A cold cache under load is one control-plane call per prefix, not per request: the
  gateway collapses concurrent misses with singleflight, and caches unknown prefixes
  negatively for a few seconds.
- `last_used_at` is touched when the gateway looks a key up, so its resolution is the
  cache TTL plus the store's one-minute throttle.
