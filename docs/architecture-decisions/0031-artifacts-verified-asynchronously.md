# 31. Artifacts are verified asynchronously, from the stored bytes

Date: 2026-09-30

## Status

Accepted. Implemented in Phase 5 (`packages/artifact`,
`services/controlplane/internal/api/artifacts.go`, `cmd/nebula-artifact-puller`).

## Context

Phases 2–4 accepted a declared SHA-256 at `finalize` and recorded
`verification: declared_checksum`: nothing read the bytes. Phase 5 adds the object
store, presigned upload and the pull path, so the checksum can be computed. Two
questions follow.

1. **When.** Hashing a multi-gigabyte model inside the `finalize` request holds an HTTP
   request, a database transaction and a load balancer timeout hostage to disk and
   network speed.
2. **Which store in development.** The plan named MinIO. Its published images stopped
   being available during this phase, so the kind environment could not pull it.

## Decision

`finalize` on a version whose URI this control plane minted answers **202** and moves
the version to `verifying`. A verifier loop in the control plane, serialised across
replicas by a PostgreSQL advisory lock, streams the stored object through SHA-256,
parses the GGUF header when the format says GGUF, and moves the version to `ready` —
or to `failed` with the reason. A transient store error (network, 5xx) is **not** a
verdict: the version stays `verifying` and is retried; only a definite answer (missing
object, size over the limit, digest mismatch, unparseable header) fails it.

The artifact puller, an initContainer, hashes again as it downloads into the node's
content-addressed cache, and refuses bytes that do not match. Its outcome is written
as JSON to the container's termination message with a distinct exit code, which the
controller turns into a named failure (`ArtifactChecksumMismatch`). So the bytes are
checked twice: once when they arrive, and once each time a node reads them, because
an object store is not immutable just because NEBULA treats it so.

External URIs (not minted by this store) keep `verification: declared_checksum`.

Development uses SeaweedFS's S3 gateway. Any S3-compatible store works; the dev choice
is a values change.

## Consequences

- Registration is two-step from a client's view: poll the version until `ready`. The
  API says so with 202 rather than claiming a result it has not got.
- A version can sit in `verifying` while the store is unreachable. That is visible, and
  preferable to a false `ready` — which is the bug the retry distinction fixed.
- The tamper test in the Phase 5 kind demo overwrites a verified object and sees the
  puller refuse it; verification at upload alone would have served the tampered bytes.
