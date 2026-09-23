# ADR-0025 — Own the migration runner rather than adopting a migration library

- **Status:** Accepted
- **Date:** 2026-09-22
- **Phase:** 1
- **Amends:** [data-model.md §10](../data-model.md#10-migration-policy) and
  [repository-structure.md](../repository-structure.md), which named `golang-migrate` during Phase 0
- **Related:** [ADR-0019](./README.md#adr-0019), [ADR-0010](./README.md#adr-0010)

## Context

Phase 0 assumed `golang-migrate` for schema migrations. Implementing Phase 1 surfaced three things
that assumption had not accounted for.

**Dependency weight.** `golang-migrate` ships drivers for roughly twenty databases and a dozen source
backends in one module, so its `go.mod` drags in AWS, Google Cloud and other SDK dependencies whether
or not they are used. NEBULA needs exactly one database and one source (an `embed.FS`). That is a
large supply-chain surface ([risk R-20](../risk-register.md)) bought for nothing.

**Checksum drift is not detected.** The most damaging migration mistake is editing a migration that
has already run: it applies to fresh databases but not to existing ones, so environments diverge
silently and nothing reports an error until something behaves differently in production. Neither
`golang-migrate` nor `goose` verifies the content of applied migrations.

**Transaction control is per-file, not global.** Most statements belong in a transaction. A few —
`CREATE INDEX CONCURRENTLY`, most notably — cannot run inside one, and PostgreSQL wraps a
multi-statement simple query in an implicit transaction regardless. This needs a per-file opt out,
declared in the file itself, which the common libraries express as global configuration or not at all.

## Decision

NEBULA implements its own migration runner in `packages/db/migrate`, roughly 350 lines with no
dependencies beyond `pgx`, which is already required.

It provides:

| Behaviour | Detail |
|-----------|--------|
| Embedded source | `embed.FS`, so `nebula-migrate` is a single static binary with no files to ship |
| Ordered, paired files | `NNNNNN_name.up.sql` and `.down.sql`; **both are mandatory** |
| Advisory locking | `pg_advisory_lock` on one connection, so two processes starting at once cannot both migrate |
| Per-migration transactions | each file in its own transaction, with the bookkeeping insert inside it |
| `-- nebula:no-transaction` | per-file opt out for statements PostgreSQL forbids inside a transaction |
| **Checksum drift detection** | SHA-256 of each up migration recorded when applied and re-verified on every subsequent run |
| Unknown-version detection | a database migrated by a newer binary is reported, not ignored |
| `up`, `down N`, `status`, `version`, `validate` | `validate` is read-only, for a deploy-time preflight check |

The schema version a service asserts at startup is derived from the same embedded set, so the
migrator and the services can never disagree about what version the code expects.

## Rationale

**The hard part of migrations is not applying SQL in order.** It is knowing that the database matches
the code. Checksum verification is the feature that makes that knowable, and it is the feature the
libraries do not have. Writing the runner is the cheapest way to get it.

**Requiring a down migration is a design constraint worth enforcing.** It forces the author to think
about reversibility while writing the change rather than during an incident, and it makes the
Phase 1 database gate — up from empty, down to empty, up again — mechanically checkable. Both common
libraries treat down migrations as optional.

**The scope is genuinely small.** One database, one source, four commands. Roughly 350 lines that are
fully unit-tested, plus integration tests covering the drift, concurrency and teardown paths. The
maintenance burden is far below the cost of auditing a large transitive dependency tree on every
upgrade.

**It is not novel.** Advisory lock, version table, ordered files: this is the same design every
migration tool uses, with one addition. Nothing here required invention, which is exactly why writing
it is low-risk.

## Alternatives rejected

**`golang-migrate`.** The Phase 0 assumption. Rejected on dependency weight, no checksum
verification, optional down migrations, and awkward per-file transaction control.

**`goose`.** Lighter than `golang-migrate` and a reasonable choice. Still no checksum verification,
still optional down migrations, and it prefers `database/sql`, which means giving up pgx's native
interface or maintaining an adapter. The remaining benefit over ~350 lines was too small to justify
the dependency.

**`atlas` or a declarative schema tool.** Powerful — diffing, linting, drift detection against a
desired schema — but it inverts the model: the schema becomes the source of truth and migrations are
generated. NEBULA's data model depends on hand-written triggers, deferred constraint triggers,
partition helper functions and row-level-security policies, which is exactly the territory where
generated diffs are least trustworthy. Hand-written migrations that a human reviewed are the safer
default here, and this is the alternative most worth revisiting if the schema becomes routine.

**An ORM with auto-migration.** Rejected outright: it hides the SQL this project exists to
demonstrate, and auto-migration in production is how a column gets dropped by surprise.

## Consequences

**Accepted costs.**

- ~350 lines to maintain and test that could have been a dependency.
- No community ecosystem: no third-party source drivers, no CLI plugins.
- Features the libraries have and this does not: multi-database targets, `force` to clear a dirty
  state, and remote sources such as S3 or GitHub. None is needed; `force` is deliberately absent
  because guessing what a half-applied migration did is exactly the decision an operator should make
  deliberately rather than with a flag.
- Phase 0 documentation named `golang-migrate`, so `data-model.md` and `repository-structure.md` now
  point here instead. This ADR is the record of that change.

**Benefits.**

- Three direct dependencies for the whole of Phase 1 (`pgx`, `google/uuid`, `yaml.v3`).
- Editing an applied migration is caught on the next run with a message naming the file, instead of
  causing a silent divergence between environments.
- `nebula-migrate validate` gives deployments a read-only preflight check that the database matches
  the binary about to be released.
- Migration behaviour is testable end to end against a real PostgreSQL, which it is: up/down/up, drift
  detection, concurrent runs, and a teardown that leaves no tables behind.
