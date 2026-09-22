# ADR-0001 — Record architecture decisions

- **Status:** Accepted
- **Date:** 2026-09-22
- **Phase:** 0
- **Deciders:** NEBULA engineering

## Context

NEBULA makes a large number of non-obvious choices: where state lives, what the request path may
depend on, which component owns which fact, what is deliberately not built. Most of these have a
defensible alternative, and several contradict the shape suggested in the original project brief.

Without a record, three things happen. Settled questions get re-opened by whoever encounters the code
next. Decisions that were deliberate become indistinguishable from accidents — a reader cannot tell
whether the router is a library because that was reasoned through or because someone did not get
around to extracting it. And a reviewer evaluating the work has no way to assess the *judgement*
behind the code, only the code.

The brief explicitly requires ADRs and requires that deviations from its suggested structure be
documented.

## Decision

Every architecturally significant decision gets an ADR in `docs/architecture-decisions/`, in
[MADR](https://adr.github.io/madr/) style: context, decision, rejected alternatives with reasons,
consequences including the bad ones.

**Significant** means at least one of:

- it constrains what the system can do later;
- it is expensive to reverse;
- a competent engineer would plausibly have chosen differently;
- it deviates from a convention, a standard, or the project brief.

Not significant: library choices with no architectural consequence, naming, formatting, anything a
lint rule can express.

Rules:

1. ADRs are numbered sequentially and **immutable once Accepted**. A changed decision is a new ADR
   that supersedes the old one; the old file stays, marked `Superseded by ADR-nnnn`. Same reasoning as
   immutable model versions — history that can be edited is not history.
2. The decision is made *before* the implementation, in the phase that needs it, not reconstructed
   afterwards.
3. Every ADR names what was rejected and why. An ADR without rejected alternatives is a description,
   not a decision.
4. Consequences include the costs accepted, not only the benefits. An ADR that reads as
   uncomplicatedly positive is not finished.
5. `docs/architecture-decisions/README.md` indexes all of them with one-paragraph summaries, and
   `make docs-check` verifies the index matches the files on disk.
6. Code that exists because of an ADR links to it in a comment where a reader would otherwise be
   puzzled.

## Consequences

**Accepted costs.** Writing an ADR takes time that feels like it belongs to implementation. The
discipline of naming rejected alternatives sometimes reveals that the decision was not actually made,
which is slower in the moment. Superseded ADRs accumulate and must be read with attention to status.

**Benefits.** Deviations from the brief become auditable positions rather than apparent oversights. A
reviewer can disagree with the reasoning specifically, which is a far more useful conversation than
disagreeing with the result. Re-litigation is cheap to shut down — the argument is already written.

**Alternatives rejected.** A single `decisions.md` file (merge conflicts, no stable references, no
immutability); commit messages as the record (unsearchable and unindexed at this scale); a wiki
(drifts from the code it describes and is not reviewed with the change that invalidates it).
