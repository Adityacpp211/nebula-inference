# 27. Eight deployment states, enforced in the database

Date: 2026-09-22

## Status

Accepted. Implemented in Phase 2 (migration `000009_deployment_lifecycle`,
`packages/lifecycle`, `services/controlplane/internal/store/deployments.go`).

Supersedes the seven-state `deployment_state` enum introduced by migration `000001`.

## Context

Phase 0 gave `deployment_state` seven values: `pending`, `progressing`, `ready`,
`degraded`, `failed`, `deleting`, `deleted`. Phase 2 has to make the lifecycle real —
a deployment record now moves through it, and Phase 5's controller will drive it —
and three problems with that set became concrete as soon as the transitions were
written down.

**`progressing` cannot express "stuck".** Two different things happen between
"accepted" and "serving": Kubernetes objects are applied, and then pods start, which
means pulling an artifact that may be several gigabytes. These fail differently and on
different timescales. An apply failure is a permission or validation problem visible
in seconds; an artifact pull failure is a network or checksum problem visible in
minutes. Collapsed into one state, the only available alert is "progressing for a long
time", which is useless for both: it fires too late for the first and too early for
the second. Operators need to distinguish "the control plane could not create the
objects" from "the objects exist and the pods cannot start", because the first is the
control plane's fault and the second is not.

**`deleting`/`deleted` conflate two operations.** Stopping a deployment and deleting it
are different intents with different consequences. A stopped deployment keeps its
revisions, its transition history, its usage records and its cost attribution, and can
be started again; the desired state is "zero replicas", not "gone". A deleted
deployment is one the user no longer wants to see. Using the same states for both makes
"scale this to nothing overnight to save money" indistinguishable from "remove this",
and makes it impossible to express the restart.

**Nothing prevented an illegal state.** The enum constrained the set of values, not the
transitions between them. A bug, or a hand-written `UPDATE` during an incident, could
move a deployment from `pending` straight to `ready` — a claim that pods are serving
when nothing was ever created.

## Decision

### 1. Eight states

```
pending → provisioning → starting → ready ⇄ degraded
                                      ↓        ↓
                                    failed ← ──┘
   ↓            ↓            ↓        ↓        ↓
   └──────── stopping ───────────────────────┘ → stopped → pending
```

| State | Meaning | Who writes it |
|---|---|---|
| `pending` | Desired state is recorded. Nothing has been created. | The API, on create and on start. |
| `provisioning` | The controller has claimed it and is applying Kubernetes objects. | The controller. |
| `starting` | Objects exist; pods are pulling artifacts and loading models. | The controller. |
| `ready` | Every desired replica is ready. | The controller. |
| `degraded` | Some replicas are ready, some are not. **Still serving.** | The controller. |
| `failed` | Work failed and will not recover without a change. | The controller, or the API on admission rejection. |
| `stopping` | A stop is requested. Replicas are being wound down. | The API, on stop. |
| `stopped` | No replicas remain. Revisions, history and cost records are kept. | The controller, having observed it. |

Deletion is no longer a state. It is `deleted_at`, constrained by
`ck_deployments__delete_only_when_resting` to the resting states (`pending`, `failed`,
`stopped`), so a deployment with pods still running cannot be made to disappear from
under the controller.

`degraded` serves traffic. Removing a deployment from rotation because one of three
pods died turns a partial failure into a total one.

### 2. The transition graph is data, in the database

`deployment_state_edges` holds the 24 legal transitions, each with a one-sentence note
saying why it exists. `trg_deployments__state_transition` refuses any `UPDATE` whose
`(OLD.state, NEW.state)` pair is not in that table, raising `check_violation`.

The same graph exists in `packages/lifecycle` as a pure mirror, so the API can reject
an illegal transition with a message naming what *would* have been legal rather than
surfacing a constraint violation as a 500. A test
(`packages/lifecycle/deployment_test.go`) parses the migration and fails if the two
graphs differ in either direction, including the notes.

### 3. The trigger, not the application, records history

The same trigger stamps `state_entered_at` and inserts a row into
`deployment_state_transitions` carrying the from-state, to-state, reason, message,
generation, and the actor read from the `app.current_actor` / `app.current_actor_type`
session variables that `store.InTxForOrg` sets.

Two consequences follow, and both are the point:

- A transition cannot happen without being recorded, including one performed by hand
  in `psql` during an incident.
- The state change and its history row commit in the same transaction, so they cannot
  come apart. A history that is missing exactly the transitions that happened during an
  outage is worse than no history.

A write where `NEW.state = OLD.state` is a no-op: the trigger returns early, so
`state_entered_at` is not restarted and no history row is written. Without that,
a controller re-reporting "still ready" every ten seconds would reset the timer that
"stuck in `starting` for 20 minutes" depends on, and fill the table with noise.

### 4. Transitions are compare-and-set

`DeploymentRepo.TransitionState` updates `WHERE ... AND state = $expected`. Two
concurrent transitions from the same state cannot both succeed: the loser affects no
rows and gets `ErrConflict`, and its caller can re-read and decide. The alternative —
last-writer-wins — produces two history rows describing transitions that did not both
happen.

### 5. `state_entered_at` is unforgeable

`nebula_controller` holds column-level `UPDATE` grants on `deployments`. `state`,
`state_reason` and `state_message` are granted; `state_entered_at` deliberately is not.
PostgreSQL checks column privileges against the statement's `SET` list rather than what
a `BEFORE` trigger assigns, so the trigger can maintain the timestamp while no role can
set it by hand.

## Consequences

**Good.**

- "Stuck" is answerable per phase: `state = 'provisioning' AND state_entered_at <
  now() - interval '2 minutes'` and the same query for `starting` with a longer window
  are different alerts with different runbooks.
- "Why was this degraded at 02:00" is a query against
  `deployment_state_transitions`, a day later. Kubernetes Events expire in about an
  hour, so without this table the explanation evaporates before the next stand-up.
- Stop and delete are separable, so scaling to nothing overnight and decommissioning are
  different operations, and restart is `stopped → pending`.
- An illegal state is impossible rather than unlikely: three independent layers (the Go
  machine, the compare-and-set, the trigger) have to fail together.

**Costs, accepted.**

- A migration that replaces an enum in use. `000009` renames the old type, creates the
  new one, converts the column with an explicit `CASE` mapping, and drops the old type.
  The down migration is lossy in one direction, which is documented in the file:
  `provisioning` and `starting` both came from `progressing` and both go back to it.
- Every transition is a trigger invocation and an insert. At the rate deployments change
  state — human and controller timescales, not request timescales — that is not a cost
  worth optimising.
- The graph is written twice, in SQL and in Go. The duplication is deliberate (the
  database must enforce it; the API must explain it) and is held together by a test
  rather than by discipline.

## Alternatives considered

**Keep `progressing`, add a sub-status column.** Rejected: it is the same information
with two places to get wrong, and every query about "stuck" would have to read both
columns and know which combinations are meaningful.

**Enforce transitions only in Go.** Rejected. The control plane will not be the only
writer for long — Phase 5 adds the controller, Phase 6 the autoscaler — and an
incident will involve someone typing SQL. A rule that lives only in the application is
a rule that holds only while every writer remembers it.

**Enforce transitions only in the database.** Rejected: a constraint violation gives a
caller "ck_..." and no indication of what was allowed instead, so the API would return
a 500 where a 422 with a useful message belongs.

**A separate `deployment_status` table.** Rejected: a join on every read of a
deployment, to separate columns that are read together on every page of the dashboard.
The spec/observed distinction is already enforced by column-level grants, which is
where it costs nothing.
