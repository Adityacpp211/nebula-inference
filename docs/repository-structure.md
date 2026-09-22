# NEBULA — Repository Structure

**Status:** Phase 0 design. Created in Phase 1.

A monorepo. One commit can change an API contract, its Go server, its TypeScript client, and the
migration behind it — which is exactly the change that goes wrong when those live in four
repositories with four release cadences.

---

## 1. Module strategy

| Language | Layout | Reason |
|----------|--------|--------|
| Go | **One** module: `github.com/<owner>/nebula` — the owner is fixed in Phase 1, when the repository is created | Shared packages are the point of the monorepo. Multiple Go modules would mean `replace` directives in development and version pinning between services that ship together — cost with no benefit at this size. |
| Python | One package per deployable under `workers/`, with `pyproject.toml` and a lockfile; runtimes are packages inside it | The worker is the only Python deployable. Runtime adapters are its plugins, not separate distributions. |
| TypeScript | One npm workspace at `dashboard/` | Single frontend. A workspace leaves room for a future shared UI package without restructuring. |

Cross-language contracts are files, not imports: `packages/api/openapi.yaml` generates Go types and
TypeScript types; `packages/api/worker.openapi.yaml` generates the Go client and the Python server
models. No language reaches into another's source tree.

---

## 2. Tree

```
nebula/
├── cmd/
│   └── nebula/                     # CLI (Go). Cobra. Thin client over the Control API.
│       ├── main.go
│       └── internal/{cmdmodel,cmddeploy,cmdrollout,cmdusage,output,config}/
│
├── services/
│   ├── gateway/                    # data plane
│   │   ├── main.go
│   │   └── internal/{server,middleware,openai,stream,dispatch,adminproxy,routerstate}/
│   ├── controlplane/               # admin API; only writer of PostgreSQL
│   │   ├── main.go
│   │   └── internal/{server,handlers,registry,deployments,routes,rollouts,keys,usage,audit}/
│   ├── controller/                 # reconcilers
│   │   ├── main.go
│   │   └── internal/{deploymentctrl,rolloutctrl,inventoryctrl,usageingest,workqueue}/
│   └── autoscaler/
│       ├── main.go
│       └── internal/{loop,signals,decide,guards}/
│
├── workers/
│   └── inference/                  # Python: FastAPI + runtime adapters
│       ├── pyproject.toml
│       ├── nebula_worker/
│       │   ├── app.py              # HTTP surface: /internal/v1/*, probes, /metrics
│       │   ├── queue.py            # bounded, priority, deadline-aware local queue
│       │   ├── deadline.py  cancel.py  telemetry.py  config.py
│       │   └── runtimes/
│       │       ├── base.py         # InferenceRuntime protocol + typed errors
│       │       ├── mock.py         # DECLARED STUB: seeded, deterministic, error injection
│       │       ├── llamacpp.py     # supervises upstream llama-server
│       │       └── vllm.py         # post-v1, GPU
│       └── tests/
│           ├── runtime_conformance.py   # the suite EVERY adapter must pass
│           └── test_{queue,deadline,cancel,app}.py
│
├── packages/                       # Go libraries. No package imports a service.
│   ├── api/                        # openapi.yaml, worker.openapi.yaml, generated types, clients
│   ├── auth/                       # key hashing, JWT, scopes, RBAC, auth context propagation
│   ├── config/                     # layered loader, validation, redaction
│   ├── db/                         # pgx pool, sqlc output, tx helpers, RLS session, migrations runner
│   ├── telemetry/                  # slog setup, OTel init, metric registry, probe handlers
│   ├── queue/                      # bounded priority deadline queue + metrics
│   ├── routing/                    # EndpointSnapshot, strategies, policy composition, bucketing
│   ├── reliability/                # retry classification, backoff+jitter, breaker, timeouts, drain
│   ├── scheduler/                  # hardware profile → constraints; capacity inventory + admission
│   ├── events/                     # NATS/JetStream subjects, publishers, durable consumers
│   ├── costing/                    # pricing profiles, micros arithmetic, attribution
│   ├── artifact/                   # ArtifactStore: s3 + file, checksum verification
│   ├── k8s/                        # typed clients, informers, apply helpers, owner refs, labels
│   └── version/                    # build info, contract version, skew detection
│
├── dashboard/                      # React + TypeScript + Vite
│   ├── src/{pages,components,lib,hooks,styles}/
│   └── src/lib/api/                # generated from openapi.yaml — never hand-edited
│
├── deploy/
│   ├── kind/{cluster.yaml,README.md}
│   ├── helm/nebula/                # umbrella chart + subcharts (see deployment-architecture.md)
│   ├── kubernetes/                 # GENERATED from helm template; do not hand-edit
│   └── grafana/                    # dashboard JSON + alert rules, version-controlled
│
├── migrations/                     # NNNNNN_name.{up,down}.sql — hand-written, forward-only in CI
│
├── tests/
│   ├── integration/                # real Postgres/Redis/NATS via testcontainers-go
│   ├── e2e/                        # kind: the Definition of Done, executable
│   ├── load/                       # k6 scripts + thresholds committed as SLOs
│   └── failure/                    # deliberate breakage: kill pod, drop DB, partition NATS
│
├── docs/
│   ├── architecture.md  data-model.md  api.md  deployment-architecture.md
│   ├── repository-structure.md  roadmap.md  risk-register.md
│   ├── development.md  model-runtime.md  observability.md  security.md  reliability.md
│   └── architecture-decisions/     # ADRs
│
├── scripts/                        # dev-up, dev-down, gen (codegen), lint, load, seed, verify-phase
├── .github/workflows/              # ci, integration, e2e, release
├── Makefile                        # the single entry point for every task
├── .golangci.yml  .gitattributes  .gitignore  CONTRIBUTING.md  LICENSE  README.md
└── go.mod
```

---

## 3. Deviations from the structure in the brief, and why

The brief's suggested tree is followed closely, with four deliberate differences. Each is an ADR.

| Brief | NEBULA | Reason |
|-------|--------|--------|
| `services/router/` | `packages/routing/` (library), consumed by the gateway | A per-request network hop to a router service adds latency and a failure domain without adding capability; the routing *state* it would centralize is available to every gateway replica over NATS and informers. [ADR-0004](./architecture-decisions/0004-router-as-library.md) |
| `services/scheduler/` | `packages/scheduler/` + the inventory reconciler in the controller | NEBULA does not bind pods. Placement is constraint generation plus capacity admission, which is a pure function plus a cache — not a service. [ADR-0005](./architecture-decisions/0005-cooperate-with-kubernetes.md) |
| `services/inference-worker/` + `runtimes/` as siblings | `workers/inference/` with `runtimes/` **inside** it | Adapters are only ever loaded in the worker process. A sibling directory implies a distribution boundary that does not exist. |
| (not listed) `services/controller/` | added, separate from `controlplane` | The brief's control plane mixes an HTTP API with reconciliation loops. Those have different failure modes, cadences, and scaling needs; the API must stay responsive when reconciliation is backed up. [ADR-0021](./architecture-decisions/README.md#adr-0021) |

`services/autoscaler/` is kept as its own service exactly as the brief has it, for the blast-radius
reason in [architecture.md §4.4](./architecture.md#44-nebula-autoscaler).

---

## 4. Dependency rules

Enforced by `go-arch-lint` (or an equivalent import-boundary linter) in CI, so these are checks, not
aspirations:

1. `packages/*` **must not** import `services/*` or `cmd/*`.
2. `services/*` must not import another `services/*`. Shared logic moves to `packages/`; cross-service
   communication is the HTTP or NATS contract.
3. Only `services/controlplane` may import `packages/db` write paths. The controller uses a distinct
   restricted query set; the gateway imports no database package at all.
4. Only `services/controller` may import `packages/k8s` write helpers.
5. `packages/routing`, `packages/queue`, `packages/reliability`, `packages/costing`, and
   `packages/scheduler` must have **no I/O**: no database, no HTTP, no clock except an injected one.
   That is what makes them unit-testable with table-driven tests and deterministic under simulated
   time, and it is the reason the tricky logic lives there rather than in a handler.
6. `dashboard/src/lib/api/` is generated; a hand edit fails CI.

---

## 5. Makefile as the single entry point

Every task has exactly one spelling, so CI and a human run the same thing:

```
make dev-up            # kind cluster + dependencies + migrations + seed + print endpoint & key
make dev-down
make build             # all Go binaries
make gen               # openapi → Go/TS, sqlc, mocks; CI fails if this produces a diff
make lint              # golangci-lint, ruff, mypy, eslint, spectral, conftest, arch-lint
make test              # unit, race, coverage
make test-integration  # testcontainers: Postgres, Redis, NATS
make test-e2e          # kind: full Definition of Done walk
make test-failure      # chaos scenarios
make load              # k6 against the mock runtime, thresholds enforced
make verify-phase      # build + test + migrate up/down/up + API drift + docs links: the phase gate
make docs-check        # dead links, ADR index consistency, TODO(NEB-*) references resolve
```

`make verify-phase` is the mechanical form of the brief's end-of-phase checklist. A phase is not
finished because it feels finished; it is finished when that target is green.

---

## 6. Conventions

- **Go**: `golangci-lint` with `errcheck`, `govet`, `staticcheck`, `revive`, `gosec`, `bodyclose`,
  `contextcheck`, `errorlint`, `sloglint`. Errors wrapped with `%w` and context; sentinel errors for
  anything a caller branches on. `context.Context` first parameter on anything that does I/O, with a
  deadline — a context without a deadline in a request path is a lint failure. No `panic` outside
  `main` and package init. `log/slog` only; no `fmt.Println` in service code.
- **Python**: `ruff` + `mypy --strict`, fully async, no bare `except`, typed errors from
  `runtimes/base.py`, `structlog` JSON to stdout.
- **TypeScript**: strict mode, no `any`, generated API types only, TanStack Query for all server
  state (no hand-rolled fetch-and-store).
- **SQL**: hand-written migrations; queries live in `.sql` files consumed by `sqlc`, so every query
  is reviewable and type-checked against the real schema.
- **Commits**: Conventional Commits with a scope (`feat(router): add latency-aware strategy`), which
  makes the changelog and the "what changed in this phase" section generatable rather than written
  from memory.
- **Branches**: short-lived, PR-reviewed, CI-green before merge. `main` is always deployable.
- **TODOs**: only in the form `TODO(NEB-123): <what must replace this>`, and `make docs-check`
  verifies each has a tracking issue. That is how axiom A9 ("every stub is declared") is enforced
  instead of hoped for.
- **`.gitattributes`**: `* text=auto eol=lf` with `*.ps1 eol=crlf` — the repository is developed on
  Windows and deployed to Linux, and a stray CRLF in a shell script inside a container is a
  half-hour someone should not spend.
