# NEBULA

**A self-hostable AI inference cloud.** Register a model, deploy it, and get a single
OpenAI-compatible endpoint with intelligent routing, bounded queuing, autoscaling, canary rollouts,
and end-to-end observability — running on Kubernetes, on a laptop.

```bash
nebula model register ./qwen2.5-0.5b-instruct-q4_k_m.gguf --name qwen2.5 --version 0.5b-q4
nebula deploy qwen2.5:0.5b-q4 --replicas 2 --route qwen2.5-chat --wait
```

```python
from openai import OpenAI
client = OpenAI(base_url="https://nebula.local/v1", api_key="nbk_...")
for chunk in client.chat.completions.create(
        model="qwen2.5-chat", messages=[{"role": "user", "content": "hello"}], stream=True):
    print(chunk.choices[0].delta.content or "", end="")
```

---

> ## Project status — Phase 1 complete
>
> **The foundation is working and tested; there is no inference yet.** The `nebula deploy` commands
> above describe the finished system, not what runs today.
>
> What works now: the PostgreSQL schema with its constraints, triggers and row-level security; an
> embedded migration runner with checksum drift detection; the layered configuration system; JSON
> structured logging with request-ID and trace correlation; and a control-plane binary serving
> liveness, readiness and health endpoints.
>
> Next: [Phase 2](docs/roadmap.md#phase-2--control-plane-and-model-registry) — the control-plane API
> and model registry. See [Capability status](#capability-status).

## Running it today

```bash
make preflight        # check the toolchain
make db-up            # PostgreSQL 16 in Docker
make migrate-up       # apply the schema
make test             # unit tests
make test-integration # tests against the real database
make run-controlplane # then: curl localhost:8082/healthz
```

---

## What NEBULA is, and is not

NEBULA is the control plane around model serving: identity, placement, admission, routing,
reliability, progressive delivery, and accounting.

It is **not** an inference engine — llama.cpp, vLLM, and TGI do the generation, behind a runtime
abstraction. It is **not** a Kubernetes replacement — it generates scheduling constraints and admits
against real capacity, then lets kube-scheduler place pods
([ADR-0005](docs/architecture-decisions/0005-cooperate-with-kubernetes.md)).

The design principle that governs the rest: **nothing is fabricated**. No synthetic metrics, no demo
data path in the dashboard, no cost figures without their assumptions, no "production" label on a
stub. Where something is designed but unverified — GPU scheduling on hardware that does not exist here
— it says so.

## Architecture

```mermaid
flowchart TB
    subgraph clients [" "]
        CLI["nebula CLI"]
        SDK["OpenAI SDK / curl"]
        UI["Dashboard"]
    end

    GW["<b>nebula-gateway</b><br/>authn · ratelimit · validate<br/>router · admission queue<br/>dispatch · stream · usage"]

    subgraph dataplane ["inference"]
        W1["<b>nebula-worker</b><br/>bounded queue<br/>InferenceRuntime<br/>llama.cpp | mock | vLLM"]
    end

    CP["<b>nebula-controlplane</b><br/>admin API · registry<br/>only writer of SQL"]
    CTRL["<b>nebula-controller</b><br/>deployment · rollout<br/>inventory · usage ingest"]
    AS["<b>nebula-autoscaler</b><br/>signals → replicas"]

    PG[("PostgreSQL<br/><i>desired state</i>")]
    RD[("Redis<br/><i>ratelimit · cache</i>")]
    NATS{{"NATS + JetStream<br/><i>heartbeats · events · usage</i>"}}
    K8S["Kubernetes API<br/><i>actual state</i>"]
    OBS["OTel Collector →<br/>Prometheus · Tempo · Loki<br/>→ Grafana"]

    CLI --> GW
    SDK --> GW
    UI --> GW
    GW -- "HTTP + SSE<br/>token path" --> W1
    GW -- "admin proxy" --> CP
    GW <--> RD
    CP --> PG
    CTRL -- "read spec" --> PG
    CTRL -- "apply · watch" --> K8S
    K8S -. "creates" .-> W1
    AS -- "desired_replicas" --> PG
    W1 -- "heartbeats" --> NATS
    GW -- "usage events" --> NATS
    NATS --> CTRL
    NATS --> AS
    GW -.-> OBS
    W1 -.-> OBS
    CTRL -.-> OBS

    classDef svc fill:#12161d,stroke:#3d4757,color:#e6e9ef
    classDef infra fill:#0d1117,stroke:#2b3340,color:#9aa4b2
    class GW,W1,CP,CTRL,AS svc
    class PG,RD,NATS,K8S,OBS infra
```

A text version of this diagram, with the request path annotated, is in
[docs/architecture.md §5](docs/architecture.md#5-architecture-diagram).

**The request path**, which is where most of the engineering is:

```
accept → identify → authenticate → authorize → validate → rate limit
       → resolve route → select endpoint → [admission queue] → dispatch
       → generate → stream → account
```

Every stage is named, bounded, instrumented, and has defined behaviour when it fails. See
[architecture.md §6](docs/architecture.md#6-the-request-path).

## Key design decisions

| Decision | Why |
|----------|-----|
| **Tokens travel over HTTP; NATS carries control and telemetry only** | A broker on the token path costs time-to-first-token and makes NATS a hard dependency of inference. Inference keeps working when NATS is down. [ADR-0003](docs/architecture-decisions/0003-push-dispatch-and-nats-scope.md) |
| **The router is a library, not a service** | A per-request hop to a router adds latency and a failure domain without adding capability, and loses the caller's own failure observations. [ADR-0004](docs/architecture-decisions/0004-router-as-library.md) |
| **NEBULA generates constraints; kube-scheduler places pods** | Kubernetes already solves placement. What it cannot do is know a GGUF needs 6 GiB — that translation is the value NEBULA adds. [ADR-0005](docs/architecture-decisions/0005-cooperate-with-kubernetes.md) |
| **Clients call a *route*, not a deployment** | This one indirection is what makes canary, A/B, blue-green, and rollback possible without clients changing anything. [ADR-0009](docs/architecture-decisions/README.md#adr-0009) |
| **Desired state in PostgreSQL, actual state in Kubernetes** | Naming one owner per fact is what keeps a control plane from acting on stale copies of itself. [ADR-0008](docs/architecture-decisions/README.md#adr-0008) |
| **Never retry after the first token** | A streamed response cannot be un-sent; retrying would concatenate two generations into text no model produced. [ADR-0013](docs/architecture-decisions/README.md#adr-0013) |
| **Model versions are immutable; rollback is a new revision** | "What exactly was serving at 14:32 last Tuesday" has to be answerable. [ADR-0010](docs/architecture-decisions/README.md#adr-0010) |

All twenty-four: [docs/architecture-decisions/](docs/architecture-decisions/).

## Documentation

| Document | Contents |
|----------|----------|
| [architecture.md](docs/architecture.md) | Design axioms, components, service boundaries, request path, control loops, event flows, failure-mode expectations, cost model, non-goals |
| [components.md](docs/components.md) | Every component as responsibility / inputs / outputs / dependencies / failure modes / scaling — and what must **not** be a separate microservice |
| [diagrams.md](docs/diagrams.md) | System, component, request, deployment, model-registration, failure/recovery, ER, Kubernetes, and dependency-graph diagrams |
| [data-model.md](docs/data-model.md) | PostgreSQL schema, constraints, immutability triggers, partitioning, row-level security, migration policy |
| [api.md](docs/api.md) | OpenAI-compatible surface, control API, internal worker contract, `InferenceRuntime` interface, CLI contract |
| [events.md](docs/events.md) | NATS subject catalogue, per-event schemas, delivery semantics, consumer contracts, schema evolution |
| [security-boundaries.md](docs/security-boundaries.md) | Trust zones, boundary contracts, identity and credentials, authorization, tenant isolation, threat table |
| [observability.md](docs/observability.md) | Telemetry pipeline, metric catalogue with cardinality budget, span model, log schema, correlation path, SLOs and alerts |
| [deployment-architecture.md](docs/deployment-architecture.md) | Namespaces, RBAC, NetworkPolicy, probes, artifact storage, Helm layout, kind topology, GPU optionality, CI |
| [repository-structure.md](docs/repository-structure.md) | Monorepo layout, module strategy, dependency rules, conventions |
| [roadmap.md](docs/roadmap.md) | Phases 0–18 with deliverables, tests, and exit criteria |
| [risk-register.md](docs/risk-register.md) | Twenty scored risks with mitigations and residuals |
| [architecture-decisions/](docs/architecture-decisions/) | ADR index (24) plus the six load-bearing ones in full |

Written during implementation: `development.md`, `deployment.md`, `model-runtime.md`,
`reliability.md`, and `security.md` (the operational threat model extending
[security-boundaries.md](docs/security-boundaries.md)).

### Design deliverable map

| # | Deliverable | Where |
|---|-------------|-------|
| 1 | System architecture diagram | [diagrams.md §1](docs/diagrams.md#1-system-architecture) · text version [architecture.md §5](docs/architecture.md#5-architecture-diagram) |
| 2 | Component diagram | [diagrams.md §2](docs/diagrams.md#2-component-diagram) |
| 3 | Request lifecycle | [diagrams.md §3](docs/diagrams.md#3-request-lifecycle) · rules [architecture.md §6](docs/architecture.md#6-the-request-path) |
| 4 | Deployment lifecycle | [diagrams.md §4](docs/diagrams.md#4-deployment-lifecycle) |
| 5 | Model registration lifecycle | [diagrams.md §5](docs/diagrams.md#5-model-registration-lifecycle) |
| 6 | Failure / recovery flows | [diagrams.md §6](docs/diagrams.md#6-failure-and-recovery-flows) · per-component [components.md §7](docs/components.md#7-failure-mode-summary) |
| 7 | Database ER diagram | [diagrams.md §7](docs/diagrams.md#7-database-er-diagram) · DDL [data-model.md](docs/data-model.md) |
| 8 | Kubernetes architecture | [diagrams.md §8](docs/diagrams.md#8-kubernetes-architecture) · detail [deployment-architecture.md](docs/deployment-architecture.md) |
| 9 | Service dependency graph | [diagrams.md §9](docs/diagrams.md#9-service-dependency-graph) |
| 10 | API contract | [api.md](docs/api.md) |
| 11 | Event / message definitions | [events.md](docs/events.md) |
| 12 | Security boundaries | [security-boundaries.md](docs/security-boundaries.md) |
| 13 | Observability architecture | [observability.md](docs/observability.md) |
| 14 | ADR list | [architecture-decisions/](docs/architecture-decisions/) |
| 15 | Phased implementation plan | [roadmap.md](docs/roadmap.md) |
| + | Per-component spec and the not-a-microservice analysis | [components.md](docs/components.md) |

## Capability status

Updated at the end of every phase. Nothing is marked working until it is demonstrable from a clean
checkout.

| Capability | Status | Phase |
|------------|--------|-------|
| Architecture, data model, API contracts, ADRs | ✅ designed | 0 |
| Schema, migrations, config, logging, request IDs, health endpoints | ✅ **working** | 1 |
| Control plane, model registry, artifact store | ⬜ not started | 2 |
| Runtime abstraction, llama.cpp + mock runtimes | ⬜ not started | 3 |
| Gateway, OpenAI-compatible API, streaming | ⬜ not started | 4 |
| Deployment controller, Kubernetes integration, Helm | ⬜ not started | 5 |
| Health-aware routing, routing strategies | ⬜ not started | 6 |
| Bounded queues, concurrency control, backpressure | ⬜ not started | 7 |
| Metrics, logs, traces, Grafana dashboards | ⬜ not started | 8 |
| Autoscaling | ⬜ not started | 9 |
| Retries, breakers, timeouts, graceful shutdown | ⬜ not started | 10 |
| Model versioning, rollback | ⬜ not started | 11 |
| Canary rollouts, A/B testing | ⬜ not started | 12 |
| Token accounting, cost estimation, usage analytics | ⬜ not started | 13 |
| CLI | ⬜ not started | 14 |
| Dashboard | ⬜ not started | 15 |
| Load testing, failure testing | ⬜ not started | 16 |
| Security hardening | ⬜ not started | 17 |
| GPU scheduling and vLLM runtime | ⚠️ designed, **cannot be verified** — no GPU on the development hardware ([R-07](docs/risk-register.md#r-07--gpu-paths-cannot-be-verified-on-the-available-hardware--l-high--i-medium)) | — |

## Known limitations (by design, in v1)

No multi-cluster federation · no training or fine-tuning · no cross-org worker sharing · cost
*estimation* only, never billing · no distributed circuit breaking · no prompt or semantic caching ·
no synchronous inference over a message broker · no statistical-significance claims beyond what the
implemented analysis supports · no `kubectl get nebuladeployments` until the CRD stage
([ADR-0006](docs/architecture-decisions/0006-staged-orchestration.md)).

Reasoning for each: [architecture.md §12](docs/architecture.md#12-explicit-non-goals-for-v1).

## Technology

Go (control plane, gateway, CLI) · Python (inference worker) · PostgreSQL · Redis · NATS + JetStream ·
Kubernetes + Helm + kind · Prometheus, OpenTelemetry, Tempo, Loki, Grafana · React + TypeScript ·
k6 · GitHub Actions.

Every dependency has a justification in
[architecture.md §3.2](docs/architecture.md#32-infrastructure-dependencies-and-why-each-exists),
including the ones that were rejected.

## License

To be decided before the first public release.
