# NEBULA — Architecture Diagrams

**Status:** Phase 0 design. Diagrams are Mermaid, which renders on GitHub and in most editors; the
system-architecture and Kubernetes views also have plain-text versions, because a diagram you cannot
read in a terminal is a diagram you will not read during an incident.

Contents: [1 System architecture](#1-system-architecture) · [2 Component diagram](#2-component-diagram) ·
[3 Request lifecycle](#3-request-lifecycle) · [4 Deployment lifecycle](#4-deployment-lifecycle) ·
[5 Model registration lifecycle](#5-model-registration-lifecycle) ·
[6 Failure and recovery flows](#6-failure-and-recovery-flows) · [7 Database ER diagram](#7-database-er-diagram) ·
[8 Kubernetes architecture](#8-kubernetes-architecture) ·
[9 Service dependency graph](#9-service-dependency-graph)

Component responsibilities, inputs, outputs, failure modes, and scaling: [components.md](./components.md).

---

## 1. System architecture

Four planes. The important property is the boundary between them: the **data plane** has no hard
dependency that can take inference down except the workers it dispatches to.

```mermaid
flowchart TB
    subgraph CLIENT ["Client zone — untrusted"]
        direction LR
        SDK["OpenAI SDK<br/>LangChain, curl"]
        CLI["nebula CLI"]
        BROWSER["Browser<br/>dashboard"]
    end

    subgraph EDGE ["Data plane — public trust zone"]
        GW["nebula-gateway<br/><br/>authn · authz · validate · ratelimit<br/>route · admit · dispatch · stream · account"]
        DASH["nebula-dashboard<br/>static assets"]
    end

    subgraph CONTROL ["Control plane — internal trust zone"]
        direction LR
        CP["nebula-controlplane<br/>admin API<br/>sole SQL writer"]
        CTRL["nebula-controller<br/>deployment · rollout<br/>inventory · usage"]
        AS["nebula-autoscaler<br/>signals to replicas"]
    end

    subgraph EXEC ["Execution plane — workload trust zone"]
        direction LR
        W1["nebula-worker<br/>runtime adapter<br/>bounded queue"]
        W2["nebula-worker<br/>other model version"]
    end

    subgraph STATE ["State and transport"]
        direction LR
        PG[("PostgreSQL<br/>desired state<br/>history · audit")]
        RD[("Redis<br/>ratelimit · cache<br/>snapshots")]
        NQ{{"NATS + JetStream<br/>heartbeats<br/>events · usage"}}
        OS[("Object store<br/>model artifacts<br/>content-addressed")]
        K8S["Kubernetes API<br/>actual state"]
    end

    subgraph OBSV ["Observability"]
        direction LR
        OTEL["OTel Collector"]
        PROM["Prometheus"]
        TEMPO["Tempo"]
        LOKI["Loki"]
        GRAF["Grafana"]
    end

    SDK --> GW
    CLI --> GW
    BROWSER --> DASH
    BROWSER --> GW
    CLI -. "artifact upload<br/>presigned" .-> OS

    GW == "HTTP + SSE<br/>TOKEN PATH" ==> W1
    GW ==> W2
    GW -- "admin proxy" --> CP
    GW <--> RD
    GW -- "usage events" --> NQ
    GW -- "endpoints" --> K8S

    CP --> PG
    CP --> RD
    CP -- "reconcile nudge" --> NQ
    CP -- "verify checksum" --> OS

    CTRL -- "read desired<br/>write observed" --> PG
    CTRL -- "apply · watch" --> K8S
    CTRL -- "consume usage<br/>publish events" --> NQ
    AS -- "desired_replicas" --> PG
    AS -- "signals" --> NQ
    AS -- "signals" --> PROM
    CTRL -- "rollout analysis" --> PROM

    K8S -. "schedules" .-> W1
    K8S -. "schedules" .-> W2
    W1 -- "heartbeat<br/>lifecycle" --> NQ
    W1 -. "artifact pull<br/>initContainer" .-> OS

    GW -.-> OTEL
    CP -.-> OTEL
    CTRL -.-> OTEL
    AS -.-> OTEL
    W1 -.-> OTEL
    OTEL --> PROM
    OTEL --> TEMPO
    OTEL --> LOKI
    PROM --> GRAF
    TEMPO --> GRAF
    LOKI --> GRAF

    classDef edge fill:#141b26,stroke:#4a90d9,color:#e6e9ef
    classDef ctl fill:#161d16,stroke:#5aa563,color:#e6e9ef
    classDef exe fill:#1d1a14,stroke:#c08a3e,color:#e6e9ef
    classDef inf fill:#0d1117,stroke:#39404d,color:#9aa4b2
    class GW,DASH edge
    class CP,CTRL,AS ctl
    class W1,W2 exe
    class PG,RD,NQ,OS,K8S,OTEL,PROM,TEMPO,LOKI,GRAF inf
```

Thick edges are the token path. Dotted edges are asynchronous or optional. The gateway is the only
component in two zones at once, which is exactly why it is where authentication, rate limiting, and
tenant scoping live — one edge, one place to get isolation right.

Plain-text version: [architecture.md §5](./architecture.md#5-architecture-diagram).

---

## 2. Component diagram

Processes with their internal libraries shown, because "which process is this code in" is the question
the service list does not answer. Libraries appear in every process that consumes them — that is the
point of a library, and the reason the boxes repeat.

```mermaid
flowchart TB
    subgraph GWP ["PROCESS: nebula-gateway (Go, N replicas, stateless)"]
        direction TB
        GWHTTP["HTTP server<br/>OpenAI surface + admin proxy"]
        GWMW["middleware chain<br/>requestID · trace · authn · authz<br/>validate · ratelimit"]
        GWRS["router state maintainer<br/>EndpointSlice informer<br/>NATS heartbeat sub<br/>Redis snapshot loader"]
        LROUTE["lib: routing<br/>Filter · LeastLoaded · LatencyAware<br/>CostAware · Capability · Failover"]
        LQUEUE["lib: queue<br/>bounded · priority · deadline"]
        LREL["lib: reliability<br/>classify · retry · breaker · timeout"]
        GWSTREAM["SSE / WS relay<br/>no full-response buffering"]
        GWHTTP --> GWMW --> LROUTE
        GWRS --> LROUTE
        LROUTE --> LQUEUE --> LREL --> GWSTREAM
    end

    subgraph CPP ["PROCESS: nebula-controlplane (Go, N replicas, stateless)"]
        direction TB
        CPAPI["admin API handlers<br/>registry · deployments · routes<br/>rollouts · keys · usage · audit"]
        LAUTH["lib: auth<br/>HMAC keys · JWT · scopes · RBAC"]
        LART["lib: artifact<br/>presign · verify · GGUF parse"]
        LDB["lib: db<br/>pgx · sqlc · RLS session · tx"]
        CPAPI --> LAUTH
        CPAPI --> LART
        CPAPI --> LDB
    end

    subgraph CTRLP ["PROCESS: nebula-controller (Go, leader-elected, 1 active)"]
        direction TB
        RDEP["deployment reconciler<br/>derive → apply → status"]
        RROLL["rollout reconciler<br/>canary state machine + analysis"]
        RINV["inventory reconciler<br/>nodes · capacity · worker_events"]
        RUSE["usage ingester<br/>JetStream → COPY → rollups"]
        LSCHED["lib: scheduler<br/>profile → constraints<br/>capacity admission"]
        LK8S["lib: k8s<br/>informers · server-side apply<br/>owner refs"]
        LCOST["lib: costing<br/>micros · attribution · pricing"]
        RDEP --> LSCHED
        RDEP --> LK8S
        RINV --> LK8S
        RUSE --> LCOST
    end

    subgraph ASP ["PROCESS: nebula-autoscaler (Go, leader-elected, 1 active)"]
        direction TB
        ASLOOP["control loop<br/>collect → normalize → decide"]
        ASGUARD["guards<br/>stabilization · cooldown · step<br/>clamp · rollout interlock"]
        ASLOOP --> ASGUARD
    end

    subgraph WP ["PROCESS: nebula-worker (Python, per replica)"]
        direction TB
        WAPI["FastAPI<br/>/internal/v1 · probes · /metrics"]
        WQ["local bounded queue<br/>priority · deadline · cancel"]
        WRT["InferenceRuntime adapter"]
        WLL["llamacpp adapter<br/>supervises child process"]
        WMOCK["mock adapter<br/>DECLARED STUB"]
        WVLLM["vllm adapter<br/>post-v1"]
        WENG["llama-server<br/>child process"]
        WAPI --> WQ --> WRT
        WRT --> WLL --> WENG
        WRT --> WMOCK
        WRT --> WVLLM
    end

    subgraph SHARED ["libs in every process"]
        LTEL["lib: telemetry<br/>slog · OTel · metrics · probes"]
        LCFG["lib: config<br/>layered · validated · redacted"]
        LEV["lib: events<br/>subjects · publishers · consumers"]
        LAPI["lib: api<br/>generated from OpenAPI"]
    end

    GWP --> WP
    GWP --> CPP
    CTRLP --> CPP
    ASP --> CPP

    classDef proc fill:#12161d,stroke:#3d4757,color:#e6e9ef
    classDef lib fill:#0f1520,stroke:#4a6285,color:#b9c4d4
    class GWHTTP,GWMW,GWRS,GWSTREAM,CPAPI,RDEP,RROLL,RINV,RUSE,ASLOOP,ASGUARD,WAPI,WQ,WRT,WLL,WMOCK,WVLLM,WENG proc
    class LROUTE,LQUEUE,LREL,LAUTH,LART,LDB,LSCHED,LK8S,LCOST,LTEL,LCFG,LEV,LAPI lib
```

The five I/O-free libraries — `routing`, `queue`, `reliability`, `scheduler`, `costing` — hold the logic
most likely to be wrong, which is why they are pure: they are testable under an injected clock with no
cluster, which matters a great deal given [risk R-01](./risk-register.md).

---

## 3. Request lifecycle

Streaming chat completion, the hot path. Timings are illustrative of shape, not measured.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant G as nebula-gateway
    participant R as lib routing
    participant Q as lib queue
    participant W as nebula-worker
    participant E as llama-server
    participant N as NATS JetStream

    C->>G: POST /v1/chat/completions stream=true
    Note over G: mint X-Request-Id UUIDv7<br/>create or continue traceparent
    G->>G: authenticate — prefix lookup, Redis hit, HMAC compare
    G->>G: authorize scope + org ownership of route
    G->>G: validate schema, max_tokens vs context window
    G->>G: ratelimit Redis token bucket RPM, TPM, concurrency

    G->>R: resolve model name to route
    R-->>G: route + weighted target, variant=baseline
    G->>R: select endpoint from healthy candidates
    Note over R: Filter by readiness, heartbeat age,<br/>breaker state, saturation<br/>then LeastLoaded by inflight + queue depth

    alt an endpoint has capacity
        R-->>G: pod-a
    else all endpoints saturated
        R-->>G: none eligible
        G->>Q: enqueue priority=NORMAL deadline=T+30s
        alt capacity frees before deadline
            Q-->>G: dequeued after wait_ms
        else queue full
            Q-->>G: reject queue_full
            G-->>C: 429 Retry-After
        else deadline passed
            Q-->>G: reject queue_timeout
            G-->>C: 504 timeout_error
        end
    end

    G->>W: POST /internal/v1/generate/stream<br/>X-Nebula-Deadline, Priority, Model-Version
    alt worker local queue full
        W-->>G: 429 Retry-After worker_saturated
        Note over G: retryable, no token emitted<br/>mark endpoint saturated, select another
    else admitted
        W->>E: generate with prompt and sampling params
        E-->>W: first token
        W-->>G: SSE chunk role delta
        G-->>C: SSE chunk — TTFT recorded here
        loop until stop, length, or deadline
            E-->>W: token
            W-->>G: SSE chunk content delta
            G-->>C: SSE chunk
        end
        E-->>W: stop reason + token counts
        W-->>G: final chunk finish_reason + usage
        G-->>C: usage chunk then [DONE]
    end

    G->>N: publish nebula.usage.records
    Note over G,N: authoritative token counts from the runtime<br/>never re-tokenized at the gateway
```

### 3.1 Stage budget and failure behaviour

| # | Stage | Owner | Typical | Failure → client sees |
|---|-------|-------|---------|----------------------|
| 1 | accept + identify | gateway | <1 ms | — |
| 2 | authenticate | `lib auth` + Redis | ~1 ms cached | 401 `authentication_error` |
| 3 | authorize | `lib auth` | <1 ms | 403, or 404 for out-of-org (no existence leak) |
| 4 | validate | gateway | <1 ms | 400 with the offending `param` named |
| 5 | rate limit | Redis Lua | ~1 ms | 429 + `Retry-After`, `X-Nebula-Reason` |
| 6 | resolve route | `lib routing` | <0.1 ms | 404 unknown model |
| 7 | select endpoint | `lib routing` | <0.1 ms | 503 `no_healthy_endpoint` |
| 8 | admission queue | `lib queue` | 0 ms when healthy | 429 `queue_full` or 504 `queue_timeout` |
| 9 | dispatch | `lib reliability` | ~1 ms | retry, then 502/503 — never after a token |
| 10 | prefill | runtime | tens–hundreds ms | 504 on TTFT timeout |
| 11 | decode | runtime | tokens/sec bound | mid-stream error frame, no `finish_reason` |
| 12 | account | `lib events` | async | never visible; drops are counted, not silent |

**The rule that shapes stage 9:** once a token has reached the client, no retry is possible — an HTTP
body cannot be un-sent ([ADR-0013](./architecture-decisions/README.md#adr-0013)).

---

## 4. Deployment lifecycle

### 4.1 Create and reconcile

```mermaid
sequenceDiagram
    autonumber
    participant U as CLI or dashboard
    participant G as gateway
    participant CP as controlplane
    participant PG as PostgreSQL
    participant N as NATS
    participant CT as controller
    participant SC as lib scheduler
    participant K as Kubernetes API
    participant KL as kubelet
    participant W as worker pod

    U->>G: POST /v1/deployments
    G->>CP: proxied with internal auth context
    CP->>SC: admit capacity for N replicas of this shape
    SC-->>CP: admitted plus constraints and headroom note
    alt not admissible
        CP-->>U: 422 with the failing constraint and observed capacity
    end
    CP->>PG: TX deployments gen=1, revision 1, route, route_targets 100
    CP->>PG: audit_log deployment.created
    CP->>N: publish nebula.control.reconcile
    CP-->>U: 202 Accepted state=pending Location header

    N->>CT: reconcile nudge
    CT->>PG: read desired spec and current revision
    CT->>K: list actual objects by label selector
    Note over CT: diff desired vs actual
    CT->>SC: derive constraints from hardware_profile
    SC-->>CT: requests, limits, nodeSelector, tolerations, spread
    CT->>K: server-side apply ConfigMap root, Deployment, Service, PDB
    CT->>PG: observed_generation=1 state=progressing
    CT->>N: publish nebula.deployment.events created

    K->>KL: schedule pods
    KL->>KL: initContainer pull artifact, verify sha256
    Note over KL: cache hit on this node means no download
    KL->>W: start worker container
    W->>W: runtime.load model
    W->>N: nebula.worker.lifecycle model_loading then ready
    W-->>KL: startupProbe /readyz passes, up to 5 min allowed
    Note over K: EndpointSlice marks pod ready
    W->>N: heartbeats begin at 1 Hz

    K-->>CT: Pod and Deployment informer events
    CT->>PG: ready_replicas=2 state=ready condition Ready=True
    CT->>N: publish nebula.deployment.events ready
    Note over G: router state converges<br/>endpoint enters selection
    G-->>U: nebula deploy --wait returns, prints endpoint
```

### 4.2 Deployment state machine

```mermaid
stateDiagram-v2
    [*] --> pending : spec created
    pending --> progressing : objects applied
    pending --> failed : capacity inadmissible or apply rejected
    progressing --> ready : ready_replicas equals desired
    progressing --> degraded : some replicas unhealthy
    progressing --> failed : model load failure on all replicas
    ready --> progressing : spec change, generation bumped
    ready --> degraded : replica lost or probe failing
    degraded --> ready : replicas recovered
    degraded --> failed : no replica recovers within threshold
    failed --> progressing : new revision applied
    ready --> deleting : delete requested
    degraded --> deleting : delete requested
    failed --> deleting : delete requested
    deleting --> [*] : objects garbage collected, row soft-deleted
```

`generation` changes only on a spec change and never on a status write — the single rule that makes
`generation <> observed_generation` a trustworthy "work to do" predicate.

### 4.3 Scale, rollback, and canary as the same mechanism

```mermaid
flowchart LR
    subgraph SCALE ["scale"]
        S1["POST /scale replicas=3"] --> S2["revision reason=scale<br/>generation++"] --> S3["controller patches<br/>Deployment replicas"]
    end
    subgraph ROLLBACK ["rollback"]
        B1["POST /rollback revision=4"] --> B2["NEW revision<br/>spec copied from 4<br/>reason=rollback"] --> B3["controller applies<br/>old artifact still cached"]
    end
    subgraph CANARY ["canary"]
        C1["POST /rollouts"] --> C2["weights on route_targets<br/>10 then 25 then 50 then 100"] --> C3["analysis per step<br/>promote or abort"]
    end
    S2 -.-> NOTE["all three are<br/>ordinary writes<br/>no special path"]
    B2 -.-> NOTE
    C2 -.-> NOTE
```

History is never rewritten: a rollback appends a revision rather than restoring one, which is what
makes an incident reviewable afterwards.

---

## 5. Model registration lifecycle

```mermaid
sequenceDiagram
    autonumber
    participant U as nebula CLI
    participant G as gateway
    participant CP as controlplane
    participant PG as PostgreSQL
    participant OS as Object store

    U->>G: POST /v1/models name=qwen2.5
    G->>CP: create model
    CP->>PG: insert models row
    CP-->>U: 201 model_id

    U->>G: POST /v1/models/{id}/versions version=0.5b-q4
    CP->>PG: insert model_versions status=uploading
    CP->>OS: presign PUT sha256/{client-declared hex}
    CP-->>U: 201 upload target, expiry, max_bytes

    U->>OS: PUT artifact bytes, resumable, progress shown
    Note over U,OS: multi-GB weights never pass<br/>through the JSON API

    U->>G: POST /v1/model-versions/{id}/finalize checksum
    CP->>PG: status=verifying
    CP->>OS: stream object, compute sha256, read size
    alt checksum or size mismatch
        CP->>PG: status=failed, failure_reason has computed value
        CP-->>U: 422 with expected and computed checksums
    else verified
        CP->>OS: read GGUF header
        Note over CP: parse context_window, quantization,<br/>parameter_count — never trust client metadata
        CP->>PG: status=ready, ready_at set
        Note over PG: trigger now rejects mutation<br/>of checksum, format, runtime,<br/>context_window, hardware_profile
        CP->>PG: audit_log model_version.created
        CP-->>U: 200 status=ready with parsed metadata
    end
```

### 5.1 Model version state machine

```mermaid
stateDiagram-v2
    [*] --> uploading : version record created
    uploading --> verifying : finalize called
    uploading --> failed : upload expired or abandoned
    verifying --> ready : checksum, size and header verified
    verifying --> failed : mismatch or unreadable artifact
    ready --> archived : operator archives, no live deployment
    failed --> [*] : deleted
    archived --> [*] : artifact retention expires
    note right of ready
        IMMUTABLE from here.
        Only status and failure_reason
        may ever change.
        A new artifact is a new version.
    end note
```

### 5.2 Artifact flow across the cluster

```mermaid
flowchart LR
    CLI["nebula model register"] -- "presigned PUT" --> OS[("Object store<br/>sha256/hex<br/>immutable")]
    OS -- "initContainer pull<br/>verify sha256" --> NC1[("node-a cache<br/>/models/sha256/hex")]
    OS -- "initContainer pull<br/>verify sha256" --> NC2[("node-b cache")]
    NC1 -- "read-only mount" --> P1["worker pod 1"]
    NC1 -- "cache hit, no download" --> P2["worker pod 2 same node"]
    NC2 -- "read-only mount" --> P3["worker pod 3"]
    NC1 -. "LRU eviction by bytes<br/>never a referenced artifact" .-> EV["DaemonSet"]
```

Content addressing is what makes the cache key provably correct, deduplicates identical artifacts, and
makes rollback free — the previous version's bytes are still on the node.

---

## 6. Failure and recovery flows

### 6.1 Worker OOMKilled mid-stream

```mermaid
sequenceDiagram
    autonumber
    participant C1 as Client A streaming
    participant C2 as Client B queued
    participant G as gateway
    participant WB as worker pod-b
    participant WA as worker pod-a
    participant K as Kubernetes
    participant CT as controller

    Note over WB: OOMKilled — KV cache growth
    WB--xG: connection reset
    G->>G: classify: retryable transport error
    G--xC1: terminal error frame stream_interrupted, no finish_reason
    Note over G: NOT retried — tokens already sent<br/>partial usage still recorded
    G->>G: breaker for pod-b opens after threshold
    G->>WA: dispatch Client B — zero tokens sent, safe to retry
    WA-->>C2: streams normally

    Note over G: t+1s heartbeat gap, pod-b marked stale
    K-->>G: t+2s EndpointSlice removes pod-b, authoritative
    K-->>CT: Pod informer OOMKilled
    CT->>CT: worker_events type=oom_killed
    CT->>CT: deployment condition Degraded=True
    K->>WB: kubelet restarts container
    WB->>WB: startupProbe covers model reload
    WB-->>K: /readyz passes
    WB->>G: heartbeats resume
    G->>WB: breaker half-open single probe
    WB-->>G: success — breaker closes, back in rotation
    CT->>CT: ready_replicas restored, condition Ready=True
```

Recovery is automatic at every step. The one user-visible cost is Client A's interrupted stream, which
is inherent to streaming rather than a gap in the design ([risk R-08](./risk-register.md)).

### 6.2 Canary rollout aborting on a metric breach

```mermaid
sequenceDiagram
    autonumber
    participant CT as rollout reconciler
    participant PG as PostgreSQL
    participant PR as Prometheus
    participant G as gateways
    participant N as NATS

    CT->>PG: rollout step 1, set canary weight 10
    G->>G: route table converges, 10 percent to canary
    Note over CT: hold window 300s
    CT->>PR: query error rate and p95 per arm over the window
    PR-->>CT: baseline 0.4 percent p95 980ms<br/>canary 3.1 percent p95 1240ms
    CT->>CT: evaluate — error delta 2.7pp exceeds 1.0pp threshold
    CT->>PG: weights back to baseline 100, canary 0
    CT->>PG: state=rolled_back, abort_reason recorded,<br/>last_analysis stores both arms' observations
    CT->>N: nebula.deployment.events rollout_aborted
    Note over G: canary receives no further traffic
    Note over CT: autoscaler interlock released
```

If the window sees fewer than `min_requests`, the step does **not** pass: it extends, pauses, or fails
per the configured `on_insufficient_data` policy. Promoting on forty requests would be theatre
([risk R-13](./risk-register.md)).

### 6.3 Dependency outage decision flow

```mermaid
flowchart TB
    START{"which dependency<br/>is down?"}
    START -->|PostgreSQL| PGD["admin API 503 + Retry-After<br/>INFERENCE CONTINUES on cached routes<br/>auth from Redis with capped TTL then fail closed<br/>controller stops changing, deletes nothing unverified"]
    START -->|Redis| RDD["conservative in-process limiter, marked approximate<br/>key lookup falls through to PostgreSQL<br/>capacity snapshot rebuilt from informers<br/>NO request dropped for this reason alone"]
    START -->|NATS| NAD["routing falls back to EndpointSlice readiness<br/>usage buffers then spools to disk then sheds<br/>drops COUNTED and alerted, never silent<br/>reconcile relies on periodic resync"]
    START -->|Kubernetes API| K8D["controller and autoscaler stop and report<br/>gateways serve last-known endpoints<br/>no new deployments"]
    START -->|Object store| OSD["registry writes fail<br/>first pod on a node cannot start<br/>running pods unaffected, artifacts cached"]
    START -->|Prometheus| PRD["rollouts pause at current step, never promote blind<br/>autoscaler HOLDS, does not scale to a default"]
    START -->|all workers for a route| WKD["503 no_healthy_endpoint<br/>or Failover strategy to a declared secondary route"]

    PGD --> OK["degraded, documented, tested in Phase 16"]
    RDD --> OK
    NAD --> OK
    K8D --> OK
    OSD --> OK
    PRD --> OK
    WKD --> OK
```

The pattern across all seven: **inference degrades last**. Every branch is a Phase 16 failure test, so
these are assertions rather than intentions.

---

## 7. Database ER diagram

Abbreviated attributes — full DDL with constraints, triggers, partitions, and RLS policies is in
[data-model.md](./data-model.md).

```mermaid
erDiagram
    ORGANIZATIONS ||--o{ USERS : employs
    ORGANIZATIONS ||--o{ API_KEYS : owns
    ORGANIZATIONS ||--o{ MODELS : owns
    ORGANIZATIONS ||--o{ DEPLOYMENTS : owns
    ORGANIZATIONS ||--o{ ROUTES : owns
    ORGANIZATIONS ||--o{ USAGE_RECORDS : accrues
    ORGANIZATIONS ||--o{ AUDIT_LOGS : records
    ORGANIZATIONS }o--|| PRICING_PROFILES : "priced by"
    USERS ||--o{ API_KEYS : created
    MODELS ||--o{ MODEL_VERSIONS : "has versions"
    MODEL_VERSIONS ||--o{ MODEL_ARTIFACTS : "consists of"
    MODEL_VERSIONS ||--o{ DEPLOYMENTS : "pinned by"
    DEPLOYMENTS ||--o{ DEPLOYMENT_REVISIONS : "snapshotted as"
    DEPLOYMENTS ||--o{ AUTOSCALING_EVENTS : "decisions for"
    DEPLOYMENTS ||--o{ WORKER_EVENTS : "history for"
    ROUTES ||--o{ ROUTE_TARGETS : "splits across"
    DEPLOYMENTS ||--o{ ROUTE_TARGETS : "targeted by"
    ROUTES ||--o{ CANARY_ROLLOUTS : "rolled out on"
    ROUTES ||--o{ EXPERIMENTS : "tested on"
    ROUTING_POLICIES ||--o{ ROUTES : governs
    ROUTING_POLICIES ||--o{ DEPLOYMENTS : governs
    RATE_LIMIT_POLICIES ||--o{ API_KEYS : limits
    PRICING_PROFILES ||--o{ USAGE_RECORDS : "priced with"
    REQUESTS }o--|| USAGE_RECORDS : "rolled up into"

    ORGANIZATIONS {
        uuid id PK
        text slug UK "DNS label — used in k8s names"
        jsonb quota
        uuid pricing_profile_id FK
    }
    USERS {
        uuid id PK
        uuid org_id FK
        text email UK
        user_role role "owner admin developer viewer"
        text password_hash "argon2id, null if OIDC"
    }
    API_KEYS {
        uuid id PK
        uuid org_id FK
        char prefix UK "nbk_ plus 7 — safe to display"
        bytea key_hash "HMAC-SHA256 with pepper"
        text_array scopes
        text priority "LOW NORMAL HIGH — not client-settable"
        timestamptz revoked_at
    }
    MODELS {
        uuid id PK
        uuid org_id FK
        text name "unique per org"
        text task "chat completion embedding"
    }
    MODEL_VERSIONS {
        uuid id PK
        uuid model_id FK
        text version "unique per model"
        text format "gguf safetensors mock"
        text runtime "llamacpp vllm mock"
        bytea checksum_sha256
        integer context_window
        jsonb hardware_profile
        model_version_status status "IMMUTABLE once ready"
    }
    MODEL_ARTIFACTS {
        uuid id PK
        uuid model_version_id FK
        text path
        bytea checksum_sha256
    }
    DEPLOYMENTS {
        uuid id PK
        uuid org_id FK
        uuid model_version_id FK
        text name "unique per org"
        integer desired_replicas "written by autoscaler"
        jsonb resources
        bigint generation "spec changes only"
        bigint observed_generation "controller writes"
        deployment_state state "OBSERVED"
        integer ready_replicas "OBSERVED"
    }
    DEPLOYMENT_REVISIONS {
        uuid id PK
        uuid deployment_id FK
        integer revision UK
        jsonb spec "immutable snapshot"
        bytea spec_hash
        text reason "create update scale rollback rollout"
    }
    ROUTES {
        uuid id PK
        uuid org_id FK
        text model_name UK "what clients send"
    }
    ROUTE_TARGETS {
        uuid id PK
        uuid route_id FK
        uuid deployment_id FK
        integer weight "must sum to 100 per route"
        boolean is_baseline
    }
    CANARY_ROLLOUTS {
        uuid id PK
        uuid route_id FK
        uuid baseline_deployment_id FK
        uuid canary_deployment_id FK
        jsonb steps
        jsonb analysis "thresholds and min_requests"
        rollout_state state "one active per route"
        jsonb last_analysis "per-arm observations"
    }
    EXPERIMENTS {
        uuid id PK
        uuid route_id FK
        text bucketing "request sticky_user sticky_org"
        experiment_state state
    }
    ROUTING_POLICIES {
        uuid id PK
        text strategy "text not enum — strategies are plugins"
        jsonb config
    }
    RATE_LIMIT_POLICIES {
        uuid id PK
        integer requests_per_minute
        integer tokens_per_minute
        integer max_concurrency
        integer max_queue_depth
    }
    REQUESTS {
        uuid id PK "partitioned by created_at, no FKs"
        uuid org_id
        integer ttft_ms
        integer queue_wait_ms
        integer prompt_tokens
        integer completion_tokens
        text error_class
        text trace_id
    }
    USAGE_RECORDS {
        uuid id PK
        timestamptz bucket_hour UK
        bigint prompt_tokens
        bigint completion_tokens
        real sample_rate "for correction, not guessing"
        uuid pricing_profile_id FK
        bigint estimated_cost_micros
    }
    PRICING_PROFILES {
        uuid id PK
        integer version UK "IMMUTABLE, effective_from"
        bigint cpu_core_hour_micros
        jsonb gpu_hour_micros
    }
    NODES {
        uuid id PK
        text name UK "OBSERVED cache of k8s Node"
        jsonb capacity
        timestamptz synced_at
    }
    WORKER_EVENTS {
        uuid id PK
        uuid deployment_id FK
        text event_type "oom_killed model_load_failed etc"
        timestamptz occurred_at
    }
    AUTOSCALING_EVENTS {
        uuid id PK
        uuid deployment_id FK
        jsonb signals "full vector"
        text decision "includes suppressed_ reasons"
    }
    AUDIT_LOGS {
        uuid id PK "partitioned, INSERT and SELECT only"
        text action
        jsonb before
        jsonb after
        uuid request_id
    }
```

Three things this diagram is designed to show at a glance: **`REQUESTS` has no foreign keys** (hot-path
insert throughput, and a parent row must remain droppable); **`NODES` connects to nothing** (it is an
observed cache, not a modelled entity); and the `ROUTES → ROUTE_TARGETS → DEPLOYMENTS → MODEL_VERSIONS`
chain is what makes canary and rollback ordinary weight updates
([ADR-0009](./architecture-decisions/README.md#adr-0009)).

---

## 8. Kubernetes architecture

```mermaid
flowchart TB
    subgraph CLUSTER ["kind cluster nebula-dev — 1 control-plane, 2 workers"]
        subgraph NSSYS ["namespace: nebula-system"]
            direction LR
            GWD["Deployment gateway<br/>2+ replicas, HPA<br/>PDB 50 percent"]
            CPD["Deployment controlplane<br/>2 replicas"]
            CTD["Deployment controller<br/>2 replicas, Lease leader"]
            ASD["Deployment autoscaler<br/>2 replicas, Lease leader"]
            DSD["Deployment dashboard<br/>nginx static"]
            MIG["Job migrations<br/>Helm pre-install and pre-upgrade hook"]
            ING["Ingress or NodePort 30080"]
        end
        subgraph NSWL ["namespace: nebula-workloads"]
            direction LR
            WD1["Deployment nebula-acme-qwen-prod<br/>initContainer artifact-puller<br/>worker container<br/>startup, readiness, liveness probes"]
            WD2["Deployment nebula-acme-qwen-canary"]
            WSVC["Services, one per deployment"]
            WPVC["PVC node artifact cache"]
        end
        subgraph NSDATA ["namespace: nebula-data"]
            direction LR
            PGS["StatefulSet PostgreSQL + PVC"]
            RDS["Deployment Redis"]
            NTS["StatefulSet NATS + JetStream PVC"]
            MIN["MinIO — dev only, real S3 in prod"]
        end
        subgraph NSOBS ["namespace: nebula-observability — optional subchart"]
            direction LR
            PRM["Prometheus + ServiceMonitors"]
            OTC["OTel Collector"]
            TMP["Tempo"]
            LOK["Loki + Promtail DaemonSet"]
            GRF["Grafana, provisioned dashboards"]
        end
        subgraph RBACB ["cluster-scoped"]
            CR["ClusterRole READ ONLY<br/>nodes, namespaces get list watch"]
        end
    end

    ING --> GWD
    GWD -->|"8081 dispatch"| WD1
    GWD -->|"8081"| WD2
    GWD -->|"8082 admin proxy"| CPD
    GWD --> RDS
    CPD --> PGS
    CTD --> PGS
    ASD --> PGS
    CTD -->|"Role scoped to<br/>nebula-workloads ONLY"| WD1
    CTD --> CR
    WD1 --> NTS
    GWD --> NTS
    CTD --> NTS
    WD1 -.->|"initContainer"| MIN
    WD1 --> WPVC
    MIG --> PGS
    GWD -.-> OTC
    WD1 -.-> OTC
    OTC --> PRM
    OTC --> TMP
    OTC --> LOK
    PRM --> GRF
    ASD --> PRM
    CTD --> PRM

    classDef sys fill:#141b26,stroke:#4a90d9,color:#e6e9ef
    classDef wl fill:#1d1a14,stroke:#c08a3e,color:#e6e9ef
    classDef data fill:#0d1117,stroke:#39404d,color:#9aa4b2
    class GWD,CPD,CTD,ASD,DSD,MIG,ING sys
    class WD1,WD2,WSVC,WPVC wl
    class PGS,RDS,NTS,MIN,PRM,OTC,TMP,LOK,GRF,CR data
```

### 8.1 The boundary that matters

```
nebula-controller's write permissions:  Role in nebula-workloads  ONLY
                                        ─────────────────────────────
  deployments, services, configmaps, PDBs, PVCs   create/update/patch/delete
  pods, pods/log                                  read
  events                                          create/patch

  ClusterRole:  nodes, namespaces  →  get/list/watch  (READ ONLY)
  Role in nebula-system:  leases  →  get/create/update  (leader election)

  NOT granted, deliberately:
    secrets                  → cannot read credentials it does not own
    clusterroles/bindings    → no privilege-escalation path
    pods/exec, portforward   → no shell into a workload
    nodes (write)            → cannot cordon or taint
    deployments in nebula-system → CANNOT MODIFY NEBULA ITSELF
```

That last line removes a whole class of self-destruction bug: the controller physically cannot roll,
scale, or delete the control plane. CI asserts each negative with `kubectl auth can-i`, because an
RBAC intention nobody tested is an RBAC hope.

### 8.2 Probe strategy

Three probes answer three different questions, and conflating them is the classic failure:

| Probe | Question | Endpoint | Config | Consequence of getting it wrong |
|-------|----------|----------|--------|---------------------------------|
| `startupProbe` | has it finished loading? | `/readyz` | period 5 s, failureThreshold 60 → 5 min | too strict: a pod with a slow model load is killed forever |
| `readinessProbe` | should it receive traffic? | `/readyz` | period 5 s, threshold 3 | too loose: traffic to a pod whose model is not loaded |
| `livenessProbe` | is the process wedged? | `/livez` — **never checks dependencies** | period 10 s, threshold 3 | checking dependencies here turns a Postgres blip into a cluster-wide restart storm |

---

## 9. Service dependency graph

Solid = hard dependency (cannot serve its purpose without it). Dashed = soft (degrades, keeps working).

```mermaid
flowchart LR
    CLIENT["clients"] --> GW
    DASH["dashboard"] --> GW
    CLI2["CLI"] --> GW

    GW["nebula-gateway"] ==> WORKER["nebula-worker"]
    GW -.-> CP["nebula-controlplane"]
    GW -.-> RD[("Redis")]
    GW -.-> NQ{{"NATS"}}
    GW -.-> K8S["Kubernetes API"]

    CP ==> PG[("PostgreSQL")]
    CP -.-> RD
    CP -.-> NQ
    CP -.-> OS[("Object store")]

    CTRL["nebula-controller"] ==> PG
    CTRL ==> K8S
    CTRL -.-> NQ
    CTRL -.-> PROM["Prometheus"]
    CTRL -.-> RD

    AS["nebula-autoscaler"] ==> PG
    AS -.-> NQ
    AS -.-> PROM

    WORKER ==> ART[("artifact on<br/>node cache")]
    WORKER -.-> NQ
    WORKER -.-> OTEL["OTel Collector"]

    GW -.-> OTEL
    CP -.-> OTEL
    CTRL -.-> OTEL
    AS -.-> OTEL

    classDef hard stroke-width:3px
    classDef svc fill:#12161d,stroke:#3d4757,color:#e6e9ef
    classDef inf fill:#0d1117,stroke:#39404d,color:#9aa4b2
    class GW,CP,CTRL,AS,WORKER svc
    class PG,RD,NQ,OS,K8S,PROM,OTEL,ART inf
```

### 9.1 Dependency matrix

`H` hard · `S` soft · `—` none. Read a row as "what this component needs".

| ↓ needs → | Postgres | Redis | NATS | K8s API | Object store | Prometheus | OTel | gateway | controlplane | worker |
|-----------|----------|-------|------|---------|--------------|------------|------|---------|--------------|--------|
| **gateway** | — | S | S | S | — | S | S | — | S | **H** |
| **controlplane** | **H** | S | S | — | S | S | S | — | — | — |
| **controller** | **H** | S | S | **H** | — | S | S | — | — | — |
| **autoscaler** | **H** | — | S | — | — | S | S | — | — | — |
| **worker** | — | — | S | — | S¹ | — | S | — | — | — |
| **dashboard** | — | — | — | — | — | — | — | **H** | — | — |
| **CLI** | — | — | — | — | S | — | — | **H** | — | — |

¹ at pod startup only, via the initContainer; a running worker needs nothing from the object store.

### 9.2 Properties this graph is designed to have

1. **No cycles.** The gateway proxies to the control plane but the control plane never calls the
   gateway; the controller writes Kubernetes objects but never calls a worker.
2. **The worker depends on almost nothing.** No database, no Redis, no control plane — and those are
   *denied by NetworkPolicy*, not merely unused. The least trusted component has the smallest reach.
3. **Exactly one hard dependency sits on the token path**: gateway → worker. Everything else the
   gateway touches is soft, which is the mechanical form of axiom A8.
4. **PostgreSQL is hard for all three control-plane services and soft for none of the data plane.** A
   database outage is a change-freeze, not an outage.
5. **Nothing depends on the dashboard.** It is a leaf, so it can be removed, rebuilt, or broken without
   consequence elsewhere.
