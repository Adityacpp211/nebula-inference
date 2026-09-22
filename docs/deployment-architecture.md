# NEBULA — Deployment Architecture

**Status:** Phase 0 design. Manifests and charts land in Phase 5 (`deploy/`).
**Target for development:** kind on Docker Desktop with the WSL2 backend, CPU-only, on a laptop.
**Target for production:** any conformant Kubernetes ≥ 1.29, optionally with GPU nodes.

The same Helm chart produces both. The difference is a values file, not a different architecture —
if the dev path and the prod path diverge structurally, the dev path stops being evidence that the
prod path works.

---

## 1. Cluster topology

### 1.1 Namespaces

| Namespace | Contents | Why separate |
|-----------|----------|--------------|
| `nebula-system` | gateway, control plane, controller, autoscaler, dashboard, migration jobs | Control plane lifecycle and RBAC are entirely different from workload lifecycle. |
| `nebula-workloads` | model deployment pods (workers) and their Services | The controller's write permissions are scoped **here only**. It cannot mutate its own Deployment, which removes a whole class of self-destruction bugs. |
| `nebula-data` | PostgreSQL, Redis, NATS | Stateful dependencies with their own storage and upgrade cadence. In production these are usually external, and a namespace boundary makes that substitution a values change. |
| `nebula-observability` | Prometheus, Grafana, Loki, Tempo, OTel Collector | Optional. An operator with an existing stack disables this whole subchart. |

Optional `tenant_namespaces: true` mode places each org's workers in `nebula-wl-<org-slug>` for
stronger isolation (per-namespace ResourceQuota, LimitRange, NetworkPolicy). Single namespace with
labels is the default because it keeps the dev cluster comprehensible; the code path that computes
the target namespace is the same in both modes, so switching is configuration rather than a rewrite.

### 1.2 Dev cluster (kind)

```
kind cluster "nebula-dev"
├── nebula-dev-control-plane   (control plane + system workloads; tainted off for workers)
├── nebula-dev-worker          labels: nebula.dev/accelerator=cpu, nebula.dev/pool=inference
└── nebula-dev-worker2         labels: nebula.dev/accelerator=cpu, nebula.dev/pool=inference
```

Two worker nodes, not one: with a single node, topology spread constraints, node-loss failure
tests, and multi-node artifact caching are all untestable, and an architecture whose placement logic
is never exercised is an architecture that is wrong in a way nobody has noticed yet.

`deploy/kind/cluster.yaml` provides `extraPortMappings` (gateway 8080→30080, Grafana 3000→30300),
node labels, and a mounted host directory for the model artifact cache so a 400 MB download survives
`kind delete cluster`. Storage uses kind's built-in `local-path-provisioner` (`standard` StorageClass).

Bootstrap is one script (`scripts/dev-up.sh`, with a PowerShell twin): create cluster → install the
`nebula-data` and `nebula-observability` subcharts → run migrations → install `nebula-system` →
seed dev org and key → register the `mock` model version → print the endpoint, the API key, and a
ready-to-paste `curl`. `scripts/dev-down.sh` reverses it.

---

## 2. Workload shapes

### 2.1 Control-plane services

Common to all four Go services:

| Setting | Value | Reason |
|---------|-------|--------|
| `securityContext` | `runAsNonRoot`, uid 65532, `readOnlyRootFilesystem: true`, `allowPrivilegeEscalation: false`, all capabilities dropped, `seccompProfile: RuntimeDefault` | Distroless static images have no reason to need anything else. |
| Resources | requests set for everything; CPU limits **omitted** on the gateway, memory limits set everywhere | CPU limits cause throttling that shows up as tail latency on a request path; memory limits are the OOM protection you actually want. This is a deliberate, documented asymmetry, not an oversight. |
| Probes | `/livez` (liveness), `/readyz` (readiness), `startupProbe` on `/readyz` | Liveness never checks dependencies, so a Postgres blip cannot cause a cluster-wide restart storm. |
| `topologySpreadConstraints` | across `kubernetes.io/hostname`, `maxSkew: 1` | A single node loss must not take out all gateway replicas. |
| `PodDisruptionBudget` | `minAvailable: 1` (gateway: 50%) | Survives drains. |
| Config | ConfigMap for non-secret config, Secret for credentials, both mounted as files with a checksum annotation on the pod template | Annotation checksum means a config change actually rolls the pods instead of silently doing nothing. |
| Shutdown | `terminationGracePeriodSeconds: 60`, `preStop` sleep 5 s | The sleep covers EndpointSlice propagation, which is the real cause of "503s during deploy". |

Per-service specifics:

- **gateway** — 2+ replicas, HPA on CPU and on `nebula_inflight_requests`. The only service exposed
  outside the cluster (Ingress in production, NodePort in kind).
- **controlplane** — 2 replicas. `NetworkPolicy` accepts ingress **only** from the gateway's
  ServiceAccount.
- **controller** — `replicas: 2` with Kubernetes Lease-based leader election; the standby holds no
  locks and does no work, so failover is seconds and there is never more than one writer.
- **autoscaler** — same pattern. Independently scalable to zero to disable autoscaling cluster-wide,
  which is the operator escape hatch that makes an autoscaler safe to ship.

### 2.2 Inference workers

One Kubernetes `Deployment` per NEBULA deployment, named `nebula-<org-slug>-<deployment-name>`,
labelled:

```yaml
labels:
  app.kubernetes.io/name: nebula-worker
  app.kubernetes.io/managed-by: nebula
  nebula.dev/org: acme
  nebula.dev/deployment: qwen-prod
  nebula.dev/deployment-id: 0192f3c1-7b5a-7c3e-9a11-0e5d2b8f4a21
  nebula.dev/model-version-id: 0192f3c0-...
  nebula.dev/runtime: llamacpp
  nebula.dev/revision: "4"
annotations:
  nebula.dev/spec-hash: sha256:...
  nebula.dev/generation: "7"
```

Every object the controller creates carries an owner reference to a per-deployment `ConfigMap` that
acts as the root object, so `kubectl delete` of that root garbage-collects the whole set, and the
controller's "is this mine?" check is a label selector rather than a naming convention.

Pod structure:

```
initContainer: artifact-puller
  - reads artifact_uri + sha256 from the ConfigMap
  - if /models/sha256/<hex> exists and verifies → exit 0 (cache hit, no download)
  - else stream from object store → verify sha256 → atomic rename
  - writes a machine-readable result file the worker reads, so a checksum failure is a
    named condition rather than an opaque CrashLoopBackOff
container: worker  (nebula-worker image: FastAPI + adapter + supervised llama-server)
  ports: 8081 (worker API), 9090 (metrics)
  env: NEBULA_RUNTIME, NEBULA_MODEL_VERSION_ID, NEBULA_MAX_QUEUE_DEPTH,
       NEBULA_PARALLEL_SLOTS, NATS_URL, OTEL_EXPORTER_OTLP_ENDPOINT
  volumeMounts: /models (node artifact cache, read-only), /dev/shm (sized), emptyDir for scratch
  startupProbe:   /readyz, failureThreshold 60, period 5s   → tolerates a 5-minute model load
  readinessProbe: /readyz, period 5s, failureThreshold 3
  livenessProbe:  /livez,  period 10s, failureThreshold 3   → never checks the model
  lifecycle.preStop: POST /internal/v1/drain, then sleep
```

The three probes are three different questions and conflating them is the classic failure: the
startup probe tolerates a slow model load, the readiness probe controls traffic, the liveness probe
detects a wedged process. Using readiness thresholds for a model that takes four minutes to load
produces a pod that Kubernetes kills forever; using liveness to check the model produces a restart
loop on a transient artifact-store blip.

### 2.3 Placement: what NEBULA contributes

NEBULA does **not** schedule pods. It translates the model version's `hardware_profile` plus the
deployment's `resources` into constraints Kubernetes understands, and admits or rejects the request
against its capacity inventory *before* creating an object that could never be scheduled.

```yaml
# derived for a CPU deployment
nodeSelector:
  nebula.dev/accelerator: cpu
resources:
  requests: { cpu: "4", memory: "8Gi" }
  limits:   { memory: "10Gi" }          # request×1.25 headroom for KV cache growth
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: kubernetes.io/hostname
    whenUnsatisfiable: ScheduleAnyway   # prefer spread, never block a single-node cluster
    labelSelector: { matchLabels: { nebula.dev/deployment-id: <id> } }
```

```yaml
# derived for a GPU deployment (production; optional)
nodeSelector:
  nebula.dev/accelerator: gpu
  nvidia.com/gpu.product: NVIDIA-A100-SXM4-40GB
resources:
  requests: { cpu: "8", memory: "32Gi", nvidia.com/gpu: 1 }
  limits:   { cpu: "8", memory: "40Gi", nvidia.com/gpu: 1 }
runtimeClassName: nvidia
tolerations:
  - key: nvidia.com/gpu
    operator: Exists
    effect: NoSchedule
```

Capacity admission answers "can 3 replicas needing 6 GiB each fit?" from the node inventory before
creating the Deployment, and returns `422` with the observed headroom when they cannot — instead of
silently producing `Pending` pods and a deployment that never becomes ready for reasons the user
must go read `kubectl describe` to discover.

What NEBULA deliberately does not build: a scheduler plugin, a scheduler extender, custom bin-packing,
or preemption. See [ADR-0005](./architecture-decisions/0005-cooperate-with-kubernetes.md).

---

## 3. RBAC

Least privilege, split across a namespaced Role and a minimal ClusterRole. The controller is the
only component with cluster write permissions, and even then only in `nebula-workloads`.

**`nebula-controller`** — `Role` in `nebula-workloads`:

| Resources | Verbs |
|-----------|-------|
| `deployments` (apps) | get, list, watch, create, update, patch, delete |
| `services`, `configmaps` | get, list, watch, create, update, patch, delete |
| `poddisruptionbudgets` (policy) | get, list, watch, create, update, patch, delete |
| `persistentvolumeclaims` | get, list, watch, create, delete |
| `pods` | get, list, watch |
| `pods/log` | get |
| `events` | create, patch |

`ClusterRole` (read-only, cluster-scoped facts only): `nodes` get/list/watch, `namespaces`
get/list/watch, `nodes/metrics` get. Plus a `Role` in `nebula-system` for `leases`
(get/create/update) for leader election.

Explicitly **not** granted, and stated so reviewers can check: `secrets` (the controller never reads
credentials it does not own), `clusterroles`/`rolebindings` (no privilege escalation path),
`pods/exec` or `pods/portforward` (no shell into workloads), `nodes` write (no cordoning), anything
in `kube-system`, and `deployments` write in `nebula-system` (it cannot modify NEBULA itself).

**`nebula-controlplane`**, **`nebula-gateway`**: no Kubernetes API access at all beyond
`EndpointSlice` get/list/watch in `nebula-workloads` for the gateway's routing view. **`nebula-autoscaler`**:
`leases` in `nebula-system`, and read-only `pods`/`deployments` in `nebula-workloads` — it writes
desired state to PostgreSQL, not to Kubernetes.

Every ServiceAccount sets `automountServiceAccountToken: false` except where the API is actually
used.

---

## 4. Network policy

Default deny ingress and egress in all four namespaces, then explicit allows. The matrix is the
security model, so it is written down rather than implied:

| From | To | Ports | Purpose |
|------|----|-------|---------|
| Ingress controller | gateway | 8080 | public traffic |
| gateway | controlplane | 8082 | admin proxy |
| gateway | workers (`nebula-workloads`) | 8081 | inference dispatch |
| gateway, controlplane, controller, autoscaler | postgres / redis / nats | 5432 / 6379 / 4222 | dependencies |
| workers | nats | 4222 | heartbeats, lifecycle events |
| all services | otel-collector | 4317 | traces and metrics |
| prometheus | all `/metrics` ports | 9090 | scraping |
| — | workers → PostgreSQL | — | **denied.** A worker has no business touching the database. |
| — | workers → workers | — | **denied.** No worker-to-worker traffic exists in the design. |
| — | anything → controlplane from outside `nebula-system` | — | **denied.** |

Egress to the object store and to the DNS service is allowed; general internet egress from workers
is denied, so a model artifact can only come from the configured store.

---

## 5. Model artifact storage

Model weights are large, immutable, and content-addressed — which makes them a poor fit for
Kubernetes-native storage and a good fit for object storage plus a node-local cache.

```
                    ┌──────────────────────────────────────────┐
  nebula model      │ Object store (S3 API)                    │
  register ────────►│   s3://nebula-models/sha256/<hex>        │
   (direct PUT,     │   immutable, content-addressed           │
    presigned)      └───────────────┬──────────────────────────┘
                                    │ initContainer pull + verify
                    ┌───────────────▼──────────────────────────┐
                    │ Node artifact cache (PVC or hostPath)    │
                    │   /models/sha256/<hex>                   │
                    │   shared by every pod on that node       │
                    └───────────────┬──────────────────────────┘
                                    │ read-only mount
                              worker container
```

Why content-addressed: the cache key is the checksum, so a cache hit is provably the right bytes; two
deployments of the same version on one node download once; and rollback never re-downloads, because
the old version's artifact is still cached.

- **Dev**: MinIO in `nebula-data` (real S3 semantics on a laptop) and a hostPath-backed cache mapped
  to a host directory, so `kind delete cluster` does not cost another download.
- **Production**: any S3-compatible store. The cache is a per-node PVC, or a ReadOnlyMany volume
  where the storage class supports it.
- The `ArtifactStore` interface has `s3` and `file` implementations; `file` exists for air-gapped
  installs and for tests, not as a shortcut.
- Cache eviction is LRU by total bytes (`NEBULA_ARTIFACT_CACHE_MAX_BYTES`), run by a small DaemonSet,
  never evicting an artifact referenced by a running pod.

Why not bake weights into container images: a 4 GB image layer makes every deployment a registry
push, kills startup time, and couples the model lifecycle to the image build. Why not a single RWX
PVC: most on-prem storage classes cannot do ReadWriteMany, and a shared volume becomes a throughput
bottleneck exactly when many pods start at once.

---

## 6. Helm chart layout

```
deploy/helm/nebula/
  Chart.yaml                      # umbrella; subcharts as conditional dependencies
  values.yaml                     # production-shaped defaults
  values-dev.yaml                 # kind: 1 replica each, no TLS, MinIO, mock runtime enabled
  values-gpu.yaml                 # GPU node pool, device plugin, DCGM, vllm runtime
  templates/
    _helpers.tpl                  # names, labels, selector labels — one definition, reused
    gateway/ controlplane/ controller/ autoscaler/ dashboard/
    migrations-job.yaml           # pre-install & pre-upgrade hook, must succeed before rollout
    rbac/  networkpolicy/  ingress.yaml  servicemonitor.yaml
    NOTES.txt                     # prints endpoint, next commands, how to get the dev API key
  charts/
    nebula-data/                  # postgres, redis, nats (condition: nebula-data.enabled)
    nebula-observability/         # prometheus, grafana, loki, tempo, otel-collector
```

Chart rules, learned the hard way and therefore written down: `helm template` must succeed for
`values.yaml`, `values-dev.yaml`, and `values-gpu.yaml` in CI, and the rendered output is checked
against `kubeconform` plus a policy test (`conftest`) asserting that every pod sets resource
requests, non-root, read-only root filesystem, and all three probes. Image tags are always explicit
digests or versions — never `latest`. Every secret value has no default, so an install that would
create a well-known credential fails instead of succeeding insecurely.

Raw manifests under `deploy/kubernetes/` are generated from the chart (`helm template`) for operators
who do not use Helm; they are build output, not a second source of truth to keep in sync by hand.

---

## 7. Ingress and TLS

- Dev: NodePort `30080` via kind's `extraPortMappings`; plain HTTP; documented as dev-only.
- Production: `Ingress` with `cert-manager`, HTTP→HTTPS redirect, HSTS, TLS 1.2+.
- Streaming requires ingress settings that are easy to get wrong and silently ruin SSE: response
  buffering off, read timeout ≥ the maximum request deadline, HTTP/1.1 keep-alive. The chart sets
  these annotations for nginx and documents the equivalents for Traefik and HAProxy.
- The gateway also emits `X-Accel-Buffering: no` per response, so a proxy nobody told us about still
  streams.
- Internal mTLS is supported via cert-manager-issued certs and a config flag. Not required, and no
  service mesh is required (see non-goals).

---

## 8. GPU support (optional, production)

GPU is an opt-in capability, not a requirement. Every architectural decision above holds on a
CPU-only laptop, which is the constraint that keeps this project actually runnable.

Requirements when enabled: NVIDIA driver on the node, `nvidia-device-plugin` DaemonSet,
`RuntimeClass: nvidia`, `dcgm-exporter` for utilization and memory metrics, node labels
(`nebula.dev/accelerator=gpu`, `nvidia.com/gpu.product`).

What changes: `hardware_profile.requires_gpu` drives GPU-aware placement constraints; the scheduler's
admission checks VRAM against `nvidia.com/gpu` capacity and the version's `min_vram_mib`; the
autoscaler's GPU-utilization signal activates; `CostAware` routing gets meaningful price differences;
the `vllm` runtime adapter becomes usable.

What is honestly untestable in the current environment: the target laptop has no NVIDIA GPU, so GPU
paths are **designed and implemented behind config but verified only by unit tests with a fake
capacity source**. This is recorded as risk R-07 and stated in the README rather than glossed over —
claiming verified GPU scheduling without a GPU would violate axiom A6.

---

## 9. Configuration model

Precedence: defaults in code → config file → environment variables → command-line flags. One
`packages/config` loader with struct tags, used by every service, so precedence is identical
everywhere.

- Every service validates its full configuration at startup and **exits non-zero with a complete
  list of problems** rather than failing on the first one at 3 a.m. under load.
- `NEBULA_ENV` ∈ `dev|staging|production`. In `production`: the `mock` runtime refuses to start, dev
  seeding is disabled, debug endpoints are off, and any default credential is a fatal error.
- Secrets arrive as files (mounted Secret) or environment variables, never as flags (flags leak into
  `ps` and into crash dumps).
- `GET /healthz` reports the effective configuration with secret values redacted, so "what is this
  pod actually running with" is answerable without a shell.

---

## 10. CI and image supply chain

GitHub Actions, four workflows:

| Workflow | Trigger | Does |
|----------|---------|------|
| `ci.yaml` | PR, push | `go build ./...`, `go vet`, `golangci-lint`, `go test -race -cover`, Python `ruff` + `mypy` + `pytest`, dashboard `tsc` + `eslint` + `vitest`, migration up/down/up, OpenAPI lint + codegen-drift check + `oasdiff` breaking-change gate, `helm template` + `kubeconform` + `conftest` |
| `integration.yaml` | PR, nightly | `docker compose` for Postgres/Redis/NATS (or testcontainers-go), integration + API + controller tests against the `mock` runtime |
| `e2e.yaml` | nightly, release | kind cluster in CI → install chart → register mock model → deploy → inference → scale → rollout → rollback → kill pod → assert recovery. This is the executable form of the Definition of Done. |
| `release.yaml` | tag | multi-arch images (distroless static for Go, slim for Python), SBOM (`syft`), vulnerability scan (`trivy`, fails on High+ with an allowlist that requires a justification comment), image signing (`cosign`), CLI binaries for linux/darwin/windows × amd64/arm64, Helm chart published as an OCI artifact |

Images: Go services build `FROM scratch`/distroless static, non-root, with `CGO_ENABLED=0` and
version/SHA stamped via ldflags. The Python worker uses a slim base with a pinned lockfile
(`uv`/`pip-tools` hashes) and a multi-stage build so compilers do not ship to production. No image
runs as root; no image contains a shell in the final stage except the Python worker, where the
supervised `llama-server` child process makes it unavoidable — noted as accepted risk rather than
silently accepted.
