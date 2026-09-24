# NEBULA inference worker

The process that actually runs a model. One replica serves exactly one model version,
exposes the internal worker API from [api.md §6](../../docs/api.md#6-internal-worker-api), and knows nothing
about routing, tenants or the registry — a router decides *which* worker; this decides
*whether it can* and *how fast*.

Phase 3 delivers the engine-neutral contract and its first two adapters. Intelligent
routing is Phase 6 and deliberately absent here.

```
client ──HTTP──► nebula_worker (FastAPI)
                   admission queue  ─ bounded, priority-ordered, deadline-aware
                   InferenceRuntime ─ the abstraction
                      ├─ llamacpp ──► llama-server (child process, loopback HTTP)
                      └─ mock ─────── declared stub, no engine
```

## The abstraction

`runtimes/base.py` is the whole contract: eight methods (`load`, `unload`, `generate`,
`stream`, `cancel`, `health`, `metrics`, `capabilities`), frozen request/result
dataclasses, and typed errors that each carry a `code` and a `retryable` flag so a
caller can decide what to do without string-matching a message.

It is a `typing.Protocol`, not a base class. An adapter conforms by having the right
shape, which means a vLLM adapter — or an out-of-tree engine someone else writes —
does not have to import anything from NEBULA to be usable by it.

The abstraction is only worth something if it is *load-bearing*, so it is verified
rather than asserted: `tests/runtime_conformance.py` is a single suite that both
adapters run unchanged. A subclass supplies a `runtime` fixture, a `spec`, and
nothing else. No test is skipped or overridden for either engine. The seven
obligations it enforces come from [api.md §7](../../docs/api.md#7-inferenceruntime-interface):

| # | Obligation |
|---|---|
| 1 | Cancellation stops compute and frees the slot, it does not merely stop delivery |
| 2 | Time-to-first-token is measured at the first token, never derived at the end |
| 3 | A deadline aborts the request and is reported as `deadline`, distinctly from `cancel` |
| 4 | Token counts come from the engine's tokenizer, never from an estimate |
| 5 | Load failures are typed, and a non-retryable failure is never retried |
| 6 | `livez` (process alive) and `readyz` (model ready) answer different questions |
| 7 | Shutdown drains: readiness fails first, in-flight work finishes, then the model unloads |

## The two adapters

**`runtimes/llamacpp.py`** supervises `llama-server` as a child process and talks to
it over loopback HTTP. It does not use Python bindings, and
[ADR-0015](../../docs/architecture-decisions/README.md#adr-0015) explains
why at length: an engine segfault becomes a child exit the worker reports as unready
rather than a dead worker; generation runs off the GIL; and cancellation genuinely
stops compute, because `llama-server` abandons a generation when its HTTP client
disconnects, which a binding call would not let us do.

It binds the engine to `127.0.0.1` on an OS-assigned port, verifies the artifact's
SHA-256 *before* starting the engine, maps engine startup failures onto the typed
errors, and restarts a crashed child a bounded number of times.

**`runtimes/mock.py`** is a declared development stub and the reference
implementation of the contract. Tokens/sec, load duration, slot count and resident
bytes are all numbers you set; so are the failure injections (`load_error`,
`load_stall_ms`, `stall_before_first_token_ms`, `fail_after_tokens`, `process_dead`).
It reports `declared_stub: true` and `generates_real_tokens: false` in its load
metadata, and it refuses to start when `NEBULA_ENV=production` — as does
configuration validation, and as does the fact that the mock container image ships no
engine binary. Three independent refusals for one stub, because a stub that reaches
production is a service quietly returning invented answers.

## Endpoints

Everything is under `/internal/` or is a probe: none of this is reachable by a
customer, and there is no authentication here because a NetworkPolicy is the boundary
([security-boundaries.md](../../docs/security-boundaries.md)).

| Method | Path | Purpose |
|---|---|---|
| POST | `/internal/v1/generate` | Synchronous generation |
| POST | `/internal/v1/generate/stream` | SSE token stream |
| POST | `/internal/v1/cancel` | Cancel by request id |
| GET | `/internal/v1/state` | Slots, queue depth, in-flight, model, engine |
| POST | `/internal/v1/drain` | Begin draining without terminating |
| GET | `/livez` | Process is alive — never gated on the model |
| GET | `/readyz` | Model is loaded and the worker will accept work |
| GET | `/healthz` | Aggregate, for humans |
| GET | `/metrics` | Prometheus |

Request context is carried by headers, not the body: `X-Request-Id`, `traceparent`,
`X-Nebula-Deadline`, `X-Nebula-Priority`, `X-Nebula-Model-Version`.

`X-Nebula-Deadline` is **absolute Unix milliseconds**, never a duration. A duration is
re-derived at every hop, which silently grants each hop a fresh budget; an absolute
deadline cannot be renewed by accident. Malformed values are rejected rather than
defaulted, including both units mistakes — a value that looks like seconds, and one
more than 24 hours out.

A model-version assertion that disagrees with what this replica serves is a `409`.
That exists for the rollout window, when a router's view of the fleet is briefly
stale: answering with the wrong model's tokens would be a correctness bug nobody
would ever see.

## Running it

```bash
cd workers/inference
pip install -e '.[dev]'
```

Mock runtime, no engine required:

```bash
NEBULA_WORKER_RUNTIME=mock \
NEBULA_WORKER_MODEL_VERSION=dev:mock \
python -m nebula_worker.main
```

```bash
curl -N -X POST localhost:8090/internal/v1/generate/stream \
  -H 'content-type: application/json' \
  -H "X-Request-Id: $(uuidgen)" \
  -H "X-Nebula-Deadline: $(( $(date +%s) * 1000 + 30000 ))" \
  -d '{"prompt":"hello","max_tokens":32}'
```

Real engine:

```bash
NEBULA_WORKER_RUNTIME=llamacpp \
NEBULA_LLAMA_SERVER_BIN=/path/to/llama-server \
NEBULA_WORKER_MODEL_VERSION=qwen2.5:0.5b-q4 \
NEBULA_WORKER_MODEL_PATH=/models/qwen2.5-0.5b-instruct-q4_k_m.gguf \
NEBULA_WORKER_MODEL_SHA256=<sha256 of that file> \
python -m nebula_worker.main
```

### Configuration

| Variable | Default | Notes |
|---|---|---|
| `NEBULA_ENV` | `dev` | `production` refuses the mock runtime and requires strict headers |
| `NEBULA_WORKER_HOST` / `_PORT` | `0.0.0.0` / `8090` | |
| `NEBULA_WORKER_RUNTIME` | `mock` | `mock` or `llamacpp` |
| `NEBULA_WORKER_MODEL_VERSION` | — | **Required.** What `X-Nebula-Model-Version` is checked against |
| `NEBULA_WORKER_MODEL_PATH` | — | Required for `llamacpp` |
| `NEBULA_WORKER_MODEL_SHA256` | — | Verified before the engine starts; a mismatch is never retried |
| `NEBULA_WORKER_CONTEXT_WINDOW` | `4096` | |
| `NEBULA_WORKER_SLOTS` | adapter's | A ceiling; the smaller of this and the adapter's declaration wins |
| `NEBULA_WORKER_MAX_QUEUE_DEPTH` | `32` | Beyond this: `429` with `Retry-After` |
| `NEBULA_WORKER_DEFAULT_TIMEOUT_S` | `60` | Only used when a request has no deadline and strict headers are off |
| `NEBULA_WORKER_STRICT_HEADERS` | `false` | Required `true` in production |
| `NEBULA_WORKER_DRAIN_DELAY_S` | `5` | Fail readiness this long before refusing traffic, so EndpointSlices propagate |
| `NEBULA_WORKER_SHUTDOWN_GRACE_S` | `25` | |
| `NEBULA_LOG_LEVEL` | `info` | |
| `NEBULA_LLAMA_SERVER_BIN` | — | Convenience passthrough into the runtime config |
| `NEBULA_WORKER_THREADS` | engine's | Convenience passthrough |
| `NEBULA_WORKER_RUNTIME_CONFIG` | `{}` | JSON, adapter-specific; see `LlamaCppConfig` / `MockConfig` |

Configuration errors report **every** problem at once and exit `2`. Fixing one
misconfiguration per restart is a bad way to spend a deployment.

## Tests

```bash
make worker-test             # fast suite, no engine needed
make worker-test-integration # + the real engine and a real model
```

The fast suite needs nothing installed beyond the dev extra. The integration suite is
marked `integration` and needs two paths:

```bash
export NEBULA_LLAMA_SERVER_BIN=/path/to/llama-server
export NEBULA_TEST_MODEL_PATH=/path/to/nebula-tiny.gguf
export NEBULA_TEST_MODEL_VERSION=nebula-tiny:fixture
```

`tests/test_app.py` runs against a **real uvicorn server on an ephemeral port**, not
`httpx.ASGITransport`. That is not fussiness: the ASGI transport buffers a streaming
response, so a test that occupies a slot by reading a stream slowly does not occupy
anything. Three tests here — the three that mattered most, about saturation and slot
accounting — passed vacuously until this changed.

### The fixture model

`tests/test_real_model.py` is the phase's exit criterion: client → worker → model →
streamed tokens, asserting on the *content* of real output and on
`runtime.name == "llamacpp"`, so a silent fallback to the stub fails the suite rather
than passing it.

That needs a real GGUF. Rather than depend on a download, the fixture is **trained
locally** by `tools/make_tiny_model.py`:

```bash
python tools/make_tiny_model.py --out /path/to/nebula-tiny.gguf
```

It trains a 2-layer, 128-dimension LLaMA-architecture model (401,280 parameters,
1.6 MB, a 285-token vocabulary) to memorise one paragraph about this worker, then
writes a GGUF that upstream `llama-server` loads unmodified. It fails loudly if
next-token accuracy is below 0.95, because a fixture that loads cleanly and emits
confident garbage is the worst possible fixture. See
[ADR-0028](../../docs/architecture-decisions/0028-locally-trained-test-fixture-model.md)
for why this is trained rather than downloaded, and for the one caveat it carries.

For development against something that can hold a conversation, the reference model is
Qwen2.5-0.5B-Instruct-Q4_K_M. Nothing in the code names it: it is a path in an
environment variable, which is the point of the abstraction.

## Container images

Two images, from one Dockerfile, because the variants differ only in whether an
engine binary is present:

```bash
docker build -f deploy/docker/Dockerfile.worker --target mock     -t nebula/worker-mock:dev     .
docker build -f deploy/docker/Dockerfile.worker --target llamacpp -t nebula/worker-llamacpp:dev .
```

The mock image contains no engine at all. One image selecting the stub by environment
variable would put the stub one variable away from production; this way it is a
different image name.

The llamacpp image pins the engine to a llama.cpp tag rather than tracking master. A
moving engine is indistinguishable from a model regression when output changes.

Neither image is distroless: Python needs an interpreter and the llamacpp variant
needs to fork a child. That is a real hardening regression relative to the Go
services and is recorded in ADR-0015 rather than glossed over.

## What is deliberately not here

- **Routing.** A worker answers for itself. Phase 6 decides which worker.
- **Authentication.** The boundary is a NetworkPolicy; adding a second one here would
  imply this port is safe to expose, which it is not.
- **Prompt logging.** Prompts and completions are never logged, at any level. Token
  *counts* and timings are. See
  [security-boundaries.md](../../docs/security-boundaries.md).
- **Batching across requests.** `llama-server`'s slots already do continuous batching;
  a second scheduler above it would fight the first.
