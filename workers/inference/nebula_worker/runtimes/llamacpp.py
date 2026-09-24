"""The ``llamacpp`` runtime: supervises the upstream ``llama-server`` binary.

Supervision rather than in-process bindings, per ADR-0015. Three reasons, and the
third is the one that matters most in production:

* It inherits llama.cpp's continuous batching, slot management and KV-cache
  handling — work we should not reimplement.
* Generation runs off the Python GIL, so the worker's own HTTP server, probes and
  cancellation path stay responsive while tokens are being produced. With in-process
  bindings, a busy model makes the worker look unhealthy.
* An engine crash is an event this adapter observes and reports, not a worker crash.
  A segfault in C++ inside our own process would take the probes down with it, and
  Kubernetes would restart the pod having learned nothing.

The cost is that child lifecycle is our problem: start, readiness, crash detection,
restart, and reaping. That is what most of this file is.

Cancellation deserves a note because it is the obligation adapters most often fake.
``llama-server`` stops generating when its client disconnects, so cancelling here
means closing the HTTP response we are streaming from. That genuinely stops
computation and frees the slot, which is what obligation 3 requires — as opposed to
stopping our own iteration and leaving the engine burning CPU on tokens nobody will
read.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import hashlib
import json
import os
import socket
import time
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

from .base import (
    ArtifactMissing,
    Cancelled,
    ChecksumMismatch,
    DeadlineExceeded,
    EngineUnavailable,
    FinishReason,
    GenerationFailed,
    GenerationRequest,
    GenerationResult,
    InsufficientMemory,
    LoadResult,
    ModelNotLoaded,
    ModelSpec,
    ModelVersionMismatch,
    RuntimeCapabilities,
    RuntimeHealth,
    RuntimeInfo,
    RuntimeMetrics,
    Timing,
    TokenChunk,
    UnsupportedQuantization,
    Usage,
)


@dataclass(slots=True)
class LlamaCppConfig:
    """How to launch and talk to the engine."""

    #: Path to the upstream ``llama-server``. Required: this adapter never guesses a
    #: binary location, because silently finding the wrong build is worse than
    #: failing to start.
    server_bin: str = ""
    host: str = "127.0.0.1"
    #: 0 asks the OS for a free port. The engine is loopback-only and never exposed;
    #: a fixed port would collide when two workers share a node in development.
    port: int = 0
    n_ctx: int = 4096
    #: Server slots. This is the adapter's parallelism, and the number the gateway's
    #: capacity model reads through ``capabilities().parallel_slots``.
    n_parallel: int = 2
    n_threads: int = 0
    n_gpu_layers: int = 0
    seed: int | None = None
    extra_args: Sequence[str] = ()
    startup_timeout_s: float = 180.0
    shutdown_timeout_s: float = 10.0
    #: Verify the artifact's digest before handing it to the engine. On by default:
    #: the registry recorded a checksum precisely so something would check it.
    verify_checksum: bool = True
    #: Bounded restarts on an unexpected child exit. Bounded, not infinite: a model
    #: that cannot start should leave the pod unready so Kubernetes can reschedule it,
    #: rather than restarting forever and looking alive.
    max_restarts: int = 3
    restart_backoff_s: float = 2.0

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> LlamaCppConfig:
        known = {f.name for f in dataclasses.fields(cls)}
        return cls(**{k: v for k, v in raw.items() if k in known})


def _free_port(host: str) -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, 0))
        return int(s.getsockname()[1])


def _classify_startup_failure(stderr: str, path: Path) -> Exception:
    """Turn the engine's exit output into a typed error.

    The distinction that matters is retryable versus not: 'this artifact is corrupt'
    must not be retried on every replica, while 'this node had no memory' should be.
    Pattern matching on engine output is fragile, so anything unrecognised becomes
    ``EngineUnavailable`` — retryable, which is the safe default for an unknown
    failure — rather than being guessed into a terminal class.
    """
    lowered = stderr.lower()
    if not path.exists():
        return ArtifactMissing(f"llamacpp: artifact {path} does not exist")
    if any(s in lowered for s in ("failed to allocate", "out of memory", "cannot allocate", "oom")):
        return InsufficientMemory(
            "llamacpp: the engine could not allocate memory for this model",
            detail={"stderr_tail": stderr[-2000:]},
        )
    if any(
        s in lowered
        for s in (
            "unknown model architecture",
            "unsupported model",
            "unknown ftype",
            "unsupported quantization",
        )
    ):
        return UnsupportedQuantization(
            "llamacpp: this build cannot read that model format or quantization",
            detail={"stderr_tail": stderr[-2000:]},
        )
    if any(s in lowered for s in ("no such file", "failed to open", "cannot open")):
        return ArtifactMissing(
            f"llamacpp: the engine could not read {path}",
            detail={"stderr_tail": stderr[-2000:]},
        )
    return EngineUnavailable(
        "llamacpp: the engine exited during startup",
        detail={"stderr_tail": stderr[-2000:]},
    )


async def _sha256_file(path: Path) -> str:
    """Hash the artifact in a worker thread, streaming it.

    Streamed because a model is gigabytes and reading one into memory to hash it
    would double the resident footprint at exactly the moment the engine is about to
    need all of it. In a thread because this is seconds of blocking CPU, and the
    event loop still has probes to answer.
    """

    def _hash() -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(4 * 1024 * 1024), b""):
                digest.update(block)
        return digest.hexdigest()

    return await asyncio.to_thread(_hash)


@dataclass(slots=True)
class _InFlight:
    request_id: str
    cancel: asyncio.Event = field(default_factory=asyncio.Event)


class LlamaCppRuntime:
    """Adapter over a supervised ``llama-server`` child process."""

    name = "llamacpp"

    def __init__(self, config: LlamaCppConfig | None = None) -> None:
        self._config = config or LlamaCppConfig()
        self.version = "unknown"
        self._proc: asyncio.subprocess.Process | None = None
        self._client: httpx.AsyncClient | None = None
        self._base_url = ""
        self._spec: ModelSpec | None = None
        self._ready = False
        self._stderr_tail: list[str] = []
        self._stderr_task: asyncio.Task[None] | None = None
        self._monitor_task: asyncio.Task[None] | None = None
        self._in_flight: dict[str, _InFlight] = {}
        self._restarts = 0
        self._tokens_generated = 0
        self._tokens_prompt = 0
        self._slots_total = 0
        self._stopping = False

    @classmethod
    def build(cls, spec_config: Mapping[str, Any], *, env: str) -> LlamaCppRuntime:
        del env  # a real engine is allowed in every environment
        cfg = LlamaCppConfig.from_mapping(spec_config)
        if not cfg.server_bin:
            cfg.server_bin = os.environ.get("NEBULA_LLAMA_SERVER_BIN", "")
        return cls(cfg)

    # -- lifecycle ---------------------------------------------------------

    async def load(self, spec: ModelSpec) -> LoadResult:
        # Idempotent (obligation 1): the same version, with a live and ready engine,
        # is a no-op. A controller retrying a load must not restart a serving engine.
        if (
            self._spec is not None
            and self._spec.model_version == spec.model_version
            and self._proc is not None
            and self._proc.returncode is None
            and self._ready
        ):
            return LoadResult(
                model_version=spec.model_version,
                load_duration_ms=0,
                resident_bytes=self._artifact_bytes(spec),
                context_window=self._config.n_ctx,
                slots=self._slots_total or self._config.n_parallel,
                engine_version=self.version,
                already_loaded=True,
            )

        # A different version replaces the current one. Stop first: two engines on one
        # node would double the memory footprint the scheduler budgeted for.
        if self._proc is not None:
            await self.unload()

        if not self._config.server_bin:
            raise EngineUnavailable(
                "llamacpp: no server binary configured; set NEBULA_LLAMA_SERVER_BIN "
                "or runtime_config.server_bin"
            )
        # Existence checks run in a thread: on a network or fuse-mounted volume — which
        # is exactly where model artifacts live — a stat can block for seconds, and the
        # event loop still has probes to answer while a load is in progress.
        binary = Path(self._config.server_bin)
        if not await asyncio.to_thread(binary.exists):
            raise EngineUnavailable(f"llamacpp: server binary {binary} does not exist")

        artifact = Path(spec.artifact_path)
        if not await asyncio.to_thread(artifact.exists):
            raise ArtifactMissing(f"llamacpp: artifact {artifact} does not exist")

        started = time.perf_counter()

        if self._config.verify_checksum and spec.checksum_sha256:
            actual = await _sha256_file(artifact)
            if actual.lower() != spec.checksum_sha256.lower():
                # Terminal. The bytes are wrong, so every replica will agree, and the
                # engine is never handed an artifact whose identity is in doubt.
                raise ChecksumMismatch(
                    "llamacpp: artifact digest does not match the registry",
                    detail={"expected": spec.checksum_sha256, "actual": actual},
                )

        port = self._config.port or _free_port(self._config.host)
        self._base_url = f"http://{self._config.host}:{port}"
        argv = self._argv(artifact, port, spec)

        self._stderr_tail = []
        self._stopping = False
        self._proc = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            # Its own process group, so a signal we send to the worker does not race
            # the engine's own shutdown, and so we can reap it deterministically.
            start_new_session=True,
        )
        self._stderr_task = asyncio.create_task(self._drain_output())

        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            # No total timeout: a long generation is not a stalled connection. The
            # deadline the caller supplied is what bounds a request, enforced per
            # request below, because only the caller knows how long its answer is
            # worth waiting for.
            timeout=httpx.Timeout(None, connect=10.0),
            limits=httpx.Limits(max_connections=max(4, self._config.n_parallel * 2)),
        )

        await self._await_ready(artifact)
        await self._read_props()

        self._spec = spec
        self._ready = True
        self._monitor_task = asyncio.create_task(self._monitor())
        return LoadResult(
            model_version=spec.model_version,
            load_duration_ms=int((time.perf_counter() - started) * 1000),
            resident_bytes=self._artifact_bytes(spec),
            context_window=self._config.n_ctx,
            slots=self._slots_total or self._config.n_parallel,
            engine_version=self.version,
            metadata={"base_url": self._base_url, "argv": argv},
        )

    def _argv(self, artifact: Path, port: int, spec: ModelSpec) -> list[str]:
        argv = [
            self._config.server_bin,
            "--model",
            str(artifact),
            "--host",
            self._config.host,
            "--port",
            str(port),
            "--ctx-size",
            str(self._config.n_ctx),
            "--parallel",
            str(self._config.n_parallel),
            "--n-gpu-layers",
            str(self._config.n_gpu_layers),
            # The engine's own name for this model, so its logs and /props agree with
            # ours rather than showing a filesystem path.
            "--alias",
            spec.model_version,
            # Metrics and slots are how this adapter answers metrics() with observed
            # numbers instead of its own guesses.
            "--metrics",
            "--slots",
            # No browser UI in a server process: it is surface area with no caller.
            "--no-webui",
        ]
        if self._config.n_threads:
            argv += ["--threads", str(self._config.n_threads)]
        if self._config.seed is not None:
            argv += ["--seed", str(self._config.seed)]
        argv += list(self._config.extra_args)
        return argv

    @staticmethod
    def _artifact_bytes(spec: ModelSpec) -> int:
        """Resident bytes, approximated by the artifact's size on disk.

        Named as the approximation it is. The engine does not report its resident set,
        and a memory-mapped GGUF's true footprint depends on how much has been touched;
        the file size is the one number that is both available and stable, and it is
        the right order of magnitude for a scheduler's headroom check.
        """
        try:
            return Path(spec.artifact_path).stat().st_size
        except OSError:
            return 0

    async def _drain_output(self) -> None:
        """Keep the child's output flowing and retain a tail for diagnostics.

        Not optional: a child whose stdout pipe fills up blocks forever, and 'the
        model hangs after a few minutes' is a miserable thing to debug. The tail is
        bounded so a chatty engine cannot grow the worker's memory.
        """
        proc = self._proc
        if proc is None or proc.stdout is None:
            return
        with contextlib.suppress(asyncio.CancelledError):
            while True:
                line = await proc.stdout.readline()
                if not line:
                    return
                self._stderr_tail.append(line.decode("utf-8", "replace").rstrip())
                if len(self._stderr_tail) > 200:
                    del self._stderr_tail[:100]

    async def _await_ready(self, artifact: Path) -> None:
        """Poll /health until the model is loaded, or fail with a typed error."""
        assert self._client is not None
        deadline = time.monotonic() + self._config.startup_timeout_s
        while time.monotonic() < deadline:
            proc = self._proc
            if proc is None or proc.returncode is not None:
                await self._reap()
                raise _classify_startup_failure("\n".join(self._stderr_tail), artifact)
            try:
                resp = await self._client.get("/health", timeout=2.0)
                if resp.status_code == 200:
                    return
            except httpx.HTTPError:
                pass  # not listening yet; that is the normal case early on
            await asyncio.sleep(0.2)

        await self._terminate()
        raise EngineUnavailable(
            f"llamacpp: the engine did not become ready within "
            f"{self._config.startup_timeout_s:.0f}s",
            detail={"stderr_tail": "\n".join(self._stderr_tail[-40:])},
        )

    async def _read_props(self) -> None:
        """Learn the engine's own view of itself: version and slot count."""
        assert self._client is not None
        with contextlib.suppress(httpx.HTTPError, ValueError, KeyError):
            resp = await self._client.get("/props", timeout=5.0)
            if resp.status_code == 200:
                props = resp.json()
                self._slots_total = int(props.get("total_slots") or self._config.n_parallel)
                build = props.get("build_info") or props.get("version")
                if build:
                    self.version = str(build)
        if self.version == "unknown":
            # Fall back to the startup banner, which carries the build number.
            for line in self._stderr_tail:
                if "version:" in line:
                    self.version = line.split("version:", 1)[1].strip().split()[0]
                    break
        if not self._slots_total:
            self._slots_total = self._config.n_parallel

    async def _monitor(self) -> None:
        """Watch for an unexpected child exit and restart a bounded number of times.

        An expected exit (``unload``) sets ``_stopping`` first, so a deliberate stop is
        never mistaken for a crash and never consumes a restart.
        """
        proc = self._proc
        spec = self._spec
        if proc is None:
            return
        with contextlib.suppress(asyncio.CancelledError):
            await proc.wait()
            if self._stopping or spec is None:
                return
            self._ready = False
            # Fail every in-flight request rather than leaving callers hanging on a
            # stream that will never produce another token.
            for entry in list(self._in_flight.values()):
                entry.cancel.set()
            if self._restarts >= self._config.max_restarts:
                return
            self._restarts += 1
            await asyncio.sleep(self._config.restart_backoff_s * self._restarts)
            with contextlib.suppress(Exception):
                await self.load(spec)

    async def unload(self) -> None:
        self._stopping = True
        self._ready = False
        for entry in list(self._in_flight.values()):
            entry.cancel.set()
        if self._monitor_task is not None:
            self._monitor_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._monitor_task
            self._monitor_task = None
        await self._terminate()
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        self._spec = None

    async def _terminate(self) -> None:
        """SIGTERM, wait, then SIGKILL. Always reap."""
        proc = self._proc
        if proc is None:
            return
        if proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=self._config.shutdown_timeout_s)
            except TimeoutError:
                with contextlib.suppress(ProcessLookupError):
                    proc.kill()
                with contextlib.suppress(Exception):
                    await proc.wait()
        await self._reap()

    async def _reap(self) -> None:
        if self._stderr_task is not None:
            self._stderr_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._stderr_task
            self._stderr_task = None
        self._proc = None

    # -- generation --------------------------------------------------------

    def _engine_payload(self, req: GenerationRequest) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "prompt": req.prompt,
            "n_predict": req.max_tokens,
            "temperature": req.temperature,
            "top_p": req.top_p,
            "stream": True,
            # Reuse the KV cache across requests sharing a prefix. This is the single
            # biggest latency win llama.cpp offers and costs nothing here.
            "cache_prompt": True,
        }
        if req.stop:
            payload["stop"] = list(req.stop)
        if req.seed is not None:
            payload["seed"] = req.seed
        payload.update(req.extra)
        return payload

    def _guard(self, req: GenerationRequest) -> None:
        if self._spec is None or not self._ready or self._client is None:
            raise ModelNotLoaded("llamacpp: no model is loaded")
        if req.model_version and req.model_version != self._spec.model_version:
            raise ModelVersionMismatch(
                f"llamacpp: this replica serves {self._spec.model_version}, not {req.model_version}"
            )

    async def _stream(self, req: GenerationRequest) -> AsyncIterator[TokenChunk]:
        self._guard(req)
        assert self._client is not None

        # An already-expired deadline is refused before the engine is touched at all.
        # Checking it only inside the read loop was a real bug, and a subtle one: the
        # request had already been sent, so the engine had already claimed a slot, and
        # with a zero timeout ``asyncio.wait`` can still return a read that completed
        # within the same event-loop tick. The result was a request that reported
        # DEADLINE while having emitted a token — and whether it did so depended on how
        # fast the model was, so it passed against a large model and failed against the
        # tiny test fixture.
        if (budget := self._remaining_budget(req)) is not None and budget <= 0:
            yield self._final(req, FinishReason.DEADLINE, 0, 0, time.perf_counter(), 0, {})
            return

        entry = _InFlight(request_id=req.request_id)
        self._in_flight[req.request_id] = entry
        started = time.perf_counter()
        emitted = 0
        ttft_ms = 0
        prompt_tokens = 0
        finish = FinishReason.STOP
        timings: dict[str, Any] = {}

        # A cancel or a deadline has to interrupt a read that is blocked waiting for
        # the next token, so both are races against the read rather than checks
        # between tokens.
        watcher = asyncio.ensure_future(entry.cancel.wait())
        try:
            async with self._client.stream(
                "POST", "/completion", json=self._engine_payload(req)
            ) as resp:
                if resp.status_code != 200:
                    body = (await resp.aread()).decode("utf-8", "replace")
                    raise GenerationFailed(
                        f"llamacpp: engine answered {resp.status_code}",
                        detail={"body": body[:2000]},
                    )

                lines = resp.aiter_lines().__aiter__()
                while True:
                    remaining = self._remaining_budget(req)
                    # Explicit rather than relying on ``timeout=0``, for the reason
                    # given above: a zero timeout is not the same as not reading.
                    if remaining is not None and remaining <= 0:
                        finish = FinishReason.DEADLINE
                        raise DeadlineExceeded("llamacpp: deadline passed")
                    read = asyncio.ensure_future(lines.__anext__())
                    done, _ = await asyncio.wait(
                        {read, watcher},
                        timeout=remaining,
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    if watcher in done:
                        read.cancel()
                        finish = FinishReason.CANCEL
                        raise Cancelled("llamacpp: cancelled mid-stream")
                    if read not in done:
                        read.cancel()
                        finish = FinishReason.DEADLINE
                        raise DeadlineExceeded("llamacpp: deadline passed")

                    try:
                        line = read.result()
                    except StopAsyncIteration:
                        break

                    event = _parse_sse_data(line)
                    if event is None:
                        continue

                    piece = event.get("content") or ""
                    if piece:
                        emitted += 1
                        self._tokens_generated += 1
                        if ttft_ms == 0:
                            # Measured at the first token that exists, never derived
                            # at the end (obligation 2).
                            ttft_ms = int((time.perf_counter() - started) * 1000)
                        yield TokenChunk(text=piece, index=emitted - 1)

                    if event.get("stop"):
                        timings = event.get("timings") or {}
                        prompt_tokens = int(
                            event.get("tokens_evaluated") or timings.get("prompt_n") or 0
                        )
                        counted = int(
                            event.get("tokens_predicted") or timings.get("predicted_n") or emitted
                        )
                        self._tokens_prompt += prompt_tokens
                        finish = _finish_reason_of(event)
                        raw_slot = event.get("id_slot")
                        yield self._final(
                            req,
                            finish,
                            prompt_tokens,
                            counted,
                            started,
                            ttft_ms,
                            timings,
                            slot=raw_slot if isinstance(raw_slot, int) and raw_slot >= 0 else None,
                        )
                        return

            # The engine closed the stream without a terminal event. That is a
            # protocol violation on its side, and reporting it as an error is better
            # than inventing a finish reason for a response we cannot vouch for.
            raise GenerationFailed("llamacpp: the engine closed the stream without finishing")
        except (Cancelled, DeadlineExceeded):
            yield self._final(req, finish, prompt_tokens, emitted, started, ttft_ms, timings)
        finally:
            watcher.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await watcher
            self._in_flight.pop(req.request_id, None)

    def stream(self, req: GenerationRequest) -> AsyncIterator[TokenChunk]:
        return self._stream(req)

    @staticmethod
    def _remaining_budget(req: GenerationRequest) -> float | None:
        if req.deadline_ms is None:
            return None
        remaining = req.deadline_ms / 1000 - time.time()
        # Zero rather than negative: asyncio.wait treats a negative timeout as
        # already-expired anyway, and clamping keeps the intent obvious.
        return max(0.0, remaining)

    def _final(
        self,
        req: GenerationRequest,
        reason: FinishReason,
        prompt_tokens: int,
        completion_tokens: int,
        started: float,
        ttft_ms: int,
        timings: Mapping[str, Any],
        slot: int | None = None,
    ) -> TokenChunk:
        total_ms = int((time.perf_counter() - started) * 1000)
        prefill_ms = int(float(timings.get("prompt_ms") or 0)) or ttft_ms
        decode_ms = int(float(timings.get("predicted_ms") or 0)) or max(0, total_ms - ttft_ms)
        return TokenChunk(
            text="",
            index=completion_tokens,
            usage=Usage(prompt_tokens=prompt_tokens, completion_tokens=completion_tokens),
            finish_reason=reason,
            timing=Timing(
                prefill_ms=prefill_ms,
                decode_ms=decode_ms,
                ttft_ms=ttft_ms,
                total_ms=total_ms,
            ),
            runtime=RuntimeInfo(
                name=self.name,
                version=self.version,
                model_version=self._spec.model_version if self._spec else "",
                slot=slot,
            ),
        )

    async def generate(self, req: GenerationRequest) -> GenerationResult:
        """Non-streaming generation over the same streaming path.

        One implementation, so the two endpoints cannot disagree about token counts,
        finish reasons or cancellation. The alternative — a second call against the
        engine's non-streaming mode — is how adapters end up with two subtly
        different behaviours that only diverge under cancellation.
        """
        parts: list[str] = []
        final: TokenChunk | None = None
        async for chunk in self.stream(req):
            parts.append(chunk.text)
            if chunk.is_final:
                final = chunk
        if final is None or final.usage is None or final.finish_reason is None:
            raise GenerationFailed("llamacpp: stream ended without a final chunk")
        return GenerationResult(
            text="".join(parts),
            finish_reason=final.finish_reason,
            usage=final.usage,
            timing=final.timing or Timing(),
            runtime=final.runtime
            or RuntimeInfo(name=self.name, version=self.version, model_version=""),
        )

    async def cancel(self, request_id: str) -> bool:
        """Stop an in-flight request by disconnecting from the engine.

        The disconnection is what makes this real: ``llama-server`` abandons a
        generation whose client has gone, so the slot is freed and the CPU stops.
        Setting a flag we check later would return ``True`` while the engine kept
        working, which obligation 3 calls a contract violation.
        """
        entry = self._in_flight.get(request_id)
        if entry is None:
            return False
        entry.cancel.set()
        return True

    # -- observation -------------------------------------------------------

    async def health(self) -> RuntimeHealth:
        proc = self._proc
        alive = proc is not None and proc.returncode is None
        ready = False
        detail = ""
        if alive and self._client is not None:
            try:
                resp = await self._client.get("/health", timeout=2.0)
                ready = resp.status_code == 200
                if not ready:
                    detail = f"engine /health returned {resp.status_code}"
            except httpx.HTTPError as exc:
                detail = f"engine /health unreachable: {exc}"
        elif not alive:
            detail = "engine process is not running"
            if self._stderr_tail:
                detail += f"; last output: {self._stderr_tail[-1]}"
        return RuntimeHealth(
            process_alive=alive,
            model_ready=alive and ready and self._spec is not None,
            detail=detail,
            engine_version=self.version,
            model_version=self._spec.model_version if self._spec else None,
        )

    def metrics(self) -> RuntimeMetrics:
        """Only what the worker cannot observe from outside.

        ``slots_busy`` is our own in-flight count rather than the engine's, because it
        is the number that must agree with the admission decisions the worker already
        made; reading it from the engine would introduce a second, slightly different
        truth for the same quantity.

        ``kv_cache_used_bytes`` is left unset. The pinned engine build's ``/metrics``
        exposes token counters, throughput and busy-slot averages but nothing about
        KV-cache occupancy, and ``/slots`` reports only ``is_processing`` and each
        slot's ``n_ctx``. Reporting zero would be inventing a measurement; leaving it
        ``None`` means the series is simply absent until an engine reports it.
        """
        return RuntimeMetrics(
            slots_total=self._slots_total if self._ready else 0,
            slots_busy=len(self._in_flight),
            tokens_generated_total=self._tokens_generated,
            tokens_prompt_total=self._tokens_prompt,
            engine_restarts_total=self._restarts,
        )

    def capabilities(self) -> RuntimeCapabilities:
        return RuntimeCapabilities(
            streaming=True,
            # Embeddings need the engine started in embedding mode, which is a
            # different deployment rather than a flag on a request. Declared false so
            # the gateway rejects them at the edge instead of the engine failing.
            embeddings=False,
            tools=False,
            json_mode=True,
            supports_cancel=True,
            max_context=self._config.n_ctx,
            parallel_slots=self._slots_total or self._config.n_parallel,
        )


def _finish_reason_of(event: Mapping[str, Any]) -> FinishReason:
    """Map the engine's terminal event onto our closed set.

    Two shapes are accepted because the engine is independently upgradable
    (ADR-0015) and it changed this field: current builds report a ``stop_type``
    string, older ones reported ``stopped_limit`` / ``stopped_eos`` /
    ``stopped_word`` booleans. Reading only the old one was a real bug in this
    adapter's first draft, caught by probing a running engine rather than by review:
    every generation came back as ``stop``, so "the model ran out of room" and "the
    model finished its sentence" were indistinguishable — which is exactly the
    difference a caller needs in order to decide whether to ask for more.

    An unrecognised value maps to ``STOP`` rather than raising. The generation did
    complete, and failing a successful request because a future engine added a stop
    type would be the worse error.
    """
    stop_type = event.get("stop_type")
    if isinstance(stop_type, str):
        return FinishReason.LENGTH if stop_type == "limit" else FinishReason.STOP
    if event.get("stopped_limit"):
        return FinishReason.LENGTH
    return FinishReason.STOP


def _parse_sse_data(line: str) -> dict[str, Any] | None:
    """Pull the JSON out of one SSE line, tolerating keep-alives and comments."""
    if not line or not line.startswith("data:"):
        return None
    payload = line[len("data:") :].strip()
    if not payload or payload == "[DONE]":
        return None
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


__all__ = ["LlamaCppConfig", "LlamaCppRuntime"]
