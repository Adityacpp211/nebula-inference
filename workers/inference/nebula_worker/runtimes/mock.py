"""DECLARED DEVELOPMENT STUB — the ``mock`` runtime.

This adapter generates nothing real. It exists for three jobs no real engine can do
well: run the conformance suite in CI without a 400 MB artifact, give load tests a
backend whose throughput is a knob rather than a property of the hardware, and make
failure modes (a stall, a crash mid-stream, a checksum mismatch) reproducible on
demand.

It is a stub, and the codebase says so everywhere it appears: the name is ``mock``,
the artifact format it accepts is ``mock``, the control plane refuses to register
mock artifacts unless ``NEBULA_DEV_MOCK_RUNTIME=true``, and ``build`` below refuses
outright when ``NEBULA_ENV=production``. Nothing has to notice a comment for the
guard to hold.

It is also the *reference implementation* of the ``InferenceRuntime`` contract. When
a real adapter and this one disagree about cancellation, finish reasons or the shape
of the final chunk, this one is right: its behaviour is what
``tests/runtime_conformance.py`` was written against.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import random
import time
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from typing import Any

from .base import (
    ArtifactMissing,
    Cancelled,
    ChecksumMismatch,
    DeadlineExceeded,
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

#: The mock's vocabulary. Deterministic, recognisable as fake at a glance, and
#: deliberately not plausible prose: nobody should be able to paste mock output into
#: a bug report and have it mistaken for a model's answer.
_VOCAB = (
    "nebula",
    "mock",
    "token",
    "alpha",
    "beta",
    "gamma",
    "delta",
    "epsilon",
    "stream",
    "slot",
    "queue",
    "deadline",
    "cancel",
    "replica",
    "tensor",
    "kv",
)

_LOAD_ERRORS = {
    "artifact_missing": ArtifactMissing,
    "checksum_mismatch": ChecksumMismatch,
    "insufficient_memory": InsufficientMemory,
    "unsupported_quantization": UnsupportedQuantization,
}


@dataclass(slots=True)
class MockConfig:
    """Everything about the mock's behaviour is a number you can set.

    That is the point: a load test needs a backend that generates at a known rate,
    and a failure test needs one that fails at a known token.
    """

    #: Generation rate. The conformance suite runs fast; load tests turn it down.
    tokens_per_second: float = 200.0
    #: Time ``load`` pretends to spend reading weights.
    load_duration_ms: int = 5
    #: Bytes ``load`` reports as resident. Fake accounting, labelled as such.
    resident_bytes: int = 64 * 1024 * 1024
    parallel_slots: int = 4
    max_context: int = 4096

    # --- failure injection -------------------------------------------------
    #: Raise this typed load error instead of loading. Keys of ``_LOAD_ERRORS``.
    load_error: str | None = None
    #: Sleep this long inside ``load``, to exercise readiness gating.
    load_stall_ms: int = 0
    #: Sleep this long before the first token, to exercise TTFT and deadlines.
    stall_before_first_token_ms: int = 0
    #: Fail mid-stream after this many tokens, to exercise partial-stream handling.
    fail_after_tokens: int | None = None
    #: Report the engine process as dead, to exercise ``livez`` versus ``readyz``.
    process_dead: bool = False

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> MockConfig:
        """Build from a runtime_config mapping, ignoring keys we do not own.

        Unknown keys are ignored rather than rejected because ``runtime_config``
        is a shared channel: a spec written for llamacpp may be pointed at the mock
        in CI, and refusing on ``n_ctx`` would make that impossible.
        """
        known = {f.name for f in dataclasses.fields(cls)}
        return cls(**{k: v for k, v in raw.items() if k in known})


@dataclass(slots=True)
class _InFlight:
    request_id: str
    cancel: asyncio.Event = field(default_factory=asyncio.Event)
    slot: int = 0


class MockRuntime:
    """A deterministic, cancellable, rate-limited fake engine."""

    name = "mock"

    def __init__(self, config: MockConfig | None = None) -> None:
        self._config = config or MockConfig()
        self.version = "mock-1"
        self._spec: ModelSpec | None = None
        self._loaded_at: float | None = None
        self._in_flight: dict[str, _InFlight] = {}
        self._free_slots: list[int] = []
        self._tokens_generated = 0
        self._tokens_prompt = 0
        self._cancels_honoured = 0

    # -- lifecycle ---------------------------------------------------------

    @classmethod
    def build(cls, spec_config: Mapping[str, Any], *, env: str) -> MockRuntime:
        """Construct the adapter, refusing to exist in production.

        Checked here as well as in the control plane's registry validation, because
        defence in depth is cheap and this is the one code path whose whole purpose is
        to return output that is not real.
        """
        if env == "production":
            raise RuntimeError(
                "the mock runtime is a development stub and is refused when NEBULA_ENV=production"
            )
        return cls(MockConfig.from_mapping(spec_config))

    async def load(self, spec: ModelSpec) -> LoadResult:
        if self._config.load_error:
            err = _LOAD_ERRORS.get(self._config.load_error)
            if err is None:
                raise GenerationFailed(
                    f"mock: unknown injected load error {self._config.load_error!r}"
                )
            raise err(f"mock: injected {self._config.load_error}")

        # Idempotent for the same version (obligation 1): a second load reports
        # already_loaded rather than paying the cost or resetting counters. A
        # controller that retries a load must not be punished for it.
        if self._spec is not None and self._spec.model_version == spec.model_version:
            return self._load_result(spec, duration_ms=0, already_loaded=True)

        started = time.perf_counter()
        if self._config.load_stall_ms:
            await asyncio.sleep(self._config.load_stall_ms / 1000)
        elif self._config.load_duration_ms:
            await asyncio.sleep(self._config.load_duration_ms / 1000)

        self._spec = spec
        self._loaded_at = time.time()
        self._free_slots = list(range(self._config.parallel_slots))
        duration_ms = int((time.perf_counter() - started) * 1000)
        return self._load_result(spec, duration_ms=duration_ms, already_loaded=False)

    def _load_result(
        self, spec: ModelSpec, *, duration_ms: int, already_loaded: bool
    ) -> LoadResult:
        return LoadResult(
            model_version=spec.model_version,
            load_duration_ms=duration_ms,
            resident_bytes=self._config.resident_bytes,
            context_window=min(spec.context_window, self._config.max_context),
            slots=self._config.parallel_slots,
            engine_version=self.version,
            already_loaded=already_loaded,
            metadata={"declared_stub": True, "generates_real_tokens": False},
        )

    async def unload(self) -> None:
        # Cancel in-flight work first: unloading under an active stream would leave a
        # caller waiting on an iterator that will never yield again.
        for entry in list(self._in_flight.values()):
            entry.cancel.set()
        self._spec = None
        self._loaded_at = None
        self._free_slots = []

    # -- generation --------------------------------------------------------

    async def generate(self, req: GenerationRequest) -> GenerationResult:
        """Non-streaming generation, implemented over ``stream``.

        One code path, so the two endpoints cannot drift in their finish reasons,
        token counts or cancellation behaviour. The cost is that a non-streaming
        request pays the per-token pacing, which for a fake engine is the honest
        answer anyway.
        """
        text: list[str] = []
        final: TokenChunk | None = None
        async for chunk in self.stream(req):
            text.append(chunk.text)
            if chunk.is_final:
                final = chunk
        if final is None or final.usage is None or final.finish_reason is None:
            raise GenerationFailed("mock: stream ended without a final chunk")
        return GenerationResult(
            text="".join(text),
            finish_reason=final.finish_reason,
            usage=final.usage,
            timing=final.timing or Timing(),
            runtime=final.runtime or self._runtime_info(None),
        )

    async def _stream(self, req: GenerationRequest) -> AsyncIterator[TokenChunk]:
        if self._spec is None:
            raise ModelNotLoaded("mock: no model is loaded")
        if req.model_version and req.model_version != self._spec.model_version:
            raise ModelVersionMismatch(
                f"mock: this replica serves {self._spec.model_version}, not {req.model_version}"
            )
        if not self._free_slots:
            raise GenerationFailed("mock: no free slot")

        slot = self._free_slots.pop(0)
        entry = _InFlight(request_id=req.request_id, slot=slot)
        self._in_flight[req.request_id] = entry

        started = time.perf_counter()
        prompt_tokens = self._count_tokens(req.prompt)
        self._tokens_prompt += prompt_tokens
        emitted = 0
        ttft_ms = 0
        first_token_at: float | None = None

        try:
            if self._config.stall_before_first_token_ms:
                await self._sleep_or_stop(
                    entry, req, self._config.stall_before_first_token_ms / 1000
                )

            # Deterministic for a given (prompt, seed): the same inputs must produce
            # the same output, or a load test's results are not comparable between
            # runs and a failing conformance case cannot be reproduced.
            # Not cryptographic, and must not be: reproducibility across runs is the
            # whole reason the mock exists for load tests.
            rng = random.Random(  # noqa: S311
                f"{req.seed}:{req.prompt}" if req.seed is not None else req.prompt
            )
            interval = (
                1.0 / self._config.tokens_per_second if self._config.tokens_per_second > 0 else 0.0
            )
            budget = max(0, req.max_tokens)

            while emitted < budget:
                if (
                    self._config.fail_after_tokens is not None
                    and emitted == self._config.fail_after_tokens
                ):
                    raise GenerationFailed(f"mock: injected failure after {emitted} token(s)")

                if emitted:
                    await self._sleep_or_stop(entry, req, interval)
                else:
                    self._check_deadline(req)
                    if entry.cancel.is_set():
                        raise Cancelled("mock: cancelled before the first token")

                word = _VOCAB[rng.randrange(len(_VOCAB))]
                piece = word if emitted == 0 else f" {word}"
                emitted += 1
                self._tokens_generated += 1
                if first_token_at is None:
                    first_token_at = time.perf_counter()
                    ttft_ms = int((first_token_at - started) * 1000)

                # A stop sequence is honoured on the accumulated tail, the same way a
                # real engine does it, so a caller's stop strings behave identically
                # against either adapter.
                yield TokenChunk(text=piece, index=emitted - 1)

                if any(s and s in piece for s in req.stop):
                    yield self._final(
                        req, FinishReason.STOP, prompt_tokens, emitted, started, ttft_ms, slot
                    )
                    return

            yield self._final(
                req,
                FinishReason.LENGTH if emitted >= budget else FinishReason.STOP,
                prompt_tokens,
                emitted,
                started,
                ttft_ms,
                slot,
            )
        except Cancelled:
            self._cancels_honoured += 1
            yield self._final(
                req, FinishReason.CANCEL, prompt_tokens, emitted, started, ttft_ms, slot
            )
        except DeadlineExceeded:
            yield self._final(
                req, FinishReason.DEADLINE, prompt_tokens, emitted, started, ttft_ms, slot
            )
        finally:
            # The slot is freed here and nowhere else, so every exit path — success,
            # cancel, deadline, injected failure, or the consumer abandoning the
            # iterator — returns capacity. A leaked slot is a replica that slowly
            # stops accepting work for no visible reason.
            self._in_flight.pop(req.request_id, None)
            if slot not in self._free_slots:
                self._free_slots.append(slot)
                self._free_slots.sort()

    def stream(self, req: GenerationRequest) -> AsyncIterator[TokenChunk]:
        return self._stream(req)

    async def _sleep_or_stop(
        self, entry: _InFlight, req: GenerationRequest, seconds: float
    ) -> None:
        """Wait, but wake immediately on cancellation, and never past the deadline.

        Written as a race against the cancel event rather than a plain sleep so that
        ``cancel`` actually stops computation (obligation 3) instead of being noticed
        at the next token boundary. With a slow engine those are very different
        things.
        """
        self._check_deadline(req)
        remaining = seconds
        if req.deadline_ms is not None:
            until_deadline = req.deadline_ms / 1000 - time.time()
            remaining = min(remaining, max(0.0, until_deadline))

        if remaining > 0:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(entry.cancel.wait(), timeout=remaining)
        if entry.cancel.is_set():
            raise Cancelled("mock: cancelled mid-stream")
        self._check_deadline(req)

    @staticmethod
    def _check_deadline(req: GenerationRequest) -> None:
        if req.deadline_ms is not None and time.time() * 1000 >= req.deadline_ms:
            raise DeadlineExceeded("mock: deadline passed")

    def _final(
        self,
        req: GenerationRequest,
        reason: FinishReason,
        prompt_tokens: int,
        completion_tokens: int,
        started: float,
        ttft_ms: int,
        slot: int,
    ) -> TokenChunk:
        total_ms = int((time.perf_counter() - started) * 1000)
        return TokenChunk(
            text="",
            index=completion_tokens,
            usage=Usage(prompt_tokens=prompt_tokens, completion_tokens=completion_tokens),
            finish_reason=reason,
            timing=Timing(
                prefill_ms=ttft_ms,
                decode_ms=max(0, total_ms - ttft_ms),
                ttft_ms=ttft_ms,
                total_ms=total_ms,
            ),
            runtime=self._runtime_info(slot),
        )

    @staticmethod
    def _count_tokens(text: str) -> int:
        """The mock's own tokenizer: whitespace words.

        Trivial, but it *is* this engine's tokenizer, so reporting its output honours
        obligation 4. The rule the obligation exists to prevent is an adapter dividing
        characters by four and calling it a token count.
        """
        return len(text.split())

    def _runtime_info(self, slot: int | None) -> RuntimeInfo:
        return RuntimeInfo(
            name=self.name,
            version=self.version,
            model_version=self._spec.model_version if self._spec else "",
            slot=slot,
            # Left unset rather than invented. The mock fakes VRAM accounting because
            # a load test needs a number there and says so; there is no equivalent
            # reason to fabricate KV-cache occupancy.
        )

    # -- control -----------------------------------------------------------

    async def cancel(self, request_id: str) -> bool:
        """Signal an in-flight request to stop.

        Returns ``False`` for an unknown id rather than ``True``: the gateway uses the
        answer to decide whether capacity came back, and a comforting lie there makes
        its capacity model wrong (obligation 3).
        """
        entry = self._in_flight.get(request_id)
        if entry is None:
            return False
        entry.cancel.set()
        return True

    async def health(self) -> RuntimeHealth:
        alive = not self._config.process_dead
        return RuntimeHealth(
            process_alive=alive,
            model_ready=alive and self._spec is not None,
            detail="declared stub: generates no real tokens",
            engine_version=self.version,
            model_version=self._spec.model_version if self._spec else None,
        )

    def metrics(self) -> RuntimeMetrics:
        return RuntimeMetrics(
            slots_total=self._config.parallel_slots if self._spec else 0,
            slots_busy=len(self._in_flight),
            tokens_generated_total=self._tokens_generated,
            tokens_prompt_total=self._tokens_prompt,
            engine_restarts_total=0,
            extra={"mock_cancels_honoured_total": float(self._cancels_honoured)},
        )

    def capabilities(self) -> RuntimeCapabilities:
        return RuntimeCapabilities(
            streaming=True,
            embeddings=False,
            tools=False,
            json_mode=False,
            supports_cancel=True,
            max_context=self._config.max_context,
            parallel_slots=self._config.parallel_slots,
        )


__all__ = ["MockConfig", "MockRuntime"]
