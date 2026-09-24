"""The ``InferenceRuntime`` protocol: the seam that keeps NEBULA from marrying one engine.

Everything in this module is engine-neutral on purpose. The shapes here were chosen
by asking what a *router* and an *autoscaler* need to know — load, slots, queue
depth, whether cancellation really works — rather than by describing what
llama.cpp happens to return. An interface shaped around its first implementation is
not an abstraction; it is that implementation with extra indirection.

Two design consequences worth naming, because they are the ones an engine would
otherwise dictate:

* Nothing here assumes a subprocess. ``llamacpp`` supervises a child process,
  ``vllm`` will drive an in-process async engine, and a hosted backend would be a
  plain HTTP client. ``load``/``unload`` are lifecycle verbs, not process verbs
  (ADR-0015).
* Nothing here assumes chat. A request carries a rendered ``prompt`` plus an
  optional structured ``messages`` form, because prompt templating belongs to the
  gateway where the model's chat template is known, not to every adapter.

The contract obligations in ``docs/api.md`` §7 are enforced by
``tests/runtime_conformance.py``, which every adapter runs. The ``mock`` adapter is
the reference implementation of correct behaviour: when the suite and an adapter
disagree, the suite is right until an ADR says otherwise.
"""

from __future__ import annotations

import enum
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

# ---------------------------------------------------------------------------
# errors
# ---------------------------------------------------------------------------


class InferenceRuntimeError(Exception):
    """Base class for every error an adapter is allowed to raise.

    ``retryable`` is the field the gateway actually branches on, so it is part of
    the type rather than something a caller infers from the message. A retry
    against another replica helps for ``EngineUnavailable`` and never helps for
    ``ChecksumMismatch``; a string comparison on the message would eventually get
    that wrong.
    """

    #: Stable, machine-readable identifier. Mirrors the error envelope's ``code``.
    code: str = "runtime_error"
    #: Whether the same request against a different replica could succeed.
    retryable: bool = False

    def __init__(self, message: str, *, detail: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.detail: dict[str, Any] = dict(detail or {})


class ArtifactMissing(InferenceRuntimeError):
    """The model artifact is not where the spec said it would be."""

    code = "artifact_missing"


class ChecksumMismatch(InferenceRuntimeError):
    """The artifact's bytes do not hash to the digest the registry recorded.

    Never retryable: the bytes on disk are wrong, and every replica pulling the same
    artifact will reach the same conclusion. Retrying would turn one clear failure
    into a cluster-wide retry storm with the same outcome.
    """

    code = "checksum_mismatch"


class InsufficientMemory(InferenceRuntimeError):
    """The model does not fit. A different replica with more headroom might."""

    code = "insufficient_memory"
    retryable = True


class UnsupportedQuantization(InferenceRuntimeError):
    """This engine cannot read that quantization format."""

    code = "unsupported_quantization"


class ModelNotLoaded(InferenceRuntimeError):
    """Generation was requested before a successful ``load``."""

    code = "model_not_loaded"


class ModelVersionMismatch(InferenceRuntimeError):
    """The caller asserted a model version this replica is not serving.

    Exists so a stale router cannot silently get the wrong model. The worker answers
    409 rather than generating from whatever it happens to have loaded.
    """

    code = "model_version_mismatch"


class EngineUnavailable(InferenceRuntimeError):
    """The engine is not answering: starting, crashed, or being restarted."""

    code = "engine_unavailable"
    retryable = True


class GenerationFailed(InferenceRuntimeError):
    """The engine accepted the request and then failed to complete it."""

    code = "generation_failed"


class CapabilityUnsupported(InferenceRuntimeError):
    """The request used a parameter this engine does not implement.

    Raised at validation time rather than generation time, so an unsupported
    parameter is a clean 400 instead of a surprise after the caller has waited.
    """

    code = "capability_unsupported"


class Cancelled(InferenceRuntimeError):
    """Generation stopped because the caller cancelled it."""

    code = "cancelled"


class DeadlineExceeded(InferenceRuntimeError):
    """Generation stopped because the request's deadline passed."""

    code = "deadline_exceeded"


# ---------------------------------------------------------------------------
# value types
# ---------------------------------------------------------------------------


class Priority(enum.StrEnum):
    """Request priority, carried by the credential and never read from a body."""

    LOW = "LOW"
    NORMAL = "NORMAL"
    HIGH = "HIGH"

    @property
    def rank(self) -> int:
        return {Priority.HIGH: 0, Priority.NORMAL: 1, Priority.LOW: 2}[self]


class FinishReason(enum.StrEnum):
    """Why generation stopped. A closed set, because clients branch on it.

    ``CANCEL`` and ``DEADLINE`` are distinct: one is the caller changing its mind,
    the other is the system running out of the budget the caller gave it. Collapsing
    them would make it impossible to tell a client-side abort from a capacity problem
    when reading a week of request history.
    """

    STOP = "stop"
    LENGTH = "length"
    CANCEL = "cancel"
    DEADLINE = "deadline"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class ModelSpec:
    """Everything an adapter needs to load one model version.

    ``model_version`` is the registry's label (``model:version``), not a path: it is
    what appears in metrics, logs and the ``X-Nebula-Model-Version`` assertion, so it
    has to survive the artifact moving.
    """

    model_version: str
    artifact_path: str
    artifact_format: str = "gguf"
    context_window: int = 4096
    quantization: str | None = None
    parameter_count: int | None = None
    checksum_sha256: str | None = None
    #: Engine-specific knobs, passed through untouched. Typed shapes belong to the
    #: adapter that understands them; a guessed shape silently drops fields.
    runtime_config: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class LoadResult:
    """What a successful load actually produced.

    ``resident_bytes`` and ``load_duration_ms`` are reported rather than estimated,
    because the scheduler's capacity model and the "why is this pod slow to start"
    question both read them.
    """

    model_version: str
    load_duration_ms: int
    resident_bytes: int
    context_window: int
    slots: int
    engine_version: str
    already_loaded: bool = False
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ChatMessage:
    role: str
    content: str


@dataclass(frozen=True, slots=True)
class GenerationRequest:
    """One unit of work.

    ``deadline_ms`` is an absolute Unix millisecond timestamp, not a duration.
    Absolute on purpose: it survives every hop without anyone recomputing "time
    left", which is the arithmetic that quietly grants an extra budget at each layer.
    """

    request_id: str
    prompt: str
    max_tokens: int = 128
    temperature: float = 0.0
    top_p: float = 1.0
    stop: tuple[str, ...] = ()
    seed: int | None = None
    priority: Priority = Priority.NORMAL
    deadline_ms: int | None = None
    model_version: str | None = None
    messages: tuple[ChatMessage, ...] = ()
    #: Engine-specific overrides for this one request.
    extra: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Usage:
    """Token counts, always from the engine's own tokenizer (obligation 4).

    Adapters never estimate. An approximate token count is an approximate invoice,
    and a client cannot tell the difference until it is disputed.
    """

    prompt_tokens: int
    completion_tokens: int

    @property
    def total_tokens(self) -> int:
        return self.prompt_tokens + self.completion_tokens


@dataclass(frozen=True, slots=True)
class Timing:
    """Where the wall clock went.

    ``ttft_ms`` is measured at the first token, not derived at the end. It is the
    number users feel, and it is the one an adapter that batches internally would
    otherwise report wrongly.
    """

    queue_ms: int = 0
    prefill_ms: int = 0
    decode_ms: int = 0
    ttft_ms: int = 0
    total_ms: int = 0


@dataclass(frozen=True, slots=True)
class RuntimeInfo:
    """Which engine, which model, which slot produced this."""

    name: str
    version: str
    model_version: str
    slot: int | None = None
    #: ``None`` means this engine does not report it. Deliberately nullable rather
    #: than defaulting to zero: an operator reading 0 concludes there is headroom,
    #: which is a worse answer than "unknown".
    kv_cache_used_bytes: int | None = None


@dataclass(frozen=True, slots=True)
class GenerationResult:
    text: str
    finish_reason: FinishReason
    usage: Usage
    timing: Timing
    runtime: RuntimeInfo


@dataclass(frozen=True, slots=True)
class TokenChunk:
    """One streamed increment.

    The final chunk always carries ``usage`` and ``finish_reason`` and is the only
    chunk that does (obligation 2). That rule is what lets a consumer stop parsing
    without a lookahead, and it is why ``is_final`` is derived rather than a
    separately settable flag that could disagree with the payload.
    """

    text: str
    index: int
    usage: Usage | None = None
    finish_reason: FinishReason | None = None
    timing: Timing | None = None
    runtime: RuntimeInfo | None = None

    @property
    def is_final(self) -> bool:
        return self.finish_reason is not None


@dataclass(frozen=True, slots=True)
class RuntimeHealth:
    """Process liveness and model readiness, separately (obligation 6).

    Two booleans rather than one status, because Kubernetes asks two different
    questions: ``livez`` decides whether to kill the container and ``readyz`` decides
    whether to send it traffic. A single "healthy" flag forces one probe to lie.
    """

    process_alive: bool
    model_ready: bool
    detail: str = ""
    engine_version: str = ""
    model_version: str | None = None


@dataclass(frozen=True, slots=True)
class RuntimeMetrics:
    """Counters and gauges the *engine* owns.

    Deliberately narrow: the worker measures everything it can measure itself
    (requests, latencies, queue depth) and asks the adapter only for what it cannot
    see from outside — slot occupancy, KV-cache use, the engine's own token totals.
    Anything the worker can count, the worker counts, so two adapters cannot
    accidentally report the same quantity differently.
    """

    slots_total: int = 0
    slots_busy: int = 0
    #: ``None`` when the engine does not expose it, which is the case for the pinned
    #: llama.cpp build. A gauge that is absent reads as unknown; a gauge that is
    #: always zero reads as "plenty of KV cache left", and that is the number an
    #: operator would be looking at while a worker OOMs.
    kv_cache_used_bytes: int | None = None
    tokens_generated_total: int = 0
    tokens_prompt_total: int = 0
    engine_restarts_total: int = 0
    extra: Mapping[str, float] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class RuntimeCapabilities:
    """What this engine can actually do (obligation 5).

    Read by the validation layer and, from Phase 6, by the router's capability-based
    strategy. Declared rather than assumed so that "this engine cannot do embeddings"
    is a 400 at the edge instead of a 500 from the engine.
    """

    streaming: bool = False
    embeddings: bool = False
    tools: bool = False
    json_mode: bool = False
    supports_cancel: bool = False
    max_context: int = 0
    parallel_slots: int = 1


# ---------------------------------------------------------------------------
# the protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class InferenceRuntime(Protocol):
    """The contract every engine adapter satisfies.

    A ``Protocol`` rather than an abstract base class: adapters are structurally
    typed, so a test double does not have to inherit from production code, and an
    adapter cannot accidentally depend on shared base-class behaviour that one engine
    needs and another does not.

    The obligations (``docs/api.md`` §7), restated where they bind on a signature:

    1. ``load`` is idempotent, reports duration and resident bytes, and raises one of
       the typed load errors — never a bare ``Exception``.
    2. ``stream`` yields its first chunk as soon as the first token exists, and its
       last chunk carries ``usage`` and ``finish_reason``.
    3. ``cancel`` really stops computation and frees the slot. Returning ``True``
       without stopping breaks the gateway's capacity model, which is worse than
       returning ``False``.
    4. Token counts come from the engine's tokenizer.
    5. ``capabilities`` is honest, including ``supports_cancel``.
    6. ``health`` separates process liveness from model readiness.
    7. The shared conformance suite passes.
    """

    #: Stable adapter name, as it appears in the registry and in metric labels.
    name: str
    #: The engine's version, discovered at load time where possible.
    version: str

    async def load(self, spec: ModelSpec) -> LoadResult:
        """Make ``spec`` ready to serve. Idempotent for the same model version."""
        ...

    async def unload(self) -> None:
        """Release the model and its resources. Safe to call when nothing is loaded."""
        ...

    async def generate(self, req: GenerationRequest) -> GenerationResult:
        """Produce a complete response."""
        ...

    def stream(self, req: GenerationRequest) -> AsyncIterator[TokenChunk]:
        """Produce incremental chunks.

        Not ``async def``: it returns the iterator directly so a caller can attach it
        to a response before the first token exists. An ``async def`` returning an
        iterator would make the caller await the first token before it could even
        start writing headers, which is precisely the latency this endpoint exists to
        avoid.
        """
        ...

    async def cancel(self, request_id: str) -> bool:
        """Stop an in-flight request. ``False`` means there was nothing to stop."""
        ...

    async def health(self) -> RuntimeHealth: ...

    def metrics(self) -> RuntimeMetrics: ...

    def capabilities(self) -> RuntimeCapabilities: ...


__all__ = [
    "ArtifactMissing",
    "Cancelled",
    "CapabilityUnsupported",
    "ChatMessage",
    "ChecksumMismatch",
    "DeadlineExceeded",
    "EngineUnavailable",
    "FinishReason",
    "GenerationFailed",
    "GenerationRequest",
    "GenerationResult",
    "InferenceRuntime",
    "InferenceRuntimeError",
    "InsufficientMemory",
    "LoadResult",
    "ModelNotLoaded",
    "ModelSpec",
    "ModelVersionMismatch",
    "Priority",
    "RuntimeCapabilities",
    "RuntimeHealth",
    "RuntimeInfo",
    "RuntimeMetrics",
    "Timing",
    "TokenChunk",
    "UnsupportedQuantization",
    "Usage",
]
