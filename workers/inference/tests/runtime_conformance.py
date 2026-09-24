"""The conformance suite every ``InferenceRuntime`` adapter must pass.

This is the mechanism that makes the abstraction real. An interface that only one
engine implements is a description of that engine; an interface with a suite that two
unrelated engines both satisfy is a contract. The seven obligations in
``docs/api.md`` §7 exist because these are precisely the places adapters diverge, and
divergence surfaces as user-visible behaviour that changes when a request happens to
land on a different replica.

A new adapter is not integrated until this suite passes against it. To add one, write
a subclass of ``RuntimeConformance`` that provides the ``runtime`` fixture — nothing
else. Any test that has to be overridden or skipped for a particular engine is a
finding about the interface, not about the engine, and belongs in an ADR rather than
in a skip marker.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator

import pytest

from nebula_worker.runtimes.base import (
    FinishReason,
    GenerationRequest,
    InferenceRuntime,
    ModelNotLoaded,
    ModelSpec,
    ModelVersionMismatch,
    Priority,
    TokenChunk,
)


def deadline_in(seconds: float) -> int:
    return int((time.time() + seconds) * 1000)


class RuntimeConformance:
    """Subclass this and provide ``runtime``. Do not override the tests."""

    #: How many tokens the cancellation and deadline tests ask for. It has to be large
    #: enough that the engine is still generating when the test intervenes, and engines
    #: differ by orders of magnitude in throughput: the mock is paced at a known rate,
    #: while a small model on a CPU produces thousands of tokens a second. A subclass
    #: sets this to whatever keeps its engine busy for roughly a second. It must stay
    #: below the engine's per-slot context, or the engine stops on its own and the test
    #: passes for the wrong reason.
    long_tokens: int = 200

    @pytest.fixture
    async def runtime(self) -> AsyncIterator[InferenceRuntime]:  # pragma: no cover
        raise NotImplementedError("a conformance subclass must provide a loaded runtime")

    @pytest.fixture
    def spec(self) -> ModelSpec:  # pragma: no cover
        raise NotImplementedError("a conformance subclass must provide its model spec")

    def request(self, **kwargs: object) -> GenerationRequest:
        params: dict[str, object] = {
            "request_id": f"conformance-{time.monotonic_ns()}",
            "prompt": "nebula streams",
            "max_tokens": 8,
            "temperature": 0.0,
            "deadline_ms": deadline_in(30),
        }
        params.update(kwargs)
        return GenerationRequest(**params)  # type: ignore[arg-type]

    # -- obligation 1: load ------------------------------------------------

    async def test_load_reports_what_it_did(
        self, runtime: InferenceRuntime, spec: ModelSpec
    ) -> None:
        result = await runtime.load(spec)
        assert result.model_version == spec.model_version
        # Reported, not estimated: the scheduler's headroom check reads this.
        assert result.resident_bytes > 0, "load must report resident bytes"
        assert result.load_duration_ms >= 0
        assert result.context_window > 0
        assert result.slots >= 1
        assert result.engine_version, "load must report the engine version"

    async def test_load_is_idempotent(self, runtime: InferenceRuntime, spec: ModelSpec) -> None:
        """A second load of the same version is a no-op, not a reload.

        A controller that retries a load — because a probe was slow, or because it
        restarted — must not cause a serving replica to drop its model.
        """
        first = await runtime.load(spec)
        second = await runtime.load(spec)
        assert second.model_version == first.model_version
        assert second.already_loaded is True
        health = await runtime.health()
        assert health.model_ready is True

    # -- obligation 6: health ----------------------------------------------

    async def test_health_separates_process_from_model(self, runtime: InferenceRuntime) -> None:
        health = await runtime.health()
        assert health.process_alive is True
        assert health.model_ready is True
        assert health.engine_version
        # The two fields exist so livez and readyz can answer different questions; a
        # ready model with a dead process is incoherent.
        assert not (health.model_ready and not health.process_alive)

    async def test_generation_before_load_is_typed(self, spec: ModelSpec) -> None:
        """An unloaded runtime raises ``ModelNotLoaded``, not something generic.

        Uses a fresh adapter rather than the loaded fixture, because the interesting
        state is the one before ``load`` has ever run.
        """
        fresh = self.make_unloaded()
        with pytest.raises(ModelNotLoaded):
            await fresh.generate(self.request())

    def make_unloaded(self) -> InferenceRuntime:  # pragma: no cover
        raise NotImplementedError("a conformance subclass must provide an unloaded runtime")

    # -- obligation 5: capabilities ----------------------------------------

    async def test_capabilities_are_declared(self, runtime: InferenceRuntime) -> None:
        caps = runtime.capabilities()
        assert caps.max_context > 0, "a runtime must declare its context window"
        assert caps.parallel_slots >= 1
        assert isinstance(caps.streaming, bool)
        assert isinstance(caps.supports_cancel, bool)
        # Everything in this suite streams and cancels; an adapter that cannot is a
        # design conversation, not a passing test with two features turned off.
        assert caps.streaming is True
        assert caps.supports_cancel is True

    # -- obligation 2: streaming -------------------------------------------

    async def test_stream_yields_tokens_then_one_final_chunk(
        self, runtime: InferenceRuntime
    ) -> None:
        chunks: list[TokenChunk] = [c async for c in runtime.stream(self.request(max_tokens=6))]
        assert chunks, "stream produced nothing"

        finals = [c for c in chunks if c.is_final]
        assert len(finals) == 1, "exactly one chunk may be final"
        assert chunks[-1] is finals[0], "the final chunk must be last"

        final = finals[0]
        assert final.usage is not None, "the final chunk must carry usage"
        assert final.finish_reason is not None
        assert final.usage.completion_tokens >= 1
        # Non-final chunks must not carry usage: a consumer reads usage from the final
        # chunk, and a mid-stream copy invites double counting.
        assert all(c.usage is None for c in chunks[:-1])

        text = "".join(c.text for c in chunks)
        assert text.strip(), "stream produced no text"

    async def test_stream_is_incremental(self, runtime: InferenceRuntime) -> None:
        """The first chunk must arrive before the last.

        This is what obligation 2 is really about: an adapter that generates
        everything and then emits it as one chunk satisfies every other assertion here
        while destroying the only property streaming exists for.
        """
        first_at: float | None = None
        last_at: float | None = None
        count = 0
        started = time.perf_counter()
        async for chunk in runtime.stream(self.request(max_tokens=24)):
            now = time.perf_counter()
            if chunk.text and first_at is None:
                first_at = now
            if chunk.text:
                count += 1
                last_at = now
        assert first_at is not None and last_at is not None
        assert count >= 2, "need at least two token chunks to judge incrementality"
        assert first_at < last_at, "every chunk arrived at the same instant"
        assert first_at - started < 5.0, "the first chunk took unreasonably long"

    async def test_final_chunk_reports_ttft(self, runtime: InferenceRuntime) -> None:
        chunks = [c async for c in runtime.stream(self.request(max_tokens=10))]
        timing = chunks[-1].timing
        assert timing is not None, "the final chunk must carry timing"
        assert timing.ttft_ms >= 0
        assert timing.total_ms >= timing.ttft_ms, (
            "total time cannot be less than time to first token"
        )

    async def test_max_tokens_is_respected(self, runtime: InferenceRuntime) -> None:
        chunks = [c async for c in runtime.stream(self.request(max_tokens=4))]
        final = chunks[-1]
        assert final.usage is not None
        assert final.usage.completion_tokens <= 4
        assert final.finish_reason is FinishReason.LENGTH, (
            "stopping because the budget ran out must be reported as 'length', not "
            "'stop': a caller decides whether to ask for more based on this"
        )

    # -- obligation 4: token counts ----------------------------------------

    async def test_usage_counts_are_present_and_plausible(self, runtime: InferenceRuntime) -> None:
        chunks = [c async for c in runtime.stream(self.request(max_tokens=6))]
        usage = chunks[-1].usage
        assert usage is not None
        assert usage.prompt_tokens > 0, (
            "prompt tokens must come from the engine's tokenizer, and a non-empty "
            "prompt cannot be zero tokens"
        )
        assert usage.completion_tokens > 0
        assert usage.total_tokens == usage.prompt_tokens + usage.completion_tokens
        emitted = sum(1 for c in chunks if c.text)
        # The engine's count and the number of chunks we saw should agree closely.
        # Not exactly: an engine may coalesce or emit a multi-token piece.
        assert abs(usage.completion_tokens - emitted) <= max(2, emitted // 2)

    # -- generate mirrors stream -------------------------------------------

    async def test_generate_agrees_with_stream(self, runtime: InferenceRuntime) -> None:
        """Deterministic settings must give the same answer through both paths.

        The two endpoints exist for different callers, not for different behaviour. An
        adapter that implements them separately eventually has one of them handle
        cancellation or token counting differently, and the difference shows up as a
        client that behaves differently when it turns streaming on.
        """
        streamed = [c async for c in runtime.stream(self.request(max_tokens=8, seed=1234))]
        result = await runtime.generate(self.request(max_tokens=8, seed=1234))

        assert result.text == "".join(c.text for c in streamed)
        assert result.finish_reason is streamed[-1].finish_reason
        assert streamed[-1].usage is not None
        assert result.usage.completion_tokens == streamed[-1].usage.completion_tokens
        assert result.runtime.name and result.runtime.version

    # -- obligation 3: cancellation ----------------------------------------

    async def test_cancel_stops_generation_and_reports_it(self, runtime: InferenceRuntime) -> None:
        """Cancelling mid-stream ends the stream early with ``cancel``.

        The assertion that matters is the token count: fewer tokens than asked for
        means computation actually stopped, which is the part obligation 3 calls a
        contract violation to fake.
        """
        req = self.request(max_tokens=self.long_tokens, deadline_ms=deadline_in(30))
        chunks: list[TokenChunk] = []

        async for chunk in runtime.stream(req):
            chunks.append(chunk)
            if len([c for c in chunks if c.text]) == 1:
                assert await runtime.cancel(req.request_id) is True, (
                    "cancel must report True for a request it is actually running"
                )

        final = chunks[-1]
        assert final.is_final
        assert final.finish_reason is FinishReason.CANCEL
        assert final.usage is not None
        assert final.usage.completion_tokens < self.long_tokens, (
            "cancellation did not stop generation early"
        )

    async def test_cancel_of_unknown_request_is_false(self, runtime: InferenceRuntime) -> None:
        """A comforting lie here makes the gateway's capacity model wrong."""
        assert await runtime.cancel("no-such-request-id") is False

    async def test_cancel_frees_the_slot(self, runtime: InferenceRuntime) -> None:
        """After a cancel, the runtime must accept new work.

        A cancelled request that leaks its slot turns one abandoned client into a
        permanently smaller replica.
        """
        req = self.request(max_tokens=self.long_tokens, deadline_ms=deadline_in(30))
        async for chunk in runtime.stream(req):
            if chunk.text:
                await runtime.cancel(req.request_id)

        after = [c async for c in runtime.stream(self.request(max_tokens=4))]
        assert after[-1].usage is not None
        assert after[-1].usage.completion_tokens >= 1, "the slot was not released"

    # -- deadlines ----------------------------------------------------------

    async def test_deadline_aborts_generation(self, runtime: InferenceRuntime) -> None:
        """A deadline must stop generation mid-flight and be reported as such.

        Stated without assuming how fast the engine is, because engines differ by
        orders of magnitude and a fixed sleep would be either flaky or meaningless.
        Exactly one of two things is acceptable:

        * the runtime reported ``deadline``, in which case it stopped near the budget
          rather than long after it; or
        * the runtime finished normally *within* the budget, which is not a deadline
          violation at all.

        Anything else — finishing after the budget without reporting ``deadline`` — is
        the failure this test exists for: a deadline that is advisory rather than
        enforced.
        """
        budget_s = 0.35
        req = self.request(max_tokens=self.long_tokens * 2, deadline_ms=deadline_in(budget_s))
        started = time.perf_counter()
        chunks = [c async for c in runtime.stream(req)]
        elapsed = time.perf_counter() - started

        final = chunks[-1]
        assert final.is_final
        if final.finish_reason is FinishReason.DEADLINE:
            # Generous upper bound: the claim is that it stopped near the deadline, not
            # that it stopped to the millisecond.
            assert elapsed < budget_s + 5.0, "the deadline was honoured far too late"
            return

        # A tolerance, not a licence: it covers the scheduling jitter between the
        # engine's last token and our loop noticing, and nothing more.
        assert elapsed <= budget_s + 0.25, (
            f"generation ran {elapsed:.3f}s against a {budget_s:.3f}s deadline and "
            f"reported {final.finish_reason}; the deadline was not enforced"
        )

    async def test_expired_deadline_produces_no_tokens(self, runtime: InferenceRuntime) -> None:
        """A deadline already in the past must not start work.

        The worker refuses these at admission, but an adapter is the last line: it must
        not spend a slot on a request whose budget is already gone.
        """
        req = self.request(max_tokens=64, deadline_ms=int((time.time() - 1) * 1000))
        chunks = [c async for c in runtime.stream(req)]
        final = chunks[-1]
        assert final.finish_reason is FinishReason.DEADLINE
        assert final.usage is not None
        assert final.usage.completion_tokens == 0

    # -- model version assertion -------------------------------------------

    async def test_model_version_mismatch_is_refused(self, runtime: InferenceRuntime) -> None:
        """A stale router must not silently get the wrong model."""
        with pytest.raises(ModelVersionMismatch):
            async for _ in runtime.stream(self.request(model_version="not-the-loaded-one")):
                pass

    async def test_matching_model_version_is_accepted(
        self, runtime: InferenceRuntime, spec: ModelSpec
    ) -> None:
        chunks = [
            c
            async for c in runtime.stream(
                self.request(max_tokens=4, model_version=spec.model_version)
            )
        ]
        assert chunks[-1].is_final

    # -- concurrency --------------------------------------------------------

    async def test_concurrent_requests_are_isolated(self, runtime: InferenceRuntime) -> None:
        """Parallel requests must not interleave their text or their counts.

        Shared mutable state in an adapter shows up exactly here, and nowhere in a
        single-request test.
        """
        slots = runtime.capabilities().parallel_slots
        n = max(2, min(slots, 3))

        async def run(i: int) -> tuple[str, int]:
            req = self.request(request_id=f"concurrent-{i}", max_tokens=6, seed=i)
            parts: list[str] = []
            final: TokenChunk | None = None
            async for chunk in runtime.stream(req):
                parts.append(chunk.text)
                if chunk.is_final:
                    final = chunk
            assert final is not None and final.usage is not None
            return "".join(parts), final.usage.completion_tokens

        results = await asyncio.gather(*(run(i) for i in range(n)))
        assert len(results) == n
        for text, count in results:
            assert text.strip(), "a concurrent request produced no text"
            assert 1 <= count <= 6

    # -- metrics ------------------------------------------------------------

    async def test_metrics_reflect_work_done(self, runtime: InferenceRuntime) -> None:
        before = runtime.metrics()
        async for _ in runtime.stream(self.request(max_tokens=5)):
            pass
        after = runtime.metrics()

        assert after.tokens_generated_total > before.tokens_generated_total, (
            "generating tokens must move the runtime's token counter"
        )
        assert after.slots_total >= 1
        assert after.slots_busy == 0, "no slot should be busy once the stream has ended"

    async def test_metrics_show_a_busy_slot_during_generation(
        self, runtime: InferenceRuntime
    ) -> None:
        """Slot occupancy must be observable while work is happening.

        This is the signal Phase 6 routing and Phase 9 autoscaling read. A runtime that
        only ever reports zero busy slots is invisible to both.
        """
        req = self.request(max_tokens=self.long_tokens, deadline_ms=deadline_in(30))
        seen_busy = False
        async for chunk in runtime.stream(req):
            if chunk.text and not seen_busy:
                seen_busy = runtime.metrics().slots_busy >= 1
                await runtime.cancel(req.request_id)
        assert seen_busy, "slots_busy stayed at zero while a request was generating"

    # -- unload -------------------------------------------------------------

    async def test_unload_then_generate_is_refused(
        self, runtime: InferenceRuntime, spec: ModelSpec
    ) -> None:
        await runtime.unload()
        health = await runtime.health()
        assert health.model_ready is False

        with pytest.raises(ModelNotLoaded):
            await runtime.generate(self.request())

        # Reloadable afterwards: unload is not a terminal state, and a controller that
        # stops and restarts a deployment depends on that.
        reloaded = await runtime.load(spec)
        assert reloaded.model_version == spec.model_version

    async def test_unload_is_safe_when_nothing_is_loaded(self) -> None:
        fresh = self.make_unloaded()
        await fresh.unload()
        await fresh.unload()

    # -- priority -----------------------------------------------------------

    async def test_priority_is_accepted_without_changing_output(
        self, runtime: InferenceRuntime
    ) -> None:
        """Priority is the worker's scheduling input, not the engine's.

        An adapter must accept it and ignore it: a HIGH request and a LOW request with
        the same seed must produce the same tokens, or priority would silently change
        answers.
        """
        high = await runtime.generate(self.request(max_tokens=6, seed=99, priority=Priority.HIGH))
        low = await runtime.generate(self.request(max_tokens=6, seed=99, priority=Priority.LOW))
        assert high.text == low.text
