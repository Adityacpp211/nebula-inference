"""Unit tests for the worker's own logic: deadlines, admission, cancellation, config.

These have no engine and no HTTP. They exist because the interesting cases — a queue
at its bound, a deadline that cannot be met, a duplicate request id — are awkward to
provoke through a socket and trivial to state directly.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from nebula_worker.cancel import CancelRegistry
from nebula_worker.config import ConfigError, WorkerConfig
from nebula_worker.deadline import DeadlineError, budget_for, parse_deadline
from nebula_worker.queue import AdmissionQueue, DeadlineUnreachable, QueueFull
from nebula_worker.runtimes import available, build
from nebula_worker.runtimes.base import Priority


class TestDeadlineParsing:
    def test_absent_is_none(self) -> None:
        assert parse_deadline(None) is None
        assert parse_deadline("  ") is None

    def test_absolute_milliseconds_are_accepted(self) -> None:
        target = int((time.time() + 5) * 1000)
        assert parse_deadline(str(target)) == target

    @pytest.mark.parametrize("raw", ["soon", "", "1.5", "-1", "0", "12e9"])
    def test_malformed_is_rejected_not_defaulted(self, raw: str) -> None:
        """Substituting our default would run the request longer than the caller agreed."""
        if raw == "":
            assert parse_deadline(raw) is None
            return
        with pytest.raises(DeadlineError):
            parse_deadline(raw)

    def test_seconds_mistaken_for_milliseconds_is_caught(self) -> None:
        """The most likely mistake, reported as itself rather than as its symptom.

        A seconds-valued deadline is a timestamp in 1970 once read as milliseconds, so
        without this check the caller is told their deadline has already passed — true,
        and no help at all in finding the bug.
        """
        with pytest.raises(DeadlineError, match="MILLISECONDS"):
            parse_deadline(str(int(time.time()) + 60))

    def test_far_future_deadline_is_caught(self) -> None:
        with pytest.raises(DeadlineError, match="24h"):
            parse_deadline(str(int((time.time() + 86_400 * 3) * 1000)))

    def test_strict_mode_requires_a_deadline(self) -> None:
        with pytest.raises(DeadlineError):
            budget_for(None, default_timeout_s=30, strict=True)

    def test_lenient_mode_supplies_one_and_says_so(self) -> None:
        budget = budget_for(None, default_timeout_s=30, strict=False)
        assert budget.defaulted is True
        assert 29 < budget.remaining_s <= 30

    def test_past_deadline_is_expired(self) -> None:
        past = int((time.time() - 1) * 1000)
        budget = budget_for(past, default_timeout_s=30, strict=False)
        assert budget.expired is True
        assert budget.defaulted is False


class TestAdmissionQueue:
    def test_rejects_a_zero_slot_worker(self) -> None:
        with pytest.raises(ValueError, match="at least one slot"):
            AdmissionQueue(slots=0, max_queue_depth=4)

    async def test_free_slot_is_granted_without_waiting(self) -> None:
        queue = AdmissionQueue(slots=2, max_queue_depth=4)
        assert await queue.acquire(Priority.NORMAL) == 0.0
        assert queue.in_flight == 1

    async def test_queue_full_is_raised_before_taking_a_place(self) -> None:
        """Rejection must cost nothing, so a 429 creates no state to unwind."""
        queue = AdmissionQueue(slots=1, max_queue_depth=0)
        await queue.acquire(Priority.NORMAL)
        with pytest.raises(QueueFull) as caught:
            queue.admit_or_raise(remaining_s=10)
        assert caught.value.retry_after_s > 0

    async def test_deadline_that_cannot_be_met_is_refused(self) -> None:
        queue = AdmissionQueue(slots=1, max_queue_depth=8, service_time_seed_s=2.0)
        await queue.acquire(Priority.NORMAL)
        with pytest.raises(DeadlineUnreachable):
            queue.admit_or_raise(remaining_s=0.05)

    async def test_a_generous_deadline_is_admitted(self) -> None:
        queue = AdmissionQueue(slots=1, max_queue_depth=8, service_time_seed_s=0.1)
        await queue.acquire(Priority.NORMAL)
        queue.admit_or_raise(remaining_s=60)  # must not raise

    async def test_release_hands_the_slot_to_the_highest_priority_waiter(self) -> None:
        """Priority comes from the credential, so honouring it is the whole point."""
        queue = AdmissionQueue(slots=1, max_queue_depth=8)
        await queue.acquire(Priority.NORMAL)

        order: list[str] = []

        async def waiter(name: str, priority: Priority) -> None:
            await queue.acquire(priority)
            order.append(name)

        low = asyncio.create_task(waiter("low", Priority.LOW))
        await asyncio.sleep(0.01)
        high = asyncio.create_task(waiter("high", Priority.HIGH))
        await asyncio.sleep(0.01)

        queue.release(0.01)
        await asyncio.sleep(0.01)
        queue.release(0.01)
        await asyncio.gather(low, high)

        assert order == ["high", "low"], "a later HIGH request must overtake a queued LOW one"

    async def test_arrival_order_is_kept_within_a_priority(self) -> None:
        """Reordering within a class turns a fair backlog into a lottery."""
        queue = AdmissionQueue(slots=1, max_queue_depth=8)
        await queue.acquire(Priority.NORMAL)
        order: list[int] = []

        async def waiter(index: int) -> None:
            await queue.acquire(Priority.NORMAL)
            order.append(index)

        tasks = []
        for i in range(3):
            tasks.append(asyncio.create_task(waiter(i)))
            await asyncio.sleep(0.01)

        for _ in range(4):
            queue.release(0.01)
            await asyncio.sleep(0.01)
        await asyncio.gather(*tasks)
        assert order == [0, 1, 2]

    async def test_cancel_while_queued_abandons_the_wait(self) -> None:
        """A client that gave up must not be served eventually anyway."""
        queue = AdmissionQueue(slots=1, max_queue_depth=8)
        await queue.acquire(Priority.NORMAL)
        cancelled = asyncio.Event()

        task = asyncio.create_task(queue.acquire(Priority.NORMAL, cancelled))
        await asyncio.sleep(0.01)
        assert queue.depth == 1
        cancelled.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert queue.depth == 0

    async def test_service_time_estimate_follows_observations(self) -> None:
        queue = AdmissionQueue(slots=1, max_queue_depth=8, service_time_seed_s=1.0)
        for _ in range(20):
            queue.release(0.1)
        assert queue.service_time_s < 0.3, "the estimate did not follow the observations"

    async def test_drain_releases_every_waiter(self) -> None:
        """A shutting-down worker must not leave callers waiting for a slot forever."""
        queue = AdmissionQueue(slots=1, max_queue_depth=8)
        await queue.acquire(Priority.NORMAL)
        tasks = [asyncio.create_task(queue.acquire(Priority.NORMAL)) for _ in range(3)]
        await asyncio.sleep(0.02)
        assert queue.drain_waiters() == 3
        await asyncio.sleep(0.02)
        for task in tasks:
            task.cancel()


class TestCancelRegistry:
    def test_cancel_of_unknown_id_is_none(self) -> None:
        assert CancelRegistry().cancel("nope") is None

    def test_cancel_sets_the_event(self) -> None:
        registry = CancelRegistry()
        entry = registry.register("r1")
        assert registry.cancel("r1") is entry
        assert entry.cancelled.is_set()

    def test_duplicate_id_cancels_the_older_registration(self) -> None:
        """One request's cancel must never stop another's."""
        registry = CancelRegistry()
        first = registry.register("same")
        second = registry.register("same")
        assert first is not second
        assert first.cancelled.is_set(), "the superseded registration must not be left running"
        assert not second.cancelled.is_set()

    def test_release_removes_the_entry(self) -> None:
        registry = CancelRegistry()
        registry.register("r1")
        registry.release("r1")
        assert len(registry) == 0
        registry.release("r1")  # idempotent

    def test_cancel_all_reports_how_many(self) -> None:
        registry = CancelRegistry()
        for i in range(3):
            registry.register(f"r{i}")
        assert registry.cancel_all() == 3


class TestConfig:
    def test_model_version_is_required(self) -> None:
        with pytest.raises(ConfigError) as caught:
            WorkerConfig.from_env({})
        assert any("MODEL_VERSION" in p for p in caught.value.problems)

    def test_every_problem_is_reported_at_once(self) -> None:
        """Fixing one variable, restarting, and finding the next is a bad night."""
        with pytest.raises(ConfigError) as caught:
            WorkerConfig.from_env(
                {
                    "NEBULA_ENV": "prd",
                    "NEBULA_LOG_LEVEL": "loud",
                    "NEBULA_WORKER_PORT": "eighty",
                }
            )
        problems = caught.value.problems
        assert len(problems) >= 4, f"expected several problems at once, got {problems}"

    def test_production_refuses_the_mock_runtime(self) -> None:
        with pytest.raises(ConfigError) as caught:
            WorkerConfig.from_env(
                {
                    "NEBULA_ENV": "production",
                    "NEBULA_WORKER_RUNTIME": "mock",
                    "NEBULA_WORKER_MODEL_VERSION": "m:1",
                    "NEBULA_WORKER_STRICT_HEADERS": "true",
                }
            )
        assert any("mock" in p for p in caught.value.problems)

    def test_production_requires_strict_headers(self) -> None:
        with pytest.raises(ConfigError) as caught:
            WorkerConfig.from_env(
                {
                    "NEBULA_ENV": "production",
                    "NEBULA_WORKER_RUNTIME": "llamacpp",
                    "NEBULA_WORKER_MODEL_VERSION": "m:1",
                    "NEBULA_WORKER_MODEL_PATH": "/models/m.gguf",
                }
            )
        assert any("STRICT_HEADERS" in p for p in caught.value.problems)

    def test_a_real_runtime_requires_a_model_path(self) -> None:
        with pytest.raises(ConfigError) as caught:
            WorkerConfig.from_env(
                {"NEBULA_WORKER_RUNTIME": "llamacpp", "NEBULA_WORKER_MODEL_VERSION": "m:1"}
            )
        assert any("MODEL_PATH" in p for p in caught.value.problems)

    def test_runtime_config_json_is_parsed(self) -> None:
        cfg = WorkerConfig.from_env(
            {
                "NEBULA_WORKER_MODEL_VERSION": "m:1",
                "NEBULA_WORKER_RUNTIME_CONFIG": '{"n_ctx": 2048, "n_parallel": 4}',
            }
        )
        assert cfg.runtime_config["n_ctx"] == 2048
        assert cfg.runtime_config["n_parallel"] == 4

    def test_malformed_runtime_config_is_reported(self) -> None:
        with pytest.raises(ConfigError) as caught:
            WorkerConfig.from_env(
                {"NEBULA_WORKER_MODEL_VERSION": "m:1", "NEBULA_WORKER_RUNTIME_CONFIG": "{oops"}
            )
        assert any("RUNTIME_CONFIG" in p for p in caught.value.problems)

    def test_server_bin_env_is_passed_to_the_adapter(self) -> None:
        cfg = WorkerConfig.from_env(
            {
                "NEBULA_WORKER_MODEL_VERSION": "m:1",
                "NEBULA_WORKER_RUNTIME": "llamacpp",
                "NEBULA_WORKER_MODEL_PATH": "/models/m.gguf",
                "NEBULA_LLAMA_SERVER_BIN": "/usr/bin/llama-server",
            }
        )
        assert cfg.runtime_config["server_bin"] == "/usr/bin/llama-server"


class TestRuntimeRegistry:
    def test_both_adapters_are_registered(self) -> None:
        assert set(available()) == {"mock", "llamacpp"}

    def test_unknown_runtime_lists_what_exists(self) -> None:
        """A silent fallback to the mock would serve fake tokens as if they were real."""
        with pytest.raises(ValueError, match="available runtimes"):
            build("tensorrt", {}, env="dev")

    def test_mock_is_refused_in_production_through_the_registry(self) -> None:
        with pytest.raises(RuntimeError, match="production"):
            build("mock", {}, env="production")
