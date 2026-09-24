"""The worker's local admission gate: bounded, priority-ordered, deadline-aware.

Three decisions, each of which is the difference between a worker that degrades and
one that collapses:

**Bounded.** An unbounded queue turns a load spike into an out-of-memory kill, and
every queued request into a response nobody is waiting for any more. When the queue
is full the worker answers 429 with ``Retry-After`` so the gateway can try another
replica — which is information, where a timeout is not.

**Priority-ordered.** Priority comes from the credential, never from the request
body, so a caller cannot promote itself. Within a priority, arrival order — a
priority queue that reorders within a class turns a fair backlog into a lottery.

**Deadline-aware.** A request is refused at admission if it cannot plausibly *start*
before its own deadline. Accepting it would spend the caller's remaining budget
sitting in a queue and then return a timeout, when the gateway could have spent that
budget on a replica with a free slot.

The estimate used for that last check is deliberately crude: an exponentially
weighted mean of recent service times, multiplied by the number of requests ahead.
Phase 7 replaces it with the real concurrency model. Crude and named as such beats
sophisticated and undocumented, and it is already enough to stop the obvious waste.
"""

from __future__ import annotations

import asyncio
import heapq
import itertools
import time
from dataclasses import dataclass, field

from .runtimes.base import Priority


class QueueFull(Exception):
    """The queue is at its bound. Maps to 429 + Retry-After + worker_saturated."""

    def __init__(self, depth: int, retry_after_s: float) -> None:
        super().__init__(f"worker queue is full ({depth} waiting)")
        self.depth = depth
        self.retry_after_s = retry_after_s


class DeadlineUnreachable(Exception):
    """The request cannot start before its own deadline, so it is refused now.

    Distinct from ``QueueFull`` because the remedy differs: a saturated worker is
    worth retrying elsewhere, while an already-doomed deadline is worth failing fast
    so the caller learns immediately rather than at the deadline.
    """

    def __init__(self, wait_estimate_s: float, remaining_s: float) -> None:
        super().__init__(
            f"estimated wait {wait_estimate_s:.2f}s exceeds the remaining budget {remaining_s:.2f}s"
        )
        self.wait_estimate_s = wait_estimate_s
        self.remaining_s = remaining_s


@dataclass(order=True, slots=True)
class _Waiter:
    priority_rank: int
    sequence: int
    ready: asyncio.Event = field(compare=False, default_factory=asyncio.Event)
    cancelled: bool = field(compare=False, default=False)


class AdmissionQueue:
    """Grants and releases slots, in priority then arrival order."""

    def __init__(
        self,
        *,
        slots: int,
        max_queue_depth: int,
        service_time_seed_s: float = 0.25,
    ) -> None:
        if slots < 1:
            raise ValueError("a worker needs at least one slot")
        self._slots = slots
        self._max_depth = max_queue_depth
        self._available = slots
        self._waiters: list[_Waiter] = []
        self._counter = itertools.count()
        #: EWMA of observed service time, seeded so the first admission decision is
        #: not made with no information at all.
        self._service_time_s = service_time_seed_s
        self._in_flight = 0

    @property
    def slots(self) -> int:
        return self._slots

    @property
    def in_flight(self) -> int:
        return self._in_flight

    @property
    def depth(self) -> int:
        return len([w for w in self._waiters if not w.cancelled])

    @property
    def service_time_s(self) -> float:
        return self._service_time_s

    def wait_estimate_s(self) -> float:
        """How long a request arriving now would wait for a slot.

        Zero when a slot is free. Otherwise the number of requests that must finish
        first, divided by the slots that will free up, times the mean service time.
        """
        if self._available > 0:
            return 0.0
        ahead = self.depth + 1
        return (ahead / self._slots) * self._service_time_s

    def admit_or_raise(self, remaining_s: float | None) -> None:
        """Check the bounds *before* taking a place in the queue.

        Separate from ``acquire`` so a rejection costs nothing and so the HTTP layer
        can answer 429 without having created state it then has to unwind.
        """
        if self._available <= 0 and self.depth >= self._max_depth:
            raise QueueFull(self.depth, retry_after_s=max(0.05, self.wait_estimate_s()))
        if remaining_s is not None:
            estimate = self.wait_estimate_s()
            if estimate >= remaining_s:
                raise DeadlineUnreachable(estimate, remaining_s)

    async def acquire(self, priority: Priority, cancelled: asyncio.Event | None = None) -> float:
        """Wait for a slot. Returns how long the wait was, in seconds.

        A cancel while queued removes the waiter and raises ``asyncio.CancelledError``,
        so a client that gave up does not get served eventually anyway.
        """
        started = time.perf_counter()
        if self._available > 0:
            self._available -= 1
            self._in_flight += 1
            return 0.0

        waiter = _Waiter(priority_rank=priority.rank, sequence=next(self._counter))
        heapq.heappush(self._waiters, waiter)

        if cancelled is None:
            await waiter.ready.wait()
        else:
            watch = asyncio.ensure_future(cancelled.wait())
            wait_ready = asyncio.ensure_future(waiter.ready.wait())
            try:
                done, _ = await asyncio.wait(
                    {watch, wait_ready}, return_when=asyncio.FIRST_COMPLETED
                )
                if wait_ready not in done:
                    # Cancelled while queued. Mark the waiter dead rather than
                    # removing it from the heap: heap removal is O(n) and a dead
                    # waiter is skipped for free on the next release.
                    waiter.cancelled = True
                    raise asyncio.CancelledError
            finally:
                for task in (watch, wait_ready):
                    if not task.done():
                        task.cancel()

        self._in_flight += 1
        return time.perf_counter() - started

    def release(self, service_time_s: float | None = None) -> None:
        """Return a slot and update the service-time estimate."""
        self._in_flight = max(0, self._in_flight - 1)
        if service_time_s is not None and service_time_s > 0:
            # 0.2 gives roughly a five-request memory: responsive enough to follow a
            # change in prompt shape, damped enough that one outlier does not start
            # rejecting everything.
            self._service_time_s = 0.8 * self._service_time_s + 0.2 * service_time_s

        while self._waiters:
            waiter = heapq.heappop(self._waiters)
            if waiter.cancelled:
                continue
            waiter.ready.set()
            return
        self._available = min(self._slots, self._available + 1)

    def drain_waiters(self) -> int:
        """Wake every waiter so a shutting-down worker does not leave them hanging."""
        count = 0
        while self._waiters:
            waiter = heapq.heappop(self._waiters)
            if waiter.cancelled:
                continue
            waiter.cancelled = True
            waiter.ready.set()
            count += 1
        return count


__all__ = ["AdmissionQueue", "DeadlineUnreachable", "QueueFull"]
