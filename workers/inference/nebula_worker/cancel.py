"""The worker's registry of in-flight requests.

Separate from the adapter's own tracking on purpose. The adapter knows about work it
has started; the worker also knows about work that is *queued*, and a cancel that
arrives while a request is still waiting for a slot has to remove it from the queue
rather than being told "no such request". Without that, a client that gives up during
a queue backlog leaves the worker to generate answers nobody will read — the exact
waste backpressure exists to prevent.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field


@dataclass(slots=True)
class Registration:
    request_id: str
    #: Set when someone cancels. Waited on by the queue and raced against reads in
    #: the adapters, so a cancel takes effect immediately rather than at the next
    #: token boundary.
    cancelled: asyncio.Event = field(default_factory=asyncio.Event)
    #: False until a slot is acquired. A cancel before that never reaches the engine.
    started: bool = False


class CancelRegistry:
    """Tracks every request the worker has accepted but not yet finished."""

    def __init__(self) -> None:
        self._entries: dict[str, Registration] = {}

    def register(self, request_id: str) -> Registration:
        # A duplicate id is the caller's problem, but silently sharing one
        # registration would let one request's cancel stop another's. The newer
        # registration replaces the older, and the older is cancelled so it cannot be
        # left running untracked.
        existing = self._entries.get(request_id)
        if existing is not None:
            existing.cancelled.set()
        entry = Registration(request_id=request_id)
        self._entries[request_id] = entry
        return entry

    def release(self, request_id: str) -> None:
        self._entries.pop(request_id, None)

    def cancel(self, request_id: str) -> Registration | None:
        entry = self._entries.get(request_id)
        if entry is None:
            return None
        entry.cancelled.set()
        return entry

    def cancel_all(self) -> int:
        for entry in self._entries.values():
            entry.cancelled.set()
        return len(self._entries)

    def get(self, request_id: str) -> Registration | None:
        return self._entries.get(request_id)

    def __len__(self) -> int:
        return len(self._entries)


__all__ = ["CancelRegistry", "Registration"]
