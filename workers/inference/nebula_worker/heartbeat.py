"""Worker heartbeats: the load signal the gateway's router reads (docs/events.md §3.1).

Once a second the worker publishes what it is doing to
``nebula.worker.heartbeat.<deployment>.<pod>`` on core NATS: in-flight and queued
requests, free slots, whether it is accepting work, and which model version it
serves. Latest-wins, no persistence — a lost heartbeat is replaced by the next one.

Two properties matter more than the payload:

- A change that should stop traffic (draining) is published at once, not at the next
  tick, so gateways stop sending before the EndpointSlice catches up.
- NATS is optional and never on the request path. With no URL configured nothing is
  published; with NATS down the loop reconnects quietly and the gateway routes on
  readiness alone (``docs/components.md``).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .app import Worker

Publish = Callable[[str, bytes], Awaitable[None]]

#: NATS subject tokens may not contain dots, spaces or wildcards.
_UNSAFE = re.compile(r"[^A-Za-z0-9_-]")


def subject_token(value: str) -> str:
    return _UNSAFE.sub("_", value) or "unknown"


class HeartbeatPublisher:
    """Publishes one worker's heartbeats until closed."""

    def __init__(self, worker: Worker, publish: Publish | None = None) -> None:
        self.worker = worker
        self.config = worker.config
        self.instance = uuid.uuid4().hex
        self.sequence = 0
        self._publish = publish
        self._kick = asyncio.Event()
        self._closed = False
        self._conn: Any = None
        self.published = 0

    @property
    def subject(self) -> str:
        return (
            "nebula.worker.heartbeat."
            f"{subject_token(self.config.deployment_id)}.{subject_token(self.config.pod_name)}"
        )

    def payload(self) -> dict[str, Any]:
        """The heartbeat, from the worker's own counters. Everything is observed."""
        w = self.worker
        queue = w.queue
        rm = w.runtime.metrics()
        loaded = w.loaded is not None
        if w.draining:
            state = "draining"
        elif not loaded:
            state = "loading"
        else:
            state = "ready"
        full = queue is not None and queue.depth >= self.config.max_queue_depth
        self.sequence += 1
        body: dict[str, Any] = {
            "pod": self.config.pod_name,
            "node": self.config.node_name,
            "deployment_id": self.config.deployment_id,
            "model_version_id": self.config.model_version_id,
            "model_version": self.config.model_version,
            "runtime": w.runtime.name,
            "state": state,
            "instance": self.instance,
            "sequence": self.sequence,
            "emitted_at_ms": int(time.time() * 1000),
            "inflight": queue.in_flight if queue else 0,
            "queue_depth": queue.depth if queue else 0,
            "queue_wait_ms_ewma": round((queue.wait_estimate_s() if queue else 0.0) * 1000, 1),
            "parallel_slots": queue.slots if queue else 0,
            "slots_busy": rm.slots_busy,
            "ttft_ms_ewma": round(w.ttft_ms_ewma, 1),
            "tokens_per_second_ewma": round(w.tokens_per_second_ewma, 1),
            "accepting": state == "ready" and not full,
            "context_window": self.config.context_window,
        }
        if rm.kv_cache_used_bytes is not None:
            body["kv_cache_used_mib"] = rm.kv_cache_used_bytes // (1 << 20)
        if self.config.advertise_url:
            body["advertise_url"] = self.config.advertise_url
        return body

    def kick(self) -> None:
        """Publish now rather than at the next tick."""
        self._kick.set()

    async def publish_once(self) -> None:
        publish = self._publish
        if publish is None:
            return
        await publish(self.subject, json.dumps(self.payload(), separators=(",", ":")).encode())
        self.published += 1

    async def run(self) -> None:
        """Connect (retrying forever) and publish every interval until closed."""
        if self._publish is None:
            await self._connect()
        interval = self.config.heartbeat_interval_s
        while not self._closed:
            try:
                await self.publish_once()
            except Exception as exc:  # a heartbeat must never take the worker down
                self.worker.log.debug(
                    "heartbeat publish failed", extra={"fields": {"cause": str(exc)}}
                )
            self._kick.clear()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._kick.wait(), timeout=interval)

    async def _connect(self) -> None:
        import nats  # imported here: only a worker configured for NATS needs it

        async def _publish(subject: str, data: bytes) -> None:
            await self._conn.publish(subject, data)

        while not self._closed:
            try:
                self._conn = await nats.connect(
                    servers=[self.config.nats_url],
                    name=f"nebula-worker/{self.config.pod_name}",
                    allow_reconnect=True,
                    max_reconnect_attempts=-1,
                    reconnect_time_wait=1,
                    connect_timeout=2,
                )
                self._publish = _publish
                self.worker.log.info(
                    "heartbeats connected", extra={"fields": {"subject": self.subject}}
                )
                return
            except Exception as exc:  # retried: NATS is a soft dependency
                self.worker.log.warning(
                    "heartbeats cannot reach NATS; retrying",
                    extra={"fields": {"cause": str(exc)}},
                )
                await asyncio.sleep(2)

    async def close(self) -> None:
        """Publish a last not-accepting heartbeat, then disconnect."""
        if self._closed:
            return
        with contextlib.suppress(Exception):
            await self.publish_once()
        self._closed = True
        self._kick.set()
        if self._conn is not None:
            with contextlib.suppress(Exception):
                await self._conn.drain()


def default_pod_name() -> str:
    """The pod name from the downward API, else the hostname (a pod's hostname)."""
    return os.environ.get("NEBULA_WORKER_POD_NAME") or os.environ.get("HOSTNAME") or "local"
