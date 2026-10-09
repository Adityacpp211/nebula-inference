"""Heartbeats: what a worker tells the gateway's router, and when."""

from __future__ import annotations

import asyncio
import json

import pytest

from nebula_worker.app import Worker
from nebula_worker.config import ConfigError, WorkerConfig
from nebula_worker.heartbeat import HeartbeatPublisher, subject_token
from nebula_worker.runtimes.mock import MockConfig, MockRuntime

DEPLOYMENT = "0192f3c1-0000-7000-8000-00000000000a"


def config(**overrides: object) -> WorkerConfig:
    base: dict[str, object] = {
        "env": "dev",
        "runtime": "mock",
        "model_version": "mock-model:v1",
        "model_format": "mock",
        "context_window": 4096,
        "max_queue_depth": 4,
        "heartbeat_interval_s": 0.05,
        "pod_name": "nebula-dev-mock-7d9f-x2k",
        "node_name": "nebula-dev-worker2",
        "deployment_id": DEPLOYMENT,
        "model_version_id": "0192f3c0-0000-7000-8000-000000000001",
    }
    base.update(overrides)
    return WorkerConfig(**base)  # type: ignore[arg-type]


class Recorder:
    def __init__(self) -> None:
        self.messages: list[tuple[str, dict[str, object]]] = []

    async def __call__(self, subject: str, data: bytes) -> None:
        self.messages.append((subject, json.loads(data)))


async def loaded_worker(**overrides: object) -> Worker:
    w = Worker(config(**overrides), MockRuntime(MockConfig(parallel_slots=4)))
    await w.load()
    return w


async def test_payload_describes_the_worker() -> None:
    w = await loaded_worker()
    rec = Recorder()
    hb = HeartbeatPublisher(w, rec)
    await hb.publish_once()
    subject, body = rec.messages[0]
    assert subject == f"nebula.worker.heartbeat.{DEPLOYMENT}.nebula-dev-mock-7d9f-x2k"
    assert body["pod"] == "nebula-dev-mock-7d9f-x2k"
    assert body["deployment_id"] == DEPLOYMENT
    assert body["model_version"] == "mock-model:v1"
    assert body["state"] == "ready"
    assert body["accepting"] is True
    assert body["parallel_slots"] == 4
    assert body["inflight"] == 0
    assert body["sequence"] == 1
    assert body["context_window"] == 4096
    assert isinstance(body["instance"], str) and body["instance"]
    await hb.publish_once()
    assert rec.messages[1][1]["sequence"] == 2, "sequence must be monotonic per process"
    await w.shutdown()


async def test_draining_is_published_at_once() -> None:
    w = await loaded_worker(heartbeat_interval_s=10.0)
    rec = Recorder()
    w.heartbeat = HeartbeatPublisher(w, rec)
    task = asyncio.create_task(w.heartbeat.run())
    for _ in range(100):
        if rec.messages:
            break
        await asyncio.sleep(0.01)
    assert rec.messages, "the first heartbeat is published immediately"

    w.set_draining(release_waiters=False)
    for _ in range(100):
        if len(rec.messages) >= 2:
            break
        await asyncio.sleep(0.01)
    # Ten-second interval, yet the drain went out within a second.
    assert len(rec.messages) >= 2
    last = rec.messages[-1][1]
    assert last["state"] == "draining"
    assert last["accepting"] is False
    await w.heartbeat.close()
    task.cancel()
    await w.shutdown()


async def test_close_sends_a_final_not_accepting_heartbeat() -> None:
    w = await loaded_worker()
    rec = Recorder()
    w.heartbeat = HeartbeatPublisher(w, rec)
    await w.shutdown()  # sets draining, then closes the publisher
    assert rec.messages and rec.messages[-1][1]["accepting"] is False


def test_subject_tokens_are_safe() -> None:
    assert subject_token("a.b c*>") == "a_b_c__"
    assert subject_token("") == "unknown"


def test_config_from_env() -> None:
    cfg = WorkerConfig.from_env(
        {
            "NEBULA_WORKER_MODEL_VERSION": "m:v1",
            "NEBULA_NATS_URL": "nats://nats.nebula-data:4222",
            "NEBULA_WORKER_POD_NAME": "pod-1",
            "NEBULA_WORKER_DEPLOYMENT_ID": DEPLOYMENT,
            "NEBULA_WORKER_ADVERTISE_URL": "http://127.0.0.1:8090/",
        }
    )
    assert cfg.nats_url == "nats://nats.nebula-data:4222"
    assert cfg.pod_name == "pod-1"
    assert cfg.advertise_url == "http://127.0.0.1:8090"
    with pytest.raises(ConfigError):
        WorkerConfig.from_env(
            {"NEBULA_WORKER_MODEL_VERSION": "m:v1", "NEBULA_NATS_URL": "http://x"}
        )
