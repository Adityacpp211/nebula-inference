"""Worker configuration, from the environment, validated once at startup.

Mirrors the Go side's two rules (``packages/config``): report *every* problem at
once rather than failing on the first one, and never invent a default for something
whose wrong value is dangerous. A worker that starts with a silently-defaulted model
path is a worker that serves the wrong model.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any


class ConfigError(Exception):
    """Aggregated configuration problems.

    Aggregated on purpose: fixing one environment variable, restarting, and
    discovering the next one is a bad night. Everything wrong is reported together.
    """

    def __init__(self, problems: list[str]) -> None:
        super().__init__("; ".join(problems))
        self.problems = problems


def _env(getenv: Mapping[str, str], key: str, default: str = "") -> str:
    return (getenv.get(key) or default).strip()


def _env_int(getenv: Mapping[str, str], key: str, default: int, problems: list[str]) -> int:
    raw = _env(getenv, key)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        problems.append(f"{key}: {raw!r} is not an integer")
        return default


def _env_float(getenv: Mapping[str, str], key: str, default: float, problems: list[str]) -> float:
    raw = _env(getenv, key)
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        problems.append(f"{key}: {raw!r} is not a number")
        return default


def _env_bool(getenv: Mapping[str, str], key: str, default: bool) -> bool:
    raw = _env(getenv, key).lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


@dataclass(slots=True)
class WorkerConfig:
    env: str = "dev"
    host: str = "0.0.0.0"  # noqa: S104 - a container port, bound inside a pod network
    port: int = 8090
    log_level: str = "info"

    #: Which adapter to run. ``mock`` is refused in production by the adapter itself.
    runtime: str = "mock"
    #: The registry's label for what this replica serves. Required: it is what the
    #: ``X-Nebula-Model-Version`` assertion is checked against, and a worker that does
    #: not know its own model cannot answer that question honestly.
    model_version: str = ""
    model_path: str = ""
    model_format: str = "gguf"
    context_window: int = 4096
    checksum_sha256: str = ""

    #: Concurrency. ``slots`` is a ceiling the worker enforces; the adapter may
    #: declare fewer, and the smaller of the two wins.
    slots: int = 0
    max_queue_depth: int = 32
    #: Default budget for a request that arrives without a deadline in non-strict
    #: mode. Strict mode (the production posture) rejects those instead.
    default_timeout_s: float = 60.0
    #: Require X-Request-Id, traceparent and X-Nebula-Deadline. On in production:
    #: an untraced, deadline-less request must never enter the runtime.
    strict_headers: bool = False

    #: Seconds to keep failing readiness before refusing traffic on SIGTERM, so
    #: EndpointSlice propagation completes first. Same reasoning as the Go services.
    drain_delay_s: float = 5.0
    shutdown_grace_s: float = 25.0

    runtime_config: Mapping[str, Any] = field(default_factory=dict)

    #: Heartbeats (docs/events.md §3.1). No NATS URL, no heartbeats: the gateway then
    #: routes on readiness alone. Identity comes from the downward API in a pod.
    nats_url: str = ""
    heartbeat_interval_s: float = 1.0
    pod_name: str = "local"
    node_name: str = ""
    deployment_id: str = ""
    model_version_id: str = ""
    #: The URL a gateway reaches this worker at, for static routing outside
    #: Kubernetes, where no EndpointSlice ties a heartbeat to an endpoint.
    advertise_url: str = ""

    @classmethod
    def from_env(cls, getenv: Mapping[str, str] | None = None) -> WorkerConfig:
        source: Mapping[str, str] = getenv if getenv is not None else os.environ
        problems: list[str] = []

        cfg = cls(
            env=_env(source, "NEBULA_ENV", "dev"),
            host=_env(source, "NEBULA_WORKER_HOST", "0.0.0.0"),  # noqa: S104
            port=_env_int(source, "NEBULA_WORKER_PORT", 8090, problems),
            log_level=_env(source, "NEBULA_LOG_LEVEL", "info").lower(),
            runtime=_env(source, "NEBULA_WORKER_RUNTIME", "mock").lower(),
            model_version=_env(source, "NEBULA_WORKER_MODEL_VERSION"),
            model_path=_env(source, "NEBULA_WORKER_MODEL_PATH"),
            model_format=_env(source, "NEBULA_WORKER_MODEL_FORMAT", "gguf").lower(),
            context_window=_env_int(source, "NEBULA_WORKER_CONTEXT_WINDOW", 4096, problems),
            checksum_sha256=_env(source, "NEBULA_WORKER_MODEL_SHA256"),
            slots=_env_int(source, "NEBULA_WORKER_SLOTS", 0, problems),
            max_queue_depth=_env_int(source, "NEBULA_WORKER_MAX_QUEUE_DEPTH", 32, problems),
            default_timeout_s=_env_float(source, "NEBULA_WORKER_DEFAULT_TIMEOUT_S", 60.0, problems),
            strict_headers=_env_bool(source, "NEBULA_WORKER_STRICT_HEADERS", False),
            drain_delay_s=_env_float(source, "NEBULA_WORKER_DRAIN_DELAY_S", 5.0, problems),
            shutdown_grace_s=_env_float(source, "NEBULA_WORKER_SHUTDOWN_GRACE_S", 25.0, problems),
            runtime_config=_runtime_config(source, problems),
            nats_url=_env(source, "NEBULA_NATS_URL"),
            heartbeat_interval_s=_env_float(
                source, "NEBULA_WORKER_HEARTBEAT_INTERVAL_S", 1.0, problems
            ),
            pod_name=_env(source, "NEBULA_WORKER_POD_NAME") or _env(source, "HOSTNAME") or "local",
            node_name=_env(source, "NEBULA_WORKER_NODE_NAME"),
            deployment_id=_env(source, "NEBULA_WORKER_DEPLOYMENT_ID"),
            model_version_id=_env(source, "NEBULA_WORKER_MODEL_VERSION_ID"),
            advertise_url=_env(source, "NEBULA_WORKER_ADVERTISE_URL").rstrip("/"),
        )

        if cfg.env not in {"dev", "staging", "production"}:
            problems.append(f"NEBULA_ENV: {cfg.env!r} is not dev, staging or production")
        if cfg.log_level not in {"debug", "info", "warn", "warning", "error"}:
            problems.append(f"NEBULA_LOG_LEVEL: {cfg.log_level!r} is not a known level")
        if not cfg.model_version:
            problems.append(
                "NEBULA_WORKER_MODEL_VERSION: required — the worker must know which "
                "model version it serves in order to honour X-Nebula-Model-Version"
            )
        if cfg.runtime != "mock" and not cfg.model_path:
            problems.append(f"NEBULA_WORKER_MODEL_PATH: required for the {cfg.runtime!r} runtime")
        if cfg.max_queue_depth < 0:
            problems.append("NEBULA_WORKER_MAX_QUEUE_DEPTH: must not be negative")
        if cfg.default_timeout_s <= 0:
            problems.append("NEBULA_WORKER_DEFAULT_TIMEOUT_S: must be positive")
        if cfg.nats_url and not cfg.nats_url.startswith(("nats://", "tls://")):
            problems.append(f"NEBULA_NATS_URL: {cfg.nats_url!r} is not a nats:// or tls:// URL")
        if not 0.1 <= cfg.heartbeat_interval_s <= 10:
            problems.append("NEBULA_WORKER_HEARTBEAT_INTERVAL_S: must be between 0.1 and 10")

        if cfg.env == "production":
            # The same posture the Go services take: development affordances are a
            # startup failure rather than a warning nobody reads.
            if cfg.runtime == "mock":
                problems.append(
                    "NEBULA_WORKER_RUNTIME: the mock runtime is a development stub and "
                    "is refused when NEBULA_ENV=production"
                )
            if not cfg.strict_headers:
                problems.append(
                    "NEBULA_WORKER_STRICT_HEADERS: must be true in production — an "
                    "untraced or deadline-less request must not enter the runtime"
                )

        if problems:
            raise ConfigError(problems)
        return cfg

    @property
    def is_production(self) -> bool:
        return self.env == "production"


def _runtime_config(source: Mapping[str, str], problems: list[str]) -> dict[str, Any]:
    """Adapter-specific settings, as JSON in one variable plus a few conveniences.

    JSON rather than one variable per knob because the set of knobs belongs to the
    adapter, and the worker should not have to learn a new environment variable every
    time an engine gains an option.
    """
    import json

    raw = _env(source, "NEBULA_WORKER_RUNTIME_CONFIG")
    out: dict[str, Any] = {}
    if raw:
        try:
            parsed = json.loads(raw)
            if not isinstance(parsed, dict):
                problems.append("NEBULA_WORKER_RUNTIME_CONFIG: must be a JSON object")
            else:
                out.update(parsed)
        except json.JSONDecodeError as exc:
            problems.append(f"NEBULA_WORKER_RUNTIME_CONFIG: invalid JSON ({exc})")

    # Convenience passthroughs for the two settings an operator changes most often.
    if bin_path := _env(source, "NEBULA_LLAMA_SERVER_BIN"):
        out.setdefault("server_bin", bin_path)
    if threads := _env(source, "NEBULA_WORKER_THREADS"):
        with_int = threads.isdigit()
        if with_int:
            out.setdefault("n_threads", int(threads))
        else:
            problems.append(f"NEBULA_WORKER_THREADS: {threads!r} is not an integer")
    return out


__all__ = ["ConfigError", "WorkerConfig"]
