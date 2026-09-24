"""Entrypoint. Owns the signal handling and the drain sequence.

The shutdown order is the same as the Go services', for the same reason: failing
readiness and then immediately refusing connections produces 503s during a rolling
deploy, because EndpointSlice propagation is not instant.

    SIGTERM → fail readiness → wait drain_delay → stop accepting → finish in-flight
    (bounded by shutdown_grace) → unload the engine → exit

The wait is the part people leave out, and it is the part that makes a deploy silent.
"""

from __future__ import annotations

import asyncio
import signal
import sys

import uvicorn

from .app import create_app
from .config import ConfigError, WorkerConfig
from .telemetry import configure_logging


async def _serve(config: WorkerConfig) -> int:
    app = create_app(config)
    worker = app.state.worker
    log = worker.log

    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host=config.host,
            port=config.port,
            log_config=None,  # our JSON handler is already installed
            access_log=False,
            # The worker speaks HTTP only. Not loading the websocket protocol removes
            # surface area we would never serve, and avoids pulling in a deprecated
            # implementation from uvicorn's optional extras.
            ws="none",
            # Streaming responses must not be buffered into a single write.
            timeout_graceful_shutdown=int(config.shutdown_grace_s),
        )
    )

    loop = asyncio.get_running_loop()
    draining = asyncio.Event()
    # A reference is kept deliberately: asyncio holds only a weak reference to a task,
    # so a fire-and-forget create_task can be garbage collected before it runs. For the
    # drain timer that would mean a SIGTERM which fails readiness and then never exits.
    pending: set[asyncio.Task[None]] = set()

    def _on_signal(name: str) -> None:
        if draining.is_set():
            # A second signal means someone is impatient, or the orchestrator has
            # escalated. Skip the grace period rather than ignoring them.
            log.warning("second signal received, exiting now", extra={"fields": {"signal": name}})
            server.should_exit = True
            server.force_exit = True
            return
        draining.set()
        log.info(
            "signal received, draining",
            extra={
                "fields": {
                    "signal": name,
                    "drain_delay_s": config.drain_delay_s,
                    "shutdown_grace_s": config.shutdown_grace_s,
                }
            },
        )
        worker.draining = True
        worker.metrics.draining.set(1)

        async def _finish() -> None:
            # Readiness is already failing. Keep serving for drain_delay so requests
            # already in flight towards this pod still land somewhere that answers.
            await asyncio.sleep(config.drain_delay_s)
            server.should_exit = True

        task = loop.create_task(_finish())
        pending.add(task)
        task.add_done_callback(pending.discard)

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _on_signal, sig.name)

    log.info(
        "starting",
        extra={
            "fields": {
                "runtime": config.runtime,
                "model_version": config.model_version,
                "addr": f"{config.host}:{config.port}",
                "env": config.env,
                "strict_headers": config.strict_headers,
            }
        },
    )
    await server.serve()
    log.info("stopped")
    return 0


def main() -> int:
    try:
        config = WorkerConfig.from_env()
    except ConfigError as exc:
        # Configuration failures happen before the logger exists, so they go to
        # stderr plainly — and every problem is listed, not just the first.
        print("nebula-inference-worker: configuration is invalid:", file=sys.stderr)
        for problem in exc.problems:
            print(f"  - {problem}", file=sys.stderr)
        return 2

    configure_logging(config.log_level)
    try:
        return asyncio.run(_serve(config))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
