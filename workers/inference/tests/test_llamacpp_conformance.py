"""The ``llamacpp`` adapter against the *same* conformance suite as the mock.

This file is the payoff for having an abstraction at all. Nothing below overrides a
test or relaxes an assertion: the only differences are the fixture that starts a real
engine and ``long_tokens``, which exists because a small model on a CPU produces
thousands of tokens a second and the cancellation test needs the engine to still be
working when it intervenes.

Skipped unless a real ``llama-server`` and a real GGUF are available (see
``tests/conftest.py``), the same way the Go integration tests skip without PostgreSQL.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from nebula_worker.runtimes.base import InferenceRuntime, ModelSpec
from nebula_worker.runtimes.llamacpp import LlamaCppConfig, LlamaCppRuntime

from .runtime_conformance import RuntimeConformance

pytestmark = pytest.mark.integration

#: The engine's total context is shared across its slots, so this is sized for
#: two slots of 4096 — enough headroom for ``long_tokens`` below to run for about a
#: second on the tiny fixture model without the engine stopping on its own.
N_CTX = 8192
N_PARALLEL = 2


class TestLlamaCppConformance(RuntimeConformance):
    #: The fixture model generates a few thousand tokens a second, so a 200-token
    #: request would be over before a cancel could land. This keeps it busy.
    long_tokens = 3000

    @pytest.fixture
    def config(self, llama_server_bin: Path) -> LlamaCppConfig:
        return LlamaCppConfig(
            server_bin=str(llama_server_bin),
            n_ctx=N_CTX,
            n_parallel=N_PARALLEL,
            n_threads=2,
            startup_timeout_s=120.0,
        )

    @pytest.fixture
    def spec(self, model_artifact: Path, model_version: str) -> ModelSpec:
        return ModelSpec(
            model_version=model_version,
            artifact_path=str(model_artifact),
            artifact_format="gguf",
            context_window=N_CTX,
        )

    @pytest.fixture
    async def runtime(
        self, config: LlamaCppConfig, spec: ModelSpec
    ) -> AsyncIterator[InferenceRuntime]:
        adapter = LlamaCppRuntime(config)
        await adapter.load(spec)
        try:
            yield adapter
        finally:
            await adapter.unload()

    def make_unloaded(self) -> InferenceRuntime:
        # No binary needed: the assertions that use this one are about the state
        # before any engine exists.
        return LlamaCppRuntime(LlamaCppConfig(server_bin="/nonexistent/llama-server"))


class TestLlamaCppSupervision:
    """Behaviour specific to supervising a child process (ADR-0015).

    The shared suite deliberately says nothing about processes — a vLLM adapter will
    have none — so these live here.
    """

    @pytest.fixture
    def config(self, llama_server_bin: Path) -> LlamaCppConfig:
        return LlamaCppConfig(
            server_bin=str(llama_server_bin), n_ctx=1024, n_parallel=1, n_threads=2
        )

    async def test_missing_artifact_is_typed_and_terminal(
        self, config: LlamaCppConfig, tmp_path: Path
    ) -> None:
        from nebula_worker.runtimes.base import ArtifactMissing

        adapter = LlamaCppRuntime(config)
        with pytest.raises(ArtifactMissing):
            await adapter.load(
                ModelSpec(model_version="absent:1", artifact_path=str(tmp_path / "nope.gguf"))
            )
        # Never retryable: the file is not there, and every replica will agree.
        assert ArtifactMissing("x").retryable is False

    async def test_checksum_mismatch_is_refused_before_the_engine_starts(
        self, config: LlamaCppConfig, model_artifact: Path
    ) -> None:
        """A wrong digest must stop the load, not be noticed later.

        This is the check that makes the registry's recorded checksum mean something:
        Phase 2 stores a digest, and this is the code that compares it to the bytes.
        """
        from nebula_worker.runtimes.base import ChecksumMismatch

        adapter = LlamaCppRuntime(config)
        with pytest.raises(ChecksumMismatch) as caught:
            await adapter.load(
                ModelSpec(
                    model_version="wrong-digest:1",
                    artifact_path=str(model_artifact),
                    checksum_sha256="00" * 32,
                )
            )
        assert "expected" in caught.value.detail
        assert "actual" in caught.value.detail
        health = await adapter.health()
        assert health.process_alive is False, "the engine must not have been started"

    async def test_correct_checksum_loads(
        self, config: LlamaCppConfig, model_artifact: Path
    ) -> None:
        import hashlib

        digest = hashlib.sha256(model_artifact.read_bytes()).hexdigest()
        adapter = LlamaCppRuntime(config)
        try:
            result = await adapter.load(
                ModelSpec(
                    model_version="verified:1",
                    artifact_path=str(model_artifact),
                    checksum_sha256=digest,
                    context_window=1024,
                )
            )
            assert result.engine_version != "unknown", (
                "the adapter must learn the engine's version, so a bug report can name it"
            )
            assert result.resident_bytes == model_artifact.stat().st_size
        finally:
            await adapter.unload()

    async def test_missing_binary_is_reported_as_engine_unavailable(
        self, model_artifact: Path
    ) -> None:
        from nebula_worker.runtimes.base import EngineUnavailable

        adapter = LlamaCppRuntime(LlamaCppConfig(server_bin="/nonexistent/llama-server"))
        with pytest.raises(EngineUnavailable):
            await adapter.load(ModelSpec(model_version="m:1", artifact_path=str(model_artifact)))

    async def test_unload_stops_the_child_process(
        self, config: LlamaCppConfig, model_artifact: Path, model_version: str
    ) -> None:
        """Unload must actually reap the engine, not orphan it.

        An orphaned llama-server keeps its model resident, so a pod that restarted a
        few times would hold several copies of the weights and be OOM-killed for
        reasons nothing in its own logs would explain.
        """
        adapter = LlamaCppRuntime(config)
        await adapter.load(
            ModelSpec(
                model_version=model_version,
                artifact_path=str(model_artifact),
                context_window=1024,
            )
        )
        health = await adapter.health()
        assert health.process_alive is True
        pid = adapter._proc.pid
        assert pid > 0

        await adapter.unload()
        after = await adapter.health()
        assert after.process_alive is False
        assert after.model_ready is False

        import os

        with pytest.raises(OSError):
            # Signal 0 probes for existence; a reaped child is gone.
            os.kill(pid, 0)

    async def test_engine_is_bound_to_loopback_only(
        self, config: LlamaCppConfig, model_artifact: Path, model_version: str
    ) -> None:
        """The supervised engine must never be reachable off-host.

        It has no authentication of its own, so its only protection is that nothing
        outside the pod can open a connection to it.
        """
        adapter = LlamaCppRuntime(config)
        try:
            result = await adapter.load(
                ModelSpec(
                    model_version=model_version,
                    artifact_path=str(model_artifact),
                    context_window=1024,
                )
            )
            assert result.metadata["base_url"].startswith("http://127.0.0.1:")
        finally:
            await adapter.unload()
