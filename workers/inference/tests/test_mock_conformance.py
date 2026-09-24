"""The ``mock`` adapter against the shared conformance suite.

The mock is the reference implementation, so this file is also the suite's own test:
if a conformance assertion is wrong, it is wrong here first.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from nebula_worker.runtimes.base import InferenceRuntime, ModelSpec
from nebula_worker.runtimes.mock import MockConfig, MockRuntime

from .runtime_conformance import RuntimeConformance

# Fast enough for the suite to run in about a second, slow enough that cancellation
# and deadlines are genuinely exercised rather than raced.
MOCK_CONFIG = MockConfig(tokens_per_second=120.0, parallel_slots=3, max_context=4096)


class TestMockConformance(RuntimeConformance):
    @pytest.fixture
    def spec(self) -> ModelSpec:
        return ModelSpec(
            model_version="mock-model:v1",
            artifact_path="/dev/null",
            artifact_format="mock",
            context_window=4096,
        )

    @pytest.fixture
    async def runtime(self, spec: ModelSpec) -> AsyncIterator[InferenceRuntime]:
        adapter = MockRuntime(MOCK_CONFIG)
        await adapter.load(spec)
        try:
            yield adapter
        finally:
            await adapter.unload()

    def make_unloaded(self) -> InferenceRuntime:
        return MockRuntime(MOCK_CONFIG)


class TestMockStubDiscipline:
    """The mock's own guarantees, which the shared suite deliberately does not assert."""

    def test_refused_in_production(self) -> None:
        """A stub that can run in production is not a declared stub."""
        with pytest.raises(RuntimeError, match="production"):
            MockRuntime.build({}, env="production")

    def test_allowed_outside_production(self) -> None:
        assert MockRuntime.build({}, env="dev").name == "mock"
        assert MockRuntime.build({}, env="staging").name == "mock"

    async def test_load_result_admits_it_is_fake(self) -> None:
        adapter = MockRuntime(MOCK_CONFIG)
        result = await adapter.load(
            ModelSpec(model_version="m:1", artifact_path="/dev/null", artifact_format="mock")
        )
        # Machine-readable, so nothing downstream has to parse a comment to find out
        # that these tokens are not real.
        assert result.metadata["declared_stub"] is True
        assert result.metadata["generates_real_tokens"] is False

    @pytest.mark.parametrize(
        ("injected", "expected"),
        [
            ("artifact_missing", "ArtifactMissing"),
            ("checksum_mismatch", "ChecksumMismatch"),
            ("insufficient_memory", "InsufficientMemory"),
            ("unsupported_quantization", "UnsupportedQuantization"),
        ],
    )
    async def test_injects_each_typed_load_error(self, injected: str, expected: str) -> None:
        """Failure injection exists so the worker's error mapping can be tested.

        Without it, testing 'what does the API return when a checksum mismatches'
        would need a deliberately corrupted multi-gigabyte artifact.
        """
        adapter = MockRuntime(MockConfig(load_error=injected))
        with pytest.raises(Exception) as caught:
            await adapter.load(
                ModelSpec(model_version="m:1", artifact_path="/dev/null", artifact_format="mock")
            )
        assert type(caught.value).__name__ == expected

    async def test_output_is_deterministic_for_a_seed(self) -> None:
        """Reproducibility is why the mock exists for load tests."""
        adapter = MockRuntime(MOCK_CONFIG)
        await adapter.load(
            ModelSpec(model_version="m:1", artifact_path="/dev/null", artifact_format="mock")
        )
        first = await adapter.generate(_req("determinism-1", seed=42))
        second = await adapter.generate(_req("determinism-2", seed=42))
        different = await adapter.generate(_req("determinism-3", seed=43))
        assert first.text == second.text
        assert first.text != different.text

    async def test_reports_a_dead_process_without_dying(self) -> None:
        """``process_dead`` exercises livez versus readyz without killing anything."""
        adapter = MockRuntime(MockConfig(process_dead=True))
        await adapter.load(
            ModelSpec(model_version="m:1", artifact_path="/dev/null", artifact_format="mock")
        )
        health = await adapter.health()
        assert health.process_alive is False
        assert health.model_ready is False


def _req(request_id: str, **kwargs: object):
    from nebula_worker.runtimes.base import GenerationRequest

    params: dict[str, object] = {
        "request_id": request_id,
        "prompt": "nebula streams",
        "max_tokens": 6,
    }
    params.update(kwargs)
    return GenerationRequest(**params)  # type: ignore[arg-type]
