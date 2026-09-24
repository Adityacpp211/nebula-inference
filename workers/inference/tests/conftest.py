"""Shared fixtures, and the discovery of the real engine and model.

The integration tests need two things the repository does not contain: an upstream
``llama-server`` binary and a GGUF artifact. Both are discovered from the
environment and the tests skip when they are absent, so ``pytest`` on a clean
checkout passes without them — the same rule the Go integration tests follow for
PostgreSQL. A test that cannot run should say so, not fail.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

ENV_SERVER_BIN = "NEBULA_LLAMA_SERVER_BIN"
ENV_MODEL_PATH = "NEBULA_TEST_MODEL_PATH"
ENV_MODEL_VERSION = "NEBULA_TEST_MODEL_VERSION"


def _existing(value: str | None) -> Path | None:
    if not value:
        return None
    path = Path(value)
    return path if path.exists() else None


@pytest.fixture(scope="session")
def llama_server_bin() -> Path:
    path = _existing(os.environ.get(ENV_SERVER_BIN))
    if path is None:
        pytest.skip(
            f"{ENV_SERVER_BIN} is not set to an existing llama-server binary; "
            "skipping the real-engine tests"
        )
    return path


@pytest.fixture(scope="session")
def model_artifact() -> Path:
    path = _existing(os.environ.get(ENV_MODEL_PATH))
    if path is None:
        pytest.skip(
            f"{ENV_MODEL_PATH} is not set to an existing GGUF artifact; skipping the "
            "real-model tests. Build one with tools/make_tiny_model.py."
        )
    return path


@pytest.fixture(scope="session")
def model_version() -> str:
    return os.environ.get(ENV_MODEL_VERSION, "nebula-tiny:fixture")
