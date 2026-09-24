"""Runtime adapters, and the registry that selects one by name."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from .base import InferenceRuntime
from .llamacpp import LlamaCppConfig, LlamaCppRuntime
from .mock import MockConfig, MockRuntime

#: Name → builder. A registry rather than an import-time switch so that adding an
#: adapter is one entry, and so a test can register a double without patching
#: module internals.
_BUILDERS: dict[str, Callable[..., InferenceRuntime]] = {
    MockRuntime.name: MockRuntime.build,
    LlamaCppRuntime.name: LlamaCppRuntime.build,
}


def available() -> tuple[str, ...]:
    return tuple(sorted(_BUILDERS))


def build(name: str, spec_config: Mapping[str, Any], *, env: str) -> InferenceRuntime:
    """Construct an adapter by name.

    An unknown name is an error listing what exists, because the alternative — a
    silent fallback to the mock — is a worker that serves fake tokens while claiming
    to be a real one.
    """
    builder = _BUILDERS.get(name.lower())
    if builder is None:
        raise ValueError(f"unknown runtime {name!r}; available runtimes: {', '.join(available())}")
    return builder(spec_config, env=env)


def register(name: str, builder: Callable[..., InferenceRuntime]) -> None:
    """Add an adapter. Used by tests and by out-of-tree engines."""
    _BUILDERS[name.lower()] = builder


__all__ = [
    "InferenceRuntime",
    "LlamaCppConfig",
    "LlamaCppRuntime",
    "MockConfig",
    "MockRuntime",
    "available",
    "build",
    "register",
]
