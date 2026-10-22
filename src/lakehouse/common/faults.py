"""Deterministic failure injection for resilience tests and drills.

Pipelines call :func:`checkpoint` at the boundaries where a crash is most dangerous (for example right
after a Delta commit and before the operational metadata is updated). A checkpoint does nothing unless
``LAKEHOUSE_FAIL_AT`` names it, so production behaviour is unchanged.
"""

from __future__ import annotations

import os

FAIL_AT_ENV = "LAKEHOUSE_FAIL_AT"

# Documented checkpoints; tests assert they are reachable.
KNOWN_CHECKPOINTS = frozenset(
    {
        "bronze.after_delta_commit",
        "silver.after_merge",
        "silver.after_rejects",
        "gold.after_tables_written",
    }
)


class InjectedFailure(RuntimeError):
    """Raised at an armed checkpoint to simulate a process crash."""


def checkpoint(name: str) -> None:
    if name not in KNOWN_CHECKPOINTS:
        raise ValueError(f"unknown checkpoint {name!r}")
    armed = {item.strip() for item in os.getenv(FAIL_AT_ENV, "").split(",") if item.strip()}
    if name in armed:
        raise InjectedFailure(f"injected failure at {name}")
