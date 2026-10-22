"""Run tracking shared by every pipeline step: one ``pipeline_runs`` row per invocation."""

from __future__ import annotations

import logging
import time
import traceback
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

from lakehouse.config import Settings
from lakehouse.metadata.repository import MetadataRepository
from lakehouse.observability.logging import bind, log_event

log = logging.getLogger(__name__)


@dataclass
class RunHandle:
    run_id: str
    correlation_id: str
    pipeline: str
    settings: Settings
    repo: MetadataRepository
    records_read: int = 0
    records_written: int = 0
    records_rejected: int = 0
    step: str = "start"
    summary: dict[str, Any] = field(default_factory=dict)

    def add(self, read: int = 0, written: int = 0, rejected: int = 0) -> None:
        self.records_read += read
        self.records_written += written
        self.records_rejected += rejected


@contextmanager
def tracked_run(
    settings: Settings,
    repo: MetadataRepository,
    pipeline: str,
    correlation_id: str | None = None,
    trigger: str = "cli",
    params: dict[str, Any] | None = None,
    attempt: int = 1,
) -> Iterator[RunHandle]:
    run_id = repo.start_run(pipeline, correlation_id, trigger, params, attempt)
    handle = RunHandle(run_id, correlation_id or run_id, pipeline, settings, repo)
    started = time.monotonic()
    with bind(run_id=run_id, correlation_id=handle.correlation_id, pipeline=pipeline):
        log_event(log, "run_started", params=params or {}, attempt=attempt, trigger=trigger)
        try:
            yield handle
        except BaseException as exc:
            repo.record_error(run_id, handle.step, exc, traceback.format_exc())
            repo.finish_run(
                run_id,
                "FAILED",
                handle.records_read,
                handle.records_written,
                handle.records_rejected,
                error=exc,
            )
            log_event(
                log,
                "run_failed",
                logging.ERROR,
                step=handle.step,
                error_type=type(exc).__name__,
                error=str(exc),
                duration_s=round(time.monotonic() - started, 3),
            )
            raise
        repo.finish_run(
            run_id, "SUCCEEDED", handle.records_read, handle.records_written, handle.records_rejected
        )
        log_event(
            log,
            "run_succeeded",
            duration_s=round(time.monotonic() - started, 3),
            records_read=handle.records_read,
            records_written=handle.records_written,
            records_rejected=handle.records_rejected,
        )
