"""Structured JSON logging with correlation context and secret redaction.

Every record carries the identifiers bound with :func:`bind` (run, correlation, entity, batch), so a
single ``correlation_id`` follows one pipeline execution across CLI, Airflow task logs and metadata.
"""

from __future__ import annotations

import json
import logging
import re
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any

_context: ContextVar[dict[str, Any] | None] = ContextVar("lakehouse_log_context", default=None)

SENSITIVE_KEY = re.compile(r"(pass(word)?|secret|token|credential|access[_-]?key|api[_-]?key)", re.IGNORECASE)
URL_CREDENTIALS = re.compile(r"(?P<scheme>[a-z][a-z0-9+.-]*://)(?P<user>[^:/@\s]+):(?P<pw>[^@/\s]+)@", re.I)
REDACTED = "***"


def redact(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: (REDACTED if SENSITIVE_KEY.search(str(k)) else redact(v)) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [redact(v) for v in value]
    if isinstance(value, str):
        return URL_CREDENTIALS.sub(lambda m: f"{m['scheme']}{m['user']}:{REDACTED}@", value)
    return value


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": redact(record.getMessage()),
        }
        # Context captured when the event was logged (falls back to the formatting-time context).
        context = getattr(record, "context", None)
        payload.update(context if context is not None else (_context.get() or {}))
        event = getattr(record, "event", None)
        if event:
            payload["event"] = event
        fields = getattr(record, "fields", None)
        if fields:
            payload.update(redact(fields))
        if record.exc_info:
            payload["exception"] = redact(self.formatException(record.exc_info))
        return json.dumps(payload, default=str, ensure_ascii=False)


def configure_logging(level: str = "INFO") -> None:
    root = logging.getLogger()
    if any(isinstance(h.formatter, JsonFormatter) for h in root.handlers):
        root.setLevel(level)
        return
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter())
    root.handlers = [handler]
    root.setLevel(level)
    # Spark's py4j gateway and boto are chatty at INFO.
    for noisy in ("py4j", "botocore", "boto3", "urllib3", "s3transfer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


@contextmanager
def bind(**fields: Any) -> Iterator[None]:
    current = dict(_context.get() or {})
    current.update({k: v for k, v in fields.items() if v is not None})
    token = _context.set(current)
    try:
        yield
    finally:
        _context.reset(token)


def current_context() -> dict[str, Any]:
    return dict(_context.get() or {})


def log_event(logger: logging.Logger, event: str, level: int = logging.INFO, **fields: Any) -> None:
    logger.log(level, event, extra={"event": event, "fields": fields, "context": current_context()})
