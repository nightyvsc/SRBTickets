"""
Structured JSON logging for all SRB components.

Each log line is a JSON object written to stdout, designed to be collected
by ``kubectl logs``.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from typing import Any

from common.config import Config


class JsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, Any] = {
            "ts": time.time(),
            "level": record.levelname,
            "component": Config.COMPONENT_NAME,
            "message": record.getMessage(),
        }
        # Merge extra fields that the caller attached
        if hasattr(record, "extra_fields"):
            log_entry.update(record.extra_fields)
        return json.dumps(log_entry)


def get_logger(name: str) -> logging.Logger:
    """Return a logger configured for structured JSON output to stdout."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
    return logger


def log_request(
    logger: logging.Logger,
    *,
    request_id: str,
    client_id: str,
    op_type: str,
    t_received: float,
    t_completed: float,
    status: str = "OK",
    extra: dict[str, Any] | None = None,
) -> None:
    """Log a completed request with timing data for metrics extraction."""
    fields: dict[str, Any] = {
        "request_id": request_id,
        "client_id": client_id,
        "op_type": op_type,
        "t_received": t_received,
        "t_completed": t_completed,
        "duration_ms": round((t_completed - t_received) * 1000, 3),
        "status": status,
    }
    if extra:
        fields.update(extra)
    record = logger.makeRecord(
        logger.name,
        logging.INFO,
        "",
        0,
        f"{op_type} {request_id} {status}",
        (),
        None,
    )
    record.extra_fields = fields  # type: ignore[attr-defined]
    logger.handle(record)
