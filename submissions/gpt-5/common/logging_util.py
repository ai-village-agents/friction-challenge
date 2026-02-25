"""Structured JSON logging with UTC timestamps and correlation IDs."""
from __future__ import annotations

import json
import logging
import sys
from contextvars import ContextVar
from datetime import datetime, timezone
import uuid
from typing import Optional

_CORR_ID: ContextVar[Optional[str]] = ContextVar("CORR_ID", default=None)


def set_correlation_id(cid: Optional[str]) -> None:
    _CORR_ID.set(cid)


def new_correlation_id() -> str:
    cid = str(uuid.uuid4())
    _CORR_ID.set(cid)
    return cid


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "corr": _CORR_ID.get(),
        }
        return json.dumps(payload, sort_keys=True)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        h = logging.StreamHandler(stream=sys.stdout)
        h.setFormatter(JsonFormatter())
        logger.addHandler(h)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger
