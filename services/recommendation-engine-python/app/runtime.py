"""Inicialização compartilhada: logs JSON estruturados e conexões."""

from __future__ import annotations

import contextvars
import logging
import sys
import time

import orjson
import redis
from psycopg_pool import ConnectionPool

from .config import Settings

request_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar("request_id", default=None)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        data = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)) + f".{int(record.msecs):03d}Z",
            "level": record.levelname.lower(),
            "logger": record.name,
            "msg": record.getMessage(),
        }
        rid = request_id_var.get()
        if rid:
            data["request_id"] = rid
        extra = getattr(record, "extra", None)
        if isinstance(extra, dict):
            data.update(extra)
        if record.exc_info:
            data["exc"] = self.formatException(record.exc_info)[-2000:]
        return orjson.dumps(data, default=str).decode()


def setup_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    for noisy in ("uvicorn.access",):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def make_redis(s: Settings, socket_timeout: float = 2.0) -> redis.Redis:
    """socket_timeout precisa ser maior que o BLOCK do XREADGROUP nos workers."""
    return redis.Redis.from_url(
        s.redis_url, decode_responses=True, socket_timeout=socket_timeout, socket_connect_timeout=2.0,
        health_check_interval=30, max_connections=64,
    )


def make_pool(s: Settings, max_size: int = 10) -> ConnectionPool:
    return ConnectionPool(
        s.database_url, min_size=1, max_size=max_size, timeout=5.0, open=False,
        kwargs={"autocommit": True, "connect_timeout": 3, "options": "-c statement_timeout=5000"},
    )
