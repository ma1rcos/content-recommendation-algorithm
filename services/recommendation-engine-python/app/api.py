"""Interface HTTP interna do motor (não exposta publicamente).

Chamado pela API Rust somente quando NÃO há feed válido em cache
(cache miss com lock de single-flight adquirido). Endpoints síncronos rodam no
threadpool do Starlette, fora do event loop.
"""

from __future__ import annotations

import hmac
import logging
import os
import uuid
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException, Path, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest, multiprocess
from pydantic import BaseModel

from . import metrics
from .config import load_settings
from .feed_builder import build_feed
from .runtime import make_pool, make_redis, request_id_var, setup_logging
from .store import UnknownUser

log = logging.getLogger("engine.api")
settings = load_settings()


class BuildResponse(BaseModel):
    user_id: int
    version: int
    size: int
    generated_at_ms: int
    strategy: str
    profile_rebuilt: bool
    timings_ms: dict[str, float]
    candidates: dict[str, int]
    filtered: dict[str, int]


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    app.state.redis = make_redis(settings)
    app.state.pool = make_pool(settings, max_size=16)
    app.state.pool.open(wait=False)
    if not settings.internal_token:
        log.warning("INTERNAL_TOKEN vazio: endpoints internos sem autenticação (somente desenvolvimento)")
    yield
    app.state.pool.close()
    app.state.redis.close()


app = FastAPI(title="recommendation-engine", version="0.1.0", lifespan=lifespan,
              docs_url=None, redoc_url=None)


@app.middleware("http")
async def correlation_id(request: Request, call_next):
    rid = request.headers.get("x-request-id") or str(uuid.uuid4())
    token = request_id_var.set(rid[:64])
    try:
        response = await call_next(request)
    finally:
        request_id_var.reset(token)
    response.headers["x-request-id"] = rid[:64]
    return response


def require_internal(x_internal_token: str | None = Header(default=None)) -> None:
    if settings.internal_token and not hmac.compare_digest(x_internal_token or "", settings.internal_token):
        raise HTTPException(status_code=401, detail={"code": "unauthorized", "message": "token interno inválido"})


@app.exception_handler(HTTPException)
async def http_error(_: Request, exc: HTTPException):
    detail = exc.detail if isinstance(exc.detail, dict) else {"code": "error", "message": str(exc.detail)}
    return JSONResponse(status_code=exc.status_code, content={"error": detail})


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/ready")
def ready(request: Request):
    checks = {}
    try:
        request.app.state.redis.ping()
        checks["redis"] = "ok"
    except Exception as exc:  # noqa: BLE001
        checks["redis"] = f"error: {type(exc).__name__}"
    try:
        with request.app.state.pool.connection(timeout=2) as conn:
            conn.execute("SELECT 1")
        checks["postgres"] = "ok"
    except Exception as exc:  # noqa: BLE001
        checks["postgres"] = f"error: {type(exc).__name__}"
    ok = all(v == "ok" for v in checks.values())
    return JSONResponse(status_code=200 if ok else 503, content={"status": "ok" if ok else "degraded", "checks": checks})


@app.get("/metrics")
def prometheus_metrics():
    # Com vários processos uvicorn (ENGINE_WORKERS > 1), agrega as métricas de
    # todos via diretório compartilhado (PROMETHEUS_MULTIPROC_DIR).
    if os.environ.get("PROMETHEUS_MULTIPROC_DIR"):
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
        return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/internal/v1/feeds/{user_id}/build", response_model=BuildResponse,
          dependencies=[Depends(require_internal)])
def build(request: Request, user_id: int = Path(ge=1, le=2**53)):
    try:
        res = build_feed(request.app.state.redis, request.app.state.pool, user_id, settings)
    except UnknownUser:
        raise HTTPException(404, {"code": "user_not_found", "message": "usuário não encontrado"})
    except Exception:
        metrics.ERRORS.labels("engine", "build").inc()
        log.exception("feed_build_failed", extra={"extra": {"user_id": user_id}})
        raise HTTPException(503, {"code": "build_failed", "message": "falha ao gerar feed"})
    log.info("feed_built", extra={"extra": {"user_id": user_id, "size": res.size,
                                            "strategy": res.strategy, "timings_ms": res.timings_ms}})
    return BuildResponse(
        user_id=user_id, version=res.version, size=res.size, generated_at_ms=res.generated_at_ms,
        strategy=res.strategy, profile_rebuilt=res.profile_rebuilt, timings_ms=res.timings_ms,
        candidates={"total": res.stats.candidates_total, **res.stats.candidates_by_source,
                    "ranked": res.stats.ranked},
        filtered=res.stats.filtered,
    )
