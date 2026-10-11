"""Worker assíncrono: consome eventos, regenera feeds e faz manutenção periódica.

    python -m workers.main --role all|events|rebuild|maintenance

Pode rodar em N réplicas: cada uma é um consumer distinto nos consumer groups
do Redis Streams, então as mensagens são divididas entre elas. A manutenção usa
um lock no Redis para rodar em apenas uma réplica por vez.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import socket
import threading
import time
import uuid

import psycopg
import redis
from prometheus_client import start_http_server

from app import keys, metrics
from app.config import Settings, load_settings
from app.events import Scripts, dead_letter, process_batch
from app.feed_builder import build_feed, release_lock
from app.runtime import make_pool, make_redis, setup_logging
from app.state_rebuild import ensure_catalog, refresh_trending
from app.store import UnknownUser

log = logging.getLogger("worker")
stop = threading.Event()
CONSUMER = f"{socket.gethostname()}-{os.getpid()}"


def ensure_group(r: redis.Redis, stream: str, group: str) -> None:
    try:
        r.xgroup_create(stream, group, id="0", mkstream=True)
    except redis.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def claim_stale(r: redis.Redis, stream: str, group: str, s: Settings) -> list[tuple[str, dict]]:
    """Reivindica mensagens pendentes de consumers que morreram. As que já
    foram entregues demais vão para a DLQ (evita "poison messages" em loop)."""
    pending = r.xpending_range(stream, group, min="-", max="+", count=100, idle=s.pending_idle_ms)
    if not pending:
        return []
    poison = [p["message_id"] for p in pending if p["times_delivered"] > s.max_event_deliveries]
    retry = [p["message_id"] for p in pending if p["times_delivered"] <= s.max_event_deliveries]
    for msg_id in poison:
        msgs = r.xrange(stream, msg_id, msg_id)
        dead_letter(r, msg_id, msgs[0][1] if msgs else {}, "max_deliveries")
        r.xack(stream, group, msg_id)
    if not retry:
        return []
    return [(m[0], m[1]) for m in r.xclaim(stream, group, CONSUMER, s.pending_idle_ms, retry) if m[1]]


def backoff(attempt: int) -> None:
    stop.wait(min(10.0, 0.2 * 2 ** attempt))


def events_loop(s: Settings) -> None:
    r, pool = make_redis(s, socket_timeout=10.0), make_pool(s)
    pool.open(wait=False)
    scripts = Scripts.register(r)
    errors, last_claim, cursor = 0, 0.0, "0"  # "0" = começa pelas próprias pendentes
    while not stop.is_set():
        try:
            ensure_group(r, keys.EVENTS_STREAM, keys.EVENTS_GROUP)
            batch: list[tuple[str, dict]] = []
            if time.monotonic() - last_claim > 30:
                batch = claim_stale(r, keys.EVENTS_STREAM, keys.EVENTS_GROUP, s)
                last_claim = time.monotonic()
            if not batch:
                resp = r.xreadgroup(keys.EVENTS_GROUP, CONSUMER, {keys.EVENTS_STREAM: cursor},
                                    count=200, block=None if cursor == "0" else 2000)
                batch = resp[0][1] if resp else []
                if cursor == "0" and not batch:
                    cursor = ">"
                    continue
            batch = [(mid, f) for mid, f in batch if f]
            if batch:
                try:
                    res = process_batch(r, pool, scripts, s, batch)
                    log.debug("batch", extra={"extra": {"size": len(batch), **res.__dict__}})
                except (redis.ConnectionError, redis.TimeoutError, psycopg.OperationalError):
                    raise
                except Exception:  # noqa: BLE001 — erro de dado/bug: isola a mensagem culpada
                    log.exception("events_batch_failed_isolating")
                    isolate_poison(r, pool, scripts, s, batch)
            errors = 0
        except (redis.ConnectionError, redis.TimeoutError) as exc:
            metrics.ERRORS.labels("worker", "redis").inc()
            log.warning("redis_unavailable", extra={"extra": {"error": str(exc)[:200]}})
            errors, cursor = errors + 1, "0"
            backoff(errors)
        except psycopg.OperationalError as exc:
            # PostgreSQL fora: nada é confirmado; as mensagens continuam pendentes.
            metrics.ERRORS.labels("worker", "postgres").inc()
            log.warning("postgres_unavailable", extra={"extra": {"error": str(exc)[:200]}})
            errors, cursor = errors + 1, "0"
            backoff(errors)
        except Exception:  # noqa: BLE001
            metrics.ERRORS.labels("worker", "unknown").inc()
            log.exception("events_loop_failed")
            errors, cursor = errors + 1, "0"
            backoff(errors)
    pool.close()


def isolate_poison(r, pool, scripts, s: Settings, batch: list[tuple[str, dict]]) -> None:
    """Reprocessa mensagem a mensagem; as que falham por erro não transitório
    vão para a DLQ e são confirmadas, sem travar o restante do stream."""
    for msg_id, fields in batch:
        try:
            process_batch(r, pool, scripts, s, [(msg_id, fields)])
        except (redis.ConnectionError, redis.TimeoutError, psycopg.OperationalError):
            raise
        except Exception as exc:  # noqa: BLE001
            log.error("event_poison", extra={"extra": {"msg_id": msg_id, "error": str(exc)[:300]}})
            dead_letter(r, msg_id, fields, "processing_error")
            r.xack(keys.EVENTS_STREAM, keys.EVENTS_GROUP, msg_id)


def rebuild_loop(s: Settings) -> None:
    r, pool = make_redis(s, socket_timeout=10.0), make_pool(s)
    pool.open(wait=False)
    errors = 0
    while not stop.is_set():
        try:
            ensure_group(r, keys.JOBS_STREAM, keys.JOBS_GROUP)
            batch = claim_stale(r, keys.JOBS_STREAM, keys.JOBS_GROUP, s)
            if not batch:
                resp = r.xreadgroup(keys.JOBS_GROUP, CONSUMER, {keys.JOBS_STREAM: ">"}, count=10, block=2000)
                batch = resp[0][1] if resp else []
            for msg_id, fields in batch:
                handle_job(r, pool, s, msg_id, fields)
            errors = 0
        except (redis.ConnectionError, redis.TimeoutError):
            metrics.ERRORS.labels("worker", "redis").inc()
            errors += 1
            backoff(errors)
        except Exception:  # noqa: BLE001
            log.exception("rebuild_loop_failed")
            errors += 1
            backoff(errors)
    pool.close()


def handle_job(r: redis.Redis, pool, s: Settings, msg_id: str, fields: dict) -> None:
    try:
        uid = int(fields.get("user_id", "0"))
        if uid <= 0:
            raise ValueError
    except ValueError:
        r.xack(keys.JOBS_STREAM, keys.JOBS_GROUP, msg_id)
        metrics.REBUILD_JOBS.labels("invalid").inc()
        return
    # Job obsoleto: o feed já foi gerado depois do pedido e não ficou stale
    # (ex.: o próprio miss síncrono da API já gerou). Descarta sem recalcular.
    meta = r.hmget(keys.feed_meta(uid), "generated_at_ms", "stale")
    job_ms = int(fields.get("ts", "0") or 0) * 1000
    forced = fields.get("reason") == "api"  # POST /v1/recommendations/rebuild sempre regenera
    if not forced and meta[0] and int(meta[0]) >= job_ms and meta[1] == "0":
        r.xack(keys.JOBS_STREAM, keys.JOBS_GROUP, msg_id)
        metrics.REBUILD_JOBS.labels("skipped_fresh").inc()
        return
    token = uuid.uuid4().hex
    if not r.set(keys.lock_build(uid), token, nx=True, ex=30):
        # Outro processo já está gerando este feed (single-flight): descarta o job.
        r.xack(keys.JOBS_STREAM, keys.JOBS_GROUP, msg_id)
        metrics.REBUILD_JOBS.labels("skipped_locked").inc()
        return
    # Libera a deduplicação ANTES de gerar: interações que chegarem durante a
    # geração enfileiram um novo job (coalescido), em vez de serem perdidas até
    # o fim da janela de dedup.
    r.delete(keys.job_pending(uid))
    try:
        build_feed(r, pool, uid, s, trigger="async")
        metrics.REBUILD_JOBS.labels("built").inc()
    except UnknownUser:
        metrics.REBUILD_JOBS.labels("unknown_user").inc()
    finally:
        release_lock(r, keys.lock_build(uid), token)
    # Sem exceção (ou usuário inexistente): confirma. Exceções sobem sem ACK e o
    # job é reentregue via claim_stale após pending_idle_ms.
    r.xack(keys.JOBS_STREAM, keys.JOBS_GROUP, msg_id)


def maintenance_loop(s: Settings, interval: float = 30.0) -> None:
    r, pool = make_redis(s, socket_timeout=10.0), make_pool(s, max_size=2)
    pool.open(wait=False)
    while not stop.is_set():
        try:
            if r.set(keys.LOCK_MAINTENANCE, CONSUMER, nx=True, ex=int(interval * 0.8)):
                if ensure_catalog(r, pool, s):
                    metrics.CATALOG_REBUILDS.inc()
                refresh_trending(r, s)
            for stream, group in ((keys.EVENTS_STREAM, keys.EVENTS_GROUP), (keys.JOBS_STREAM, keys.JOBS_GROUP)):
                for g in r.xinfo_groups(stream) if r.exists(stream) else []:
                    if g["name"] == group:
                        metrics.STREAM_PENDING.labels(stream).set(g.get("pending", 0) or 0)
                        metrics.STREAM_LAG.labels(stream).set(g.get("lag", 0) or 0)
        except (redis.ConnectionError, redis.TimeoutError):
            metrics.ERRORS.labels("worker", "redis").inc()
        except Exception:  # noqa: BLE001
            log.exception("maintenance_failed")
        stop.wait(interval)
    pool.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", choices=["all", "events", "rebuild", "maintenance"], default="all")
    parser.add_argument("--metrics-port", type=int, default=int(os.environ.get("WORKER_METRICS_PORT", "9100")))
    args = parser.parse_args()
    setup_logging(os.environ.get("LOG_LEVEL", "INFO"))
    s = load_settings()
    start_http_server(args.metrics_port)

    def on_signal(signum, _frame):
        log.info("shutdown_requested", extra={"extra": {"signal": signum}})
        stop.set()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    # Em produção, rode "events" e "rebuild" em processos/containers separados:
    # a geração de feed é CPU-bound e, no mesmo processo, disputa o GIL com o
    # consumo de eventos. "all" é conveniente para desenvolvimento.
    roles = {"events": [events_loop, maintenance_loop], "rebuild": [rebuild_loop],
             "maintenance": [maintenance_loop],
             "all": [events_loop, rebuild_loop, maintenance_loop]}[args.role]
    threads = [threading.Thread(target=fn, args=(s,), name=fn.__name__, daemon=True) for fn in roles]
    for t in threads:
        t.start()
    log.info("worker_started", extra={"extra": {"role": args.role, "consumer": CONSUMER}})
    while not stop.is_set():
        stop.wait(1.0)
    # Encerramento gracioso: loops terminam o lote atual (block máx. 2 s).
    for t in threads:
        t.join(timeout=15)
    log.info("worker_stopped")


if __name__ == "__main__":
    main()
