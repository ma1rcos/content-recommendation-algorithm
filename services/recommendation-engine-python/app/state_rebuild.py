"""Reconstrução dos dados derivados no Redis a partir do PostgreSQL.

É o caminho de recuperação quando o Redis perde dados (reinício sem
persistência, flush, eviction): catálogo, pools de candidatos, estatísticas,
trending. Perfis são reconstruídos sob demanda, por usuário (store.py).
Impressões e exposição vivem apenas no Redis e NÃO são recuperadas.
"""

from __future__ import annotations

import logging
import time

import redis
from psycopg_pool import ConnectionPool

from . import keys
from .config import INTERACTION_WEIGHTS, STAT_FIELD, Settings
from .recommender.types import ContentMeta
from .store import SQL_CONTENT, now_s, write_content

log = logging.getLogger(__name__)
BATCH = 2000

WEIGHT_CASE = "CASE type " + " ".join(
    f"WHEN '{t}' THEN {w}" for t, w in INTERACTION_WEIGHTS.items()) + " ELSE 0 END"


def refresh_trending(r: redis.Redis, s: Settings, now: int | None = None) -> int:
    """Agrega os buckets horários das últimas N horas com decaimento."""
    now = now or now_s()
    hour = now // 3600
    weights = {keys.trend_bucket(hour - i): 0.5 ** (i / s.trending_half_life_h)
               for i in range(s.trending_hours)}
    pipe = r.pipeline(transaction=True)
    pipe.zunionstore(keys.TRENDING, weights)
    pipe.zremrangebyrank(keys.TRENDING, 0, -(s.trending_size + 1))
    pipe.zcard(keys.TRENDING)
    return pipe.execute()[-1]


def rebuild_catalog(r: redis.Redis, pool: ConnectionPool, s: Settings) -> dict[str, int]:
    t0 = time.monotonic()
    counts = {"contents": 0, "stats": 0, "trend_rows": 0}
    with pool.connection() as conn, conn.transaction():
        # Reconstrução é um job em lote: timeout maior que o das consultas online.
        conn.execute("SET LOCAL statement_timeout = '120s'")
        with conn.cursor(name="catalog_cursor") as cur:  # cursor no servidor: memória limitada
            cur.itersize = BATCH
            cur.execute(SQL_CONTENT + " GROUP BY c.id, cat.slug ORDER BY c.id")
            pipe = r.pipeline(transaction=False)
            for row in cur:
                meta = ContentMeta(row[0], row[1], row[2], tuple(row[6]), int(row[5]), row[4])
                write_content(pipe, meta, row[3], s)
                counts["contents"] += 1
                if counts["contents"] % BATCH == 0:
                    pipe.execute()
            pipe.execute()

        # Estatísticas por conteúdo a partir das interações persistidas.
        cols = ", ".join(f"count(*) FILTER (WHERE type = '{t}')" for t in STAT_FIELD)
        rows = conn.execute(
            f"SELECT content_id, {cols}, sum({WEIGHT_CASE}) FILTER (WHERE type <> 'content_viewed') "
            "FROM interactions GROUP BY content_id").fetchall()
        pipe = r.pipeline(transaction=False)
        total_views = 0
        total_eng = 0.0
        for row in rows:
            cid, *vals, eng = row
            mapping = {field: int(v) for field, v in zip(STAT_FIELD.values(), vals)}
            mapping["eng"] = float(eng or 0)
            pipe.hset(keys.stats(cid), mapping=mapping)
            total_views += mapping["views"]
            total_eng += mapping["eng"]
            counts["stats"] += 1
            if counts["stats"] % BATCH == 0:
                pipe.execute()
        pipe.hset(keys.GLOBAL_STATS, mapping={"views": total_views, "eng": total_eng})
        pipe.execute()

        # Buckets de trending das últimas 24h.
        rows = conn.execute(
            f"SELECT content_id, (EXTRACT(EPOCH FROM occurred_at)::bigint / 3600) AS h, sum({WEIGHT_CASE}) "
            "FROM interactions WHERE occurred_at > now() - make_interval(hours => %s) GROUP BY 1, 2",
            (s.trending_hours,)).fetchall()
    pipe = r.pipeline(transaction=False)
    touched = set()
    for cid, h, w in rows:
        pipe.zincrby(keys.trend_bucket(int(h)), float(w), cid)
        touched.add(int(h))
        counts["trend_rows"] += 1
    for h in touched:
        pipe.expire(keys.trend_bucket(h), (s.trending_hours + 2) * 3600)
    pipe.execute()
    refresh_trending(r, s)
    r.set(keys.CATALOG_READY, str(now_s()))
    counts["elapsed_ms"] = int((time.monotonic() - t0) * 1000)
    log.info("catalog_rebuilt", extra={"extra": counts})
    return counts


def ensure_catalog(r: redis.Redis, pool: ConnectionPool, s: Settings) -> bool:
    """Reconstrói o catálogo se o marcador sumiu. Um lock evita que várias
    réplicas façam o mesmo trabalho em paralelo. Retorna True se reconstruiu."""
    if r.exists(keys.CATALOG_READY):
        return False
    if not r.set(keys.LOCK_CATALOG, "1", nx=True, ex=300):
        return False
    try:
        if r.exists(keys.CATALOG_READY):
            return False
        rebuild_catalog(r, pool, s)
        return True
    finally:
        r.delete(keys.LOCK_CATALOG)
