"""Acesso a Redis e PostgreSQL usado pelo motor e pelos workers.

Leituras no Redis são sempre feitas em pipeline (um round-trip por lote).
Consultas ao PostgreSQL usam `= ANY(%s)` em lote — nunca uma por item.
"""

from __future__ import annotations

import random
import time
from typing import Iterable

import redis
from psycopg_pool import ConnectionPool

from . import keys
from .config import Settings
from .recommender.profile import ComputedProfile, compute_profile
from .recommender.types import ContentMeta, ContentStats, UserProfile


def now_s() -> int:
    return int(time.time())


def jitter_ttl(base: int, frac: float) -> int:
    """TTL com jitter evita que milhares de feeds expirem no mesmo instante."""
    return max(1, int(base * (1.0 + random.uniform(-frac, frac))))


# ---------------------------------------------------------------- catálogo

def catalog_mapping(meta: ContentMeta, title: str) -> dict[str, str]:
    return {
        "author_id": str(meta.author_id),
        "category": meta.category,
        "tags": ",".join(meta.tags),
        "published_at": str(meta.published_at),
        "status": meta.status,
        "title": title,
    }


def parse_catalog(cid: int, h: dict) -> ContentMeta | None:
    if not h:
        return None
    tags = h.get("tags") or ""
    return ContentMeta(
        id=cid,
        author_id=int(h["author_id"]),
        category=h["category"],
        tags=tuple(t for t in tags.split(",") if t),
        published_at=int(h["published_at"]),
        status=h.get("status", "active"),
    )


def fetch_catalog(r: redis.Redis, ids: Iterable[int]) -> dict[int, ContentMeta]:
    ids = list(ids)
    if not ids:
        return {}
    pipe = r.pipeline(transaction=False)
    for cid in ids:
        pipe.hgetall(keys.catalog(cid))
    out = {}
    for cid, h in zip(ids, pipe.execute()):
        meta = parse_catalog(cid, h)
        if meta:
            out[cid] = meta
    return out


def fetch_stats(r: redis.Redis, ids: list[int]) -> dict[int, ContentStats]:
    if not ids:
        return {}
    pipe = r.pipeline(transaction=False)
    for cid in ids:
        pipe.hmget(keys.stats(cid), "views", "impressions", "eng")
    out = {}
    for cid, (v, i, e) in zip(ids, pipe.execute()):
        out[cid] = ContentStats(int(v or 0), int(i or 0), float(e or 0.0))
    return out


def prior_engagement_rate(r: redis.Redis) -> float:
    views, eng = r.hmget(keys.GLOBAL_STATS, "views", "eng")
    v, e = float(views or 0), float(eng or 0)
    return e / v if v > 100 else 0.3


SQL_CONTENT = """
SELECT c.id, c.author_id, cat.slug, c.title, c.status,
       EXTRACT(EPOCH FROM c.published_at)::bigint,
       COALESCE(array_agg(t.slug ORDER BY t.slug) FILTER (WHERE t.slug IS NOT NULL), '{}')
FROM contents c
JOIN categories cat ON cat.id = c.category_id
LEFT JOIN content_tags ct ON ct.content_id = c.id
LEFT JOIN tags t ON t.id = ct.tag_id
"""


def db_contents(pool: ConnectionPool, ids: list[int]) -> dict[int, tuple[ContentMeta, str]]:
    if not ids:
        return {}
    with pool.connection() as conn:
        rows = conn.execute(SQL_CONTENT + " WHERE c.id = ANY(%s) GROUP BY c.id, cat.slug", (ids,)).fetchall()
    return {
        row[0]: (ContentMeta(row[0], row[1], row[2], tuple(row[6]), int(row[5]), row[4]), row[3])
        for row in rows
    }


def write_content(pipe, meta: ContentMeta, title: str, s: Settings) -> None:
    """Indexa um conteúdo no catálogo e nos pools de candidatos (com limites)."""
    pipe.hset(keys.catalog(meta.id), mapping=catalog_mapping(meta, title))
    if meta.status != "active":
        remove_from_pools(pipe, meta)
        return
    score = meta.published_at
    for key, cap in ((keys.pool_cat(meta.category), s.pool_cat_cap),
                     (keys.pool_author(meta.author_id), s.pool_author_cap),
                     (keys.POOL_RECENT, s.pool_recent_cap)):
        pipe.zadd(key, {meta.id: score})
        pipe.zremrangebyrank(key, 0, -(cap + 1))
    for t in meta.tags:
        pipe.zadd(keys.pool_tag(t), {meta.id: score})
        pipe.zremrangebyrank(keys.pool_tag(t), 0, -(s.pool_tag_cap + 1))


def remove_from_pools(pipe, meta: ContentMeta) -> None:
    pipe.zrem(keys.pool_cat(meta.category), meta.id)
    pipe.zrem(keys.pool_author(meta.author_id), meta.id)
    pipe.zrem(keys.POOL_RECENT, meta.id)
    pipe.zrem(keys.TRENDING, meta.id)
    for t in meta.tags:
        pipe.zrem(keys.pool_tag(t), meta.id)


# ----------------------------------------------------------------- perfil

def load_profile(r: redis.Redis, uid: int) -> UserProfile | None:
    pipe = r.pipeline(transaction=False)
    pipe.hgetall(keys.profile_meta(uid))
    pipe.zrange(keys.profile_cat(uid), 0, -1, withscores=True)
    pipe.zrange(keys.profile_tag(uid), 0, -1, withscores=True)
    pipe.zrange(keys.profile_author(uid), 0, -1, withscores=True)
    pipe.zrevrange(keys.recent(uid), 0, -1)
    pipe.zrange(keys.seen(uid), 0, -1)
    pipe.zrange(keys.exposure(uid), 0, -1, withscores=True)
    meta, cats, tags, authors, recent, seen, exposure = pipe.execute()
    if not meta:
        return None
    blocked = {int(x) for x in (meta.get("blocked") or "").split(",") if x}
    return UserProfile(
        user_id=uid,
        categories=dict(cats),
        tags=dict(tags),
        authors={int(a): w for a, w in authors},
        recent=[int(x) for x in recent],
        seen={int(x) for x in seen},
        exposure={int(c): w for c, w in exposure},
        blocked_authors=blocked,
        interaction_count=int(meta.get("count", 0)),
    )


def write_profile(pipe, uid: int, p: ComputedProfile, blocked: list[int], now: int, rebuilt_at_ms: int,
                  s: Settings) -> None:
    for key in (keys.profile_cat(uid), keys.profile_tag(uid), keys.profile_author(uid),
                keys.recent(uid), keys.seen(uid)):
        pipe.delete(key)
    if p.categories:
        pipe.zadd(keys.profile_cat(uid), p.categories)
    if p.tags:
        pipe.zadd(keys.profile_tag(uid), p.tags)
    if p.authors:
        pipe.zadd(keys.profile_author(uid), {str(a): w for a, w in p.authors.items()})
    if p.recent:
        pipe.zadd(keys.recent(uid), {str(c): ts for c, ts in p.recent})
    if p.seen:
        pipe.zadd(keys.seen(uid), {str(c): ts for c, ts in p.seen})
    pipe.hset(keys.profile_meta(uid), mapping={
        "ts": now,
        "count": p.interaction_count,
        "blocked": ",".join(map(str, sorted(blocked))),
        "rebuilt_at_ms": rebuilt_at_ms,
    })
    for key in (keys.profile_cat(uid), keys.profile_tag(uid), keys.profile_author(uid),
                keys.recent(uid), keys.profile_meta(uid)):
        pipe.expire(key, s.profile_ttl_s)
    pipe.expire(keys.seen(uid), s.seen_ttl_s)


class UnknownUser(Exception):
    pass


def rebuild_profile_from_db(r: redis.Redis, pool: ConnectionPool, uid: int, s: Settings) -> UserProfile:
    """Reconstrói o perfil do usuário a partir das interações persistidas.
    Acionado quando o Redis não tem o perfil (usuário novo ou cache perdido)."""
    since_days = s.profile_history_days
    with pool.connection() as conn:
        with conn.transaction():
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            row = conn.execute(
                "SELECT (EXTRACT(EPOCH FROM now()) * 1000)::bigint, EXISTS(SELECT 1 FROM users WHERE id = %s)",
                (uid,)).fetchone()
            rebuilt_at_ms, exists = row
            if not exists:
                raise UnknownUser(uid)
            declared = [x[0] for x in conn.execute(
                "SELECT c.slug FROM user_declared_interests d JOIN categories c ON c.id = d.category_id "
                "WHERE d.user_id = %s ORDER BY c.slug", (uid,)).fetchall()]
            blocked = [x[0] for x in conn.execute(
                "SELECT author_id FROM user_blocks WHERE user_id = %s", (uid,)).fetchall()]
            rows = conn.execute(
                "SELECT i.type, i.content_id, EXTRACT(EPOCH FROM i.occurred_at)::bigint "
                "FROM interactions i WHERE i.user_id = %s AND i.occurred_at > now() - make_interval(days => %s) "
                "ORDER BY i.occurred_at DESC LIMIT 5000", (uid, since_days)).fetchall()
    content_ids = list({cid for _, cid, _ in rows})
    metas = {cid: m for cid, (m, _) in db_contents(pool, content_ids).items()}
    interactions = [(t, metas[cid], ts) for t, cid, ts in rows if cid in metas]
    now = now_s()
    computed = compute_profile(
        interactions, declared, now, s.profile_half_life_s, s.seen_ttl_s,
        (s.profile_cat_cap, s.profile_tag_cap, s.profile_author_cap), s.recent_cap, s.seen_cap)
    pipe = r.pipeline(transaction=True)
    write_profile(pipe, uid, computed, blocked, now, rebuilt_at_ms, s)
    pipe.execute()
    return load_profile(r, uid) or UserProfile(user_id=uid)
