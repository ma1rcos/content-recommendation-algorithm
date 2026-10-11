"""Geração de feed com I/O: carrega dados do Redis (e do PostgreSQL só quando o
perfil ou o catálogo não estão em cache), chama o pipeline puro e grava o feed
versionado no Redis."""

from __future__ import annotations

import logging
import time
from collections import OrderedDict
from dataclasses import dataclass, field

import redis
from psycopg_pool import ConnectionPool

from . import keys, metrics
from .config import Settings
from .recommender.pipeline import merge_sources, rank_feed
from .recommender.types import BuildStats, FeedItem, UserProfile
from .state_rebuild import ensure_catalog
from .store import (fetch_catalog, fetch_stats, jitter_ttl, load_profile, now_s,
                    prior_engagement_rate, rebuild_profile_from_db)

log = logging.getLogger(__name__)

# Compare-and-delete: só libera o lock se ainda for o dono.
RELEASE_LOCK = """
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
"""


@dataclass
class BuildResult:
    user_id: int
    version: int
    size: int
    generated_at_ms: int
    strategy: str
    timings_ms: dict[str, float] = field(default_factory=dict)
    stats: BuildStats = field(default_factory=BuildStats)
    profile_rebuilt: bool = False
    items: list[FeedItem] = field(default_factory=list)


def collect_sources(r: redis.Redis, profile: UserProfile, s: Settings) -> dict[str, list[int]]:
    lim = s.candidates
    recent_meta = fetch_catalog(r, profile.recent[:10])
    # Tags dos itens consumidos recentemente, priorizadas pelo peso no perfil.
    recent_tags: dict[str, float] = {}
    for m in recent_meta.values():
        for t in m.tags:
            recent_tags[t] = max(recent_tags.get(t, 0.0), profile.tags.get(t, 0.0) + 1e-9)
    top_tags = sorted(recent_tags, key=lambda t: (-recent_tags[t], t))[:lim.tags_top]
    top_authors = sorted(profile.authors, key=lambda a: (-profile.authors[a], a))[:lim.authors_top]
    top_cats = sorted(profile.categories, key=lambda c: (-profile.categories[c], c))[:lim.categories_top]

    pipe = r.pipeline(transaction=False)
    for a in top_authors:
        pipe.zrevrange(keys.pool_author(a), 0, lim.per_author - 1)
    for c in top_cats:
        pipe.zrevrange(keys.pool_cat(c), 0, lim.per_category - 1)
    for t in top_tags:
        pipe.zrevrange(keys.pool_tag(t), 0, lim.per_tag - 1)
    pipe.zrevrange(keys.TRENDING, 0, lim.trending - 1)
    pipe.zrevrange(keys.POOL_RECENT, 0, lim.explore_scan - 1)
    res = pipe.execute()

    def flat(lists):  # intercala as listas para não favorecer o 1º autor/categoria
        out, seen_ = [], set()
        for i in range(max((len(x) for x in lists), default=0)):
            for lst in lists:
                if i < len(lst):
                    v = int(lst[i])
                    if v not in seen_:
                        seen_.add(v)
                        out.append(v)
        return out

    na, nc, nt = len(top_authors), len(top_cats), len(top_tags)
    return {
        "author": flat(res[:na]),
        "category": flat(res[na:na + nc]),
        "similar": flat(res[na + nc:na + nc + nt]),
        "trending": [int(x) for x in res[-2]],
        "explore_scan": [int(x) for x in res[-1]],
        "_recent": list(recent_meta),
    }


@dataclass
class RankInputs:
    """Tudo que o pipeline puro precisa, já carregado do Redis/PostgreSQL."""
    profile: UserProfile
    candidates: OrderedDict
    catalog: dict
    stats: dict
    trending: dict[int, float]
    recent_items: list
    prior: float
    profile_rebuilt: bool
    candidates_ms: float = 0.0
    load_ms: float = 0.0


def load_inputs(r: redis.Redis, pool: ConnectionPool, uid: int, s: Settings) -> RankInputs:
    t0 = time.perf_counter()
    if not r.exists(keys.CATALOG_READY):
        ensure_catalog(r, pool, s)

    profile = load_profile(r, uid)
    rebuilt = False
    if profile is None:
        profile = rebuild_profile_from_db(r, pool, uid, s)  # pode lançar UnknownUser
        rebuilt = True

    src = collect_sources(r, profile, s)
    recent_ids = src.pop("_recent")
    explore_scan = src.pop("explore_scan")
    union = set(explore_scan) | set(recent_ids)
    for lst in src.values():
        union.update(lst)
    catalog = fetch_catalog(r, list(union))
    # Exploração: conteúdos recentes de autores que o usuário ainda não conhece.
    src["explore"] = [c for c in explore_scan
                      if c in catalog and catalog[c].author_id not in profile.authors][:s.candidates.explore]
    for name, cap in (("author", s.candidates.authors_top * s.candidates.per_author),
                      ("category", s.candidates.categories_top * s.candidates.per_category),
                      ("similar", s.candidates.tags_top * s.candidates.per_tag)):
        src[name] = src[name][:cap]
    candidates: OrderedDict = merge_sources(src, s.candidates.total)
    t_cand = time.perf_counter()

    cand_ids = list(candidates)
    trending: dict[int, float] = {}
    if cand_ids:
        scores = r.zmscore(keys.TRENDING, cand_ids)
        trending = {cid: float(sc) for cid, sc in zip(cand_ids, scores) if sc}
    stats = fetch_stats(r, cand_ids)
    prior = prior_engagement_rate(r)
    recent_items = [catalog[c] for c in recent_ids if c in catalog]
    t_load = time.perf_counter()
    return RankInputs(profile, candidates, catalog, stats, trending, recent_items, prior, rebuilt,
                      round((t_cand - t0) * 1000, 2), round((t_load - t_cand) * 1000, 2))


def build_feed(r: redis.Redis, pool: ConnectionPool, uid: int, s: Settings,
               now: int | None = None, trigger: str = "sync") -> BuildResult:
    """trigger: "sync" (chamado pela API num cache miss) ou "async" (worker)."""
    t0 = time.perf_counter()
    started_ms = int(time.time() * 1000)
    now = now or now_s()
    inp = load_inputs(r, pool, uid, s)
    t_load = time.perf_counter()

    bstats = BuildStats()
    items = rank_feed(inp.profile, inp.candidates, inp.catalog, inp.stats, inp.trending, inp.recent_items,
                      inp.prior, now, s, bstats)
    t_rank = time.perf_counter()

    version = int(time.time() * 1000)
    write_feed(r, uid, items, version, bstats.strategy, s, started_ms)
    t_end = time.perf_counter()

    timings = {
        "candidates": inp.candidates_ms,
        "load_features": inp.load_ms,
        "ranking": round((t_rank - t_load) * 1000, 2),
        "write": round((t_end - t_rank) * 1000, 2),
        "total": round((t_end - t0) * 1000, 2),
    }
    metrics.observe_build(timings, bstats, len(items), trigger)
    return BuildResult(uid, version, len(items), version, bstats.strategy, timings, bstats,
                       inp.profile_rebuilt, items)


def write_feed(r: redis.Redis, uid: int, items: list[FeedItem], version: int, strategy: str,
               s: Settings, started_ms: int | None = None) -> None:
    """Grava o feed de forma atômica: escreve em chaves temporárias e faz RENAME
    dentro de MULTI/EXEC, então leitores nunca veem um feed pela metade."""
    ttl = jitter_ttl(s.feed_ttl_s, s.feed_ttl_jitter)
    # Interações chegadas DURANTE a geração não estão neste feed: mantém stale.
    stale_at = int(r.hget(keys.feed_meta(uid), "stale_at_ms") or 0)
    stale = 1 if started_ms is not None and stale_at >= started_ms else 0
    tmp_feed, tmp_info = f"{keys.feed(uid)}:tmp:{version}", f"{keys.feed_info(uid)}:tmp:{version}"
    n = len(items)
    pipe = r.pipeline(transaction=True)
    if items:
        # score do ZSET = posição invertida (n..1): ZREVRANGE devolve a ordem final.
        pipe.zadd(tmp_feed, {str(it.content_id): n - i for i, it in enumerate(items)})
        pipe.hset(tmp_info, mapping={str(it.content_id): f"{it.score:.6f}|{it.reason}" for it in items})
        pipe.rename(tmp_feed, keys.feed(uid))
        pipe.rename(tmp_info, keys.feed_info(uid))
    else:
        pipe.delete(keys.feed(uid), keys.feed_info(uid))
    pipe.hset(keys.feed_meta(uid), mapping={
        "version": version, "generated_at_ms": version, "stale": stale, "size": n, "strategy": strategy,
    })
    for key in (keys.feed(uid), keys.feed_info(uid), keys.feed_meta(uid)):
        pipe.expire(key, ttl)
    pipe.execute()


def release_lock(r: redis.Redis, key: str, token: str) -> None:
    r.eval(RELEASE_LOCK, 1, key, token)
