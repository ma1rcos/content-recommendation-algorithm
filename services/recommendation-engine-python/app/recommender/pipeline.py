"""Pipeline puro (sem I/O): candidatos → filtros → features → ranking → diversidade.

A camada de I/O (app/feed_builder.py) só carrega dados do Redis/PostgreSQL e
chama `rank_feed`; por isso todo o algoritmo é testável com dados em memória.
"""

from __future__ import annotations

from collections import OrderedDict

from ..config import Settings
from .diversity import rerank
from .features import compute_features
from .ranker import linear_score, top_k
from .types import BuildStats, ContentMeta, ContentStats, FeedItem, ScoredItem, UserProfile

SOURCE_ORDER = ("author", "category", "similar", "trending", "explore")


def merge_sources(sources: dict[str, list[int]], total_cap: int) -> "OrderedDict[int, tuple[str, ...]]":
    """Une as listas de cada fonte (já limitadas individualmente), elimina
    duplicatas guardando todas as fontes de cada item e aplica o teto global.
    As fontes são intercaladas (round-robin) para que o teto não descarte uma
    fonte inteira só por ela vir por último."""
    merged: "OrderedDict[int, list[str]]" = OrderedDict()
    lists = [(name, sources.get(name, [])) for name in SOURCE_ORDER if sources.get(name)]
    idx = 0
    while lists and len(merged) < total_cap:
        progressed = False
        for name, items in lists:
            if idx < len(items):
                progressed = True
                cid = items[idx]
                if cid in merged:
                    if name not in merged[cid]:
                        merged[cid].append(name)
                elif len(merged) < total_cap:
                    merged[cid] = [name]
        if not progressed:
            break
        idx += 1
    # Itens já admitidos ainda podem aparecer em fontes cujo índice não foi
    # alcançado; registramos essas fontes extras sem admitir novos itens.
    for name, items in lists:
        for cid in items[idx:]:
            if cid in merged and name not in merged[cid]:
                merged[cid].append(name)
    return OrderedDict((cid, tuple(src)) for cid, src in merged.items())


def eligibility_filter(candidates: "OrderedDict[int, tuple[str, ...]]", catalog: dict[int, ContentMeta],
                       profile: UserProfile, stats: BuildStats) -> list[tuple[ContentMeta, tuple[str, ...]]]:
    """Filtro obrigatório e independente do score: nada aqui é "penalizado",
    é removido. Conta os motivos para métricas."""
    out = []
    reasons = {"missing": 0, "removed": 0, "blocked_author": 0, "seen": 0}
    for cid, src in candidates.items():
        meta = catalog.get(cid)
        if meta is None:
            reasons["missing"] += 1
        elif meta.status != "active":
            reasons["removed"] += 1
        elif meta.author_id in profile.blocked_authors:
            reasons["blocked_author"] += 1
        elif cid in profile.seen:
            reasons["seen"] += 1
        else:
            out.append((meta, src))
    stats.filtered = reasons
    return out


def reason_for(item: ScoredItem) -> str:
    """Motivo principal exibido ao cliente (útil para depuração e explicação)."""
    f = item.features
    contributions = {
        "author": f.author, "interest": f.interest, "similar": f.similarity,
        "trending": f.popularity, "explore": f.exploration,
    }
    best = max(contributions, key=lambda k: (contributions[k], k))
    if contributions[best] <= 0.05:
        return "recent"
    return best


def rank_feed(profile: UserProfile, candidates: "OrderedDict[int, tuple[str, ...]]",
              catalog: dict[int, ContentMeta], stats: dict[int, ContentStats],
              trending: dict[int, float], recent_items: list[ContentMeta],
              prior_rate: float, now: int, settings: Settings,
              build_stats: BuildStats | None = None) -> list[FeedItem]:
    build_stats = build_stats if build_stats is not None else BuildStats()
    build_stats.candidates_total = len(candidates)
    by_source: dict[str, int] = {}
    for src in candidates.values():
        for s in src:
            by_source[s] = by_source.get(s, 0) + 1
    build_stats.candidates_by_source = by_source

    eligible = eligibility_filter(candidates, catalog, profile, build_stats)
    weights = settings.cold_start_weights if profile.is_cold else settings.weights
    build_stats.strategy = "cold_start" if profile.is_cold else "personalized"
    max_trending = max((trending.get(m.id, 0.0) for m, _ in eligible), default=0.0)

    scored = []
    for meta, src in eligible:
        f = compute_features(
            profile, meta, stats.get(meta.id, ContentStats()), trending.get(meta.id, 0.0),
            max_trending, recent_items, src, now, prior_rate,
            settings.recency_half_life_h, settings.engagement_prior_strength, settings.exposure_scale,
        )
        scored.append(ScoredItem(meta, linear_score(f, weights), f, src))

    # Só precisamos de ~3x o tamanho do feed para a etapa de diversidade.
    ranked = top_k(scored, settings.feed_size * 3)
    build_stats.ranked = len(ranked)
    final = rerank(ranked, settings.feed_size, settings.diversity)
    return [FeedItem(it.content.id, round(it.score, 6), reason_for(it)) for it in final]
