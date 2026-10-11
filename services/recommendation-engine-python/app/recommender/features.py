"""Cálculo de características. Todas as features retornam valores em [0, 1],
para que os pesos sejam comparáveis entre si (ver docs/ALGORITHM.md)."""

from __future__ import annotations

import hashlib
import math

from .types import ContentMeta, ContentStats, Features, UserProfile

LN2 = math.log(2.0)


def clamp01(x: float) -> float:
    if x != x:  # NaN
        return 0.0
    return 0.0 if x < 0.0 else 1.0 if x > 1.0 else x


def max_normalize(weights: dict, key) -> float:
    """Afinidade relativa ao maior peso do perfil: 1.0 = interesse principal."""
    if not weights:
        return 0.0
    top = max(weights.values())
    if top <= 0:
        return 0.0
    return clamp01(weights.get(key, 0.0) / top)


def interest_score(profile: UserProfile, content: ContentMeta) -> float:
    cat = max_normalize(profile.categories, content.category)
    if not content.tags or not profile.tags:
        return clamp01(cat)
    top = max(profile.tags.values())
    tag = sum(profile.tags.get(t, 0.0) for t in content.tags) / (top * len(content.tags)) if top > 0 else 0.0
    return clamp01(0.7 * cat + 0.3 * tag)


def jaccard(a: tuple[str, ...] | frozenset, b: tuple[str, ...] | frozenset) -> float:
    sa, sb = set(a), set(b)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def similarity_score(content: ContentMeta, recent_items: list[ContentMeta]) -> float:
    """Similaridade conteúdo-conteúdo: maior Jaccard de tags com algo que o
    usuário consumiu recentemente (mesma categoria soma um pequeno bônus)."""
    best = 0.0
    for r in recent_items:
        if r.id == content.id:
            continue
        s = jaccard(content.tags, r.tags)
        if r.category == content.category:
            s = 0.8 * s + 0.2
        if s > best:
            best = s
    return clamp01(best)


def engagement_score(stats: ContentStats, prior_rate: float, strength: float) -> float:
    """Taxa de engajamento por visualização com suavização bayesiana.

    smoothed = (eng + m·p) / (views + m); retorna smoothed / (smoothed + p),
    que vale 0.5 quando o conteúdo está exatamente na média global e não
    explode para conteúdos com poucas visualizações.
    """
    p = max(prior_rate, 1e-6)
    smoothed = (stats.engagement + strength * p) / (stats.views + strength)
    return clamp01(smoothed / (smoothed + p))


def popularity_score(trending: float, max_trending: float) -> float:
    if trending <= 0 or max_trending <= 0:
        return 0.0
    return clamp01(math.log1p(trending) / math.log1p(max_trending))


def recency_score(published_at: int, now: int, half_life_h: float) -> float:
    age_h = max(0.0, (now - published_at) / 3600.0)
    return clamp01(math.exp(-LN2 * age_h / half_life_h))


def exposure_score(impressions_without_view: float, scale: float) -> float:
    if impressions_without_view <= 0:
        return 0.0
    return clamp01(1.0 - math.exp(-impressions_without_view / scale))


def stable_uniform(*parts: object) -> float:
    """Pseudoaleatório determinístico em [0, 1): mesma entrada → mesmo valor.
    Usado na exploração para manter o ranking reproduzível em testes."""
    h = hashlib.blake2b(":".join(map(str, parts)).encode(), digest_size=8).digest()
    return int.from_bytes(h, "big") / 2**64


def exploration_score(profile: UserProfile, content: ContentMeta, stats: ContentStats,
                      sources: tuple[str, ...], now: int) -> float:
    """Descoberta controlada: autor desconhecido E conteúdo ainda pouco exposto
    (novo ou com poucas views). O valor varia de forma determinística por
    (usuário, conteúdo, dia) para não promover sempre os mesmos itens."""
    unknown_author = content.author_id not in profile.authors
    fresh = stats.views < 30 or (now - content.published_at) < 86400
    if not (unknown_author and (fresh or "explore" in sources)):
        return 0.0
    day = now // 86400
    return 0.5 + 0.5 * stable_uniform(profile.user_id, content.id, day)


def compute_features(profile: UserProfile, content: ContentMeta, stats: ContentStats,
                     trending: float, max_trending: float, recent_items: list[ContentMeta],
                     sources: tuple[str, ...], now: int, prior_rate: float,
                     recency_half_life_h: float, prior_strength: float,
                     exposure_scale: float) -> Features:
    return Features(
        interest=interest_score(profile, content),
        author=max_normalize(profile.authors, content.author_id),
        engagement=engagement_score(stats, prior_rate, prior_strength),
        popularity=popularity_score(trending, max_trending),
        recency=recency_score(content.published_at, now, recency_half_life_h),
        similarity=similarity_score(content, recent_items),
        exploration=exploration_score(profile, content, stats, sources, now),
        exposure=exposure_score(profile.exposure.get(content.id, 0.0), exposure_scale),
    )
