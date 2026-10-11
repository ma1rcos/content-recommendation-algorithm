"""Score linear configurável + ordenação determinística com top-K."""

from __future__ import annotations

import heapq
from typing import Iterable

from ..config import RankingWeights
from .types import Features, ScoredItem


def linear_score(f: Features, w: RankingWeights) -> float:
    return (
        w.interest * f.interest
        + w.author * f.author
        + w.engagement * f.engagement
        + w.popularity * f.popularity
        + w.recency * f.recency
        + w.similarity * f.similarity
        + w.exploration * f.exploration
        - w.exposure_penalty * f.exposure
    )


def sort_key(item: ScoredItem) -> tuple:
    """Desempate explícito: score ↓, publicação ↓ (mais novo primeiro), id ↑."""
    return (-round(item.score, 9), -item.content.published_at, item.content.id)


def top_k(items: Iterable[ScoredItem], k: int) -> list[ScoredItem]:
    """Seleção top-K em O(n log k) em vez de ordenar a lista inteira."""
    return heapq.nsmallest(k, items, key=sort_key)
