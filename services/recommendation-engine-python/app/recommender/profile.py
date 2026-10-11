"""Perfil de interesses com decaimento exponencial no tempo.

peso(T) = Σ w_tipo · 0.5^((T − t_i) / meia_vida)

A mesma fórmula é usada (a) na reconstrução a partir do PostgreSQL (aqui) e
(b) incrementalmente no Redis pelo script Lua app/lua/apply_event.lua, que
decai o perfil armazenado até "agora" e soma a nova contribuição. As duas
formas produzem o mesmo resultado (ver tests/test_profile.py).
"""

from __future__ import annotations

from dataclasses import dataclass

from ..config import INTERACTION_WEIGHTS
from .types import ContentMeta

TAG_FACTOR = 0.5  # cada tag recebe metade do peso da interação


def decay(age_s: float, half_life_s: float) -> float:
    return 0.5 ** (max(0.0, age_s) / half_life_s)


@dataclass
class ComputedProfile:
    categories: dict[str, float]
    tags: dict[str, float]
    authors: dict[int, float]
    recent: list[tuple[int, int]]   # (content_id, ts) mais recentes primeiro
    seen: list[tuple[int, int]]
    interaction_count: int


def _cap(d: dict, cap: int) -> dict:
    if len(d) <= cap:
        return d
    return dict(sorted(d.items(), key=lambda kv: (-kv[1], str(kv[0])))[:cap])


def compute_profile(interactions: list[tuple[str, ContentMeta, int]], declared: list[str], now: int,
                    half_life_s: float, seen_window_s: int, caps: tuple[int, int, int],
                    recent_cap: int, seen_cap: int) -> ComputedProfile:
    cats: dict[str, float] = {c: 1.0 for c in declared}
    tags: dict[str, float] = {}
    authors: dict[int, float] = {}
    last_seen: dict[int, int] = {}
    for etype, meta, ts in interactions:
        w = INTERACTION_WEIGHTS.get(etype)
        if w is None:
            continue
        contrib = w * decay(now - ts, half_life_s)
        cats[meta.category] = cats.get(meta.category, 0.0) + contrib
        authors[meta.author_id] = authors.get(meta.author_id, 0.0) + contrib
        for t in meta.tags:
            tags[t] = tags.get(t, 0.0) + contrib * TAG_FACTOR
        if ts > last_seen.get(meta.id, -1):
            last_seen[meta.id] = ts
    ordered = sorted(last_seen.items(), key=lambda kv: (-kv[1], kv[0]))
    return ComputedProfile(
        categories=_cap(cats, caps[0]),
        tags=_cap(tags, caps[1]),
        authors=_cap(authors, caps[2]),
        recent=ordered[:recent_cap],
        seen=[(cid, ts) for cid, ts in ordered if now - ts <= seen_window_s][:seen_cap],
        interaction_count=len(interactions),
    )
