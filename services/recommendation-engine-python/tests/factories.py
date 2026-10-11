"""Fábricas de dados em memória para os testes unitários do pipeline."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import replace

from app.config import Settings
from app.recommender.types import ContentMeta, ContentStats, UserProfile

NOW = 1_760_000_000  # instante de referência fixo


def content(cid: int, author: int = 1, category: str = "games", tags=("rpg",), age_h: float = 1.0,
            status: str = "active") -> ContentMeta:
    return ContentMeta(cid, author, category, tuple(tags), int(NOW - age_h * 3600), status)


def settings(**kw) -> Settings:
    s = Settings()
    return replace(s, **kw)


def candidates(ids, source="category") -> "OrderedDict[int, tuple[str, ...]]":
    return OrderedDict((i, (source,)) for i in ids)


def profile(uid=1, **kw) -> UserProfile:
    p = UserProfile(user_id=uid)
    for k, v in kw.items():
        setattr(p, k, v)
    return p


def stats_for(ids, views=50, eng=10.0) -> dict[int, ContentStats]:
    return {i: ContentStats(views=views, impressions=views * 3, engagement=eng) for i in ids}
