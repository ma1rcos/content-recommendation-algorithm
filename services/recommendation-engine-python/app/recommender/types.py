"""Tipos de dados do pipeline (sem I/O)."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ContentMeta:
    id: int
    author_id: int
    category: str
    tags: tuple[str, ...]
    published_at: int  # epoch segundos, UTC
    status: str = "active"


@dataclass(frozen=True)
class ContentStats:
    views: int = 0
    impressions: int = 0
    engagement: float = 0.0  # soma ponderada de interações explícitas


@dataclass
class UserProfile:
    user_id: int
    categories: dict[str, float] = field(default_factory=dict)
    tags: dict[str, float] = field(default_factory=dict)
    authors: dict[int, float] = field(default_factory=dict)
    recent: list[int] = field(default_factory=list)       # conteúdos com interação positiva recente
    seen: set[int] = field(default_factory=set)           # vistos/interagidos recentemente → filtrados
    exposure: dict[int, float] = field(default_factory=dict)  # impressões sem visualização
    blocked_authors: set[int] = field(default_factory=set)
    interaction_count: int = 0

    @property
    def is_cold(self) -> bool:
        return self.interaction_count == 0


@dataclass(frozen=True)
class Features:
    interest: float
    author: float
    engagement: float
    popularity: float
    recency: float
    similarity: float
    exploration: float
    exposure: float

    def as_dict(self) -> dict[str, float]:
        return {k: round(getattr(self, k), 4) for k in self.__dataclass_fields__}


@dataclass(frozen=True)
class ScoredItem:
    content: ContentMeta
    score: float
    features: Features
    sources: tuple[str, ...]

    @property
    def is_exploration(self) -> bool:
        return "explore" in self.sources or self.features.exploration > 0


@dataclass(frozen=True)
class FeedItem:
    content_id: int
    score: float
    reason: str


@dataclass
class BuildStats:
    candidates_total: int = 0
    candidates_by_source: dict[str, int] = field(default_factory=dict)
    filtered: dict[str, int] = field(default_factory=dict)
    ranked: int = 0
    strategy: str = "personalized"
