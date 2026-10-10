"""Configuração do motor via variáveis de ambiente.

Tudo que afeta o ranking (pesos, meias-vidas, limites) fica aqui, com valores
padrão documentados em docs/ALGORITHM.md. Pesos podem ser sobrescritos por
RANKING_WEIGHTS (JSON) ou RANKING_WEIGHTS_FILE (caminho para um JSON).
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field, fields, replace
from typing import Any

# Pesos de cada tipo de interação no perfil, nas estatísticas e no trending.
# Impressão (exposição) não é sinal positivo: peso 0, tratada à parte.
INTERACTION_WEIGHTS: dict[str, float] = {
    "content_viewed": 1.0,
    "content_liked": 3.0,
    "content_commented": 4.0,
    "content_saved": 5.0,
    "content_shared": 6.0,
}
INTERACTION_TYPES = frozenset(INTERACTION_WEIGHTS)
EXPLICIT_TYPES = frozenset(INTERACTION_TYPES - {"content_viewed"})
STAT_FIELD = {
    "content_viewed": "views",
    "content_liked": "likes",
    "content_commented": "comments",
    "content_saved": "saves",
    "content_shared": "shares",
}


@dataclass(frozen=True)
class RankingWeights:
    interest: float = 0.30
    author: float = 0.20
    engagement: float = 0.15
    popularity: float = 0.10
    recency: float = 0.15
    similarity: float = 0.10
    exploration: float = 0.06
    exposure_penalty: float = 0.25

    @classmethod
    def from_mapping(cls, data: dict[str, Any], base: "RankingWeights | None" = None) -> "RankingWeights":
        base = base or cls()
        known = {f.name for f in fields(cls)}
        unknown = set(data) - known
        if unknown:
            raise ValueError(f"pesos desconhecidos: {sorted(unknown)}")
        clean: dict[str, float] = {}
        for k, v in data.items():
            fv = float(v)
            if not 0.0 <= fv <= 10.0:
                raise ValueError(f"peso {k}={fv} fora do intervalo [0, 10]")
            clean[k] = fv
        return replace(base, **clean)


# Usuário sem nenhuma interação: as features de afinidade valem zero, então
# deslocamos massa para sinais que não dependem de histórico.
COLD_START_WEIGHTS = RankingWeights(
    interest=0.30, author=0.0, engagement=0.25, popularity=0.25,
    recency=0.20, similarity=0.0, exploration=0.10, exposure_penalty=0.25,
)


@dataclass(frozen=True)
class CandidateLimits:
    authors_top: int = 15
    per_author: int = 10
    categories_top: int = 4
    per_category: int = 60
    tags_top: int = 6
    per_tag: int = 20
    trending: int = 150
    explore_scan: int = 300
    explore: int = 80
    total: int = 800


@dataclass(frozen=True)
class DiversityConfig:
    author_gap: int = 4            # mesmo autor não reaparece em menos de N posições
    category_window: int = 6       # janela deslizante para categorias
    category_max_in_window: int = 3
    author_max_share: float = 0.08  # fração máxima do feed por autor (mínimo 2 itens)
    similarity_max: float = 0.6    # Jaccard máximo de tags com o item anterior
    explore_every: int = 8         # 1 slot de descoberta a cada N posições
    lookahead: int = 60


@dataclass(frozen=True)
class Settings:
    redis_url: str = "redis://localhost:6379/0"
    database_url: str = "postgresql://postgres@localhost:5432/reco"
    internal_token: str = ""
    feed_size: int = 300
    feed_ttl_s: int = 3600
    feed_ttl_jitter: float = 0.10
    profile_half_life_s: int = 14 * 86400
    profile_ttl_s: int = 90 * 86400
    profile_history_days: int = 90
    recency_half_life_h: float = 48.0
    engagement_prior_strength: float = 20.0
    trending_half_life_h: float = 6.0
    trending_hours: int = 24
    trending_size: int = 1000
    seen_cap: int = 500
    seen_ttl_s: int = 30 * 86400
    exposure_cap: int = 1000
    exposure_ttl_s: int = 7 * 86400
    exposure_scale: float = 3.0
    recent_cap: int = 20
    idempotency_ttl_s: int = 7 * 86400
    events_stream_maxlen: int = 1_000_000
    jobs_stream_maxlen: int = 100_000
    max_event_deliveries: int = 5
    rebuild_dedup_s: int = 120
    pending_idle_ms: int = 60_000
    pool_cat_cap: int = 1000
    pool_author_cap: int = 100
    pool_tag_cap: int = 500
    pool_recent_cap: int = 5000
    profile_cat_cap: int = 50
    profile_tag_cap: int = 100
    profile_author_cap: int = 200
    weights: RankingWeights = field(default_factory=RankingWeights)
    cold_start_weights: RankingWeights = COLD_START_WEIGHTS
    candidates: CandidateLimits = field(default_factory=CandidateLimits)
    diversity: DiversityConfig = field(default_factory=DiversityConfig)


def _int(name: str, default: int) -> int:
    v = os.environ.get(name)
    return int(v) if v not in (None, "") else default


def _float(name: str, default: float) -> float:
    v = os.environ.get(name)
    return float(v) if v not in (None, "") else default


def load_weights() -> RankingWeights:
    weights = RankingWeights()
    path = os.environ.get("RANKING_WEIGHTS_FILE")
    if path:
        with open(path, encoding="utf-8") as fh:
            weights = RankingWeights.from_mapping(json.load(fh), weights)
    raw = os.environ.get("RANKING_WEIGHTS")
    if raw:
        weights = RankingWeights.from_mapping(json.loads(raw), weights)
    return weights


def load_settings() -> Settings:
    d = Settings()
    return Settings(
        redis_url=os.environ.get("REDIS_URL", d.redis_url),
        database_url=os.environ.get("DATABASE_URL", d.database_url),
        internal_token=os.environ.get("INTERNAL_TOKEN", ""),
        feed_size=_int("FEED_SIZE", d.feed_size),
        feed_ttl_s=_int("FEED_TTL_SECONDS", d.feed_ttl_s),
        profile_half_life_s=_int("PROFILE_HALF_LIFE_SECONDS", d.profile_half_life_s),
        recency_half_life_h=_float("RECENCY_HALF_LIFE_HOURS", d.recency_half_life_h),
        rebuild_dedup_s=_int("REBUILD_DEDUP_SECONDS", d.rebuild_dedup_s),
        trending_half_life_h=_float("TRENDING_HALF_LIFE_HOURS", d.trending_half_life_h),
        weights=load_weights(),
    )
