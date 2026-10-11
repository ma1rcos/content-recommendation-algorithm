"""Métricas Prometheus do motor e dos workers.
Nenhum label usa IDs de usuário/conteúdo (cardinalidade limitada)."""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

from .recommender.types import BuildStats

STAGE_BUCKETS = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5)

BUILD_DURATION = Histogram("reco_engine_build_duration_seconds", "Tempo de geração do feed por etapa",
                           ["stage"], buckets=STAGE_BUCKETS)
BUILDS = Counter("reco_engine_builds_total", "Feeds gerados", ["strategy", "trigger"])
CANDIDATES = Counter("reco_engine_candidates_total", "Candidatos gerados por fonte", ["source"])
FILTERED = Counter("reco_engine_candidates_filtered_total", "Candidatos removidos por motivo", ["reason"])
FEED_SIZE = Histogram("reco_engine_feed_size", "Itens por feed gerado",
                      buckets=(0, 10, 25, 50, 100, 150, 200, 250, 300, 400))
ERRORS = Counter("reco_errors_total", "Erros por componente e dependência", ["component", "dependency"])

EVENTS = Counter("reco_worker_events_total", "Eventos processados", ["type", "result"])
EVENT_LAG = Histogram("reco_worker_event_lag_seconds", "Atraso entre o evento e seu processamento",
                      buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 300))
STREAM_PENDING = Gauge("reco_worker_stream_pending", "Mensagens entregues e não confirmadas", ["stream"])
STREAM_LAG = Gauge("reco_worker_stream_lag", "Mensagens ainda não entregues ao grupo", ["stream"])
DLQ = Counter("reco_worker_dead_letters_total", "Mensagens movidas para a DLQ", ["reason"])
REBUILD_JOBS = Counter("reco_worker_rebuild_jobs_total", "Jobs de regeneração de feed", ["result"])
CATALOG_REBUILDS = Counter("reco_catalog_rebuilds_total", "Reconstruções do catálogo a partir do PostgreSQL")


def observe_build(timings_ms: dict[str, float], stats: BuildStats, size: int, trigger: str = "sync") -> None:
    for stage, ms in timings_ms.items():
        BUILD_DURATION.labels(stage).observe(ms / 1000.0)
    BUILDS.labels(stats.strategy, trigger).inc()
    for src, n in stats.candidates_by_source.items():
        CANDIDATES.labels(src).inc(n)
    for reason, n in stats.filtered.items():
        if n:
            FILTERED.labels(reason).inc(n)
    FEED_SIZE.observe(size)
