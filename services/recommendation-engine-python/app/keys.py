"""Convenção de chaves Redis — espelhada em services/recommendation-api-rust/src/keys.rs
e documentada em contracts/redis-keys.md. Todas têm o prefixo `rec:`.
"""

EVENTS_STREAM = "rec:events"
EVENTS_GROUP = "rec-workers"
EVENTS_DLQ = "rec:events:dlq"
JOBS_STREAM = "rec:jobs:rebuild"
JOBS_GROUP = "rec-builders"

CATALOG_READY = "rec:catalog:ready"
TRENDING = "rec:trending"
POOL_RECENT = "rec:pool:recent"
GLOBAL_STATS = "rec:stats:global"
LOCK_CATALOG = "rec:lock:catalog"
LOCK_MAINTENANCE = "rec:lock:maintenance"


def catalog(cid: int) -> str:
    return f"rec:catalog:{cid}"


def stats(cid: int) -> str:
    return f"rec:stats:{cid}"


def pool_cat(category: str) -> str:
    return f"rec:pool:cat:{category}"


def pool_author(aid: int) -> str:
    return f"rec:pool:author:{aid}"


def pool_tag(tag: str) -> str:
    return f"rec:pool:tag:{tag}"


def trend_bucket(hour: int) -> str:
    """hour = epoch_seconds // 3600 (UTC)."""
    return f"rec:trend:{hour}"


def profile_cat(uid: int) -> str:
    return f"rec:profile:cat:{uid}"


def profile_tag(uid: int) -> str:
    return f"rec:profile:tag:{uid}"


def profile_author(uid: int) -> str:
    return f"rec:profile:author:{uid}"


def profile_meta(uid: int) -> str:
    return f"rec:profile:meta:{uid}"


def recent(uid: int) -> str:
    return f"rec:recent:{uid}"


def seen(uid: int) -> str:
    return f"rec:seen:{uid}"


def exposure(uid: int) -> str:
    return f"rec:exposure:{uid}"


def feed(uid: int) -> str:
    return f"rec:feed:{uid}"


def feed_info(uid: int) -> str:
    return f"rec:feed:info:{uid}"


def feed_meta(uid: int) -> str:
    return f"rec:feed:meta:{uid}"


def lock_build(uid: int) -> str:
    return f"rec:lock:build:{uid}"


def job_pending(uid: int) -> str:
    return f"rec:job:pending:{uid}"


def event_done(event_id: str) -> str:
    return f"rec:evt:{event_id}"
