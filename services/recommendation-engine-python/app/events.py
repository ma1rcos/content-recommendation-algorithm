"""Processamento de eventos do stream `rec:events` (idempotente).

Ordem por lote:
 1. valida o envelope (contrato em contracts/schemas/event-envelope.schema.json);
 2. persiste interações no PostgreSQL com ON CONFLICT DO NOTHING (event_id é PK);
 3. aplica os efeitos no Redis via Lua, que também checa a chave de idempotência;
 4. XACK.
Se o processo cair entre 2 e 4, a reentrega é segura: o INSERT vira no-op e o
Lua detecta `rec:evt:{event_id}` já aplicado.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import orjson
import redis
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from . import keys, metrics
from .config import EXPLICIT_TYPES, INTERACTION_TYPES, INTERACTION_WEIGHTS, STAT_FIELD, Settings
from .recommender.types import ContentMeta
from .store import db_contents, fetch_catalog, remove_from_pools, write_content

log = logging.getLogger(__name__)
LUA_DIR = Path(__file__).parent / "lua"
MAX_FUTURE_SKEW_S = 300

EventType = Literal["content_viewed", "content_liked", "content_saved", "content_shared",
                    "content_commented", "feed_impression", "content_published", "content_removed"]


class EventEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    v: Literal[1]
    event_id: str = Field(pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    type: EventType
    user_id: int | None = Field(default=None, ge=1)
    content_id: int | None = Field(default=None, ge=1)
    content_ids: list[int] | None = Field(default=None, max_length=100)
    occurred_at: datetime
    received_at: datetime
    request_id: str | None = Field(default=None, max_length=64)

    @model_validator(mode="after")
    def _check(self) -> "EventEnvelope":
        if self.occurred_at.tzinfo is None:
            raise ValueError("occurred_at precisa de fuso horário (UTC)")
        if self.type in INTERACTION_TYPES and (self.user_id is None or self.content_id is None):
            raise ValueError("interação exige user_id e content_id")
        if self.type == "feed_impression" and (self.user_id is None or not self.content_ids):
            raise ValueError("feed_impression exige user_id e content_ids")
        if self.type in ("content_published", "content_removed") and self.content_id is None:
            raise ValueError(f"{self.type} exige content_id")
        return self

    @property
    def ts(self) -> int:
        return int(self.occurred_at.astimezone(timezone.utc).timestamp())


@dataclass
class Scripts:
    interaction: object
    impression: object

    @classmethod
    def register(cls, r: redis.Redis) -> "Scripts":
        return cls(
            interaction=r.register_script((LUA_DIR / "apply_interaction.lua").read_text()),
            impression=r.register_script((LUA_DIR / "apply_impression.lua").read_text()),
        )


@dataclass
class BatchResult:
    applied: int = 0
    duplicates: int = 0
    dead_lettered: int = 0


def parse(fields: dict) -> EventEnvelope:
    raw = fields.get("payload")
    if raw is None:
        raise ValueError("mensagem sem campo payload")
    return EventEnvelope.model_validate(orjson.loads(raw))


def dead_letter(r: redis.Redis, msg_id: str, fields: dict, reason: str) -> None:
    r.xadd(keys.EVENTS_DLQ, {"original_id": msg_id, "reason": reason,
                             "payload": fields.get("payload", "")}, maxlen=10_000, approximate=True)
    metrics.DLQ.labels(reason).inc()


INSERT_SQL = """
WITH data AS (
  SELECT * FROM unnest(%s::uuid[], %s::bigint[], %s::bigint[], %s::text[], %s::timestamptz[])
         AS t(event_id, user_id, content_id, type, occurred_at)
), ins AS (
  INSERT INTO interactions (event_id, user_id, content_id, type, occurred_at)
  SELECT * FROM data
  ON CONFLICT (event_id) DO NOTHING
  RETURNING event_id, ingested_at
)
SELECT event_id::text, (EXTRACT(EPOCH FROM ingested_at) * 1000)::bigint FROM ins
UNION ALL
SELECT i.event_id::text, (EXTRACT(EPOCH FROM i.ingested_at) * 1000)::bigint
FROM interactions i JOIN data d ON d.event_id = i.event_id
"""


def process_batch(r: redis.Redis, pool: ConnectionPool, scripts: Scripts, s: Settings,
                  messages: list[tuple[str, dict]]) -> BatchResult:
    """Processa um lote. Lança exceção em falha de dependência (Redis/PostgreSQL):
    nesse caso nada é confirmado e o lote volta a ser entregue."""
    res = BatchResult()
    now = int(time.time())
    to_ack: list[str] = []
    interactions: list[tuple[str, EventEnvelope]] = []
    others: list[tuple[str, EventEnvelope]] = []

    for msg_id, fields in messages:
        try:
            ev = parse(fields)
            if ev.ts > now + MAX_FUTURE_SKEW_S:
                raise ValueError("occurred_at no futuro")
        except (ValueError, ValidationError, orjson.JSONDecodeError) as exc:
            dead_letter(r, msg_id, fields, "invalid")
            log.warning("event_invalid", extra={"extra": {"msg_id": msg_id, "error": str(exc)[:200]}})
            res.dead_lettered += 1
            to_ack.append(msg_id)
            continue
        (interactions if ev.type in INTERACTION_TYPES else others).append((msg_id, ev))

    if interactions:
        _process_interactions(r, pool, scripts, s, interactions, now, res, to_ack)
    for msg_id, ev in others:
        _process_other(r, pool, scripts, s, msg_id, ev, now, res)
        to_ack.append(msg_id)

    if to_ack:
        r.xack(keys.EVENTS_STREAM, keys.EVENTS_GROUP, *to_ack)
    return res


def _process_interactions(r, pool, scripts, s, items, now, res, to_ack) -> None:
    user_ids = sorted({ev.user_id for _, ev in items})
    content_ids = sorted({ev.content_id for _, ev in items})
    catalog = fetch_catalog(r, content_ids)
    missing = [c for c in content_ids if c not in catalog]
    if missing:
        catalog.update({cid: m for cid, (m, _) in db_contents(pool, missing).items()})
    with pool.connection() as conn:
        known_users = {row[0] for row in conn.execute(
            "SELECT id FROM users WHERE id = ANY(%s)", (user_ids,)).fetchall()}

    valid: list[tuple[str, EventEnvelope, ContentMeta]] = []
    for msg_id, ev in items:
        if ev.user_id not in known_users:
            reason = "unknown_user"
        elif ev.content_id not in catalog:
            reason = "unknown_content"
        else:
            valid.append((msg_id, ev, catalog[ev.content_id]))
            continue
        dead_letter(r, msg_id, {"payload": ev.model_dump_json()}, reason)
        res.dead_lettered += 1
        to_ack.append(msg_id)
    if not valid:
        return

    with pool.connection() as conn:
        rows = conn.execute(INSERT_SQL, (
            [ev.event_id for _, ev, _ in valid], [ev.user_id for _, ev, _ in valid],
            [ev.content_id for _, ev, _ in valid], [ev.type for _, ev, _ in valid],
            [ev.occurred_at for _, ev, _ in valid])).fetchall()
    ingested = dict(rows)

    pipe = r.pipeline(transaction=False)
    hour = now // 3600
    for _, ev, meta in valid:
        uid = ev.user_id
        scripts.interaction(
            keys=[keys.event_done(ev.event_id), keys.stats(meta.id), keys.trend_bucket(hour),
                  keys.GLOBAL_STATS, keys.profile_meta(uid), keys.profile_cat(uid), keys.profile_tag(uid),
                  keys.profile_author(uid), keys.seen(uid), keys.recent(uid), keys.feed_meta(uid),
                  keys.job_pending(uid), keys.JOBS_STREAM],
            args=[meta.id, uid, ev.ts, now, ingested.get(ev.event_id, 0),
                  INTERACTION_WEIGHTS[ev.type], STAT_FIELD[ev.type], 1 if ev.type in EXPLICIT_TYPES else 0,
                  meta.category, meta.author_id, ",".join(meta.tags), s.profile_half_life_s,
                  s.idempotency_ttl_s, (s.trending_hours + 2) * 3600, s.seen_cap, s.seen_ttl_s,
                  s.recent_cap, s.profile_ttl_s, s.profile_cat_cap, s.profile_tag_cap,
                  s.profile_author_cap, s.jobs_stream_maxlen, s.rebuild_dedup_s],
            client=pipe)
    results = pipe.execute()
    for (msg_id, ev, _), out in zip(valid, results):
        to_ack.append(msg_id)
        if out == 0:
            res.duplicates += 1
            metrics.EVENTS.labels(ev.type, "duplicate").inc()
        else:
            res.applied += 1
            metrics.EVENTS.labels(ev.type, "applied").inc()
            metrics.EVENT_LAG.observe(max(0.0, time.time() - ev.occurred_at.timestamp()))


def _process_other(r, pool, scripts, s, msg_id, ev: EventEnvelope, now, res) -> None:
    if ev.type == "feed_impression":
        out = scripts.impression(
            keys=[keys.event_done(ev.event_id), keys.exposure(ev.user_id), keys.GLOBAL_STATS],
            args=[s.idempotency_ttl_s, s.exposure_cap, s.exposure_ttl_s, *ev.content_ids])
    else:  # content_published / content_removed: reindexa a partir da fonte de verdade
        if not r.set(keys.event_done(ev.event_id), "1", nx=True, ex=s.idempotency_ttl_s):
            out = 0
        else:
            found = db_contents(pool, [ev.content_id])
            pipe = r.pipeline(transaction=False)
            if ev.content_id in found:
                meta, title = found[ev.content_id]
                write_content(pipe, meta, title, s)
                if meta.status != "active":
                    remove_from_pools(pipe, meta)
            pipe.execute()
            out = 1
    if out == 0:
        res.duplicates += 1
        metrics.EVENTS.labels(ev.type, "duplicate").inc()
    else:
        res.applied += 1
        metrics.EVENTS.labels(ev.type, "applied").inc()
        metrics.EVENT_LAG.observe(max(0.0, time.time() - ev.occurred_at.timestamp()))
