"""Integração com Redis e PostgreSQL reais: eventos → perfil → feed."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import orjson
import pytest

from app import keys
from app.events import Scripts, process_batch
from app.feed_builder import build_feed
from app.store import UnknownUser, load_profile, rebuild_profile_from_db

pytestmark = pytest.mark.integration


def envelope(etype, user_id=None, content_id=None, content_ids=None, event_id=None):
    ts = datetime.now(timezone.utc).isoformat()
    return {"payload": orjson.dumps({
        "v": 1, "event_id": event_id or str(uuid.uuid4()), "type": etype, "user_id": user_id,
        "content_id": content_id, "content_ids": content_ids, "occurred_at": ts, "received_at": ts,
        "request_id": None}).decode()}


def msg(fields, n=[0]):  # noqa: B006 — contador de IDs falsos de stream
    n[0] += 1
    return (f"{n[0]}-0", fields)


def a_content(pool, category=None):
    sql = ("SELECT c.id FROM contents c JOIN categories k ON k.id = c.category_id "
           "WHERE c.status = 'active'" + (" AND k.slug = %s" if category else "") + " ORDER BY c.id LIMIT 1")
    with pool.connection() as conn:
        return conn.execute(sql, (category,) if category else ()).fetchone()[0]


def test_event_processing_is_idempotent(catalog_ready, pool, settings):
    r = catalog_ready
    scripts = Scripts.register(r)
    cid = a_content(pool)
    fields = envelope("content_liked", 10, cid)
    likes_before = int(r.hget(keys.stats(cid), "likes") or 0)

    first = process_batch(r, pool, scripts, settings, [msg(fields)])
    again = process_batch(r, pool, scripts, settings, [msg(fields)])  # reentrega do mesmo evento

    assert (first.applied, again.applied, again.duplicates) == (1, 0, 1)
    assert int(r.hget(keys.stats(cid), "likes")) == likes_before + 1
    event_id = orjson.loads(fields["payload"])["event_id"]
    with pool.connection() as conn:
        assert conn.execute("SELECT count(*) FROM interactions WHERE event_id = %s", (event_id,)).fetchone()[0] == 1


def test_invalid_and_unknown_events_go_to_dlq(catalog_ready, pool, settings):
    r = catalog_ready
    scripts = Scripts.register(r)
    bad = [msg({"payload": "{not json"}), msg(envelope("content_liked", 999_999, a_content(pool))),
           msg(envelope("content_liked", 10, 999_999_999))]
    res = process_batch(r, pool, scripts, settings, bad)
    assert res.dead_lettered == 3
    reasons = sorted(e[1]["reason"] for e in r.xrange(keys.EVENTS_DLQ))
    assert reasons == ["invalid", "unknown_content", "unknown_user"]


def test_impression_is_exposure_not_interest(catalog_ready, pool, settings):
    r = catalog_ready
    scripts = Scripts.register(r)
    uid = 11
    rebuild_profile_from_db(r, pool, uid, settings)
    before = load_profile(r, uid)
    cid = a_content(pool, "humor")
    process_batch(r, pool, scripts, settings, [msg(envelope("feed_impression", uid, content_ids=[cid]))])
    after = load_profile(r, uid)
    assert after.categories == before.categories
    assert after.exposure[cid] == 1.0
    assert int(r.hget(keys.stats(cid), "impressions")) >= 1


def test_interactions_update_profile_mark_feed_stale_and_enqueue_rebuild(catalog_ready, pool, settings):
    r = catalog_ready
    scripts = Scripts.register(r)
    uid = 12
    build_feed(r, pool, uid, settings)
    before = load_profile(r, uid).categories.get("pets", 0.0)
    pets = a_content(pool, "pets")
    process_batch(r, pool, scripts, settings, [msg(envelope("content_shared", uid, pets))])

    after = load_profile(r, uid)
    assert after.categories["pets"] == pytest.approx(before * 1.0 + 6.0, rel=0.01)
    assert pets in after.seen and after.recent[0] == pets
    assert r.hget(keys.feed_meta(uid), "stale") == "1"
    jobs = r.xrange(keys.JOBS_STREAM)
    assert [j[1]["user_id"] for j in jobs] == [str(uid)]
    # segunda interação dentro da janela de dedup não enfileira outro job
    process_batch(r, pool, scripts, settings, [msg(envelope("content_liked", uid, a_content(pool, "arte")))])
    assert r.xlen(keys.JOBS_STREAM) == 1


def test_incremental_profile_matches_full_rebuild(catalog_ready, pool, settings):
    r = catalog_ready
    scripts = Scripts.register(r)
    uid = 13
    rebuild_profile_from_db(r, pool, uid, settings)
    for etype, cat in (("content_liked", "games"), ("content_saved", "ciencia"), ("content_viewed", "moda")):
        process_batch(r, pool, scripts, settings, [msg(envelope(etype, uid, a_content(pool, cat)))])
    incremental = load_profile(r, uid)
    r.delete(keys.profile_meta(uid))
    rebuilt = rebuild_profile_from_db(r, pool, uid, settings)
    assert incremental.categories.keys() == rebuilt.categories.keys()
    for k, v in rebuilt.categories.items():
        assert incremental.categories[k] == pytest.approx(v, rel=1e-3)
    assert incremental.interaction_count == rebuilt.interaction_count


def test_feed_is_versioned_and_written_atomically(catalog_ready, pool, settings):
    r = catalog_ready
    res = build_feed(r, pool, 1, settings)
    stored = [int(x) for x in r.zrevrange(keys.feed(1), 0, -1)]
    assert stored == [i.content_id for i in res.items]
    meta = r.hgetall(keys.feed_meta(1))
    assert int(meta["version"]) == res.version and meta["stale"] == "0" and int(meta["size"]) == res.size
    assert 0 < r.ttl(keys.feed(1)) <= settings.feed_ttl_s * 1.11
    assert r.hget(keys.feed_info(1), str(stored[0])).split("|")[1] in {
        "author", "interest", "similar", "trending", "explore", "recent"}
    assert not list(r.scan_iter(match="rec:feed:*:tmp:*"))


def test_recovery_after_redis_loss_reproduces_feed(catalog_ready, pool, settings):
    r = catalog_ready
    first = build_feed(r, pool, 2, settings)
    r.flushdb()  # simula reinício do Redis sem persistência
    second = build_feed(r, pool, 2, settings)
    assert second.profile_rebuilt and r.exists(keys.CATALOG_READY)
    assert [i.content_id for i in second.items[:30]] == [i.content_id for i in first.items[:30]]


def test_unknown_user(catalog_ready, pool, settings):
    with pytest.raises(UnknownUser):
        build_feed(catalog_ready, pool, 987_654, settings)


def test_cold_start_users(catalog_ready, pool, settings):
    declared = build_feed(catalog_ready, pool, 3, settings)   # declarou música
    blank = build_feed(catalog_ready, pool, 4, settings)      # nada declarado
    assert declared.strategy == blank.strategy == "cold_start"
    assert declared.size > 0 and blank.size > 0
