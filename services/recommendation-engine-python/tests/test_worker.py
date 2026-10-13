"""Worker: jobs de regeneração, recuperação de pendentes e DLQ."""

from __future__ import annotations

import time

import pytest

from app import keys
from app.events import Scripts
from app.feed_builder import build_feed
from workers import main as worker

pytestmark = pytest.mark.integration


def job(r, uid, reason="interaction", ts=None):
    worker.ensure_group(r, keys.JOBS_STREAM, keys.JOBS_GROUP)
    r.xadd(keys.JOBS_STREAM, {"user_id": uid, "reason": reason, "ts": ts or int(time.time())})
    return r.xreadgroup(keys.JOBS_GROUP, "t", {keys.JOBS_STREAM: ">"}, count=1)[0][1][0]


def test_job_builds_feed_and_releases_dedup(catalog_ready, pool, settings):
    r = catalog_ready
    r.set(keys.job_pending(30), "1", ex=60)
    msg_id, fields = job(r, 30)
    worker.handle_job(r, pool, settings, msg_id, fields)
    assert int(r.hget(keys.feed_meta(30), "size")) > 0
    assert not r.exists(keys.job_pending(30)), "dedup liberada no início do job"
    assert not r.xpending(keys.JOBS_STREAM, keys.JOBS_GROUP)["pending"]


def test_obsolete_job_is_skipped_but_api_rebuild_is_forced(catalog_ready, pool, settings):
    r = catalog_ready
    first = build_feed(r, pool, 31, settings)
    msg_id, fields = job(r, 31, ts=int(time.time()) - 60)  # pedido anterior à geração
    worker.handle_job(r, pool, settings, msg_id, fields)
    assert int(r.hget(keys.feed_meta(31), "version")) == first.version
    time.sleep(0.01)
    msg_id, fields = job(r, 31, reason="api", ts=int(time.time()) - 60)
    worker.handle_job(r, pool, settings, msg_id, fields)
    assert int(r.hget(keys.feed_meta(31), "version")) > first.version


def test_stale_pending_messages_are_claimed_and_poison_goes_to_dlq(catalog_ready, pool, settings):
    from dataclasses import replace
    r = catalog_ready
    s = replace(settings, pending_idle_ms=0, max_event_deliveries=1)
    worker.ensure_group(r, keys.EVENTS_STREAM, keys.EVENTS_GROUP)
    r.xadd(keys.EVENTS_STREAM, {"payload": "{}"})
    r.xreadgroup(keys.EVENTS_GROUP, "consumer-morto", {keys.EVENTS_STREAM: ">"})  # entregue e nunca confirmado
    claimed = worker.claim_stale(r, keys.EVENTS_STREAM, keys.EVENTS_GROUP, s)
    assert len(claimed) == 1  # 1ª reivindicação: reprocessa
    again = worker.claim_stale(r, keys.EVENTS_STREAM, keys.EVENTS_GROUP, s)
    assert again == [] and r.xlen(keys.EVENTS_DLQ) == 1  # entregue demais: DLQ
    assert r.xpending(keys.EVENTS_STREAM, keys.EVENTS_GROUP)["pending"] == 0


def test_isolate_poison_keeps_good_events(catalog_ready, pool, settings, monkeypatch):
    from app import events as ev_module
    from .test_integration import a_content, envelope, msg
    r = catalog_ready
    worker.ensure_group(r, keys.EVENTS_STREAM, keys.EVENTS_GROUP)
    good, bad = msg(envelope("content_viewed", 40, a_content(pool))), msg(envelope("content_viewed", 41, a_content(pool)))
    real = ev_module.process_batch

    def flaky(r_, p_, sc_, s_, batch):
        if any(m[0] == bad[0] for m in batch):
            raise RuntimeError("bug simulado")
        return real(r_, p_, sc_, s_, batch)

    monkeypatch.setattr(worker, "process_batch", flaky)
    worker.isolate_poison(r, pool, Scripts.register(r), settings, [good, bad])
    assert r.xlen(keys.EVENTS_DLQ) == 1
    assert r.xrange(keys.EVENTS_DLQ)[0][1]["reason"] == "processing_error"
    assert r.zscore(keys.seen(40), a_content(pool)) is not None
