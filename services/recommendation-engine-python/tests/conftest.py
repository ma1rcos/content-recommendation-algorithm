"""Fixtures de integração: PostgreSQL e Redis reais.

Requer TEST_DATABASE_URL (um banco descartável — o schema public é recriado) e
TEST_REDIS_URL (use um DB lógico dedicado, ex.: redis://localhost:6379/15 — ele é
esvaziado). Sem essas variáveis, os testes marcados `integration` são pulados.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import pytest

TEST_DB = os.environ.get("TEST_DATABASE_URL")
TEST_REDIS = os.environ.get("TEST_REDIS_URL")
MIGRATIONS = Path(__file__).resolve().parents[3] / "database" / "migrations"

if TEST_DB and TEST_REDIS:
    # o módulo app.api lê a configuração no import
    os.environ["DATABASE_URL"] = TEST_DB
    os.environ["REDIS_URL"] = TEST_REDIS
    os.environ.setdefault("INTERNAL_TOKEN", "test-internal-token")


def pytest_collection_modifyitems(config, items):
    if TEST_DB and TEST_REDIS:
        return
    skip = pytest.mark.skip(reason="defina TEST_DATABASE_URL e TEST_REDIS_URL para testes de integração")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def settings():
    from app.config import Settings
    return replace(Settings(), redis_url=TEST_REDIS, database_url=TEST_DB, feed_size=100)


@pytest.fixture(scope="session")
def seeded(settings):
    from app.migrate import migrate
    from app.seed import generate, write_db

    with psycopg.connect(TEST_DB, autocommit=True) as conn:
        conn.execute("DROP SCHEMA public CASCADE")
        conn.execute("CREATE SCHEMA public")
    migrate(TEST_DB, MIGRATIONS)
    args = argparse.Namespace(users=200, authors=40, contents=1500, days=30, seed=7, reset=True)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    write_db(TEST_DB, generate(args, now), reset=True)
    return True


@pytest.fixture()
def r(settings, seeded):
    from app.runtime import make_redis
    client = make_redis(settings)
    client.flushdb()
    yield client
    client.close()


@pytest.fixture(scope="session")
def pool(settings, seeded):
    from app.runtime import make_pool
    p = make_pool(settings)
    p.open()
    yield p
    p.close()


@pytest.fixture()
def catalog_ready(r, pool, settings):
    from app.state_rebuild import rebuild_catalog
    rebuild_catalog(r, pool, settings)
    return r
