import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from jsonschema import Draft202012Validator

pytestmark = pytest.mark.integration
SCHEMA = json.loads((Path(__file__).resolve().parents[3] / "contracts/schemas/build-response.schema.json").read_text())


@pytest.fixture()
def client(catalog_ready):
    from app.api import app
    with TestClient(app) as c:
        yield c


def test_health_and_ready(client):
    assert client.get("/health").json() == {"status": "ok"}
    body = client.get("/ready").json()
    assert body["checks"] == {"redis": "ok", "postgres": "ok"}


def test_build_requires_internal_token(client):
    assert client.post("/internal/v1/feeds/1/build").status_code == 401


def test_build_response_follows_contract(client):
    resp = client.post("/internal/v1/feeds/1/build", headers={"x-internal-token": "test-internal-token",
                                                               "x-request-id": "abc"})
    assert resp.status_code == 200 and resp.headers["x-request-id"] == "abc"
    Draft202012Validator(SCHEMA).validate(resp.json())


def test_unknown_user_is_404(client):
    resp = client.post("/internal/v1/feeds/987654/build", headers={"x-internal-token": "test-internal-token"})
    assert resp.status_code == 404 and resp.json()["error"]["code"] == "user_not_found"


def test_metrics_exposed(client):
    client.post("/internal/v1/feeds/1/build", headers={"x-internal-token": "test-internal-token"})
    text = client.get("/metrics").text
    assert "reco_engine_build_duration_seconds" in text and "user_id" not in text
