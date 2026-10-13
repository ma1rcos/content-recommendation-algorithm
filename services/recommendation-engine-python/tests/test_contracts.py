"""Testes de contrato: os mesmos fixtures de contracts/examples são validados
aqui contra os JSON Schemas e, no lado Rust, desserializados nos structs da API
(services/recommendation-api-rust/tests/contracts.rs)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from app.events import EventEnvelope

ROOT = Path(__file__).resolve().parents[3] / "contracts"


def schema(name):
    return Draft202012Validator(json.loads((ROOT / "schemas" / name).read_text()), format_checker=FormatChecker())


def example(name):
    return json.loads((ROOT / "examples" / name).read_text())


@pytest.mark.parametrize("name", ["event-envelope.like.json", "event-envelope.impression.json",
                                  "event-envelope.published.json"])
def test_event_fixtures_match_schema_and_python_model(name):
    data = example(name)
    schema("event-envelope.schema.json").validate(data)
    EventEnvelope.model_validate(data)


def test_schema_and_model_reject_the_same_invalid_events():
    v = schema("event-envelope.schema.json")
    bad = dict(example("event-envelope.like.json"), user_id=None)
    assert not v.is_valid(bad)
    with pytest.raises(ValueError):
        EventEnvelope.model_validate(bad)
    extra = dict(example("event-envelope.like.json"), unexpected=1)
    assert not v.is_valid(extra)
    with pytest.raises(ValueError):
        EventEnvelope.model_validate(extra)


def test_build_and_feed_fixtures_match_schema():
    schema("build-response.schema.json").validate(example("build-response.json"))
    schema("feed-response.schema.json").validate(example("feed-response.json"))


def test_python_build_response_model_matches_schema():
    from app.api import BuildResponse
    model = BuildResponse.model_validate(example("build-response.json"))
    schema("build-response.schema.json").validate(json.loads(model.model_dump_json()))
