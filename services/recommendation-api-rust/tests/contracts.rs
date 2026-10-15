//! Contrato Rust ↔ Python: os mesmos fixtures de contracts/examples que o
//! Python valida contra os JSON Schemas são desserializados (com
//! deny_unknown_fields) nos structs da API e reserializados sem perda.

use std::path::PathBuf;

use recommendation_api::{
    events::{Envelope, EventType},
    feed::FeedResponse,
};
use serde_json::Value;

fn fixture(name: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../contracts/examples")
        .join(name);
    serde_json::from_str(&std::fs::read_to_string(&path).expect("fixture existe"))
        .expect("JSON válido")
}

/// Normaliza timestamps para comparação (o Rust emite milissegundos).
fn same_instant(a: &Value, b: &Value) -> bool {
    let p = |v: &Value| chrono::DateTime::parse_from_rfc3339(v.as_str().unwrap()).unwrap();
    p(a) == p(b)
}

#[test]
fn event_envelopes_roundtrip() {
    for (name, kind) in [
        ("event-envelope.like.json", EventType::ContentLiked),
        ("event-envelope.impression.json", EventType::FeedImpression),
        ("event-envelope.published.json", EventType::ContentPublished),
    ] {
        let original = fixture(name);
        let env: Envelope = serde_json::from_value(original.clone()).expect(name);
        assert_eq!(env.kind, kind);
        let back = serde_json::to_value(&env).unwrap();
        for (k, v) in original.as_object().unwrap() {
            if k.ends_with("_at") {
                assert!(same_instant(v, &back[k]), "{name}: {k}");
            } else {
                assert_eq!(v, &back[k], "{name}: campo {k}");
            }
        }
        assert_eq!(
            back.as_object().unwrap().len(),
            original.as_object().unwrap().len()
        );
    }
}

#[test]
fn envelope_rejects_unknown_fields_like_the_schema() {
    let mut v = fixture("event-envelope.like.json");
    v["unexpected"] = Value::from(1);
    assert!(serde_json::from_value::<Envelope>(v).is_err());
}

#[test]
fn feed_response_matches_fixture_shape() {
    let original = fixture("feed-response.json");
    let parsed: FeedResponse =
        serde_json::from_value(original.clone()).expect("feed-response.json");
    assert_eq!(serde_json::to_value(&parsed).unwrap(), original);
}
