//! Integração da API com Redis real (e PostgreSQL para o fallback final).
//!
//! Requer TEST_REDIS_URL (DB lógico descartável, ex. redis://127.0.0.1:6379/14)
//! e TEST_DATABASE_URL (banco com o schema migrado e dados do seed). Sem as
//! variáveis, os testes retornam cedo e aparecem como "ok" com aviso no stderr.
//! O motor Python é simulado por uma porta fechada (indisponível) — o caminho
//! com o motor real é coberto pelo teste ponta a ponta (scripts/e2e.sh).

use actix_web::{App, http::StatusCode, middleware::from_fn, test, web};
use recommendation_api::{auth, config::Config, configure, keys, observe, state::AppState};
use redis::AsyncCommands;
use serde_json::{Value, json};

const SECRET: &str = "test-secret-test-secret-test-secret!!";
const DEAD_ENGINE: &str = "http://127.0.0.1:9"; // porta "discard": conexão recusada

fn env() -> Option<(String, String)> {
    match (
        std::env::var("TEST_REDIS_URL"),
        std::env::var("TEST_DATABASE_URL"),
    ) {
        (Ok(r), Ok(d)) => Some((r, d)),
        _ => {
            eprintln!(
                "AVISO: TEST_REDIS_URL/TEST_DATABASE_URL ausentes — teste de integração ignorado"
            );
            None
        }
    }
}

fn token(uid: i64) -> String {
    format!(
        "Bearer {}",
        auth::sign(SECRET, "u", uid, chrono::Utc::now().timestamp() + 3600)
    )
}

fn admin() -> String {
    format!(
        "Bearer {}",
        auth::sign(SECRET, "a", 0, chrono::Utc::now().timestamp() + 3600)
    )
}

/// Os testes compartilham um DB lógico do Redis e fazem FLUSHDB: rodam em série.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

type Guard = tokio::sync::MutexGuard<'static, ()>;

async fn setup() -> Option<(
    web::Data<AppState>,
    redis::aio::MultiplexedConnection,
    Guard,
)> {
    let (redis_url, db_url) = env()?;
    let guard = SERIAL.lock().await;
    let state = web::Data::new(
        AppState::new(Config::for_tests(&redis_url, &db_url, DEAD_ENGINE))
            .await
            .unwrap(),
    );
    let client = redis::Client::open(redis_url).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await.unwrap();
    Some((state, conn, guard))
}

macro_rules! app {
    ($state:expr) => {
        test::init_service(
            App::new()
                .app_data($state.clone())
                .wrap(from_fn(observe))
                .configure(|c| configure(c, 64 * 1024)),
        )
        .await
    };
}

/// Grava catálogo + feed no formato que o motor Python grava.
async fn write_feed(
    conn: &mut redis::aio::MultiplexedConnection,
    uid: i64,
    ids: &[i64],
    version: i64,
) {
    for &id in ids {
        let _: () = conn
            .hset_multiple(
                keys::catalog(id),
                &[
                    ("author_id", (id % 5 + 1).to_string()),
                    ("category", "games".into()),
                    ("tags", "rpg,indie".into()),
                    ("title", format!("Conteúdo {id}")),
                    ("published_at", "1760000000".into()),
                    ("status", "active".into()),
                ],
            )
            .await
            .unwrap();
    }
    let n = ids.len() as i64;
    let members: Vec<(i64, i64)> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| (n - i as i64, *id))
        .collect();
    let _: () = conn.zadd_multiple(keys::feed(uid), &members).await.unwrap();
    for id in ids {
        let _: () = conn
            .hset(keys::feed_info(uid), id, format!("0.{id}|interest"))
            .await
            .unwrap();
    }
    let now_ms = chrono::Utc::now().timestamp_millis();
    let _: () = conn
        .hset_multiple(
            keys::feed_meta(uid),
            &[
                ("version", version.to_string()),
                ("generated_at_ms", now_ms.to_string()),
                ("stale", "0".into()),
                ("size", n.to_string()),
                ("strategy", "personalized".into()),
            ],
        )
        .await
        .unwrap();
}

#[actix_web::test]
async fn auth_is_enforced() {
    let Some((state, _, _g)) = setup().await else {
        return;
    };
    let app = app!(state);
    let no_token = test::TestRequest::get().uri("/v1/feed/1").to_request();
    assert_eq!(
        test::call_service(&app, no_token).await.status(),
        StatusCode::UNAUTHORIZED
    );
    let other_user = test::TestRequest::get()
        .uri("/v1/feed/1")
        .insert_header(("authorization", token(2)))
        .to_request();
    let resp = test::call_service(&app, other_user).await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    let body: Value = test::read_body_json(resp).await;
    assert_eq!(body["error"]["code"], "forbidden");
    let not_admin = test::TestRequest::post()
        .uri("/v1/content")
        .insert_header(("authorization", token(1)))
        .set_json(json!({"author_id": 1, "category": "games", "title": "x"}))
        .to_request();
    assert_eq!(
        test::call_service(&app, not_admin).await.status(),
        StatusCode::FORBIDDEN
    );
}

#[actix_web::test]
async fn cached_feed_paginates_with_cursor_and_filters_seen_and_blocked() {
    let Some((state, mut conn, _g)) = setup().await else {
        return;
    };
    let ids: Vec<i64> = (101..=130).collect();
    write_feed(&mut conn, 7, &ids, 555).await;
    let _: () = conn.zadd(keys::seen(7), 103, 1).await.unwrap(); // já visto
    let _: () = conn
        .hset(keys::profile_meta(7), "blocked", "5")
        .await
        .unwrap(); // autor 5 = ids % 5 == 4
    let app = app!(state);

    let req = test::TestRequest::get()
        .uri("/v1/feed/7?limit=10")
        .insert_header(("authorization", token(7)))
        .insert_header(("x-request-id", "rid-1"))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get("x-request-id").unwrap(), "rid-1");
    let page1: Value = test::read_body_json(resp).await;
    assert_eq!(page1["source"], "cache");
    assert_eq!(page1["feed_version"], 555);
    let got: Vec<i64> = page1["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["content_id"].as_i64().unwrap())
        .collect();
    assert_eq!(got.len(), 10);
    assert!(!got.contains(&103), "item já visto é filtrado");
    assert!(
        got.iter().all(|id| id % 5 != 4),
        "autor bloqueado é filtrado"
    );
    assert_eq!(page1["items"][0]["reason"], "interest");

    let cursor = page1["next_cursor"].as_str().unwrap().to_string();
    let req = test::TestRequest::get()
        .uri(&format!("/v1/feed/7?limit=10&cursor={cursor}"))
        .insert_header(("authorization", token(7)))
        .to_request();
    let page2: Value = test::read_body_json(test::call_service(&app, req).await).await;
    let got2: Vec<i64> = page2["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["content_id"].as_i64().unwrap())
        .collect();
    assert!(
        got2.iter().all(|id| !got.contains(id)),
        "páginas não se repetem"
    );
    assert!(got2.first() > got.last());

    // impressões são publicadas de forma assíncrona no stream de eventos
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let n: usize = conn.xlen(keys::EVENTS_STREAM).await.unwrap();
    assert_eq!(n, 2);
}

#[actix_web::test]
async fn stale_feed_is_served_and_rebuild_enqueued_once() {
    let Some((state, mut conn, _g)) = setup().await else {
        return;
    };
    write_feed(&mut conn, 8, &[201, 202, 203], 1).await;
    let _: () = conn.hset(keys::feed_meta(8), "stale", "1").await.unwrap();
    let app = app!(state);
    for _ in 0..3 {
        let req = test::TestRequest::get()
            .uri("/v1/feed/8")
            .insert_header(("authorization", token(8)))
            .to_request();
        let body: Value = test::read_body_json(test::call_service(&app, req).await).await;
        assert_eq!(body["source"], "cache_stale");
        assert_eq!(body["stale"], true);
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await; // enfileiramento é assíncrono
    let jobs: usize = conn.xlen(keys::JOBS_STREAM).await.unwrap();
    assert_eq!(jobs, 1, "deduplicação de regeneração por usuário");
}

#[actix_web::test]
async fn miss_with_engine_down_falls_back_to_trending_then_breaker_opens() {
    let Some((state, mut conn, _g)) = setup().await else {
        return;
    };
    write_feed(&mut conn, 99, &[301, 302, 303, 304], 1).await; // só para popular o catálogo
    let _: () = conn
        .del(&[keys::feed(99), keys::feed_meta(99), keys::feed_info(99)])
        .await
        .unwrap();
    let _: () = conn
        .zadd_multiple(keys::TRENDING, &[(10, 301), (9, 302), (8, 303), (7, 304)])
        .await
        .unwrap();
    let app = app!(state);
    for uid in [21, 22, 23] {
        let req = test::TestRequest::get()
            .uri(&format!("/v1/feed/{uid}?limit=3"))
            .insert_header(("authorization", token(uid)))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["source"], "fallback_trending");
        assert_eq!(body["items"][0]["content_id"], 301);
        assert_eq!(body["items"][0]["reason"], "trending");
    }
    assert!(
        state.breaker.is_open(),
        "breaker abre após falhas consecutivas (limite de teste = 2)"
    );
    let ready: Value = test::read_body_json(
        test::call_service(&app, test::TestRequest::get().uri("/ready").to_request()).await,
    )
    .await;
    assert_eq!(ready["checks"]["engine_breaker"], "open");
    assert_eq!(ready["status"], "degraded");
}

#[actix_web::test]
async fn empty_redis_falls_back_to_recent_contents_from_postgres() {
    let Some((state, _, _g)) = setup().await else {
        return;
    };
    let app = app!(state);
    let req = test::TestRequest::get()
        .uri("/v1/feed/1?limit=5")
        .insert_header(("authorization", token(1)))
        .to_request();
    let body: Value = test::read_body_json(test::call_service(&app, req).await).await;
    assert_eq!(body["source"], "fallback_recent");
    assert_eq!(body["items"].as_array().unwrap().len(), 5);
    assert_eq!(body["items"][0]["reason"], "fallback");
}

#[actix_web::test]
async fn events_are_validated_and_published() {
    let Some((state, mut conn, _g)) = setup().await else {
        return;
    };
    let app = app!(state);
    let event_id = "6a0b8c1e-2f3d-4e5f-8a9b-0c1d2e3f4a5b";
    let ok = test::TestRequest::post().uri("/v1/events").insert_header(("authorization", token(5)))
        .set_json(json!({"events": [{"event_id": event_id, "type": "content_liked", "user_id": 5, "content_id": 42},
                                    {"type": "content_viewed", "user_id": 5, "content_id": 43}]})).to_request();
    let resp = test::call_service(&app, ok).await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let body: Value = test::read_body_json(resp).await;
    assert_eq!(body["accepted"], 2);
    assert_eq!(body["event_ids"][0], event_id);

    let entries: Vec<(String, Vec<(String, String)>)> = redis::cmd("XRANGE")
        .arg(keys::EVENTS_STREAM)
        .arg("-")
        .arg("+")
        .query_async(&mut conn)
        .await
        .unwrap();
    let payload: Value = serde_json::from_str(&entries[0].1[0].1).unwrap();
    assert_eq!(payload["v"], 1);
    assert_eq!(payload["type"], "content_liked");
    assert_eq!(payload["event_id"], event_id);

    for (body, status) in [
        (
            json!({"events": [{"type": "content_liked", "user_id": 6, "content_id": 1}]}),
            StatusCode::FORBIDDEN,
        ),
        (
            json!({"events": [{"type": "feed_impression", "user_id": 5, "content_id": 1}]}),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            json!({"events": [{"type": "content_liked", "user_id": 5, "content_id": 1, "occurred_at": "2001-01-01T00:00:00Z"}]}),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (json!({"events": []}), StatusCode::BAD_REQUEST),
        (
            json!({"events": [{"type": "nope", "user_id": 5, "content_id": 1}]}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"events": [{"type": "content_liked", "user_id": 5, "content_id": 1, "extra": true}]}),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        let req = test::TestRequest::post()
            .uri("/v1/events")
            .insert_header(("authorization", token(5)))
            .set_json(body.clone())
            .to_request();
        assert_eq!(
            test::call_service(&app, req).await.status(),
            status,
            "{body}"
        );
    }
    let n: usize = conn.xlen(keys::EVENTS_STREAM).await.unwrap();
    assert_eq!(n, 2, "eventos rejeitados não são publicados");
}

#[actix_web::test]
async fn rebuild_endpoint_deduplicates_and_input_limits_hold() {
    let Some((state, _, _g)) = setup().await else {
        return;
    };
    let app = app!(state);
    let mut statuses = vec![];
    for _ in 0..2 {
        let req = test::TestRequest::post()
            .uri("/v1/recommendations/rebuild/9")
            .insert_header(("authorization", token(9)))
            .to_request();
        let body: Value = test::read_body_json(test::call_service(&app, req).await).await;
        statuses.push(body["status"].as_str().unwrap().to_string());
    }
    assert_eq!(statuses, ["queued", "already_queued"]);

    for uri in [
        "/v1/feed/9?limit=0",
        "/v1/feed/9?limit=51",
        "/v1/feed/9?cursor=xyz",
        "/v1/feed/abc",
    ] {
        let req = test::TestRequest::get()
            .uri(uri)
            .insert_header(("authorization", admin()))
            .to_request();
        assert_eq!(
            test::call_service(&app, req).await.status(),
            StatusCode::BAD_REQUEST,
            "{uri}"
        );
    }
    let big = "x".repeat(70 * 1024);
    let req = test::TestRequest::post()
        .uri("/v1/events")
        .insert_header(("authorization", admin()))
        .insert_header(("content-type", "application/json"))
        .set_payload(big)
        .to_request();
    assert_eq!(
        test::call_service(&app, req).await.status(),
        StatusCode::BAD_REQUEST,
        "payload acima do limite"
    );
    let metrics =
        test::call_and_read_body(&app, test::TestRequest::get().uri("/metrics").to_request()).await;
    let text = String::from_utf8(metrics.to_vec()).unwrap();
    assert!(
        text.contains("route=\"/v1/feed/{user_id}\""),
        "rota como padrão, nunca o ID"
    );
}
