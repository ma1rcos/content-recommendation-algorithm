//! Handlers HTTP.

use std::collections::HashMap;

use actix_web::{HttpMessage, HttpRequest, HttpResponse, web};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::Row;
use uuid::Uuid;

use crate::{
    auth::Identity,
    error::{ApiError, ApiResult},
    events::{self, Envelope, EventType},
    feed::{self, Cursor},
    keys, metrics,
    state::AppState,
};

#[derive(Clone)]
pub struct RequestId(pub String);

fn request_id(req: &HttpRequest) -> String {
    req.extensions()
        .get::<RequestId>()
        .map(|r| r.0.clone())
        .unwrap_or_default()
}

fn valid_slug(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 40
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
}

fn check_id(id: i64) -> ApiResult<i64> {
    if id >= 1 {
        Ok(id)
    } else {
        Err(ApiError::BadRequest("identificador inválido".into()))
    }
}

/// Limite de taxa em janela fixa compartilhado entre réplicas (Redis).
/// Em falha do Redis, deixa passar (fail-open): rate limit não pode derrubar a API.
async fn rate_limit(
    state: &AppState,
    scope: &str,
    who: &str,
    limit: u64,
    window_secs: u64,
) -> ApiResult<()> {
    let window = Utc::now().timestamp() as u64 / window_secs;
    let key = keys::rate_limit(scope, who, window);
    let mut conn = state.redis();
    let res: Result<(u64,), _> = redis::pipe()
        .cmd("INCR")
        .arg(&key)
        .cmd("EXPIRE")
        .arg(&key)
        .arg(window_secs + 1)
        .ignore()
        .query_async(&mut conn)
        .await;
    match res {
        Ok((n,)) if n > limit => Err(ApiError::RateLimited),
        Ok(_) => Ok(()),
        Err(_) => {
            metrics::dependency_error("redis");
            Ok(())
        }
    }
}

// ------------------------------------------------------------------ saúde

pub async fn health() -> HttpResponse {
    HttpResponse::Ok().json(json!({"status": "ok"}))
}

pub async fn ready(state: web::Data<AppState>) -> HttpResponse {
    let mut conn = state.redis();
    let redis_ok = redis::cmd("PING")
        .query_async::<String>(&mut conn)
        .await
        .is_ok();
    let pg_ok = sqlx::query("SELECT 1").execute(&state.pg).await.is_ok();
    let engine_ok = state
        .http
        .get(format!("{}/health", state.cfg.engine_url))
        .timeout(std::time::Duration::from_millis(300))
        .send()
        .await
        .map(|r| r.status().is_success())
        .unwrap_or(false);
    let st = |ok: bool| if ok { "ok" } else { "error" };
    // Redis é obrigatório (feeds e eventos). PostgreSQL e motor degradam com fallback.
    let body = json!({
        "status": if redis_ok && pg_ok && engine_ok { "ok" } else if redis_ok { "degraded" } else { "unavailable" },
        "checks": {"redis": st(redis_ok), "postgres": st(pg_ok), "engine": st(engine_ok),
                   "engine_breaker": if state.breaker.is_open() { "open" } else { "closed" }}
    });
    if redis_ok {
        HttpResponse::Ok().json(body)
    } else {
        HttpResponse::ServiceUnavailable().json(body)
    }
}

pub async fn prometheus() -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/plain; version=0.0.4")
        .body(metrics::render())
}

// ------------------------------------------------------------------- feed

#[derive(Deserialize)]
pub struct FeedQuery {
    limit: Option<usize>,
    cursor: Option<String>,
}

pub async fn get_feed(
    req: HttpRequest,
    state: web::Data<AppState>,
    who: Identity,
    path: web::Path<i64>,
    q: web::Query<FeedQuery>,
) -> ApiResult<HttpResponse> {
    let uid = check_id(path.into_inner())?;
    who.require_user(uid)?;
    let limit = q.limit.unwrap_or(20);
    if limit == 0 || limit > state.cfg.max_page_size {
        return Err(ApiError::BadRequest(format!(
            "limit deve estar entre 1 e {}",
            state.cfg.max_page_size
        )));
    }
    let cursor = q.cursor.as_deref().map(Cursor::parse).transpose()?;
    let resp = feed::serve_feed(&state, uid, limit, cursor, &request_id(&req)).await?;
    // Origem também no header: útil para depuração e para testes de carga sem ler o corpo.
    let source = serde_json::to_value(resp.source)
        .ok()
        .and_then(|v| v.as_str().map(String::from));
    let mut out = HttpResponse::Ok();
    if let Some(src) = source {
        out.insert_header(("x-feed-source", src));
    }
    Ok(out.json(resp))
}

// ---------------------------------------------------------------- eventos

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventIn {
    event_id: Option<Uuid>,
    #[serde(rename = "type")]
    kind: EventType,
    user_id: i64,
    content_id: i64,
    occurred_at: Option<DateTime<Utc>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventsBody {
    events: Vec<EventIn>,
}

#[derive(Serialize)]
struct EventsAccepted {
    accepted: usize,
    event_ids: Vec<Uuid>,
}

pub async fn post_events(
    req: HttpRequest,
    state: web::Data<AppState>,
    who: Identity,
    body: web::Json<EventsBody>,
) -> ApiResult<HttpResponse> {
    let body = body.into_inner();
    let max = state.cfg.max_events_per_request;
    if body.events.is_empty() || body.events.len() > max {
        return Err(ApiError::BadRequest(format!(
            "envie entre 1 e {max} eventos"
        )));
    }
    rate_limit(
        &state,
        "events",
        &who.rate_key(),
        state.cfg.rate_limit_events_rps,
        1,
    )
    .await?;
    let now = Utc::now();
    let rid = request_id(&req);
    let mut envelopes = Vec::with_capacity(body.events.len());
    for (i, ev) in body.events.into_iter().enumerate() {
        if !ev.kind.is_client_interaction() {
            return Err(ApiError::Unprocessable(format!(
                "events[{i}].type não pode ser enviado pelo cliente"
            )));
        }
        check_id(ev.user_id)?;
        check_id(ev.content_id)?;
        who.require_user(ev.user_id)?;
        let at = ev.occurred_at.unwrap_or(now);
        if at > now + Duration::minutes(5) || at < now - Duration::days(7) {
            return Err(ApiError::Unprocessable(format!(
                "events[{i}].occurred_at fora da janela aceita (-7d, +5min)"
            )));
        }
        // event_id do cliente permite reenvio seguro (idempotência ponta a ponta).
        let mut env = Envelope::new(
            ev.kind,
            ev.event_id.unwrap_or_else(Uuid::new_v4),
            at,
            Some(rid.clone()),
        );
        env.user_id = Some(ev.user_id);
        env.content_id = Some(ev.content_id);
        envelopes.push(env);
    }
    let mut pipe = redis::pipe();
    for env in &envelopes {
        events::xadd_event(&mut pipe, env, state.cfg.events_stream_maxlen);
    }
    pipe.query_async::<()>(&mut state.redis()).await?;
    for env in &envelopes {
        metrics::EVENTS_INGESTED
            .with_label_values(&[env.kind.as_str()])
            .inc();
    }
    Ok(HttpResponse::Accepted().json(EventsAccepted {
        accepted: envelopes.len(),
        event_ids: envelopes.iter().map(|e| e.event_id).collect(),
    }))
}

// --------------------------------------------------------------- conteúdo

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentIn {
    author_id: i64,
    category: String,
    title: String,
    #[serde(default)]
    tags: Vec<String>,
}

#[derive(Serialize)]
struct ContentOut {
    content_id: i64,
    author_id: i64,
    category: String,
    tags: Vec<String>,
    title: String,
    status: &'static str,
    published_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    stats: Option<serde_json::Map<String, serde_json::Value>>,
}

pub async fn post_content(
    req: HttpRequest,
    state: web::Data<AppState>,
    who: Identity,
    body: web::Json<ContentIn>,
) -> ApiResult<HttpResponse> {
    who.require_admin()?;
    let mut c = body.into_inner();
    check_id(c.author_id)?;
    let title_len = c.title.trim().chars().count();
    if !(1..=200).contains(&title_len) {
        return Err(ApiError::Unprocessable(
            "title deve ter entre 1 e 200 caracteres".into(),
        ));
    }
    c.tags.sort();
    c.tags.dedup();
    if c.tags.len() > 10 || !c.tags.iter().all(|t| valid_slug(t)) || !valid_slug(&c.category) {
        return Err(ApiError::Unprocessable(
            "category/tags inválidas (slugs, no máximo 10 tags)".into(),
        ));
    }
    let mut tx = state.pg.begin().await?;
    let category_id: Option<i16> = sqlx::query_scalar("SELECT id FROM categories WHERE slug = $1")
        .bind(&c.category)
        .fetch_optional(&mut *tx)
        .await?;
    let Some(category_id) = category_id else {
        return Err(ApiError::Unprocessable(format!(
            "categoria desconhecida: {}",
            c.category
        )));
    };
    let author_ok: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM authors WHERE id = $1)")
        .bind(c.author_id)
        .fetch_one(&mut *tx)
        .await?;
    if !author_ok {
        return Err(ApiError::Unprocessable("autor desconhecido".into()));
    }
    let tag_ids: Vec<i32> = sqlx::query_scalar("SELECT id FROM tags WHERE slug = ANY($1)")
        .bind(&c.tags)
        .fetch_all(&mut *tx)
        .await?;
    if tag_ids.len() != c.tags.len() {
        return Err(ApiError::Unprocessable(
            "uma ou mais tags não existem".into(),
        ));
    }
    let row = sqlx::query(
        "INSERT INTO contents (author_id, category_id, title) VALUES ($1, $2, $3) RETURNING id, published_at",
    )
    .bind(c.author_id)
    .bind(category_id)
    .bind(c.title.trim())
    .fetch_one(&mut *tx)
    .await?;
    let id: i64 = row.try_get("id")?;
    let published_at: DateTime<Utc> = row.try_get("published_at")?;
    sqlx::query("INSERT INTO content_tags (content_id, tag_id) SELECT $1, unnest($2::int[])")
        .bind(id)
        .bind(&tag_ids)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;

    // Indexação assíncrona pelo worker (catálogo e pools de candidatos).
    let mut env = Envelope::new(
        EventType::ContentPublished,
        Uuid::new_v4(),
        published_at,
        Some(request_id(&req)),
    );
    env.content_id = Some(id);
    if events::publish(&mut state.redis(), &env, state.cfg.events_stream_maxlen)
        .await
        .is_err()
    {
        metrics::dependency_error("redis");
        tracing::warn!(content_id = id, "content_published_event_not_sent");
    } else {
        metrics::EVENTS_INGESTED
            .with_label_values(&["content_published"])
            .inc();
    }
    Ok(HttpResponse::Created().json(ContentOut {
        content_id: id,
        author_id: c.author_id,
        category: c.category,
        tags: c.tags,
        title: c.title.trim().to_string(),
        status: "active",
        published_at: events::rfc3339(published_at),
        stats: None,
    }))
}

pub async fn get_content(
    state: web::Data<AppState>,
    _who: Identity,
    path: web::Path<i64>,
) -> ApiResult<HttpResponse> {
    let id = check_id(path.into_inner())?;
    let mut conn = state.redis();
    let catalog = feed::load_catalog(&state, &mut conn, &[id]).await?;
    let Some(c) = catalog.get(&id) else {
        return Err(ApiError::NotFound("conteúdo não encontrado"));
    };
    let raw: HashMap<String, String> = redis::cmd("HGETALL")
        .arg(keys::stats(id))
        .query_async(&mut conn)
        .await
        .unwrap_or_default();
    // Contadores são inteiros; `eng` (engajamento ponderado) é decimal.
    let stats = raw
        .into_iter()
        .map(|(k, v)| {
            let n = match v.parse::<i64>() {
                Ok(i) => json!(i),
                Err(_) => json!(v.parse::<f64>().unwrap_or(0.0)),
            };
            (k, n)
        })
        .collect();
    Ok(HttpResponse::Ok().json(ContentOut {
        content_id: id,
        author_id: c.author_id,
        category: c.category.clone(),
        tags: c.tags.clone(),
        title: c.title.clone(),
        status: if c.active { "active" } else { "removed" },
        published_at: DateTime::<Utc>::from_timestamp(c.published_at, 0)
            .map(events::rfc3339)
            .unwrap_or_default(),
        stats: Some(stats),
    }))
}

pub async fn delete_content(
    req: HttpRequest,
    state: web::Data<AppState>,
    who: Identity,
    path: web::Path<i64>,
) -> ApiResult<HttpResponse> {
    who.require_admin()?;
    let id = check_id(path.into_inner())?;
    let updated = sqlx::query("UPDATE contents SET status = 'removed' WHERE id = $1")
        .bind(id)
        .execute(&state.pg)
        .await?;
    if updated.rows_affected() == 0 {
        return Err(ApiError::NotFound("conteúdo não encontrado"));
    }
    // Efeito imediato na entrega (o filtro lê o status do catálogo) e reindexação pelo worker.
    let mut conn = state.redis();
    let mut env = Envelope::new(
        EventType::ContentRemoved,
        Uuid::new_v4(),
        Utc::now(),
        Some(request_id(&req)),
    );
    env.content_id = Some(id);
    let mut pipe = redis::pipe();
    pipe.cmd("HSET")
        .arg(keys::catalog(id))
        .arg("status")
        .arg("removed")
        .ignore();
    events::xadd_event(&mut pipe, &env, state.cfg.events_stream_maxlen);
    pipe.query_async::<()>(&mut conn).await?;
    Ok(HttpResponse::NoContent().finish())
}

// ---------------------------------------------------------------- usuários

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UserIn {
    handle: String,
    #[serde(default)]
    declared_categories: Vec<String>,
}

pub async fn post_user(
    state: web::Data<AppState>,
    who: Identity,
    body: web::Json<UserIn>,
) -> ApiResult<HttpResponse> {
    who.require_admin()?;
    let mut u = body.into_inner();
    let handle = u.handle.trim().to_string();
    if !(1..=40).contains(&handle.chars().count()) {
        return Err(ApiError::Unprocessable(
            "handle deve ter entre 1 e 40 caracteres".into(),
        ));
    }
    u.declared_categories.sort();
    u.declared_categories.dedup();
    if u.declared_categories.len() > 12 || !u.declared_categories.iter().all(|c| valid_slug(c)) {
        return Err(ApiError::Unprocessable(
            "declared_categories inválidas".into(),
        ));
    }
    let mut tx = state.pg.begin().await?;
    let cat_ids: Vec<i16> = sqlx::query_scalar("SELECT id FROM categories WHERE slug = ANY($1)")
        .bind(&u.declared_categories)
        .fetch_all(&mut *tx)
        .await?;
    if cat_ids.len() != u.declared_categories.len() {
        return Err(ApiError::Unprocessable(
            "categoria desconhecida em declared_categories".into(),
        ));
    }
    let id: Option<i64> = sqlx::query_scalar(
        "INSERT INTO users (handle) VALUES ($1) ON CONFLICT (handle) DO NOTHING RETURNING id",
    )
    .bind(&handle)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(id) = id else {
        return Err(ApiError::Unprocessable("handle já existe".into()));
    };
    sqlx::query("INSERT INTO user_declared_interests (user_id, category_id) SELECT $1, unnest($2::smallint[])")
        .bind(id)
        .bind(&cat_ids)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(HttpResponse::Created().json(
        json!({"user_id": id, "handle": handle, "declared_categories": u.declared_categories}),
    ))
}

// ------------------------------------------------------- regeneração/perfil

pub async fn post_rebuild(
    state: web::Data<AppState>,
    who: Identity,
    path: web::Path<i64>,
) -> ApiResult<HttpResponse> {
    let uid = check_id(path.into_inner())?;
    who.require_user(uid)?;
    rate_limit(
        &state,
        "rebuild",
        &format!("{uid}"),
        state.cfg.rate_limit_rebuild_per_min,
        60,
    )
    .await?;
    let mut conn = state.redis();
    let res = events::enqueue_rebuild(
        &mut conn,
        uid,
        "api",
        state.cfg.rebuild_dedup_secs,
        state.cfg.jobs_stream_maxlen,
    )
    .await?;
    let label = match res {
        events::EnqueueResult::Queued => "queued",
        events::EnqueueResult::AlreadyQueued => "already_queued",
    };
    metrics::REBUILD_REQUESTS
        .with_label_values(&["api", label])
        .inc();
    Ok(HttpResponse::Accepted().json(json!({"user_id": uid, "status": res})))
}

pub async fn get_interests(
    state: web::Data<AppState>,
    who: Identity,
    path: web::Path<i64>,
) -> ApiResult<HttpResponse> {
    let uid = check_id(path.into_inner())?;
    who.require_user(uid)?;
    let mut conn = state.redis();
    type Weighted = Vec<(String, f64)>;
    let (meta, cats, tags, authors): (HashMap<String, String>, Weighted, Weighted, Weighted) =
        redis::pipe()
            .cmd("HGETALL")
            .arg(keys::profile_meta(uid))
            .cmd("ZREVRANGE")
            .arg(keys::profile_cat(uid))
            .arg(0)
            .arg(9)
            .arg("WITHSCORES")
            .cmd("ZREVRANGE")
            .arg(keys::profile_tag(uid))
            .arg(0)
            .arg(9)
            .arg("WITHSCORES")
            .cmd("ZREVRANGE")
            .arg(keys::profile_author(uid))
            .arg(0)
            .arg(9)
            .arg("WITHSCORES")
            .query_async(&mut conn)
            .await?;
    // Pesos relativos ao maior interesse (0–1): o valor absoluto decai com o tempo.
    let norm = |v: Vec<(String, f64)>| -> Vec<serde_json::Value> {
        let top = v.first().map(|x| x.1).filter(|x| *x > 0.0).unwrap_or(1.0);
        v.into_iter()
            .map(|(k, w)| json!({"key": k, "weight": (w / top * 1000.0).round() / 1000.0}))
            .collect()
    };
    Ok(HttpResponse::Ok().json(json!({
        "user_id": uid,
        "profile_loaded": !meta.is_empty(),
        "interaction_count": meta.get("count").and_then(|c| c.parse::<i64>().ok()).unwrap_or(0),
        "categories": norm(cats),
        "tags": norm(tags),
        "authors": norm(authors),
    })))
}
