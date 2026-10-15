//! Entrega do feed.
//!
//! Caminho rápido (cache quente): HGETALL meta → ZREVRANGE de uma faixa do feed
//! pré-calculado → ZMSCORE no conjunto "já visto" + HMGET de metadados, tudo em
//! pipelines. Nenhuma chamada ao motor Python, nenhum acesso ao PostgreSQL.
//!
//! Cache miss: single-flight por usuário (lock no Redis) + chamada síncrona
//! ao motor com timeout, circuit breaker e limite de concorrência. Quem não
//! obtém o lock — ou se o motor falhar — recebe o fallback de trending na
//! hora, sem esperar. Sem Redis, último recurso: conteúdos recentes do
//! PostgreSQL (sem personalização).

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use chrono::{DateTime, Utc};
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use uuid::Uuid;

use crate::{
    error::{ApiError, ApiResult},
    events::{self, EnqueueResult, Envelope, EventType},
    keys, metrics,
    state::AppState,
};

const VERSION_TRENDING: i64 = 0;
const VERSION_RECENT: i64 = -1;
const MAX_FALLBACK_OFFSET: usize = 500;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    Cache,
    CacheStale,
    Built,
    FallbackTrending,
    FallbackRecent,
}

impl Source {
    fn label(&self) -> &'static str {
        match self {
            Source::Cache => "cache",
            Source::CacheStale => "cache_stale",
            Source::Built => "built",
            Source::FallbackTrending => "fallback_trending",
            Source::FallbackRecent => "fallback_recent",
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedItem {
    pub content_id: i64,
    pub position: usize,
    pub author_id: i64,
    pub category: String,
    pub tags: Vec<String>,
    pub title: String,
    pub published_at: String,
    pub score: Option<f64>,
    pub reason: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedResponse {
    pub user_id: i64,
    pub source: Source,
    pub feed_version: Option<i64>,
    pub stale: bool,
    pub next_cursor: Option<String>,
    pub items: Vec<FeedItem>,
}

// ------------------------------------------------------------------ cursor

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cursor {
    pub version: i64,
    pub offset: usize,
}

impl Cursor {
    /// Formato opaco para o cliente: `<versão do feed>:<offset>`. Se o feed for
    /// regenerado entre duas páginas, a versão muda e a paginação recomeça.
    pub fn parse(raw: &str) -> Result<Self, ApiError> {
        let bad = || ApiError::BadRequest("cursor inválido".into());
        let (v, o) = raw.split_once(':').ok_or_else(bad)?;
        let version: i64 = v.parse().map_err(|_| bad())?;
        let offset: usize = o.parse().map_err(|_| bad())?;
        if offset > 100_000 {
            return Err(bad());
        }
        Ok(Self { version, offset })
    }

    pub fn encode(&self) -> String {
        format!("{}:{}", self.version, self.offset)
    }
}

// ----------------------------------------------------------------- catálogo

#[derive(Debug, Clone, PartialEq)]
pub struct Catalog {
    pub author_id: i64,
    pub category: String,
    pub tags: Vec<String>,
    pub title: String,
    pub published_at: i64,
    pub active: bool,
}

const CATALOG_FIELDS: [&str; 6] = [
    "author_id",
    "category",
    "tags",
    "title",
    "published_at",
    "status",
];

fn parse_catalog(v: &[Option<String>]) -> Option<Catalog> {
    Some(Catalog {
        author_id: v.first()?.as_ref()?.parse().ok()?,
        category: v.get(1)?.clone()?,
        tags: v
            .get(2)?
            .as_deref()
            .unwrap_or("")
            .split(',')
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect(),
        title: v.get(3)?.clone().unwrap_or_default(),
        published_at: v.get(4)?.as_ref()?.parse().ok()?,
        active: v.get(5)?.as_deref() == Some("active"),
    })
}

/// SQL estático (literal) com o sufixo dado: nada vindo do cliente entra na string,
/// apenas em parâmetros `$n`.
macro_rules! content_sql {
    ($tail:literal) => {
        concat!(
            "SELECT c.id, c.author_id, k.slug, c.title, c.status, ",
            "EXTRACT(EPOCH FROM c.published_at)::bigint AS published, ",
            "COALESCE(array_agg(t.slug ORDER BY t.slug) FILTER (WHERE t.slug IS NOT NULL), '{}') AS tags ",
            "FROM contents c JOIN categories k ON k.id = c.category_id ",
            "LEFT JOIN content_tags ct ON ct.content_id = c.id LEFT JOIN tags t ON t.id = ct.tag_id ",
            $tail
        )
    };
}

fn row_to_catalog(row: &sqlx::postgres::PgRow) -> Result<(i64, Catalog), sqlx::Error> {
    let status: String = row.try_get("status")?;
    Ok((
        row.try_get("id")?,
        Catalog {
            author_id: row.try_get("author_id")?,
            category: row.try_get("slug")?,
            tags: row.try_get("tags")?,
            title: row.try_get("title")?,
            published_at: row.try_get("published")?,
            active: status == "active",
        },
    ))
}

pub async fn catalog_from_db(state: &AppState, ids: &[i64]) -> ApiResult<HashMap<i64, Catalog>> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let sql = content_sql!("WHERE c.id = ANY($1) GROUP BY c.id, k.slug");
    let rows = sqlx::query(sql).bind(ids).fetch_all(&state.pg).await?;
    rows.iter()
        .map(row_to_catalog)
        .collect::<Result<_, _>>()
        .map_err(ApiError::from)
}

/// Metadados dos conteúdos: Redis em um pipeline; os ausentes (ex.: catálogo
/// ainda sendo reconstruído) vêm do PostgreSQL numa única consulta `ANY($1)`.
pub async fn load_catalog(
    state: &AppState,
    conn: &mut ConnectionManager,
    ids: &[i64],
) -> ApiResult<HashMap<i64, Catalog>> {
    let mut pipe = redis::pipe();
    push_catalog_cmds(&mut pipe, ids);
    let rows: Vec<Vec<Option<String>>> = pipe.query_async(conn).await?;
    complete_catalog(state, ids, rows).await
}

fn push_catalog_cmds(pipe: &mut redis::Pipeline, ids: &[i64]) {
    for id in ids {
        pipe.cmd("HMGET")
            .arg(keys::catalog(*id))
            .arg(&CATALOG_FIELDS);
    }
}

async fn complete_catalog(
    state: &AppState,
    ids: &[i64],
    rows: Vec<Vec<Option<String>>>,
) -> ApiResult<HashMap<i64, Catalog>> {
    let mut out = HashMap::with_capacity(ids.len());
    let mut missing = Vec::new();
    for (id, row) in ids.iter().zip(rows) {
        match parse_catalog(&row) {
            Some(c) => {
                out.insert(*id, c);
            }
            None => missing.push(*id),
        }
    }
    if !missing.is_empty() {
        // Falha do PostgreSQL aqui não derruba a página: só perdemos esses itens.
        match catalog_from_db(state, &missing).await {
            Ok(found) => out.extend(found),
            Err(_) => tracing::warn!(missing = missing.len(), "catalog_fallback_failed"),
        }
    }
    Ok(out)
}

fn conv<T: redis::FromRedisValue>(v: redis::Value) -> ApiResult<T> {
    T::from_redis_value(v).map_err(|e| {
        tracing::warn!(error = %e, "redis_parse_error");
        ApiError::Unavailable
    })
}

/// "Já visto", score/motivo e metadados de uma faixa do feed em UM round-trip.
async fn fetch_details(
    state: &AppState,
    conn: &mut ConnectionManager,
    uid: i64,
    ids: &[i64],
    info_key: Option<&str>,
) -> ApiResult<(Vec<Option<f64>>, Vec<Option<String>>, HashMap<i64, Catalog>)> {
    let mut pipe = redis::pipe();
    pipe.cmd("ZMSCORE").arg(keys::seen(uid)).arg(ids);
    if let Some(info) = info_key {
        pipe.cmd("HMGET").arg(info).arg(ids);
    }
    push_catalog_cmds(&mut pipe, ids);
    let mut values: std::collections::VecDeque<redis::Value> =
        pipe.query_async::<Vec<redis::Value>>(conn).await?.into();
    let seen: Vec<Option<f64>> = conv(values.pop_front().unwrap_or(redis::Value::Nil))?;
    let infos: Vec<Option<String>> = match info_key {
        Some(_) => conv(values.pop_front().unwrap_or(redis::Value::Nil))?,
        None => vec![None; ids.len()],
    };
    let rows = values
        .into_iter()
        .map(conv)
        .collect::<ApiResult<Vec<Vec<Option<String>>>>>()?;
    let catalog = complete_catalog(state, ids, rows).await?;
    Ok((seen, infos, catalog))
}

// -------------------------------------------------------- seleção da página

#[derive(Debug, Default, PartialEq)]
pub struct FilterCounts {
    pub seen: u64,
    pub removed: u64,
    pub blocked: u64,
    pub missing: u64,
}

pub struct Candidate {
    pub content_id: i64,
    pub position: usize,
    pub seen: bool,
    pub info: Option<String>,
}

/// Filtro obrigatório na entrega (independe do score): já visto desde a
/// geração do feed, removido, autor bloqueado ou sem metadados.
/// Retorna os itens aceitos e quantos candidatos foram consumidos.
pub fn select_page(
    candidates: Vec<Candidate>,
    catalog: &HashMap<i64, Catalog>,
    blocked: &HashSet<i64>,
    limit: usize,
    already: usize,
    default_reason: &str,
    counts: &mut FilterCounts,
) -> (Vec<FeedItem>, usize) {
    let mut out = Vec::new();
    let mut consumed = 0;
    for c in candidates {
        if already + out.len() >= limit {
            break;
        }
        consumed += 1;
        let Some(meta) = catalog.get(&c.content_id) else {
            counts.missing += 1;
            continue;
        };
        if !meta.active {
            counts.removed += 1;
        } else if blocked.contains(&meta.author_id) {
            counts.blocked += 1;
        } else if c.seen {
            counts.seen += 1;
        } else {
            let (score, reason) = match c.info.as_deref().and_then(|s| s.split_once('|')) {
                Some((s, r)) => (s.parse().ok(), r.to_string()),
                None => (None, default_reason.to_string()),
            };
            out.push(FeedItem {
                content_id: c.content_id,
                position: c.position,
                author_id: meta.author_id,
                category: meta.category.clone(),
                tags: meta.tags.clone(),
                title: meta.title.clone(),
                published_at: DateTime::<Utc>::from_timestamp(meta.published_at, 0)
                    .map(events::rfc3339)
                    .unwrap_or_default(),
                score,
                reason,
            });
        }
    }
    (out, consumed)
}

/// Itens lidos por rodada: o dobro da página absorve os filtros na maioria dos casos.
fn chunk_size(limit: usize) -> usize {
    (limit * 2).max(10)
}

/// Lista ordenada no Redis de onde a página é lida.
struct RankedList<'a> {
    key: &'a str,
    /// Hash `content_id -> "score|motivo"` (só o feed personalizado tem).
    info_key: Option<&'a str>,
    default_reason: &'a str,
    /// Primeira faixa já lida junto com os metadados (economiza um round-trip).
    prefetched: Option<Vec<i64>>,
}

/// Lê a próxima página de uma lista ordenada no Redis (feed ou trending),
/// aplicando os filtros. Faz no máximo 4 rodadas para completar `limit`.
async fn read_ranked(
    state: &AppState,
    conn: &mut ConnectionManager,
    uid: i64,
    list: RankedList<'_>,
    start: usize,
    limit: usize,
    blocked: &HashSet<i64>,
) -> ApiResult<(Vec<FeedItem>, usize, bool)> {
    let RankedList {
        key: list_key,
        info_key,
        default_reason,
        mut prefetched,
    } = list;
    let mut items = Vec::new();
    let mut offset = start;
    let mut exhausted = false;
    let mut counts = FilterCounts::default();
    let chunk = chunk_size(limit);
    for _ in 0..4 {
        let ids: Vec<i64> = match prefetched.take() {
            Some(ids) => ids,
            None => {
                redis::cmd("ZREVRANGE")
                    .arg(list_key)
                    .arg(offset)
                    .arg(offset + chunk - 1)
                    .query_async(conn)
                    .await?
            }
        };
        if ids.is_empty() {
            exhausted = true;
            break;
        }
        let (seen, infos, catalog) = fetch_details(state, conn, uid, &ids, info_key).await?;
        let n = ids.len();
        let candidates = ids
            .into_iter()
            .enumerate()
            .map(|(i, id)| Candidate {
                content_id: id,
                position: offset + i,
                seen: seen.get(i).copied().flatten().is_some(),
                info: infos.get(i).cloned().flatten(),
            })
            .collect();
        let (mut page, consumed) = select_page(
            candidates,
            &catalog,
            blocked,
            limit,
            items.len(),
            default_reason,
            &mut counts,
        );
        items.append(&mut page);
        offset += consumed;
        if n < chunk && consumed == n {
            exhausted = true;
            break;
        }
        if items.len() >= limit {
            break;
        }
    }
    for (reason, n) in [
        ("seen", counts.seen),
        ("removed", counts.removed),
        ("blocked_author", counts.blocked),
        ("missing", counts.missing),
    ] {
        if n > 0 {
            metrics::FILTERED.with_label_values(&[reason]).inc_by(n);
        }
    }
    Ok((items, offset, exhausted))
}

// ------------------------------------------------------------ motor (miss)

enum BuildOutcome {
    Built,
    UnknownUser,
    /// Lock em uso, breaker aberto, motor saturado/indisponível: usar fallback.
    Fallback,
}

const RELEASE_LOCK: &str =
    "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0";

async fn release_lock(conn: &mut ConnectionManager, uid: i64, token: &str) {
    let _: Result<i64, _> = redis::Script::new(RELEASE_LOCK)
        .key(keys::lock_build(uid))
        .arg(token)
        .invoke_async(conn)
        .await;
}

async fn build_with_engine(
    state: &AppState,
    conn: &mut ConnectionManager,
    uid: i64,
    rid: &str,
) -> ApiResult<BuildOutcome> {
    let token = Uuid::new_v4().to_string();
    let got: Option<String> = redis::cmd("SET")
        .arg(keys::lock_build(uid))
        .arg(&token)
        .arg("NX")
        .arg("EX")
        .arg(state.cfg.build_lock_secs)
        .query_async(conn)
        .await?;
    if got.is_none() {
        metrics::ENGINE_CALLS
            .with_label_values(&["skipped_locked"])
            .inc();
        return Ok(BuildOutcome::Fallback);
    }
    if !state.breaker.allow() {
        metrics::ENGINE_CALLS
            .with_label_values(&["breaker_open"])
            .inc();
        release_lock(conn, uid, &token).await;
        enqueue(state, conn, uid, "miss_breaker_open").await;
        return Ok(BuildOutcome::Fallback);
    }
    let Ok(_permit) = state.engine_slots.clone().try_acquire_owned() else {
        metrics::ENGINE_CALLS
            .with_label_values(&["overloaded"])
            .inc();
        release_lock(conn, uid, &token).await;
        enqueue(state, conn, uid, "miss_overloaded").await;
        return Ok(BuildOutcome::Fallback);
    };
    let started = Instant::now();
    let resp = state
        .http
        .post(format!(
            "{}/internal/v1/feeds/{uid}/build",
            state.cfg.engine_url
        ))
        .header("x-internal-token", &state.cfg.internal_token)
        .header("x-request-id", rid)
        .send()
        .await;
    metrics::ENGINE_LATENCY.observe(started.elapsed().as_secs_f64());
    match resp {
        Ok(r) if r.status().is_success() => {
            state.breaker.record_success();
            release_lock(conn, uid, &token).await;
            metrics::ENGINE_CALLS.with_label_values(&["ok"]).inc();
            Ok(BuildOutcome::Built)
        }
        Ok(r) if r.status().as_u16() == 404 => {
            state.breaker.record_success();
            release_lock(conn, uid, &token).await;
            metrics::ENGINE_CALLS
                .with_label_values(&["unknown_user"])
                .inc();
            Ok(BuildOutcome::UnknownUser)
        }
        other => {
            state.breaker.record_failure();
            let timed_out = matches!(&other, Err(e) if e.is_timeout());
            // Em timeout o motor provavelmente ainda está gerando: mantém o lock
            // (expira sozinho) para não disparar gerações duplicadas.
            if !timed_out {
                release_lock(conn, uid, &token).await;
            }
            metrics::ENGINE_CALLS
                .with_label_values(&[if timed_out { "timeout" } else { "error" }])
                .inc();
            tracing::warn!(user_id = uid, timed_out, "engine_call_failed");
            enqueue(state, conn, uid, "miss_engine_failed").await;
            Ok(BuildOutcome::Fallback)
        }
    }
}

/// Enfileira fora do caminho da resposta (stale-while-revalidate).
fn spawn_enqueue(state: &AppState, mut conn: ConnectionManager, uid: i64, reason: &'static str) {
    let (dedup, maxlen) = (state.cfg.rebuild_dedup_secs, state.cfg.jobs_stream_maxlen);
    actix_web::rt::spawn(async move {
        let label = match events::enqueue_rebuild(&mut conn, uid, reason, dedup, maxlen).await {
            Ok(EnqueueResult::Queued) => "queued",
            Ok(EnqueueResult::AlreadyQueued) => "already_queued",
            Err(_) => "error",
        };
        metrics::REBUILD_REQUESTS
            .with_label_values(&[reason, label])
            .inc();
    });
}

async fn enqueue(state: &AppState, conn: &mut ConnectionManager, uid: i64, reason: &str) {
    let res = events::enqueue_rebuild(
        conn,
        uid,
        reason,
        state.cfg.rebuild_dedup_secs,
        state.cfg.jobs_stream_maxlen,
    )
    .await;
    let label = match &res {
        Ok(EnqueueResult::Queued) => "queued",
        Ok(EnqueueResult::AlreadyQueued) => "already_queued",
        Err(_) => "error",
    };
    metrics::REBUILD_REQUESTS
        .with_label_values(&[reason, label])
        .inc();
}

// ---------------------------------------------------------------- serviço

struct Meta {
    version: i64,
    generated_at_ms: i64,
    stale: bool,
    size: usize,
}

/// Lê metadados do feed e autores bloqueados. Com `prefetch = Some((start, n))`,
/// já traz no MESMO round-trip a primeira faixa do feed (leitura otimista: só é
/// usada se a versão do cursor bater com a do feed).
async fn read_meta(
    conn: &mut ConnectionManager,
    uid: i64,
    prefetch: Option<(usize, usize)>,
) -> ApiResult<(Option<Meta>, HashSet<i64>, Option<Vec<i64>>)> {
    let mut pipe = redis::pipe();
    pipe.cmd("HGETALL").arg(keys::feed_meta(uid));
    pipe.cmd("HGET").arg(keys::profile_meta(uid)).arg("blocked");
    if let Some((start, n)) = prefetch {
        pipe.cmd("ZREVRANGE")
            .arg(keys::feed(uid))
            .arg(start)
            .arg(start + n - 1);
    }
    let mut values: std::collections::VecDeque<redis::Value> =
        pipe.query_async::<Vec<redis::Value>>(conn).await?.into();
    let meta: HashMap<String, String> = conv(values.pop_front().unwrap_or(redis::Value::Nil))?;
    let blocked: Option<String> = conv(values.pop_front().unwrap_or(redis::Value::Nil))?;
    let ids: Option<Vec<i64>> = match values.pop_front() {
        Some(v) => Some(conv(v)?),
        None => None,
    };
    let blocked = blocked
        .unwrap_or_default()
        .split(',')
        .filter_map(|s| s.parse().ok())
        .collect();
    let get = |k: &str| meta.get(k).and_then(|v| v.parse::<i64>().ok());
    let meta = match (get("version"), get("generated_at_ms"), get("size")) {
        (Some(version), Some(generated_at_ms), Some(size)) => Some(Meta {
            version,
            generated_at_ms,
            stale: get("stale") == Some(1),
            size: size.max(0) as usize,
        }),
        _ => None,
    };
    Ok((meta, blocked, ids))
}

pub async fn serve_feed(
    state: &AppState,
    uid: i64,
    limit: usize,
    cursor: Option<Cursor>,
    rid: &str,
) -> ApiResult<FeedResponse> {
    let mut conn = state.redis();
    let resp = match serve_from_redis(state, &mut conn, uid, limit, cursor, rid).await {
        Ok(r) => r,
        Err(ApiError::Unavailable) => {
            // Redis indisponível: degrada para conteúdos recentes do PostgreSQL.
            metrics::FEED_CACHE
                .with_label_values(&["redis_error"])
                .inc();
            serve_recent_from_db(state, uid, limit, cursor).await?
        }
        Err(e) => return Err(e),
    };
    metrics::FEED_SOURCE
        .with_label_values(&[resp.source.label()])
        .inc();
    observe_page(&resp.items);
    if !resp.items.is_empty() && resp.source != Source::FallbackRecent {
        record_impression(state, conn, uid, &resp.items, rid);
    }
    Ok(resp)
}

async fn serve_from_redis(
    state: &AppState,
    conn: &mut ConnectionManager,
    uid: i64,
    limit: usize,
    cursor: Option<Cursor>,
    rid: &str,
) -> ApiResult<FeedResponse> {
    let wanted_start = cursor.filter(|c| c.version > 0).map_or(0, |c| c.offset);
    let (mut meta, blocked, mut prefetched) =
        read_meta(conn, uid, Some((wanted_start, chunk_size(limit)))).await?;
    let mut source = Source::Cache;

    // Cursor de fallback (trending/recentes) continua no fallback enquanto o
    // feed personalizado não existir; quando existir, a paginação recomeça nele.
    // Um feed gerado vazio (size 0) é resposta válida: vai ao trending sem
    // chamar o motor de novo a cada requisição.
    if meta.is_none() {
        metrics::FEED_CACHE.with_label_values(&["miss"]).inc();
        match build_with_engine(state, conn, uid, rid).await? {
            BuildOutcome::Built => {
                (meta, _, prefetched) = read_meta(conn, uid, None).await?;
                source = Source::Built;
            }
            BuildOutcome::UnknownUser => return Err(ApiError::NotFound("usuário não encontrado")),
            BuildOutcome::Fallback => meta = None,
        }
    } else {
        metrics::FEED_CACHE.with_label_values(&["hit"]).inc();
    }

    let meta = match meta {
        Some(m) if m.size > 0 => m,
        _ => return serve_trending(state, conn, uid, limit, cursor, &blocked).await,
    };

    let stale = meta.stale
        || Utc::now().timestamp_millis() - meta.generated_at_ms > state.cfg.feed_soft_ttl_ms;
    if stale && source == Source::Cache {
        // stale-while-revalidate: entrega o feed atual e agenda a regeneração.
        source = Source::CacheStale;
        metrics::FEED_CACHE.with_label_values(&["stale"]).inc();
        spawn_enqueue(state, conn.clone(), uid, "stale");
    }
    let start = match cursor {
        Some(c) if c.version == meta.version => c.offset,
        _ => 0,
    };
    if start != wanted_start {
        prefetched = None; // versão do cursor não bate: a faixa lida não serve
    }
    let (feed_key, info_key) = (keys::feed(uid), keys::feed_info(uid));
    let (items, offset, exhausted) = read_ranked(
        state,
        conn,
        uid,
        RankedList {
            key: &feed_key,
            info_key: Some(&info_key),
            default_reason: "recent",
            prefetched,
        },
        start,
        limit,
        &blocked,
    )
    .await?;
    let next = (!exhausted && offset < meta.size).then(|| {
        Cursor {
            version: meta.version,
            offset,
        }
        .encode()
    });
    Ok(FeedResponse {
        user_id: uid,
        source,
        feed_version: Some(meta.version),
        stale,
        next_cursor: next,
        items,
    })
}

async fn serve_trending(
    state: &AppState,
    conn: &mut ConnectionManager,
    uid: i64,
    limit: usize,
    cursor: Option<Cursor>,
    blocked: &HashSet<i64>,
) -> ApiResult<FeedResponse> {
    let start = match cursor {
        Some(c) if c.version == VERSION_TRENDING => c.offset.min(MAX_FALLBACK_OFFSET),
        _ => 0,
    };
    let (items, offset, exhausted) = read_ranked(
        state,
        conn,
        uid,
        RankedList {
            key: keys::TRENDING,
            info_key: None,
            default_reason: "trending",
            prefetched: None,
        },
        start,
        limit,
        blocked,
    )
    .await?;
    if items.is_empty() && start == 0 {
        // Trending vazio (ex.: Redis acabou de reiniciar): recorre ao PostgreSQL.
        return serve_recent_from_db(state, uid, limit, None).await;
    }
    let next = (!exhausted && offset < MAX_FALLBACK_OFFSET).then(|| {
        Cursor {
            version: VERSION_TRENDING,
            offset,
        }
        .encode()
    });
    Ok(FeedResponse {
        user_id: uid,
        source: Source::FallbackTrending,
        feed_version: None,
        stale: true,
        next_cursor: next,
        items,
    })
}

async fn serve_recent_from_db(
    state: &AppState,
    uid: i64,
    limit: usize,
    cursor: Option<Cursor>,
) -> ApiResult<FeedResponse> {
    let Ok(_slot) = state.db_fallback_slots.clone().try_acquire_owned() else {
        metrics::FEED_SOURCE.with_label_values(&["shed"]).inc();
        return Err(ApiError::Unavailable);
    };
    let offset = match cursor {
        Some(c) if c.version == VERSION_RECENT => c.offset.min(MAX_FALLBACK_OFFSET),
        _ => 0,
    };
    let sql = content_sql!(
        "WHERE c.status = 'active' GROUP BY c.id, k.slug ORDER BY c.published_at DESC, c.id DESC LIMIT $1 OFFSET $2"
    );
    let rows = sqlx::query(sql)
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&state.pg)
        .await?;
    let mut items = Vec::with_capacity(rows.len());
    for (i, row) in rows.iter().enumerate() {
        let (id, c) = row_to_catalog(row)?;
        items.push(FeedItem {
            content_id: id,
            position: offset + i,
            author_id: c.author_id,
            category: c.category,
            tags: c.tags,
            title: c.title,
            published_at: DateTime::<Utc>::from_timestamp(c.published_at, 0)
                .map(events::rfc3339)
                .unwrap_or_default(),
            score: None,
            reason: "fallback".into(),
        });
    }
    let next_offset = offset + items.len();
    let next = (items.len() == limit && next_offset < MAX_FALLBACK_OFFSET).then(|| {
        Cursor {
            version: VERSION_RECENT,
            offset: next_offset,
        }
        .encode()
    });
    Ok(FeedResponse {
        user_id: uid,
        source: Source::FallbackRecent,
        feed_version: None,
        stale: true,
        next_cursor: next,
        items,
    })
}

fn observe_page(items: &[FeedItem]) {
    metrics::ITEMS_SERVED.inc_by(items.len() as u64);
    if items.is_empty() {
        return;
    }
    let mut per_author: HashMap<i64, usize> = HashMap::new();
    for it in items {
        *per_author.entry(it.author_id).or_default() += 1;
    }
    metrics::PAGE_AUTHORS.observe(per_author.len() as f64);
    let top = per_author.values().copied().max().unwrap_or(0);
    metrics::PAGE_TOP_AUTHOR_SHARE.observe(top as f64 / items.len() as f64);
}

/// Impressão = exposição, publicada fora do caminho da resposta.
fn record_impression(
    state: &AppState,
    mut conn: ConnectionManager,
    uid: i64,
    items: &[FeedItem],
    rid: &str,
) {
    let mut env = Envelope::new(
        EventType::FeedImpression,
        Uuid::new_v4(),
        Utc::now(),
        Some(rid.to_string()),
    );
    env.user_id = Some(uid);
    env.content_ids = Some(items.iter().map(|i| i.content_id).collect());
    let maxlen = state.cfg.events_stream_maxlen;
    actix_web::rt::spawn(async move {
        if events::publish(&mut conn, &env, maxlen).await.is_err() {
            metrics::dependency_error("redis");
        } else {
            metrics::EVENTS_INGESTED
                .with_label_values(&["feed_impression"])
                .inc();
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cat(author: i64, active: bool) -> Catalog {
        Catalog {
            author_id: author,
            category: "games".into(),
            tags: vec!["rpg".into()],
            title: "t".into(),
            published_at: 1_700_000_000,
            active,
        }
    }

    fn cand(id: i64, seen: bool) -> Candidate {
        Candidate {
            content_id: id,
            position: id as usize,
            seen,
            info: Some(format!("0.{id}|author")),
        }
    }

    #[test]
    fn cursor_roundtrip_and_validation() {
        let c = Cursor {
            version: 1_760_000_000_123,
            offset: 40,
        };
        assert_eq!(Cursor::parse(&c.encode()).unwrap(), c);
        assert_eq!(Cursor::parse("-1:20").unwrap().version, -1);
        for bad in ["", "abc", "1:", ":3", "1:-2", "1:999999999"] {
            assert!(Cursor::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn select_page_filters_and_counts() {
        let mut catalog = HashMap::new();
        catalog.insert(1, cat(10, true));
        catalog.insert(2, cat(11, false)); // removido
        catalog.insert(3, cat(66, true)); // autor bloqueado
        catalog.insert(4, cat(12, true)); // já visto
        catalog.insert(5, cat(13, true));
        let blocked = HashSet::from([66]);
        let mut counts = FilterCounts::default();
        let cands = vec![
            cand(1, false),
            cand(2, false),
            cand(3, false),
            cand(4, true),
            cand(9, false),
            cand(5, false),
        ];
        let (items, consumed) =
            select_page(cands, &catalog, &blocked, 10, 0, "recent", &mut counts);
        assert_eq!(
            items.iter().map(|i| i.content_id).collect::<Vec<_>>(),
            vec![1, 5]
        );
        assert_eq!(consumed, 6);
        assert_eq!(
            counts,
            FilterCounts {
                seen: 1,
                removed: 1,
                blocked: 1,
                missing: 1
            }
        );
        assert_eq!(items[0].reason, "author");
        assert_eq!(items[0].score, Some(0.1));
    }

    #[test]
    fn select_page_stops_at_limit_and_reports_consumed() {
        let catalog: HashMap<i64, Catalog> = (1..=10).map(|i| (i, cat(i, true))).collect();
        let mut counts = FilterCounts::default();
        let cands = (1..=10).map(|i| cand(i, false)).collect();
        let (items, consumed) = select_page(
            cands,
            &catalog,
            &HashSet::new(),
            5,
            2,
            "recent",
            &mut counts,
        );
        assert_eq!(items.len(), 3);
        assert_eq!(consumed, 3, "o offset avança só pelo que foi examinado");
    }

    #[test]
    fn parse_catalog_requires_core_fields() {
        let full = vec![
            Some("7".into()),
            Some("games".into()),
            Some("rpg,fps".into()),
            Some("Título".into()),
            Some("1700000000".into()),
            Some("active".into()),
        ];
        let c = parse_catalog(&full).unwrap();
        assert_eq!(c.tags, vec!["rpg", "fps"]);
        assert!(c.active);
        let empty: Vec<Option<String>> = vec![None; 6];
        assert!(parse_catalog(&empty).is_none());
    }
}
