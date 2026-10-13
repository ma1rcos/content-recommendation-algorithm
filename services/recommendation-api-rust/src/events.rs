//! Envelope de eventos publicado em `rec:events` (contrato:
//! contracts/schemas/event-envelope.schema.json) e enfileiramento de jobs.

use chrono::{DateTime, SecondsFormat, Utc};
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::keys;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    ContentViewed,
    ContentLiked,
    ContentSaved,
    ContentShared,
    ContentCommented,
    FeedImpression,
    ContentPublished,
    ContentRemoved,
}

impl EventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventType::ContentViewed => "content_viewed",
            EventType::ContentLiked => "content_liked",
            EventType::ContentSaved => "content_saved",
            EventType::ContentShared => "content_shared",
            EventType::ContentCommented => "content_commented",
            EventType::FeedImpression => "feed_impression",
            EventType::ContentPublished => "content_published",
            EventType::ContentRemoved => "content_removed",
        }
    }

    /// Tipos que o cliente pode enviar por POST /v1/events.
    pub fn is_client_interaction(&self) -> bool {
        matches!(
            self,
            EventType::ContentViewed
                | EventType::ContentLiked
                | EventType::ContentSaved
                | EventType::ContentShared
                | EventType::ContentCommented
        )
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Envelope {
    pub v: u8,
    pub event_id: Uuid,
    #[serde(rename = "type")]
    pub kind: EventType,
    pub user_id: Option<i64>,
    pub content_id: Option<i64>,
    pub content_ids: Option<Vec<i64>>,
    pub occurred_at: String,
    pub received_at: String,
    pub request_id: Option<String>,
}

pub fn rfc3339(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Millis, true)
}

impl Envelope {
    pub fn new(
        kind: EventType,
        event_id: Uuid,
        occurred_at: DateTime<Utc>,
        request_id: Option<String>,
    ) -> Self {
        Self {
            v: 1,
            event_id,
            kind,
            user_id: None,
            content_id: None,
            content_ids: None,
            occurred_at: rfc3339(occurred_at),
            received_at: rfc3339(Utc::now()),
            request_id: request_id.map(|r| r.chars().take(64).collect()),
        }
    }
}

/// Adiciona o comando XADD (com MAXLEN aproximado) a um pipeline.
pub fn xadd_event(pipe: &mut redis::Pipeline, env: &Envelope, maxlen: usize) {
    let payload = serde_json::to_string(env).expect("envelope serializável");
    pipe.cmd("XADD")
        .arg(keys::EVENTS_STREAM)
        .arg("MAXLEN")
        .arg("~")
        .arg(maxlen)
        .arg("*")
        .arg("payload")
        .arg(payload)
        .ignore();
}

pub async fn publish(
    conn: &mut ConnectionManager,
    env: &Envelope,
    maxlen: usize,
) -> redis::RedisResult<()> {
    let mut pipe = redis::pipe();
    xadd_event(&mut pipe, env, maxlen);
    pipe.query_async::<()>(conn).await
}

#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EnqueueResult {
    Queued,
    AlreadyQueued,
}

/// Enfileira regeneração do feed com deduplicação: no máximo um job por
/// usuário por janela (`rec:job:pending:{uid}` com SET NX EX).
pub async fn enqueue_rebuild(
    conn: &mut ConnectionManager,
    uid: i64,
    reason: &str,
    dedup_secs: u64,
    maxlen: usize,
) -> redis::RedisResult<EnqueueResult> {
    let now = Utc::now().timestamp();
    let acquired: Option<String> = redis::cmd("SET")
        .arg(keys::job_pending(uid))
        .arg(now)
        .arg("NX")
        .arg("EX")
        .arg(dedup_secs)
        .query_async(conn)
        .await?;
    if acquired.is_none() {
        return Ok(EnqueueResult::AlreadyQueued);
    }
    redis::cmd("XADD")
        .arg(keys::JOBS_STREAM)
        .arg("MAXLEN")
        .arg("~")
        .arg(maxlen)
        .arg("*")
        .arg("user_id")
        .arg(uid)
        .arg("reason")
        .arg(reason)
        .arg("ts")
        .arg(now)
        .query_async::<()>(conn)
        .await?;
    Ok(EnqueueResult::Queued)
}
