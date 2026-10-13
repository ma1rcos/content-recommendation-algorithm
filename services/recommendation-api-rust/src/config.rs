//! Configuração por variáveis de ambiente (ver .env.example na raiz).

use std::{env, time::Duration};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthMode {
    /// Tokens HMAC assinados (padrão). Ver `auth.rs`.
    Hmac,
    /// Sem autenticação — apenas desenvolvimento local. O `user_id` do path é aceito.
    Disabled,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub bind_addr: String,
    pub workers: usize,
    pub redis_url: String,
    pub redis_connections: usize,
    pub redis_timeout: Duration,
    pub database_url: String,
    pub db_max_connections: u32,
    pub engine_url: String,
    pub internal_token: String,
    pub engine_timeout: Duration,
    pub engine_max_concurrency: usize,
    pub breaker_failures: u32,
    pub breaker_open: Duration,
    pub feed_soft_ttl_ms: i64,
    pub rebuild_dedup_secs: u64,
    pub build_lock_secs: u64,
    pub auth_mode: AuthMode,
    pub auth_secret: String,
    pub cors_origins: Vec<String>,
    pub events_stream_maxlen: usize,
    pub jobs_stream_maxlen: usize,
    pub rate_limit_events_rps: u64,
    pub rate_limit_rebuild_per_min: u64,
    pub max_page_size: usize,
    pub json_limit_bytes: usize,
    pub max_events_per_request: usize,
}

fn var(name: &str, default: &str) -> String {
    env::var(name)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn num<T: std::str::FromStr>(name: &str, default: T) -> Result<T, String> {
    match env::var(name) {
        Ok(v) if !v.is_empty() => v.parse().map_err(|_| format!("{name} inválido: {v}")),
        _ => Ok(default),
    }
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let auth_mode = match var("AUTH_MODE", "hmac").as_str() {
            "hmac" => AuthMode::Hmac,
            "disabled" => AuthMode::Disabled,
            other => return Err(format!("AUTH_MODE inválido: {other} (use hmac|disabled)")),
        };
        let auth_secret = var("AUTH_SECRET", "");
        if auth_mode == AuthMode::Hmac && auth_secret.len() < 32 {
            return Err(
                "AUTH_SECRET precisa de pelo menos 32 caracteres com AUTH_MODE=hmac".into(),
            );
        }
        Ok(Self {
            bind_addr: var("BIND_ADDR", "0.0.0.0:8080"),
            workers: num(
                "HTTP_WORKERS",
                std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(2),
            )?,
            redis_url: var("REDIS_URL", "redis://127.0.0.1:6379/0"),
            redis_connections: num("REDIS_CONNECTIONS", 4usize)?.max(1),
            redis_timeout: Duration::from_millis(num("REDIS_TIMEOUT_MS", 300u64)?),
            database_url: var("DATABASE_URL", "postgresql://reco_app@127.0.0.1:5432/reco"),
            db_max_connections: num("DB_MAX_CONNECTIONS", 10u32)?,
            engine_url: var("ENGINE_URL", "http://127.0.0.1:8000"),
            internal_token: var("INTERNAL_TOKEN", ""),
            engine_timeout: Duration::from_millis(num("ENGINE_TIMEOUT_MS", 800u64)?),
            engine_max_concurrency: num("ENGINE_MAX_CONCURRENCY", 4usize)?.max(1),
            breaker_failures: num("BREAKER_FAILURES", 5u32)?.max(1),
            breaker_open: Duration::from_millis(num("BREAKER_OPEN_MS", 10_000u64)?),
            feed_soft_ttl_ms: num("FEED_SOFT_TTL_SECONDS", 600i64)? * 1000,
            rebuild_dedup_secs: num("REBUILD_DEDUP_SECONDS", 120u64)?.max(1),
            build_lock_secs: num("BUILD_LOCK_SECONDS", 15u64)?.max(1),
            auth_mode,
            auth_secret,
            cors_origins: var("CORS_ALLOWED_ORIGINS", "")
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            events_stream_maxlen: num("EVENTS_STREAM_MAXLEN", 1_000_000usize)?,
            jobs_stream_maxlen: num("JOBS_STREAM_MAXLEN", 100_000usize)?,
            rate_limit_events_rps: num("RATE_LIMIT_EVENTS_RPS", 20u64)?,
            rate_limit_rebuild_per_min: num("RATE_LIMIT_REBUILD_PER_MIN", 6u64)?,
            max_page_size: 50,
            json_limit_bytes: 64 * 1024,
            max_events_per_request: 100,
        })
    }

    /// Configuração para testes (sem ler o ambiente além do necessário).
    pub fn for_tests(redis_url: &str, database_url: &str, engine_url: &str) -> Self {
        Self {
            bind_addr: "127.0.0.1:0".into(),
            workers: 1,
            redis_url: redis_url.into(),
            redis_connections: 1,
            redis_timeout: Duration::from_millis(500),
            database_url: database_url.into(),
            db_max_connections: 2,
            engine_url: engine_url.into(),
            internal_token: "test-internal-token".into(),
            engine_timeout: Duration::from_millis(3000),
            engine_max_concurrency: 4,
            breaker_failures: 2,
            breaker_open: Duration::from_millis(60_000),
            feed_soft_ttl_ms: 600_000,
            rebuild_dedup_secs: 10,
            build_lock_secs: 15,
            auth_mode: AuthMode::Hmac,
            auth_secret: "test-secret-test-secret-test-secret!!".into(),
            cors_origins: vec![],
            events_stream_maxlen: 10_000,
            jobs_stream_maxlen: 10_000,
            rate_limit_events_rps: 1000,
            rate_limit_rebuild_per_min: 1000,
            max_page_size: 50,
            json_limit_bytes: 64 * 1024,
            max_events_per_request: 100,
        }
    }
}
