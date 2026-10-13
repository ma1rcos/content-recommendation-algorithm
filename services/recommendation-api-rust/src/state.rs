//! Estado compartilhado entre os workers do Actix. Nada aqui guarda dados de
//! usuário: a API é stateless e pode ter N réplicas atrás de um balanceador.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use tokio::sync::Semaphore;

use crate::{breaker::Breaker, config::Config};

pub struct AppState {
    pub cfg: Config,
    redis: Vec<ConnectionManager>,
    next: AtomicUsize,
    pub pg: PgPool,
    pub http: reqwest::Client,
    pub breaker: Breaker,
    /// Limita chamadas simultâneas ao motor (backpressure): excedente vai ao fallback.
    pub engine_slots: Arc<Semaphore>,
    /// Limita o fallback no PostgreSQL: sob sobrecarga, o fallback não pode
    /// virar uma segunda onda de carga sobre o banco (cascata). Excedente → 503.
    pub db_fallback_slots: Arc<Semaphore>,
}

impl AppState {
    pub async fn new(cfg: Config) -> Result<Self, String> {
        let client =
            redis::Client::open(cfg.redis_url.as_str()).map_err(|e| format!("REDIS_URL: {e}"))?;
        let rcfg = ConnectionManagerConfig::new()
            .set_response_timeout(Some(cfg.redis_timeout))
            .set_connection_timeout(Some(std::time::Duration::from_secs(2)))
            .set_number_of_retries(2)
            .set_max_delay(std::time::Duration::from_millis(500));
        // Algumas conexões multiplexadas reduzem head-of-line blocking sob carga.
        let mut redis = Vec::with_capacity(cfg.redis_connections);
        for _ in 0..cfg.redis_connections {
            let conn = ConnectionManager::new_with_config(client.clone(), rcfg.clone())
                .await
                .map_err(|e| format!("conexão com Redis: {e}"))?;
            redis.push(conn);
        }
        let pg_opts: PgConnectOptions = cfg
            .database_url
            .parse::<PgConnectOptions>()
            .map_err(|e| format!("DATABASE_URL: {e}"))?
            .options([("statement_timeout", "2000")]);
        // connect_lazy: a API sobe mesmo com o PostgreSQL fora (feeds em cache continuam servidos).
        let pg = PgPoolOptions::new()
            .max_connections(cfg.db_max_connections)
            .acquire_timeout(std::time::Duration::from_secs(1))
            .connect_lazy_with(pg_opts);
        let http = reqwest::Client::builder()
            .timeout(cfg.engine_timeout)
            .connect_timeout(std::time::Duration::from_millis(300))
            .pool_max_idle_per_host(cfg.engine_max_concurrency)
            .build()
            .map_err(|e| format!("cliente HTTP: {e}"))?;
        Ok(Self {
            breaker: Breaker::new(cfg.breaker_failures, cfg.breaker_open),
            engine_slots: Arc::new(Semaphore::new(cfg.engine_max_concurrency)),
            db_fallback_slots: Arc::new(Semaphore::new(
                (cfg.db_max_connections as usize / 2).max(1),
            )),
            redis,
            next: AtomicUsize::new(0),
            pg,
            http,
            cfg,
        })
    }

    pub fn redis(&self) -> ConnectionManager {
        let i = self.next.fetch_add(1, Ordering::Relaxed) % self.redis.len();
        self.redis[i].clone()
    }
}
