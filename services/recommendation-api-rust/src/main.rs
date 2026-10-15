use actix_cors::Cors;
use actix_web::{App, HttpServer, middleware::from_fn, web};
use recommendation_api::{config::Config, configure, metrics, observe, state::AppState};
use tracing_subscriber::EnvFilter;

fn cors(origins: &[String]) -> Cors {
    // Restritivo por padrão: sem CORS_ALLOWED_ORIGINS, nenhuma origem de navegador é aceita.
    origins.iter().fold(
        Cors::default()
            .allowed_methods(vec!["GET", "POST", "DELETE"])
            .allowed_headers(vec!["authorization", "content-type", "x-request-id"])
            .max_age(600),
        |c, o| c.allowed_origin(o),
    )
}

/// `recommendation-api healthcheck`: GET /health local, sem depender de curl na
/// imagem (o runtime é debian-slim sem utilitários de rede).
fn healthcheck() -> std::io::Result<()> {
    use std::io::{Read, Write};
    let addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".into());
    let port = addr.rsplit(':').next().unwrap_or("8080");
    let mut s = std::net::TcpStream::connect(format!("127.0.0.1:{port}"))?;
    s.set_read_timeout(Some(std::time::Duration::from_secs(2)))?;
    s.write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")?;
    let mut buf = [0u8; 64];
    let n = s.read(&mut buf)?;
    if buf[..n].starts_with(b"HTTP/1.1 200") {
        Ok(())
    } else {
        Err(std::io::Error::other("health != 200"))
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        return healthcheck();
    }
    tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_current_span(false)
        .with_env_filter(
            EnvFilter::try_from_env("LOG_LEVEL").unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cfg = Config::from_env().map_err(std::io::Error::other)?;
    metrics::init();
    if cfg.auth_mode == recommendation_api::config::AuthMode::Disabled {
        tracing::warn!(
            "AUTH_MODE=disabled: qualquer cliente age como qualquer usuário (somente desenvolvimento)"
        );
    }
    let bind = cfg.bind_addr.clone();
    let workers = cfg.workers;
    let json_limit = cfg.json_limit_bytes;
    let origins = cfg.cors_origins.clone();
    let state = web::Data::new(AppState::new(cfg).await.map_err(std::io::Error::other)?);
    tracing::info!(%bind, workers, "api_starting");

    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .app_data(web::PayloadConfig::new(json_limit))
            .wrap(from_fn(observe))
            .wrap(cors(&origins))
            .configure(|c| configure(c, json_limit))
    })
    .workers(workers)
    .backlog(2048)
    .max_connections(25_000)
    .client_request_timeout(std::time::Duration::from_secs(5))
    .keep_alive(std::time::Duration::from_secs(30))
    // SIGTERM: para de aceitar conexões e espera as requisições em curso.
    .shutdown_timeout(20)
    .bind(&bind)?
    .run()
    .await
}
