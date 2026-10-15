//! API de recomendação (Actix Web). `configure` monta as rotas e é usado tanto
//! pelo binário quanto pelos testes de integração.

pub mod auth;
pub mod breaker;
pub mod config;
pub mod error;
pub mod events;
pub mod feed;
pub mod keys;
pub mod metrics;
pub mod routes;
pub mod state;

use std::time::Instant;

use actix_web::{
    Error, HttpMessage, HttpResponse,
    body::MessageBody,
    dev::{ServiceRequest, ServiceResponse},
    http::header::{HeaderName, HeaderValue},
    middleware::Next,
    web,
};

use crate::{error::ApiError, routes::RequestId};

pub fn configure(cfg: &mut web::ServiceConfig, json_limit: usize) {
    cfg.app_data(
        web::JsonConfig::default()
            .limit(json_limit)
            .error_handler(|err, _| {
                actix_web::error::InternalError::from_response(
                    err.to_string(),
                    HttpResponse::BadRequest().json(serde_json::json!({
                        "error": {"code": "bad_request", "message": format!("JSON inválido: {err}")}
                    })),
                )
                .into()
            }),
    )
    .app_data(
        web::PathConfig::default()
            .error_handler(|_, _| ApiError::BadRequest("parâmetro de rota inválido".into()).into()),
    )
    .app_data(
        web::QueryConfig::default()
            .error_handler(|e, _| ApiError::BadRequest(format!("query inválida: {e}")).into()),
    )
    .route("/health", web::get().to(routes::health))
    .route("/ready", web::get().to(routes::ready))
    .route("/metrics", web::get().to(routes::prometheus))
    .service(
        web::scope("/v1")
            .route("/feed/{user_id}", web::get().to(routes::get_feed))
            .route("/events", web::post().to(routes::post_events))
            .route("/content", web::post().to(routes::post_content))
            .route("/content/{content_id}", web::get().to(routes::get_content))
            .route(
                "/content/{content_id}",
                web::delete().to(routes::delete_content),
            )
            .route("/users", web::post().to(routes::post_user))
            .route(
                "/users/{user_id}/interests",
                web::get().to(routes::get_interests),
            )
            .route(
                "/recommendations/rebuild/{user_id}",
                web::post().to(routes::post_rebuild),
            ),
    )
    .default_service(web::to(|| async {
        ApiError::NotFound("rota não encontrada").into_response()
    }));
}

trait IntoResp {
    fn into_response(self) -> HttpResponse;
}
impl IntoResp for ApiError {
    fn into_response(self) -> HttpResponse {
        actix_web::ResponseError::error_response(&self)
    }
}

static X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");

/// Middleware: ID de correlação (aceita o do cliente se for curto e ASCII),
/// métricas HTTP com a rota como padrão e log estruturado por requisição.
pub async fn observe(
    req: ServiceRequest,
    next: Next<impl MessageBody>,
) -> Result<ServiceResponse<impl MessageBody>, Error> {
    let rid = req
        .headers()
        .get(&X_REQUEST_ID)
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty() && v.len() <= 64 && v.bytes().all(|b| b.is_ascii_graphic()))
        .map(String::from)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    req.extensions_mut().insert(RequestId(rid.clone()));
    let method = req.method().as_str().to_owned();
    let route = req.match_pattern().unwrap_or_else(|| "unmatched".into());
    let started = Instant::now();
    let mut res = next.call(req).await?;
    let elapsed = started.elapsed();
    let status = res.status().as_u16();
    metrics::HTTP_REQUESTS
        .with_label_values(&[&method, &route, &status.to_string()])
        .inc();
    metrics::HTTP_LATENCY
        .with_label_values(&[&method, &route])
        .observe(elapsed.as_secs_f64());
    if let Ok(v) = HeaderValue::from_str(&rid) {
        res.headers_mut().insert(X_REQUEST_ID.clone(), v);
    }
    if route != "/metrics" && route != "/health" {
        tracing::info!(request_id = %rid, method = %method, route = %route, status,
                       duration_ms = elapsed.as_secs_f64() * 1000.0, "http_request");
    }
    Ok(res)
}
