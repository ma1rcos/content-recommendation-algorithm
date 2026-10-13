//! Erros padronizados: `{"error": {"code", "message", "request_id"}}`.
//! Mensagens internas (Redis/PostgreSQL) são logadas, nunca devolvidas ao cliente.

use actix_web::{HttpResponse, ResponseError, http::StatusCode};
use serde::Serialize;

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("{0}")]
    BadRequest(String),
    #[error("não autenticado")]
    Unauthorized,
    #[error("sem permissão para este recurso")]
    Forbidden,
    #[error("{0}")]
    NotFound(&'static str),
    #[error("{0}")]
    Unprocessable(String),
    #[error("limite de requisições excedido")]
    RateLimited,
    #[error("serviço temporariamente indisponível")]
    Unavailable,
    #[error("erro interno")]
    Internal,
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    error: ErrorDetail<'a>,
}

#[derive(Serialize)]
struct ErrorDetail<'a> {
    code: &'a str,
    message: String,
}

impl ApiError {
    pub fn code(&self) -> &'static str {
        match self {
            ApiError::BadRequest(_) => "bad_request",
            ApiError::Unauthorized => "unauthorized",
            ApiError::Forbidden => "forbidden",
            ApiError::NotFound(_) => "not_found",
            ApiError::Unprocessable(_) => "unprocessable",
            ApiError::RateLimited => "rate_limited",
            ApiError::Unavailable => "unavailable",
            ApiError::Internal => "internal",
        }
    }
}

impl ResponseError for ApiError {
    fn status_code(&self) -> StatusCode {
        match self {
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::Unauthorized => StatusCode::UNAUTHORIZED,
            ApiError::Forbidden => StatusCode::FORBIDDEN,
            ApiError::NotFound(_) => StatusCode::NOT_FOUND,
            ApiError::Unprocessable(_) => StatusCode::UNPROCESSABLE_ENTITY,
            ApiError::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            ApiError::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            ApiError::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse {
        let mut resp = HttpResponse::build(self.status_code());
        if matches!(self, ApiError::RateLimited) {
            resp.insert_header(("Retry-After", "1"));
        }
        resp.json(ErrorBody {
            error: ErrorDetail {
                code: self.code(),
                message: self.to_string(),
            },
        })
    }
}

impl From<redis::RedisError> for ApiError {
    fn from(e: redis::RedisError) -> Self {
        crate::metrics::dependency_error("redis");
        tracing::warn!(error = %e, "redis_error");
        ApiError::Unavailable
    }
}

impl From<sqlx::Error> for ApiError {
    fn from(e: sqlx::Error) -> Self {
        crate::metrics::dependency_error("postgres");
        tracing::warn!(error = %e, "postgres_error");
        ApiError::Unavailable
    }
}

pub type ApiResult<T> = Result<T, ApiError>;
