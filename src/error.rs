use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

/// 领域错误：请求路径上可预期的失败，全部带稳定错误码。
#[derive(Debug, thiserror::Error)]
pub enum DomainError {
    #[error("未认证或登录已过期")]
    Unauthorized,
    #[error("用户名或密码错误")]
    BadCredentials,
    #[error("用户名已存在")]
    UserExists,
    #[error("用户名长度需 3-32 位，密码至少 6 位")]
    WeakCredentials,
    #[error("委托不存在")]
    OrderNotFound,
    #[error("委托已结束，无法撤单")]
    OrderNotCancellable,
    #[error("参数错误: {0}")]
    BadRequest(String),
    #[error("账户状态异常")]
    AccountMissing,
    #[error("行情未就绪: {0}")]
    QuoteUnavailable(String),
}

impl DomainError {
    pub fn code(&self) -> &'static str {
        match self {
            DomainError::Unauthorized => "unauthorized",
            DomainError::BadCredentials => "bad_credentials",
            DomainError::UserExists => "user_exists",
            DomainError::WeakCredentials => "weak_credentials",
            DomainError::OrderNotFound => "order_not_found",
            DomainError::OrderNotCancellable => "order_not_cancellable",
            DomainError::BadRequest(_) => "bad_request",
            DomainError::AccountMissing => "account_missing",
            DomainError::QuoteUnavailable(_) => "quote_unavailable",
        }
    }

    pub fn status(&self) -> StatusCode {
        match self {
            DomainError::Unauthorized => StatusCode::UNAUTHORIZED,
            DomainError::BadCredentials => StatusCode::UNAUTHORIZED,
            DomainError::UserExists => StatusCode::CONFLICT,
            DomainError::WeakCredentials => StatusCode::BAD_REQUEST,
            DomainError::OrderNotFound => StatusCode::NOT_FOUND,
            DomainError::OrderNotCancellable => StatusCode::CONFLICT,
            DomainError::BadRequest(_) => StatusCode::BAD_REQUEST,
            DomainError::AccountMissing => StatusCode::FORBIDDEN,
            DomainError::QuoteUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
}

/// API 统一错误体。
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl From<sqlx::Error> for AppError {
    fn from(e: sqlx::Error) -> Self {
        AppError::Internal(anyhow::anyhow!("db: {e}"))
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        match self {
            AppError::Domain(e) => {
                tracing::debug!(error = %e, "领域错误");
                (
                    e.status(),
                    Json(json!({ "code": e.code(), "message": e.to_string() })),
                )
                    .into_response()
            }
            AppError::Internal(e) => {
                tracing::error!(error = %e, "内部错误");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "code": "internal", "message": "服务内部错误" })),
                )
                    .into_response()
            }
        }
    }
}
