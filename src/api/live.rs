use crate::api::AppState;
use axum::body::Bytes;
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use std::sync::Arc;

/// 实时行情透传：/api/live/<path> → tdx-api（cfg.tdx_base）。
/// 自选股的当日盘口与逐笔走这里，与回放数据完全隔离。
pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/live/{*path}", any(proxy))
        .with_state(state)
}

async fn proxy(
    State(st): State<Arc<AppState>>,
    Path(path): Path<String>,
    RawQuery(query): RawQuery,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let mut url = format!("{}/{}", st.cfg.tdx_base.trim_end_matches('/'), path);
    if let Some(q) = query {
        url.push('?');
        url.push_str(&q);
    }

    let client = reqwest::Client::new();
    let mut req = client
        .request(method.clone(), &url)
        .timeout(std::time::Duration::from_secs(20));
    if let Some(ct) = headers.get(reqwest::header::CONTENT_TYPE) {
        req = req.header(reqwest::header::CONTENT_TYPE, ct);
    }
    if method != Method::GET && !body.is_empty() {
        req = req.body(body.to_vec());
    }

    match req.send().await {
        Ok(resp) => {
            let code = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let text = resp.text().await.unwrap_or_default();
            (code, [("content-type", "application/json; charset=utf-8")], text).into_response()
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            [("content-type", "application/json; charset=utf-8")],
            format!(
                "{{\"code\":-1,\"message\":\"行情源不可达: {}\"}}",
                e.to_string().replace('"', "'")
            ),
        )
            .into_response(),
    }
}
