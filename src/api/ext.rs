use crate::api::AppState;
use axum::body::Bytes;
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use std::sync::Arc;

/// 数据接口透传：把 /api/ext/<path> 原样转给本机 data_interface（D1/D2/D4/D6），
/// 前端只与本服务同源通信，避免跨域，也便于统一加超时与错误格式。
pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/ext/{*path}", any(proxy))
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
    let mut url = format!("{}/{}", st.cfg.d5_base.trim_end_matches('/'), path);
    if let Some(q) = query {
        url.push('?');
        url.push_str(&q);
    }

    let client = reqwest::Client::new();
    let mut req = client
        .request(method.clone(), &url)
        .timeout(std::time::Duration::from_secs(25));
    // 只透传内容类型，避免把 Host/Content-Length 等逐跳头带过去
    if let Some(ct) = headers.get(reqwest::header::CONTENT_TYPE) {
        req = req.header(reqwest::header::CONTENT_TYPE, ct);
    }
    if method != Method::GET && !body.is_empty() {
        req = req.body(body.to_vec());
    }

    match req.send().await {
        Ok(resp) => {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            let code =
                StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            // 上游返回什么就原样回什么，前端直接按 JSON 解析
            (code, [("content-type", "application/json; charset=utf-8")], text).into_response()
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            [("content-type", "application/json; charset=utf-8")],
            format!(
                "{{\"code\":-1,\"message\":\"数据源不可达: {}\"}}",
                e.to_string().replace('"', "'")
            ),
        )
            .into_response(),
    }
}
