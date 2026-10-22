pub mod ext;
pub mod handlers;
pub mod live;
pub mod replay;
pub mod ws;

use crate::config::Config;
use crate::error::{AppError, DomainError};
use crate::ingest::Ingestor;
use crate::market::Market;
use crate::replay::Replay;
use crate::service::Core;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct Session {
    pub user_id: i64,
    pub username: String,
    pub token: String,
}

pub struct AppState {
    pub cfg: Arc<Config>,
    pub db: crate::db::Db,
    pub market: Arc<Market>,
    pub core: Arc<Core>,
    pub replay: Arc<Replay>,
    pub ingestor: Arc<Ingestor>,
    pub sessions: Mutex<HashMap<String, Session>>,
}

/// 交易日以行情源为准（tdx-api /api/workday），拿不到再退回本机日期
pub async fn today_authoritative(tdx_base: &str) -> (String, bool, &'static str) {
    let url = format!("{}/api/workday", tdx_base.trim_end_matches('/'));
    if let Ok(resp) = reqwest::Client::new().get(&url).timeout(std::time::Duration::from_secs(5)).send().await {
        if let Ok(v) = resp.json::<serde_json::Value>().await {
            let d = v.pointer("/data/date/numeric").and_then(|x| x.as_str()).map(str::to_string);
            let wd = v.pointer("/data/is_workday").and_then(|x| x.as_bool()).unwrap_or(true);
            if let Some(d) = d {
                if d.len() == 8 {
                    return (d, wd, "tdx-api");
                }
            }
        }
    }
    (crate::model::today_cn(), false, "local")
}

impl AppState {
    pub fn new(
        db: crate::db::Db,
        cfg: Arc<Config>,
        market: Arc<Market>,
        core: Arc<Core>,
        replay: Arc<Replay>,
        ingestor: Arc<Ingestor>,
        persisted_sessions: Vec<(String, i64, String)>,
    ) -> Arc<AppState> {
        let mut map = HashMap::new();
        for (token, user_id, username) in persisted_sessions {
            map.insert(
                token.clone(),
                Session {
                    user_id,
                    username,
                    token,
                },
            );
        }
        Arc::new(AppState {
            db,
            sessions: Mutex::new(map),
            cfg,
            market,
            core,
            replay,
            ingestor,
        })
    }

    pub fn new_token(&self, user_id: i64, username: &str) -> String {
        let bytes: [u8; 24] = rand::random();
        let token: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        let session = Session {
            user_id,
            username: username.to_string(),
            token: token.clone(),
        };
        if let Ok(mut map) = self.sessions.lock() {
            map.insert(token.clone(), session);
        }
        token
    }

    pub fn session_of(&self, token: &str) -> Option<Session> {
        self.sessions.lock().ok()?.get(token).cloned()
    }
}

/// 从 Authorization 头取会话。
pub struct AuthUser(pub Session);

impl FromRequestParts<Arc<AppState>> for AuthUser {
    type Rejection = AppError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        let token = parts
            .headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .map(|s| s.to_string())
            .ok_or(DomainError::Unauthorized)?;
        state
            .session_of(&token)
            .map(AuthUser)
            .ok_or_else(|| DomainError::Unauthorized.into())
    }
}
