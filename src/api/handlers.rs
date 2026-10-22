use crate::api::{AppState, AuthUser};
use crate::error::{AppError, DomainError};
use crate::model::{Level, Order, QuoteSnapshot, Side, StockMeta, Tick, TradeRecord};
use crate::service::OrderRequest;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/health", get(health))
        .route("/api/clock", get(clock))
        .route("/api/status", get(status))
        .route("/api/auth/register", post(register))
        .route("/api/auth/login", post(login))
        .route("/api/auth/logout", post(logout))
        .route("/api/me/settings", get(get_settings).post(save_settings))
        .route("/api/me/reset", post(reset_account))
        .route("/api/me/practice/next", post(practice_next))
        .route("/api/me/practice/clock", post(save_clock))
        .route("/api/quotes", get(quotes))
        .route("/api/quotes/{code}/minute", get(minute))
        .route("/api/quotes/{code}/ticks", get(ticks))
        .route("/api/search", get(search))
        .route("/api/fee-estimate", get(fee_estimate))
        .route("/api/me/account", get(account))
        .route("/api/me/orders", get(my_orders))
        .route("/api/me/trades", get(my_trades))
        .route("/api/orders", post(place_order))
        .route("/api/orders/{id}", delete(cancel_order))
        .route("/api/watchlist", get(watchlist).post(add_watch))
        .route("/api/watchlist/{code}", delete(remove_watch))
        .merge(crate::api::ws::router())
        .merge(crate::api::replay::router())
        .with_state(state)
}

async fn health() -> Json<Value> {
    Json(json!({ "ok": true }))
}

/// 民用历 → 天数（Howard Hinnant 算法，避免引入 chrono）
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn parse_ymd(s: &str) -> Option<(i64, i64, i64)> {
    if s.len() != 8 {
        return None;
    }
    Some((s[0..4].parse().ok()?, s[4..6].parse().ok()?, s[6..8].parse().ok()?))
}

/// 练手日按真实日期推进：过去一个工作日就 +1 天，跳过周末，最多到当天
fn advance_practice_day(start: &str, anchor: &str, today: &str) -> String {
    let (Some((sy, sm, sd)), Some((ay, am, ad)), Some((ty, tm, td))) =
        (parse_ymd(start), parse_ymd(anchor), parse_ymd(today))
    else {
        return start.to_string();
    };
    let mut cur = days_from_civil(sy, sm, sd);
    let anchor_days = days_from_civil(ay, am, ad);
    let today_days = days_from_civil(ty, tm, td);
    let mut elapsed = today_days - anchor_days;
    while elapsed > 0 {
        cur += 1;
        let (_, m, d) = civil_from_days(cur);
        let (y2, m2, d2) = civil_from_days(cur);
        let wd = (cur + 4) % 7; // 0=周日 6=周六
        let _ = (m, d, y2, m2, d2);
        if wd != 0 && wd != 6 {
            elapsed -= 1;
        }
        if cur >= today_days {
            break;
        }
    }
    if cur > today_days {
        cur = today_days;
    }
    let (y, m, d) = civil_from_days(cur);
    format!("{y:04}{m:02}{d:02}")
}

/// 前往下一天：练手日 +1（跳过周末，不超过权威交易日）
async fn practice_next(
    State(st): State<Arc<AppState>>,
    user: AuthUser,
) -> Result<Json<SettingsView>, AppError> {
    let (today, _, _) = crate::api::today_authoritative(&st.cfg.tdx_base).await;
    let acc = st.db.load_account(user.0.user_id).await?;
    let initial = acc.map(|(a, _)| a.initial).unwrap_or(0);
    let Some((cb, mc, sb, sd)) = st.db.load_user_settings(user.0.user_id).await? else {
        return Err(DomainError::BadRequest("请先完成初始设置".into()).into());
    };
    let next = advance_practice_day(&sd, &sd, "99999999");
    // advance_practice_day 以「锚点→今天」推进，这里只走一天：手工算下一个工作日
    let (y, m, d) = parse_ymd(&sd).ok_or_else(|| DomainError::BadRequest("练手日格式错误".into()))?;
    let mut cur = days_from_civil(y, m, d) + 1;
    while matches!((cur + 4) % 7, 0 | 6) {
        cur += 1;
    }
    let (ty, tm, td) = parse_ymd(&today).unwrap_or((9999, 12, 31));
    let today_days = days_from_civil(ty, tm, td);
    if cur > today_days {
        cur = today_days;
    }
    let (ny, nm, nd) = civil_from_days(cur);
    let new_day = format!("{ny:04}{nm:02}{nd:02}");
    // 当日尚未收盘（东八区 15:00 前）不允许跳到"今天"，否则分时还没走完
    if new_day == today {
        let ms = crate::model::now_ms() + 8 * 3_600_000;
        let mins = (ms / 60_000) % (24 * 60);
        if mins < 15 * 60 {
            return Err(DomainError::BadRequest(format!(
                "今天还没收盘（当前 {}），收盘后才能前往下一天",
                format!("{:02}:{:02}", mins / 60, mins % 60)
            ))
            .into());
        }
    }
    let _ = next;
    st.db
        .save_user_settings(user.0.user_id, cb, mc, sb, &new_day)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("保存练手日失败: {e}")))?;
    let _ = st.db.save_practice_clock(user.0.user_id, "09:15:00").await;
    let state = st.db.load_practice_state(user.0.user_id).await.ok().flatten();
    Ok(Json(SettingsView {
        configured: true,
        initial,
        commission_bp: cb,
        min_commission: mc,
        stamp_bp: sb,
        start_date: new_day,
        initial_date: state.as_ref().map(|s| s.0.clone()).unwrap_or_default(),
        practice_clock: "09:15:00".into(),
    }))
}

/// 前端每 10 秒回报一次分时进度，刷新后可从该时刻续播
async fn save_clock(
    State(st): State<Arc<AppState>>,
    user: AuthUser,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, AppError> {
    let clock = body.get("clock").and_then(|v| v.as_str()).unwrap_or("").to_string();
    if clock.len() != 8 {
        return Err(DomainError::BadRequest("时间格式应为 HH:MM:SS".into()).into());
    }
    st.db
        .save_practice_clock(user.0.user_id, &clock)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("保存进度失败: {e}")))?;
    Ok(Json(serde_json::json!({ "ok": true, "clock": clock })))
}

/// 转世重生：清空持仓/委托/成交，资金回到初始值
async fn reset_account(
    State(st): State<Arc<AppState>>,
    user: AuthUser,
) -> Result<Json<serde_json::Value>, AppError> {
    st.db
        .clear_trading(user.0.user_id)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("清空交易记录失败: {e}")))?;
    st.core.accounts.reset_user(user.0.user_id);
    // 让内存里的挂单也从撮合队列撤掉
    st.core.cancel_active_of(user.0.user_id);
    Ok(Json(serde_json::json!({ "ok": true })))
}

/// 服务端权威日期：给前端做"不许超前"的上限校验
async fn clock(State(st): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let (today, workday, source) = crate::api::today_authoritative(&st.cfg.tdx_base).await;
    Json(serde_json::json!({ "today": today, "is_workday": workday, "source": source }))
}

async fn status(State(st): State<Arc<AppState>>) -> Json<Value> {
    let ms = st.market.status();
    Json(json!({
        "mode": st.cfg.mode,
        "market": ms,
        "pending_orders": st.core.engine.pending_count(),
        "ws_clients": st.core.ws.client_count(),
        "match_latency_us": st.core.match_latency_us.iter().map(|e| (e.key().clone(), *e.value())).collect::<HashMap<_,_>>(),
    }))
}

#[derive(Deserialize)]
struct Credentials {
    username: String,
    password: String,
}

#[derive(Serialize)]
struct AuthResponse {
    token: String,
    user_id: i64,
    username: String,
}

fn hash_password(pw: &str) -> Result<String, AppError> {
    let salt_bytes: [u8; 16] = rand::random();
    let salt = SaltString::encode_b64(&salt_bytes)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("盐值编码失败: {e}")))?;
    Argon2::default()
        .hash_password(pw.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| AppError::Internal(anyhow::anyhow!("口令哈希失败: {e}")))
}

fn verify_password(pw: &str, hash: &str) -> bool {
    match PasswordHash::new(hash) {
        Ok(parsed) => Argon2::default()
            .verify_password(pw.as_bytes(), &parsed)
            .is_ok(),
        Err(_) => false,
    }
}

async fn register(
    State(st): State<Arc<AppState>>,
    Json(body): Json<Credentials>,
) -> Result<Json<AuthResponse>, AppError> {
    let username = body.username.trim().to_string();
    if username.chars().count() < 3 || username.chars().count() > 32 || body.password.len() < 6 {
        return Err(DomainError::WeakCredentials.into());
    }
    if st.core.db.find_user(&username).await?.is_some() {
        return Err(DomainError::UserExists.into());
    }
    let hash = hash_password(&body.password)?;
    let user_id = st
        .core
        .db
        .create_user(&username, &hash, st.cfg.initial_cash)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("创建用户失败: {e}")))?;
    st.core.cache_username(user_id, &username);
    st.core.ensure_loaded(user_id).await?;
    let token = st.new_token(user_id, &username);
    Ok(Json(AuthResponse {
        token,
        user_id,
        username,
    }))
}

async fn login(
    State(st): State<Arc<AppState>>,
    Json(body): Json<Credentials>,
) -> Result<Json<AuthResponse>, AppError> {
    let username = body.username.trim().to_string();
    let Some((user_id, uname, hash)) = st.core.db.find_user(&username).await? else {
        return Err(DomainError::BadCredentials.into());
    };
    if !verify_password(&body.password, &hash) {
        return Err(DomainError::BadCredentials.into());
    }
    st.core.cache_username(user_id, &uname);
    st.core.ensure_loaded(user_id).await?;
    let token = st.new_token(user_id, &uname);
    // 令牌落库：服务重启后仍然有效
    if let Err(e) = st.db.save_session(&token, user_id, &uname).await {
        tracing::warn!(error = %e, "登录令牌落库失败");
    }
    Ok(Json(AuthResponse {
        token,
        user_id,
        username: uname,
    }))
}

async fn logout(State(st): State<Arc<AppState>>, user: AuthUser) -> StatusCode {
    if let Ok(mut map) = st.sessions.lock() {
        map.remove(&user.0.token);
    }
    if let Err(e) = st.db.delete_session(&user.0.token).await {
        tracing::warn!(error = %e, "注销令牌清理失败");
    }
    StatusCode::NO_CONTENT
}

#[derive(Serialize, Deserialize)]
pub struct SettingsView {
    pub configured: bool,
    /// 初始资金（厘）
    pub initial: i64,
    /// 佣金，基点（万分之几 × 10）
    pub commission_bp: i64,
    /// 最低佣金（厘）
    pub min_commission: i64,
    /// 印花税，基点（千分之几 × 10）
    pub stamp_bp: i64,
    /// 开始实盘操练的日期 YYYYMMDD（当前练手日）
    pub start_date: String,
    /// 最初的练手日
    #[serde(default)]
    pub initial_date: String,
    /// 分时进度（09:15~15:00），由服务端维护
    #[serde(default)]
    pub practice_clock: String,
}

async fn get_settings(
    State(st): State<Arc<AppState>>,
    user: AuthUser,
) -> Result<Json<SettingsView>, AppError> {
    let acc = st.db.load_account(user.0.user_id).await?;
    let initial = acc.map(|(a, _)| a.initial).unwrap_or(0);
    let (today, _, _) = crate::api::today_authoritative(&st.cfg.tdx_base).await;
    match st.db.load_user_settings(user.0.user_id).await? {
        Some((cb, mc, sb, sd)) => {
            // 练手日随真实时间推进：anchor 记在 updated_at 上
            let anchor = st.db.settings_anchor(user.0.user_id).await.unwrap_or_default();
            let advanced = if sd.len() == 8 && anchor.len() == 8 {
                advance_practice_day(&sd, &anchor, &today)
            } else {
                sd.clone()
            };
            if advanced != sd {
                let _ = st
                    .db
                    .save_user_settings(user.0.user_id, cb, mc, sb, &advanced)
                    .await;
            }
            let mut state = st.db.load_practice_state(user.0.user_id).await.ok().flatten();
            // 老数据没有 initial_date：用当前练手日补一次
            if let Some((init, cur, _)) = state.clone() {
                if init.is_empty() && cur.len() == 8 {
                    let _ = st.db.save_user_settings(user.0.user_id, cb, mc, sb, &cur).await;
                    state = Some((cur.clone(), cur, "09:15:00".to_string()));
                }
            }
            Ok(Json(SettingsView {
                configured: true,
                initial,
                commission_bp: cb,
                min_commission: mc,
                stamp_bp: sb,
                start_date: advanced,
                initial_date: state.as_ref().map(|s| s.0.clone()).unwrap_or_default(),
                practice_clock: state.as_ref().map(|s| s.2.clone()).unwrap_or_else(|| "09:15:00".into()),
            }))
        }
        None => Ok(Json(SettingsView {
            configured: false,
            initial,
            commission_bp: 25,
            min_commission: 5000,
            stamp_bp: 10,
            start_date: String::new(),
            initial_date: String::new(),
            practice_clock: "09:15:00".into(),
        })),
    }
}

async fn save_settings(
    State(st): State<Arc<AppState>>,
    user: AuthUser,
    Json(body): Json<SettingsView>,
) -> Result<Json<SettingsView>, AppError> {
    if body.initial <= 0 {
        return Err(AppError::Domain(DomainError::BadRequest("初始资金必须大于 0".into())));
    }
    // 只允许在建仓前调整初始资金
    let has_pos = !st.core.accounts.positions_of(user.0.user_id).is_empty();
    if !has_pos {
        st.db.set_initial_cash(user.0.user_id, body.initial).await?;
        st.core.accounts.reset_initial(user.0.user_id, body.initial);
    }
    st.db
        .save_user_settings(
            user.0.user_id,
            body.commission_bp,
            body.min_commission,
            body.stamp_bp,
            &body.start_date,
        )
        .await?;
    let state = st.db.load_practice_state(user.0.user_id).await.ok().flatten();
    Ok(Json(SettingsView {
        configured: true,
        initial_date: state.as_ref().map(|s| s.0.clone()).unwrap_or_default(),
        practice_clock: state
            .as_ref()
            .map(|s| s.2.clone())
            .unwrap_or_else(|| "09:15:00".into()),
        ..body
    }))
}

#[derive(Serialize)]
pub struct QuoteView {
    pub code: String,
    pub name: String,
    pub last: i64,
    pub prev_close: i64,
    pub open: i64,
    pub high: i64,
    pub low: i64,
    pub total_hand: i64,
    pub amount: i64,
    pub limit_up: i64,
    pub limit_down: i64,
    pub is_st: bool,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub ts: i64,
}

fn to_view(q: &QuoteSnapshot, bids: Vec<Level>, asks: Vec<Level>) -> QuoteView {
    QuoteView {
        code: q.code.clone(),
        name: q.name.clone(),
        last: q.last,
        prev_close: q.prev_close,
        open: q.open,
        high: q.high,
        low: q.low,
        total_hand: q.total_hand,
        amount: q.amount,
        limit_up: q.limit_up,
        limit_down: q.limit_down,
        is_st: q.is_st,
        bids,
        asks,
        ts: q.ts,
    }
}

#[derive(Deserialize)]
struct CodesQuery {
    codes: Option<String>,
}

async fn quotes(
    State(st): State<Arc<AppState>>,
    Query(q): Query<CodesQuery>,
) -> Result<Json<Vec<QuoteView>>, AppError> {
    let codes: Vec<String> = q
        .codes
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if codes.is_empty() {
        return Ok(Json(vec![]));
    }
    // 回放模式下不允许向实时行情源补拉，避免把当日实时数据混入回放
    if st.cfg.mode == "live" {
        let missing: Vec<String> = codes
            .iter()
            .filter(|c| st.market.quote(c).is_none())
            .cloned()
            .collect();
        if !missing.is_empty() {
            let _ = st.market.refresh(&missing, &st.core).await;
        }
    }
    let mut out = Vec::new();
    for code in codes {
        if let Some(q) = st.market.quote(&code) {
            let (bids, asks) = st
                .core
                .engine
                .l2_levels(&code)
                .unwrap_or_else(|| (q.bids.clone(), q.asks.clone()));
            out.push(to_view(&q, bids, asks));
        }
    }
    Ok(Json(out))
}

async fn minute(
    State(st): State<Arc<AppState>>,
    Path(code): Path<String>,
) -> Result<Json<Value>, AppError> {
    let points = st
        .market
        .minute_cached(&code)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("分时数据获取失败: {e}")))?;
    let prev_close = st.market.quote(&code).map(|q| q.prev_close).unwrap_or(0);
    let name = st.market.name(&code);
    Ok(Json(json!({
        "code": code,
        "name": name,
        "prev_close": prev_close,
        "points": points,
    })))
}

async fn ticks(State(st): State<Arc<AppState>>, Path(code): Path<String>) -> Json<Vec<Tick>> {
    let _ = st.market.ensure_ticks(&code).await;
    Json(st.market.ticks_of(&code).as_ref().clone())
}

#[derive(Deserialize)]
struct SearchQuery {
    q: Option<String>,
    keyword: Option<String>,
}

async fn search(
    State(st): State<Arc<AppState>>,
    Query(q): Query<SearchQuery>,
) -> Json<Vec<StockMeta>> {
    let kw = q.q.or(q.keyword).unwrap_or_default();
    Json(st.market.search(&kw, 30))
}

async fn account(
    State(st): State<Arc<AppState>>,
    user: AuthUser,
) -> Result<Json<crate::model::AccountView>, AppError> {
    st.core.ensure_loaded(user.0.user_id).await?;
    st.core
        .account_view(user.0.user_id)
        .map(Json)
        .ok_or_else(|| DomainError::AccountMissing.into())
}

#[derive(Deserialize)]
struct FeeQuery {
    side: String,
    price: i64,
    qty: i64,
}

/// 下单前的费用预估，与成交时的实际计费使用同一套规则（佣金按委托累计、最低 5 元）。
async fn fee_estimate(Query(q): Query<FeeQuery>) -> Result<Json<Value>, AppError> {
    let side = Side::parse(&q.side)
        .ok_or_else(|| DomainError::BadRequest(format!("方向非法: {}", q.side)))?;
    if q.price <= 0 || q.qty <= 0 {
        return Err(DomainError::BadRequest("价格和数量必须大于 0".into()).into());
    }
    let f = crate::engine::fees::compute(q.price, q.qty, side);
    Ok(Json(json!({
        "amount": f.amount,
        "commission": f.commission,
        "stamp_tax": f.stamp_tax,
        "total_fee": f.total_fee(),
        "net": f.net(side),
    })))
}

#[derive(Deserialize)]
struct LimitQuery {
    limit: Option<i64>,
}

async fn my_orders(
    State(st): State<Arc<AppState>>,
    user: AuthUser,
    Query(q): Query<LimitQuery>,
) -> Result<Json<Vec<Order>>, AppError> {
    let limit = q.limit.unwrap_or(200).clamp(1, 1000);
    // 库内历史 + 内存中最新的委托状态（落盘存在批量窗口）
    let mut merged: HashMap<String, Order> = st
        .core
        .db
        .orders_of(user.0.user_id, limit)
        .await?
        .into_iter()
        .map(|o| (o.id.clone(), o))
        .collect();
    for o in st.core.recent_orders(user.0.user_id, 300) {
        merged.insert(o.id.clone(), o);
    }
    let mut rows: Vec<Order> = merged.into_values().collect();
    rows.sort_by(|a, b| b.created_at.cmp(&a.created_at).then(b.seq.cmp(&a.seq)));
    rows.truncate(limit as usize);
    Ok(Json(rows))
}

async fn my_trades(
    State(st): State<Arc<AppState>>,
    user: AuthUser,
    Query(q): Query<LimitQuery>,
) -> Result<Json<Vec<TradeRecord>>, AppError> {
    let limit = q.limit.unwrap_or(200).clamp(1, 1000);
    let rows = st.core.db.trades_of(user.0.user_id, limit).await?;
    Ok(Json(rows))
}

#[derive(Deserialize)]
struct PlaceOrderBody {
    code: String,
    side: String,
    price: i64,
    qty: i64,
}

async fn place_order(
    State(st): State<Arc<AppState>>,
    user: AuthUser,
    Json(body): Json<PlaceOrderBody>,
) -> Result<Json<Order>, AppError> {
    let side = Side::parse(&body.side)
        .ok_or_else(|| DomainError::BadRequest(format!("方向非法: {}", body.side)))?;
    st.core.ensure_loaded(user.0.user_id).await?;
    let quote = match st.market.quote(&body.code) {
        Some(q) => q,
        None => {
            if st.cfg.mode == "live" {
                let _ = st.market.refresh(&[body.code.clone()], &st.core).await;
            }
            st.market
                .quote(&body.code)
                .ok_or_else(|| DomainError::QuoteUnavailable(body.code.clone()))?
        }
    };
    let order = st.core.submit(
        user.0.user_id,
        OrderRequest {
            code: body.code,
            side,
            price: body.price,
            qty: body.qty,
        },
        &quote,
    )?;
    Ok(Json(order))
}

async fn cancel_order(
    State(st): State<Arc<AppState>>,
    user: AuthUser,
    Path(id): Path<String>,
) -> Result<Json<Order>, AppError> {
    st.core.ensure_loaded(user.0.user_id).await?;
    let o = st.core.cancel(user.0.user_id, &id)?;
    Ok(Json(o))
}

#[derive(Serialize)]
struct WatchItem {
    code: String,
    name: String,
}

async fn watchlist(
    State(st): State<Arc<AppState>>,
    user: AuthUser,
) -> Result<Json<Vec<WatchItem>>, AppError> {
    let codes = st.core.db.watchlist(user.0.user_id).await?;
    Ok(Json(
        codes
            .into_iter()
            .map(|code| WatchItem {
                name: st.market.name(&code),
                code,
            })
            .collect(),
    ))
}

#[derive(Deserialize)]
struct WatchBody {
    code: String,
}

async fn add_watch(
    State(st): State<Arc<AppState>>,
    user: AuthUser,
    Json(body): Json<WatchBody>,
) -> Result<Json<Vec<WatchItem>>, AppError> {
    if !st.market.known(&body.code) {
        let _ = st.market.refresh(&[body.code.clone()], &st.core).await;
    }
    st.core
        .db
        .add_watch(user.0.user_id, &body.code)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("自选写入失败: {e}")))?;
    watchlist(State(st), user).await
}

async fn remove_watch(
    State(st): State<Arc<AppState>>,
    user: AuthUser,
    Path(code): Path<String>,
) -> Result<Json<Vec<WatchItem>>, AppError> {
    st.core
        .db
        .remove_watch(user.0.user_id, &code)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("自选删除失败: {e}")))?;
    watchlist(State(st), user).await
}
