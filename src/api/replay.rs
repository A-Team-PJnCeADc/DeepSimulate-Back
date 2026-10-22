use crate::api::AppState;
use crate::error::{AppError, DomainError};
use crate::replay::{parse_clock_us, ReplayStatus, StartReq};
use std::collections::HashMap;
use axum::extract::{Path, Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/replay/status", get(status))
        .route("/api/replay/days", get(days))
        .route("/api/replay/days/{date}/codes", get(codes))
        .route("/api/replay/import", post(import))
        .route("/api/replay/import/status", get(import_status))
        .route("/api/replay/start", post(start))
        .route("/api/replay/control", post(control))
        .route("/api/replay/stop", post(stop))
        .route("/api/replay/probe/{date}/{code}", get(probe))
        .route("/api/replay/minute/{code}", get(minute))
        .route("/api/replay/quotes", get(replay_quotes))
        .route("/api/replay/quote/{code}", get(replay_quote))
        .route("/api/replay/ticks/{code}", get(replay_ticks))
}

async fn status(State(st): State<Arc<AppState>>) -> Json<ReplayStatus> {
    Json(st.replay.status())
}

#[derive(Deserialize)]
struct DaysQuery {
    #[serde(default)]
    source: Option<String>,
}

/// 已导入交易日；source=d5 时同时返回 D5 可用的最新日期。
async fn days(
    State(st): State<Arc<AppState>>,
    Query(q): Query<DaysQuery>,
) -> Result<Json<Value>, AppError> {
    let imported = st
        .ingestor
        .imported_days()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("读取导入记录失败: {e}")))?;
    let (today, _, _) = crate::api::today_authoritative(&st.cfg.tdx_base).await;
    // 今天是否已有归档：D5 的当日文件要收盘后才可下载，所以今天通常是 false
    let today_ready = imported.iter().any(|(d, _)| d == &today);
    let mut out = json!({
        "today": today,
        "today_ready": today_ready,
        "imported": imported.iter().map(|(d, n)| json!({"date": d, "codes": n})).collect::<Vec<_>>(),
    });
    if q.source.as_deref() == Some("d5") {
        let available = st
            .ingestor
            .d5_dates()
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("读取 D5 日期失败: {e}")))?;
        out["available"] = json!(available.iter().take(60).collect::<Vec<_>>());
    }
    Ok(Json(out))
}

async fn codes(
    State(st): State<Arc<AppState>>,
    Path(date): Path<String>,
) -> Result<Json<Value>, AppError> {
    let list = st
        .ingestor
        .imported_codes(&date)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("读取导入股票失败: {e}")))?;
    Ok(Json(json!({ "date": date, "codes": list })))
}

#[derive(Deserialize)]
struct ImportReq {
    date: String,
    /// 不填则导入自选股池
    #[serde(default)]
    codes: Option<Vec<String>>,
    #[serde(default)]
    force: Option<bool>,
}

async fn import(
    State(st): State<Arc<AppState>>,
    Json(body): Json<ImportReq>,
) -> Result<Json<Value>, AppError> {
    if body.date.len() != 8 {
        return Err(DomainError::BadRequest("date 需为 YYYYMMDD".into()).into());
    }
    let codes = match body.codes {
        Some(c) if !c.is_empty() => c,
        _ => st
            .market
            .default_codes()
            .into_iter()
            .collect::<Vec<String>>(),
    };
    let running = st.ingestor.status().running;
    if running {
        return Err(DomainError::BadRequest("已有导入任务在运行".into()).into());
    }
    let ingestor = st.ingestor.clone();
    let date = body.date.clone();
    let force = body.force.unwrap_or(true);
    let count = codes.len();
    let codes_for_task = codes.clone();
    tokio::spawn(async move {
        ingestor.set_force(force);
        if let Err(e) = ingestor.import_day(&date, &codes_for_task).await {
            tracing::error!(error = %e, "导入任务失败");
        }
    });
    Ok(Json(json!({ "started": true, "date": body.date, "codes": count })))
}

async fn import_status(State(st): State<Arc<AppState>>) -> Json<Value> {
    Json(json!(st.ingestor.status()))
}

async fn start(
    State(st): State<Arc<AppState>>,
    Json(body): Json<StartReq>,
) -> Result<Json<ReplayStatus>, AppError> {
    // 回放与实时轮询可以共存：回放只按虚拟时钟推进历史归档，实时行情不受影响
    // 日期不得超前于权威交易日（以行情源 /api/workday 为准）
    let (today, _, _) = crate::api::today_authoritative(&st.cfg.tdx_base).await;
    if body.date.as_str() > today.as_str() {
        return Err(DomainError::BadRequest(format!(
            "回放日期 {} 晚于当前交易日 {}",
            body.date, today
        ))
        .into());
    }
    let clock = body.clock.unwrap_or_else(|| "09:25:00".into());
    parse_clock_us(&body.date, &clock).map_err(|e| DomainError::BadRequest(e.to_string()))?;
    let speed = body.speed.unwrap_or(1.0);
    let codes = body.codes.unwrap_or_else(|| st.market.default_codes());
    st.replay
        .start(
            st.market.clone(),
            st.core.clone(),
            body.date.clone(),
            clock,
            speed,
            codes,
        )
        .map_err(|e| DomainError::BadRequest(e.to_string()))?;
    Ok(Json(st.replay.status()))
}

#[derive(Deserialize)]
struct ControlReq {
    action: String,
    #[serde(default)]
    value: Option<f64>,
}

async fn control(
    State(st): State<Arc<AppState>>,
    Json(body): Json<ControlReq>,
) -> Result<Json<ReplayStatus>, AppError> {
    match body.action.as_str() {
        "pause" => st.replay.pause(true),
        "resume" => st.replay.pause(false),
        "speed" => st
            .replay
            .set_speed(body.value.unwrap_or(1.0)),
        _ => return Err(DomainError::BadRequest(format!("未知动作: {}", body.action)).into()),
    }
    Ok(Json(st.replay.status()))
}

async fn stop(State(st): State<Arc<AppState>>) -> Json<ReplayStatus> {
    st.replay.stop();
    Json(st.replay.status())
}

/// 回放日分时：由真实 10 档快照按分钟聚合，均价为当日累计 VWAP。
/// 回放日某只标的的最后一帧盘口（从 QuestDB 读，回放不在跑也能用）
async fn replay_quote(
    State(st): State<Arc<AppState>>,
    Path(code): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> Result<Json<Value>, AppError> {
    let date = q.get("date").cloned().unwrap_or_else(|| st.replay.status().date);
    if date.len() != 8 {
        return Err(DomainError::BadRequest("当前没有回放日".into()).into());
    }
    let day = format!("{}-{}-{}", &date[0..4], &date[4..6], &date[6..8]);
    let until = q.get("until").cloned().unwrap_or_default();
    let bound = if until.len() >= 5 { format!(" AND ts <= '{day}T{until}'") } else { String::new() };
    let sql = format!(
        "SELECT ts,last,cum_vol,cum_amt,open_p,high_p,low_p,prev_close,
                bid1,bid2,bid3,bid4,bid5,bidv1,bidv2,bidv3,bidv4,bidv5,
                ask1,ask2,ask3,ask4,ask5,askv1,askv2,askv3,askv4,askv5
         FROM snapshots WHERE code = '{code}' AND ts IN '{day}'{bound} ORDER BY ts DESC LIMIT 1"
    );
    let rows = st.replay.exec(&sql).await.map_err(|e| AppError::Internal(anyhow::anyhow!("查询盘口失败: {e}")))?;
    let Some(r) = rows.first() else {
        return Err(DomainError::BadRequest(format!("{code} 在 {date} 没有快照")).into());
    };
    let n = |i: usize| r.get(i).and_then(|v| v.as_i64()).unwrap_or(0);
    let mut bids = Vec::new();
    let mut asks = Vec::new();
    for i in 0..5 {
        let bp = n(8 + i);
        let bv = n(13 + i);
        if bp > 0 {
            bids.push(json!({"price": bp, "volume": bv}));
        }
        let ap = n(18 + i);
        let av = n(23 + i);
        if ap > 0 {
            asks.push(json!({"price": ap, "volume": av}));
        }
    }
    let last = n(1);
    let prev_close = n(7);
    let pct = if prev_close > 0 { 0.1 } else { 0.1 };
    let round10 = |v: f64| (v / 10.0).round() * 10.0;
    Ok(Json(json!({
        "date": date,
        "code": code,
        "last": last,
        "prev_close": prev_close,
        "open": n(4),
        "high": n(5),
        "low": n(6),
        "total_hand": n(2) / 100,
        "amount": n(3),
        "limit_up": round10(prev_close as f64 * (1.0 + pct)) as i64,
        "limit_down": round10(prev_close as f64 * (1.0 - pct)) as i64,
        "bids": bids,
        "asks": asks,
        "ts": r.first().and_then(|v| v.as_str()).unwrap_or(""),
    })))
}

/// 批量：多个标的在回放某时刻的最后一帧（自选股列表用）
async fn replay_quotes(
    State(st): State<Arc<AppState>>,
    Query(q): Query<HashMap<String, String>>,
) -> Result<Json<Value>, AppError> {
    let date = q.get("date").cloned().unwrap_or_else(|| st.replay.status().date);
    if date.len() != 8 {
        return Err(DomainError::BadRequest("当前没有回放日".into()).into());
    }
    let day = format!("{}-{}-{}", &date[0..4], &date[4..6], &date[6..8]);
    let until = q.get("until").cloned().unwrap_or_default();
    let bound = if until.len() >= 5 { format!(" AND ts <= '{day}T{until}'") } else { String::new() };
    let codes: Vec<String> = q
        .get("codes")
        .map(|v| v.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect())
        .unwrap_or_default();
    let mut out = serde_json::Map::new();
    for code in codes.iter().take(50) {
        let sql = format!(
            "SELECT last,prev_close FROM snapshots
             WHERE code = '{code}' AND ts IN '{day}'{bound} ORDER BY ts DESC LIMIT 1"
        );
        if let Ok(rows) = st.replay.exec(&sql).await {
            if let Some(r) = rows.first() {
                let last = r.first().and_then(|v| v.as_i64()).unwrap_or(0);
                let prev = r.get(1).and_then(|v| v.as_i64()).unwrap_or(0);
                if last > 0 {
                    out.insert(code.clone(), json!({ "code": code, "last": last, "prev_close": prev }));
                }
            }
        }
    }
    Ok(Json(json!({ "date": date, "quotes": out })))
}

/// 回放日某只标的的逐笔成交（时间倒序）
async fn replay_ticks(
    State(st): State<Arc<AppState>>,
    Path(code): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> Result<Json<Value>, AppError> {
    let date = q.get("date").cloned().unwrap_or_else(|| st.replay.status().date);
    if date.len() != 8 {
        return Err(DomainError::BadRequest("当前没有回放日".into()).into());
    }
    let day = format!("{}-{}-{}", &date[0..4], &date[4..6], &date[6..8]);
    let limit = q.get("limit").and_then(|v| v.parse::<i64>().ok()).unwrap_or(2000).clamp(1, 20000);
    let until = q.get("until").cloned().unwrap_or_default();
    let bound = if until.len() >= 5 { format!(" AND ts <= '{day}T{until}'") } else { String::new() };
    let sql = format!(
        "SELECT ts,price,qty,side,flag FROM trades_l2
         WHERE code = '{code}' AND ts IN '{day}'{bound} ORDER BY ts DESC LIMIT {limit}"
    );
    let rows = st.replay.exec(&sql).await.map_err(|e| AppError::Internal(anyhow::anyhow!("查询逐笔失败: {e}")))?;
    let mut out = Vec::new();
    for r in rows {
        let ts = r.first().and_then(|v| v.as_str()).unwrap_or("");
        let price = r.get(1).and_then(|v| v.as_i64()).unwrap_or(0);
        let qty = r.get(2).and_then(|v| v.as_i64()).unwrap_or(0);
        let side = r.get(3).and_then(|v| v.as_str()).unwrap_or("");
        let flag = r.get(4).and_then(|v| v.as_str()).unwrap_or("");
        if price <= 0 || qty <= 0 || flag == "C" {
            continue;
        }
        out.push(json!({
            "time": if ts.len() >= 19 { &ts[11..19] } else { ts },
            "price": price,
            "volume": qty,
            "side": if side == "B" { "buy" } else { "sell" },
        }));
    }
    Ok(Json(json!({ "date": date, "code": code, "ticks": out })))
}

async fn minute(
    State(st): State<Arc<AppState>>,
    Path(code): Path<String>,
) -> Result<Json<Value>, AppError> {
    let date = st.replay.status().date;
    if date.len() != 8 {
        return Err(DomainError::BadRequest("当前没有回放日".into()).into());
    }
    let day = format!("{}-{}-{}", &date[0..4], &date[4..6], &date[6..8]);
    let sql = format!(
        "SELECT ts, last(last) p, max(cum_vol) cv, max(cum_amt) ca
         FROM snapshots WHERE code = '{code}' AND ts IN '{day}'
         SAMPLE BY 1m ALIGN TO CALENDAR"
    );
    let rows = st
        .replay
        .exec(&sql)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("查询分时失败: {e}")))?;
    let mut points = Vec::new();
    let mut prev_cv = 0i64;
    for r in rows {
        let ts = r.first().and_then(|v| v.as_str()).unwrap_or("");
        let p = r.get(1).and_then(|v| v.as_i64()).unwrap_or(0);
        let cv = r.get(2).and_then(|v| v.as_i64()).unwrap_or(0);
        let ca = r.get(3).and_then(|v| v.as_i64()).unwrap_or(0);
        if p <= 0 || ts.len() < 16 {
            prev_cv = cv;
            continue;
        }
        let hhmm = &ts[11..16];
        let vol = (cv - prev_cv).max(0);
        prev_cv = cv;
        let avg = if cv > 0 { ca / cv } else { p };
        points.push(json!({ "time": hhmm, "price": p, "volume": vol, "avg": avg }));
    }
    let q = st.market.quote(&code);
    Ok(Json(json!({
        "code": code,
        "name": st.market.name(&code),
        "prev_close": q.map(|x| x.prev_close).unwrap_or(0),
        "date": date,
        "points": points,
    })))
}

/// 数据自检：某日某股的三张表条数与时间范围。
async fn probe(
    State(st): State<Arc<AppState>>,
    Path((date, code)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    let (s, e, t) = st
        .ingestor
        .counts(&date, &code)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("统计失败: {e}")))?;
    let desc = st
        .ingestor
        .describe(&date, &code)
        .await
        .unwrap_or_else(|e| e.to_string());
    Ok(Json(json!({
        "date": date, "code": code,
        "snapshots": s, "entrusts": e, "trades": t,
        "detail": desc,
    })))
}
