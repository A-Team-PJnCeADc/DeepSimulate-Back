use crate::ingest::Ingestor;
use crate::market::Market;
use crate::model::{now_ms, Level, QuoteSnapshot, Tick, TickSide};
use crate::service::Core;
use crate::tsdb::{fmt_clock, to_us, QuestDb};
use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// 回放状态，供 REST 查询。
#[derive(Debug, Clone, Default, Serialize)]
pub struct ReplayStatus {
    pub status: &'static str,
    pub date: String,
    pub clock: String,
    pub start: String,
    pub end: String,
    pub speed: f64,
    pub paused: bool,
    pub snapshots: usize,
    pub trades: usize,
    pub codes: Vec<String>,
    pub error: Option<String>,
    pub progress: f64,
    pub session_id: u64,
    /// 缺失数据自动补拉时的进度描述
    pub detail: String,
}

#[derive(Clone)]
struct SnapRow {
    ts: i64,
    code: String,
    last: i64,
    cum_vol: i64,
    cum_amt: i64,
    open: i64,
    high: i64,
    low: i64,
    prev_close: i64,
    bids: [(i64, i64); 10],
    asks: [(i64, i64); 10],
}

#[derive(Clone)]
struct TradeRow {
    ts: i64,
    code: String,
    price: i64,
    qty: i64,
    side: TickSide,
    /// true = 撤单流水（成交代码 C）
    cancel: bool,
    /// 撤单时的被撤方向（由叫买/叫卖序号判断）
    cancel_side: Option<crate::model::Side>,
    ask_seq: i64,
    bid_seq: i64,
}

#[derive(Clone)]
struct EntrustRow {
    ts: i64,
    seq: i64,
    code: String,
    side: crate::model::Side,
    price: i64,
    qty: i64,
}

struct Loaded {
    date: String,
    key: String,
    snaps: Vec<SnapRow>,
    trades: Vec<TradeRow>,
    entrusts: Vec<EntrustRow>,
}

/// 回放控制器：从 QuestDB 读取真实 L2 数据，按虚拟时钟驱动撮合与推送。
pub struct Replay {
    qdb: QuestDb,
    ingestor: Arc<Mutex<Option<Arc<Ingestor>>>>,
    status: Arc<Mutex<ReplayStatus>>,
    cache: Arc<Mutex<Option<Loaded>>>,
    session: Arc<AtomicU64>,
    running: Arc<Mutex<bool>>,
}

impl Replay {
    pub fn new(qdb: QuestDb) -> Self {
        Replay {
            qdb,
            ingestor: Arc::new(Mutex::new(None)),
            status: Arc::new(Mutex::new(ReplayStatus {
                status: "idle",
                speed: 1.0,
                ..Default::default()
            })),
            cache: Arc::new(Mutex::new(None)),
            session: Arc::new(AtomicU64::new(1)),
            running: Arc::new(Mutex::new(false)),
        }
    }

    /// 任意 SQL 查询（供回放相关只读接口复用）。
    pub async fn exec(&self, sql: &str) -> Result<Vec<Vec<serde_json::Value>>> {
        self.qdb.exec(sql).await
    }

    pub fn status(&self) -> ReplayStatus {
        self.status.lock().map(|s| s.clone()).unwrap_or_default()
    }

    fn set(&self, f: impl FnOnce(&mut ReplayStatus)) {
        if let Ok(mut st) = self.status.lock() {
            f(&mut st);
        }
    }

    /// 注入导入器：回放前自动补齐缺失的 L2 数据。
    pub fn set_ingestor(&self, ing: Arc<Ingestor>) {
        if let Ok(mut slot) = self.ingestor.lock() {
            *slot = Some(ing);
        }
    }

    pub fn pause(&self, paused: bool) {
        self.set(|s| s.paused = paused);
    }

    pub fn set_speed(&self, speed: f64) {
        self.set(|s| s.speed = speed.clamp(0.1, 200.0));
    }

    pub fn stop(&self) {
        self.set(|s| {
            s.status = "idle";
            s.paused = true;
        });
        if let Ok(mut r) = self.running.lock() {
            *r = false;
        }
    }

    /// 启动回放。会先撤销所有未完成委托，再从指定时刻重建盘口。
    pub fn start(
        self: &Arc<Self>,
        market: Arc<Market>,
        core: Arc<Core>,
        date: String,
        start_clock: String,
        speed: f64,
        codes: Vec<String>,
    ) -> Result<()> {
        if let Ok(r) = self.running.lock() {
            if *r {
                return Err(anyhow!("回放进行中，请先停止或等待结束"));
            }
        }
        let start_us = parse_clock_us(&date, &start_clock)?;
        // 每次启动都重新读库：导入新标的后无需重启进程即可生效
        if let Ok(mut c) = self.cache.lock() {
            *c = None;
        }
        let session = self.session.fetch_add(1, Ordering::Relaxed) + 1;
        core.cancel_all_active();
        self.set(|s| {
            s.status = "loading";
            s.date = date.clone();
            s.start = start_clock.clone();
            s.clock = start_clock.clone();
            s.speed = speed.clamp(0.1, 200.0);
            s.paused = false;
            s.error = None;
            s.progress = 0.0;
            s.session_id = session;
            s.snapshots = 0;
            s.trades = 0;
            s.codes = vec![];
            s.detail = String::new();
        });
        if let Ok(mut r) = self.running.lock() {
            *r = true;
        }
        let this = self.clone();
        tokio::spawn(async move {
            if let Err(e) = this.run(market, core, date, start_us, session, codes).await {
                tracing::error!(error = %e, "回放失败");
                this.set(|s| {
                    s.status = "error";
                    s.error = Some(e.to_string());
                });
            }
            if let Ok(mut r) = this.running.lock() {
                *r = false;
            }
        });
        Ok(())
    }

    /// 补齐缺失标的：已导入的跳过，其余从 D5 拉取后写入 QuestDB。
    async fn ensure_data(&self, date: &str, codes: &[String]) -> Result<()> {
        let ing = self
            .ingestor
            .lock()
            .ok()
            .and_then(|s| s.clone());
        let Some(ing) = ing else { return Ok(()) };
        if codes.is_empty() {
            return Ok(());
        }
        let have = ing.imported_codes(date).await.unwrap_or_default();
        let missing: Vec<String> = codes
            .iter()
            .filter(|c| !have.contains(c))
            .cloned()
            .collect();
        if missing.is_empty() {
            return Ok(());
        }
        tracing::info!(date, count = missing.len(), "回放前自动补数据");
        self.set(|s| {
            s.status = "importing";
            s.detail = format!("补数据 0/{}", missing.len());
        });
        for (i, code) in missing.iter().enumerate() {
            self.set(|s| s.detail = format!("补数据 {}/{}：{}", i, missing.len(), code));
            if let Err(e) = ing.import_one_public(date, code).await {
                tracing::warn!(date, code, error = %e, "自动补数据失败");
            }
        }
        self.set(|s| {
            s.status = "loading";
            s.detail = format!("已补 {} 只", missing.len());
        });
        Ok(())
    }

    async fn load(&self, date: &str, codes: &[String]) -> Result<Loaded> {
        let mut sorted = codes.to_vec();
        sorted.sort();
        let key = format!("{date}|{}", sorted.join(","));
        if let Ok(c) = self.cache.lock() {
            if let Some(l) = c.as_ref() {
                if l.key == key {
                    return Ok(Loaded {
                        date: l.date.clone(),
                        key: l.key.clone(),
                        snaps: l.snaps.clone(),
                        trades: l.trades.clone(),
                        entrusts: l.entrusts.clone(),
                    });
                }
            }
        }
        let day = format!("{}-{}-{}", &date[0..4], &date[4..6], &date[6..8]);
        let start = Instant::now();
        let filter = code_filter(codes);
        let snap_sql = format!(
            "SELECT ts,code,last,cum_vol,cum_amt,open_p,high_p,low_p,prev_close,
                    bid1,bid2,bid3,bid4,bid5,bid6,bid7,bid8,bid9,bid10,
                    bidv1,bidv2,bidv3,bidv4,bidv5,bidv6,bidv7,bidv8,bidv9,bidv10,
                    ask1,ask2,ask3,ask4,ask5,ask6,ask7,ask8,ask9,ask10,
                    askv1,askv2,askv3,askv4,askv5,askv6,askv7,askv8,askv9,askv10
             FROM snapshots WHERE ts IN '{day}'{filter} ORDER BY ts"
        );
        let csv = self.qdb.export_csv(&snap_sql).await.context("读取快照失败")?;
        let snaps = parse_snaps(&csv)?;
        let trade_sql = format!(
            "SELECT ts,code,price,qty,side,flag,ask_seq,bid_seq FROM trades_l2
             WHERE ts IN '{day}'{filter} ORDER BY ts"
        );
        let csv = self.qdb.export_csv(&trade_sql).await.context("读取逐笔成交失败")?;
        let mut trades = parse_trades(&csv)?;
        let ent_sql = format!(
            "SELECT ts,code,seq,side,price,qty FROM entrusts WHERE ts IN '{day}' AND qty > 0{filter} ORDER BY ts"
        );
        let csv = self.qdb.export_csv(&ent_sql).await.context("读取逐笔委托失败")?;
        let entrusts = parse_entrusts(&csv)?;
        // 撤单流水只有被撤委托号，用委托表还原其价格与方向
        let mut seq_map: std::collections::HashMap<i64, (crate::model::Side, i64)> =
            std::collections::HashMap::with_capacity(entrusts.len());
        for e in &entrusts {
            seq_map.insert(e.seq, (e.side, e.price));
        }
        trades.retain_mut(|t| {
            if !t.cancel {
                return t.price > 0 && t.qty > 0;
            }
            let seq = if t.bid_seq > 0 { t.bid_seq } else { t.ask_seq };
            match seq_map.get(&seq) {
                Some((side, price)) => {
                    t.cancel_side = Some(*side);
                    t.price = *price;
                    *price > 0 && t.qty > 0
                }
                None => false,
            }
        });
        tracing::info!(
            date,
            snaps = snaps.len(),
            trades = trades.len(),
            entrusts = entrusts.len(),
            ms = start.elapsed().as_millis(),
            "回放数据加载完成"
        );
        let loaded = Loaded {
            date: date.to_string(),
            key: key.clone(),
            snaps,
            trades,
            entrusts,
        };
        if let Ok(mut c) = self.cache.lock() {
            *c = Some(Loaded {
                date: loaded.date.clone(),
                key: loaded.key.clone(),
                snaps: loaded.snaps.clone(),
                trades: loaded.trades.clone(),
                entrusts: loaded.entrusts.clone(),
            });
        }
        Ok(loaded)
    }

    async fn run(
        &self,
        market: Arc<Market>,
        core: Arc<Core>,
        date: String,
        start_us: i64,
        session: u64,
        codes: Vec<String>,
    ) -> Result<()> {
        // 缺失标的直接向 D5 拉取并写入时序库（幂等缓存），无需人工预导入
        self.ensure_data(&date, &codes).await?;
        let loaded = self.load(&date, &codes).await?;
        if loaded.snaps.is_empty() {
            return Err(anyhow!("该日无快照数据，请先导入"));
        }
        let end_us = to_us(&date, 150_000_000);
        let codes: Vec<String> = {
            let mut v: Vec<String> = loaded.snaps.iter().map(|s| s.code.clone()).collect();
            v.sort();
            v.dedup();
            v
        };
        self.set(|s| {
            s.status = "running";
            s.codes = codes.clone();
            s.end = "15:00:00".into();
            s.snapshots = loaded.snaps.len();
            s.trades = loaded.trades.len();
        });

        let mut si = 0usize;
        let mut ti = 0usize;
        let mut ei = 0usize;
        // 定位到起始时刻
        while si < loaded.snaps.len() && loaded.snaps[si].ts < start_us {
            si += 1;
        }
        while ti < loaded.trades.len() && loaded.trades[ti].ts < start_us {
            ti += 1;
        }
        while ei < loaded.entrusts.len() && loaded.entrusts[ei].ts < start_us {
            ei += 1;
        }
        let mut clock = start_us;
        let mut last_real = Instant::now();
        let mut last_push: std::collections::HashMap<String, Instant> = Default::default();
        let mut tick_buf: Vec<Tick> = Vec::with_capacity(64);
        let mut tick_code = String::new();

        loop {
            if self.session.load(Ordering::Relaxed) != session {
                return Ok(());
            }
            {
                let st = self.status.lock().map(|s| s.clone()).unwrap_or_default();
                if st.paused {
                    last_real = Instant::now();
                    tokio::time::sleep(Duration::from_millis(120)).await;
                    continue;
                }
                if st.status == "idle" {
                    return Ok(());
                }
                clock += (last_real.elapsed().as_micros() as f64 * st.speed) as i64;
            }
            last_real = Instant::now();

            if clock >= end_us {
                if !tick_buf.is_empty() {
                    flush_ticks(&core, &mut tick_buf, &tick_code, &market);
                }
                self.set(|s| {
                    s.status = "finished";
                    s.clock = fmt_clock(end_us);
                    s.progress = 1.0;
                });
                return Ok(());
            }

            // 按时间序消费事件
            let mut touched = false;
            while (si < loaded.snaps.len() && loaded.snaps[si].ts <= clock)
                || (ti < loaded.trades.len() && loaded.trades[ti].ts <= clock)
                || (ei < loaded.entrusts.len() && loaded.entrusts[ei].ts <= clock)
            {
                // 三路按时间戳归并：快照 / 逐笔委托 / 逐笔成交
                let s_ts = loaded.snaps.get(si).map(|x| x.ts).unwrap_or(i64::MAX);
                let t_ts = loaded.trades.get(ti).map(|x| x.ts).unwrap_or(i64::MAX);
                let e_ts = loaded.entrusts.get(ei).map(|x| x.ts).unwrap_or(i64::MAX);
                let min = s_ts.min(t_ts).min(e_ts);
                if e_ts == min && e_ts != i64::MAX {
                    let row = &loaded.entrusts[ei];
                    ei += 1;
                    core.engine.on_entrust_add(&row.code, row.side, row.price, row.qty);
                    continue;
                }
                let take_snap = s_ts == min;
                if take_snap {
                    let row = &loaded.snaps[si];
                    si += 1;
                    let mut qv = snap_to_quote(row);
                    qv.name = market.name(&row.code);
                    let q = Arc::new(qv);
                    market.publish_quote(q.clone());
                    let batch = core.engine.on_snapshot(&q);
                    core.dispatch(batch);
                    let now = Instant::now();
                    let due = last_push
                        .get(&row.code)
                        .map(|t| now.duration_since(*t) > Duration::from_millis(120))
                        .unwrap_or(true);
                    if due {
                        last_push.insert(row.code.clone(), now);
                        if let Some((bids, asks)) = core.engine.l2_levels(&row.code) {
                            core.ws.push_replay_quote(&q, &bids, &asks);
                        }
                    }
                    touched = true;
                } else {
                    let row = &loaded.trades[ti];
                    ti += 1;
                    if row.cancel {
                        if let Some(side) = row.cancel_side {
                            flush_ticks(&core, &mut tick_buf, &tick_code, &market);
                            tick_code.clear();
                            let batch = core.engine.on_entrust_cancel(&row.code, side, row.price, row.qty);
                            core.dispatch(batch);
                        }
                        continue;
                    }
                    if tick_code != row.code || tick_buf.len() >= 80 {
                        flush_ticks(&core, &mut tick_buf, &tick_code, &market);
                        tick_code = row.code.clone();
                    }
                    tick_buf.push(Tick {
                        time: fmt_clock(row.ts),
                        price: row.price,
                        volume: row.qty,
                        side: row.side,
                        number: 1,
                    });
                    touched = true;
                }
            }
            if !tick_buf.is_empty() {
                flush_ticks(&core, &mut tick_buf, &tick_code, &market);
            }
            if touched {
                let p = (clock - start_us) as f64 / (end_us - start_us) as f64;
                self.set(|s| {
                    s.clock = fmt_clock(clock);
                    s.progress = p.clamp(0.0, 1.0);
                });
            }
            tokio::time::sleep(Duration::from_millis(16)).await;
        }
    }
}

/// QuestDB 代码过滤片段；为空表示全部标的。
fn code_filter(codes: &[String]) -> String {
    if codes.is_empty() {
        return String::new();
    }
    let list = codes
        .iter()
        .map(|c| format!("'{c}'"))
        .collect::<Vec<_>>()
        .join(",");
    format!(" AND code IN ({list})")
}

fn flush_ticks(core: &Arc<Core>, buf: &mut Vec<Tick>, code: &str, market: &Arc<Market>) {
    if buf.is_empty() || code.is_empty() {
        buf.clear();
        return;
    }
    let batch = core.engine.on_ticks(code, buf);
    core.dispatch(batch);
    core.ws.push_ticks(code, buf);
    market.push_display_ticks(code, buf);
    buf.clear();
}

fn snap_to_quote(r: &SnapRow) -> QuoteSnapshot {
    let mut bids: Vec<Level> = r
        .bids
        .iter()
        .filter(|(p, _)| *p > 0)
        .map(|(p, v)| Level {
            price: *p,
            volume: *v,
        })
        .take(5)
        .collect();
    let asks: Vec<Level> = r
        .asks
        .iter()
        .filter(|(p, _)| *p > 0)
        .map(|(p, v)| Level {
            price: *p,
            volume: *v,
        })
        .take(5)
        .collect();
    while bids.len() < 5 {
        bids.push(Level { price: 0, volume: 0 });
    }
    let (up, down) = crate::model::limit_prices(r.prev_close, false);
    QuoteSnapshot {
        code: r.code.clone(),
        name: r.code.clone(),
        last: r.last,
        prev_close: r.prev_close,
        open: r.open,
        high: r.high,
        low: r.low,
        total_hand: r.cum_vol / 100,
        amount: r.cum_amt / 1000,
        bids,
        asks,
        limit_up: up,
        limit_down: down,
        is_st: false,
        ts: now_ms(),
    }
}

pub fn parse_clock_us(date: &str, clock: &str) -> Result<i64> {
    let parts: Vec<&str> = clock.split(':').collect();
    if parts.len() < 2 {
        return Err(anyhow!("时间格式应为 HH:MM 或 HH:MM:SS"));
    }
    let h: i64 = parts[0].parse().map_err(|_| anyhow!("小时非法"))?;
    let m: i64 = parts[1].parse().map_err(|_| anyhow!("分钟非法"))?;
    let s: i64 = parts
        .get(2)
        .map(|x| x.parse().unwrap_or(0))
        .unwrap_or(0);
    let hhmmssmmm = ((h * 100 + m) * 100 + s) * 1000;
    Ok(to_us(date, hhmmssmmm))
}

fn parse_snaps(csv: &str) -> Result<Vec<SnapRow>> {
    let mut out = Vec::new();
    let mut lines = csv.lines();
    let head = lines.next().unwrap_or("").replace('"', "");
    if !head.starts_with("ts") {
        return Err(anyhow!("快照导出格式异常: {}", head.chars().take(80).collect::<String>()));
    }
    for line in lines {
        let line = line.replace('"', "");
        let f: Vec<&str> = line.split(',').collect();
        if f.len() < 49 {
            continue;
        }
        let n = |i: usize| f.get(i).and_then(|s| s.parse::<i64>().ok()).unwrap_or(0);
        let ts = parse_ts(f[0])?;
        let mut bids = [(0i64, 0i64); 10];
        let mut asks = [(0i64, 0i64); 10];
        for i in 0..10 {
            bids[i] = (n(9 + i), n(19 + i));
            asks[i] = (n(29 + i), n(39 + i));
        }
        out.push(SnapRow {
            ts,
            code: f[1].to_string(),
            last: n(2),
            cum_vol: n(3),
            cum_amt: n(4),
            open: n(5),
            high: n(6),
            low: n(7),
            prev_close: n(8),
            bids,
            asks,
        });
    }
    out.sort_by_key(|r| r.ts);
    Ok(out)
}

fn parse_trades(csv: &str) -> Result<Vec<TradeRow>> {
    let mut out = Vec::new();
    let mut lines = csv.lines();
    lines.next();
    for line in lines {
        let line = line.replace('"', "");
        let f: Vec<&str> = line.split(',').collect();
        if f.len() < 5 {
            continue;
        }
        let price = f[2].parse::<i64>().unwrap_or(0);
        let qty = f[3].parse::<i64>().unwrap_or(0);
        if qty <= 0 {
            continue;
        }
        let flag = f.get(4).map(|s| s.trim()).unwrap_or("");
        out.push(TradeRow {
            ts: parse_ts(f[0])?,
            code: f[1].to_string(),
            price,
            qty,
            side: match f[4].trim() {
                "B" => TickSide::Buy,
                "S" => TickSide::Sell,
                _ => TickSide::Neutral,
            },
            cancel: flag == "C",
            cancel_side: None,
            ask_seq: f.get(6).and_then(|s| s.parse().ok()).unwrap_or(0),
            bid_seq: f.get(7).and_then(|s| s.parse().ok()).unwrap_or(0),
        });
    }
    out.sort_by_key(|r| r.ts);
    Ok(out)
}

fn parse_entrusts(csv: &str) -> Result<Vec<EntrustRow>> {
    let mut out = Vec::new();
    let mut lines = csv.lines();
    lines.next();
    for line in lines {
        let line = line.replace('"', "");
        let f: Vec<&str> = line.split(',').collect();
        if f.len() < 6 {
            continue;
        }
        let price = f[4].parse::<i64>().unwrap_or(0);
        let qty = f[5].parse::<i64>().unwrap_or(0);
        if qty <= 0 {
            continue;
        }
        let side = match f[3].trim() {
            "B" => crate::model::Side::Buy,
            "S" => crate::model::Side::Sell,
            _ => continue,
        };
        out.push(EntrustRow {
            ts: parse_ts(f[0])?,
            seq: f[2].parse::<i64>().unwrap_or(0),
            code: f[1].to_string(),
            side,
            price,
            qty,
        });
    }
    out.sort_by_key(|r| r.ts);
    Ok(out)
}

/// QuestDB 导出时间戳形如 2026-09-30T01:30:00.000000Z（UTC，已对齐交易所挂钟）。
fn parse_ts(s: &str) -> Result<i64> {
    let s = s.trim();
    let (date_part, time_part) = s
        .split_once('T')
        .ok_or_else(|| anyhow!("时间戳格式异常: {s}"))?;
    let d: Vec<&str> = date_part.split('-').collect();
    let t = time_part.trim_end_matches('Z');
    let hm: Vec<&str> = t.split(':').collect();
    if d.len() != 3 || hm.len() < 3 {
        return Err(anyhow!("时间戳格式异常: {s}"));
    }
    let hh: i64 = hm[0].parse().unwrap_or(0);
    let mm: i64 = hm[1].parse().unwrap_or(0);
    let sec: f64 = hm[2].parse().unwrap_or(0.0);
    let date = format!("{}{}{}", d[0], d[1], d[2]);
    let base = crate::tsdb::day_start_us(&date);
    Ok(base + (hh * 3600 + mm * 60) as i64 * 1_000_000 + (sec * 1_000_000.0).round() as i64)
}

#[derive(Debug, Deserialize)]
pub struct StartReq {
    pub date: String,
    #[serde(default)]
    pub clock: Option<String>,
    #[serde(default)]
    pub speed: Option<f64>,
    /// 需要交易的标的；缺失数据会自动从 D5 拉取并缓存
    #[serde(default)]
    pub codes: Option<Vec<String>>,
}
