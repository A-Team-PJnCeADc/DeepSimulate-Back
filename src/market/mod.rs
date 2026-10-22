pub mod client;

use crate::config::Config;
use crate::model::{limit_prices, now_ms, Level, Price, Qty, QuoteSnapshot, StockMeta, Tick};
use crate::service::{Core, QuoteSource};
use anyhow::Result;
use client::{RawSnapshot, TdxClient};
use dashmap::DashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, serde::Serialize)]
pub struct MinutePoint {
    pub time: String,
    pub price: Price,
    pub volume: Qty,
    /// 当日累计均价（VWAP，厘）
    pub avg: Price,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct MarketStatus {
    pub ok: bool,
    pub last_error: Option<String>,
    pub last_ok_ms: i64,
    pub rounds: u64,
    pub subscribed: usize,
}

/// 行情内存态。实时数据只存内存，不入库。
pub struct Market {
    pub cfg: Arc<Config>,
    client: TdxClient,
    pub quotes: DashMap<String, Arc<QuoteSnapshot>>,
    /// 最近逐笔，最新在前
    pub ticks: DashMap<String, Arc<Vec<Tick>>>,
    pub minute: DashMap<String, Arc<Vec<MinutePoint>>>,
    minute_at: DashMap<String, i64>,
    pub meta: DashMap<String, StockMeta>,
    /// 订阅计数，>0 的代码才会被轮询
    subs: DashMap<String, i64>,
    prev_head: DashMap<String, Tick>,
    status: Mutex<MarketStatus>,
}

impl Market {
    pub fn new(cfg: Arc<Config>) -> Result<Arc<Market>> {
        let client = TdxClient::new(&cfg.tdx_base)?;
        Ok(Arc::new(Market {
            cfg,
            client,
            quotes: DashMap::new(),
            ticks: DashMap::new(),
            minute: DashMap::new(),
            minute_at: DashMap::new(),
            meta: DashMap::new(),
            subs: DashMap::new(),
            prev_head: DashMap::new(),
            status: Mutex::new(MarketStatus {
                ok: false,
                last_error: Some("尚未连接行情源".into()),
                last_ok_ms: 0,
                rounds: 0,
                subscribed: 0,
            }),
        }))
    }

    pub fn status(&self) -> MarketStatus {
        let mut s = self
            .status
            .lock()
            .map(|s| s.clone())
            .unwrap_or(MarketStatus {
                ok: false,
                last_error: Some("状态锁异常".into()),
                last_ok_ms: 0,
                rounds: 0,
                subscribed: 0,
            });
        s.subscribed = self.subs.len();
        s
    }

    fn set_ok(&self, rounds: u64) {
        if let Ok(mut s) = self.status.lock() {
            s.ok = true;
            s.last_error = None;
            s.last_ok_ms = now_ms();
            s.rounds = rounds;
        }
    }

    fn set_err(&self, msg: String) {
        if let Ok(mut s) = self.status.lock() {
            s.ok = false;
            s.last_error = Some(msg);
        }
    }

    pub fn subscribe(&self, codes: &[String]) {
        for c in codes {
            *self.subs.entry(c.clone()).or_insert(0) += 1;
        }
    }

    pub fn unsubscribe(&self, codes: &[String]) {
        for c in codes {
            if let Some(mut v) = self.subs.get_mut(c) {
                *v -= 1;
                if *v <= 0 {
                    drop(v);
                    self.subs.remove(c);
                }
            }
        }
    }

    pub fn active_codes(&self) -> Vec<String> {
        self.subs.iter().map(|e| e.key().clone()).collect()
    }

    /// 默认自选股池（导入与预热用）。
    pub fn default_codes(&self) -> Vec<String> {
        ["000001", "600519", "300750", "601318", "600036", "002594", "000858", "601899"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    pub fn name(&self, code: &str) -> String {
        self.meta
            .get(code)
            .map(|m| m.name.clone())
            .unwrap_or_else(|| code.to_string())
    }

    pub fn quote(&self, code: &str) -> Option<Arc<QuoteSnapshot>> {
        self.quotes.get(code).map(|q| q.clone())
    }

    pub fn ticks_of(&self, code: &str) -> Arc<Vec<Tick>> {
        self.ticks
            .get(code)
            .map(|t| t.clone())
            .unwrap_or_else(|| Arc::new(vec![]))
    }

    pub fn search(&self, keyword: &str, limit: usize) -> Vec<StockMeta> {
        let k = keyword.trim().to_uppercase();
        if k.is_empty() {
            return vec![];
        }
        let mut out = Vec::new();
        for m in self.meta.iter() {
            if m.code.contains(&k) || m.name.to_uppercase().contains(&k) {
                out.push(m.value().clone());
                if out.len() >= limit {
                    break;
                }
            }
        }
        out.sort_by(|a, b| a.code.cmp(&b.code));
        out
    }

    pub fn known(&self, code: &str) -> bool {
        self.meta.contains_key(code) || self.quotes.contains_key(code)
    }

    /// 回放模式下写入快照（与实时轮询共用同一份内存态）。
    pub fn publish_quote(&self, q: Arc<QuoteSnapshot>) {
        self.quotes.insert(q.code.clone(), q);
        let rounds = self.status.lock().map(|s| s.rounds).unwrap_or(0);
        self.set_ok(rounds);
    }

    /// 回放模式下的逐笔展示列表（最新在前）。
    pub fn push_display_ticks(&self, code: &str, ticks: &[Tick]) {
        let mut v: Vec<Tick> = ticks.iter().rev().cloned().collect();
        if let Some(old) = self.ticks.get(code) {
            v.extend(old.iter().cloned());
        }
        v.truncate(self.cfg.tick_keep);
        self.ticks.insert(code.to_string(), Arc::new(v));
    }

    pub async fn minute_cached(&self, code: &str) -> Result<Arc<Vec<MinutePoint>>> {
        let fresh = self
            .minute_at
            .get(code)
            .map(|t| now_ms() - *t < 10_000)
            .unwrap_or(false);
        if fresh {
            if let Some(m) = self.minute.get(code) {
                return Ok(m.clone());
            }
        }
        let raw = self.client.minute(code).await?;
        let mut pts = Vec::with_capacity(raw.len());
        let mut cum_amount: i64 = 0;
        let mut cum_volume: i64 = 0;
        for (time, price, hand) in raw {
            let vol = hand * 100;
            cum_amount += price * vol;
            cum_volume += vol;
            let avg = if cum_volume > 0 {
                cum_amount / cum_volume
            } else {
                price
            };
            pts.push(MinutePoint {
                time,
                price,
                volume: vol,
                avg,
            });
        }
        let arc = Arc::new(pts);
        self.minute.insert(code.to_string(), arc.clone());
        self.minute_at.insert(code.to_string(), now_ms());
        Ok(arc)
    }

    pub async fn load_meta(&self) -> Result<usize> {
        let list = self.client.codes().await?;
        let n = list.len();
        for m in list {
            self.meta.insert(m.code.clone(), m);
        }
        Ok(n)
    }

    /// 启动阶段加载代码表。ST 判定依赖名称，缺失会导致涨跌幅限制错误。
    pub async fn load_meta_with_retry(&self, attempts: u32) -> bool {
        for attempt in 1..=attempts {
            match self.load_meta().await {
                Ok(n) => {
                    tracing::info!(count = n, "股票代码表加载完成");
                    return true;
                }
                Err(e) => {
                    tracing::warn!(attempt, error = %e, "股票代码表加载失败，稍后重试");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
        tracing::warn!("代码表未加载，名称与 ST 判定将退化为股票代码");
        false
    }

    /// 拉取指定代码的五档并写入内存（REST 首次访问时补齐）。
    pub async fn refresh(&self, codes: &[String], core: &Arc<Core>) -> Result<()> {
        if codes.is_empty() {
            return Ok(());
        }
        match self.client.quotes(codes).await {
            Ok(list) => {
                for raw in list {
                    self.store_and_match(raw, core);
                }
                let rounds = self.status.lock().map(|s| s.rounds).unwrap_or(0);
                self.set_ok(rounds);
                Ok(())
            }
            Err(e) => {
                self.set_err(e.to_string());
                Err(e)
            }
        }
    }

    /// 逐笔列表为空时按需拉取一次，供未订阅的 REST 访问使用。
    pub async fn ensure_ticks(&self, code: &str) -> Result<()> {
        if self.ticks.contains_key(code) {
            return Ok(());
        }
        let ticks = self.client.ticks(code).await?;
        let keep = self.cfg.tick_keep;
        let display: Vec<Tick> = ticks.iter().rev().take(keep).cloned().collect();
        if let Some(last) = ticks.last() {
            self.prev_head.insert(code.to_string(), last.clone());
        }
        self.ticks.insert(code.to_string(), Arc::new(display));
        Ok(())
    }

    fn build_snapshot(&self, raw: RawSnapshot) -> QuoteSnapshot {
        let name = self.name(&raw.code);
        let is_st = name.contains("ST");
        let (limit_up, limit_down) = limit_prices(raw.prev_close, is_st);
        QuoteSnapshot {
            code: raw.code.clone(),
            name,
            last: raw.last,
            prev_close: raw.prev_close,
            open: raw.open,
            high: raw.high,
            low: raw.low,
            total_hand: raw.total_hand,
            amount: raw.amount,
            bids: raw
                .bids
                .into_iter()
                .take(5)
                .map(|(price, volume)| Level { price, volume })
                .collect(),
            asks: raw
                .asks
                .into_iter()
                .take(5)
                .map(|(price, volume)| Level { price, volume })
                .collect(),
            limit_up,
            limit_down,
            is_st,
            ts: now_ms(),
        }
    }

    /// 与上一轮比较，取出新增逐笔（时间正序）。首次拉取只记水位，不回放历史成交。
    fn diff_new(&self, code: &str, list: &[Tick]) -> Vec<Tick> {
        let head = self.prev_head.get(code).map(|t| t.clone());
        let Some(head) = head else {
            if let Some(last) = list.last() {
                self.prev_head.insert(code.to_string(), last.clone());
            }
            return vec![];
        };
        let new: Vec<Tick> = match list.iter().rposition(|t| *t == head) {
            Some(i) => list[i + 1..].to_vec(),
            // 头部已滚出列表（长期未拉取）：只回放最近 20 笔，避免整表重放
            None => list.iter().rev().take(20).rev().cloned().collect(),
        };
        if let Some(last) = list.last() {
            self.prev_head.insert(code.to_string(), last.clone());
        }
        new
    }

    async fn poll_ticks(&self, code: &str, core: &Arc<Core>) -> Result<usize> {
        let ticks = self.client.ticks(code).await?;
        let new = self.diff_new(code, &ticks);
        let keep = self.cfg.tick_keep;
        let display: Vec<Tick> = ticks.iter().rev().take(keep).cloned().collect();
        self.ticks.insert(code.to_string(), Arc::new(display));
        if new.is_empty() {
            return Ok(0);
        }
        let n = new.len();
        let batch = core.engine.on_ticks(code, &new);
        core.dispatch(batch);
        core.ws.push_ticks(code, &new);
        Ok(n)
    }

    fn store_and_match(&self, raw: RawSnapshot, core: &Arc<Core>) {
        let q = Arc::new(self.build_snapshot(raw));
        let code = q.code.clone();
        let t0 = Instant::now();
        let batch = core.engine.on_snapshot(&q);
        core.match_latency_us
            .insert(code.clone(), t0.elapsed().as_micros() as u64);
        self.quotes.insert(code.clone(), q.clone());
        core.dispatch(batch);
        if let Some((bids, asks)) = core.engine.l2_levels(&code) {
            core.ws.push_quote(&q, &bids, &asks);
        }
    }
}

impl QuoteSource for Market {
    fn l1(&self, code: &str) -> Option<(String, Price)> {
        self.quotes.get(code).map(|q| (q.name.clone(), q.last))
    }
}

/// 行情任务：独立 Tokio 循环，以轮询方式拉取五档与逐笔。
/// 撮合只在行情更新后触发，撮合路径内不访问行情源。
pub async fn run(market: Arc<Market>, core: Arc<Core>) {
    let mut tick_cursor = 0usize;
    let mut last_tick = Instant::now();
    let mut rounds: u64 = 0;

    loop {
        let codes = market.active_codes();
        if codes.is_empty() {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }

        if last_tick.elapsed().as_millis() as u64 >= market.cfg.tick_interval_ms {
            let n = market.cfg.tick_codes_per_cycle.min(codes.len());
            for i in 0..n {
                let code = codes[(tick_cursor + i) % codes.len()].clone();
                if let Err(e) = market.poll_ticks(&code, &core).await {
                    tracing::debug!(code = %code, error = %e, "逐笔拉取失败");
                }
            }
            if !codes.is_empty() {
                tick_cursor = (tick_cursor + n) % codes.len();
            }
            last_tick = Instant::now();
        }

        match market.client.quotes(&codes).await {
            Ok(list) => {
                rounds += 1;
                for raw in list {
                    market.store_and_match(raw, &core);
                }
                market.set_ok(rounds);
            }
            Err(e) => {
                market.set_err(e.to_string());
                tracing::warn!(error = %e, "五档拉取失败");
                tokio::time::sleep(Duration::from_millis(market.cfg.retry_ms)).await;
                continue;
            }
        }

        tokio::time::sleep(Duration::from_millis(market.cfg.quote_interval_ms)).await;
    }
}
