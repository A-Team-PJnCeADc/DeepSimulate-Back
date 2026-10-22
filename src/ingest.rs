use crate::tsdb::{fmt_clock, to_us, QuestDb};
use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// D5 归档接口客户端（本机 data_interface 代理）。
#[derive(Clone)]
pub struct D5Client {
    base: String,
    http: reqwest::Client,
}

pub struct DayFiles {
    pub snapshots: String,
    pub entrusts: String,
    pub trades: String,
}

impl D5Client {
    pub fn new(base: &str) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(180))
            .build()?;
        Ok(D5Client {
            base: base.trim_end_matches('/').to_string(),
            http,
        })
    }

    pub async fn dates(&self) -> Result<Vec<String>> {
        let body: serde_json::Value = self
            .http
            .get(format!("{}/d5/l2plus/dates", self.base))
            .send()
            .await?
            .json()
            .await?;
        let mut out: Vec<String> = body
            .pointer("/data/dates")
            .and_then(|d| d.as_array())
            .map(|a| a.iter().filter_map(|v| v.as_i64().map(|n| n.to_string())).collect())
            .unwrap_or_default();
        out.sort();
        out.reverse();
        Ok(out)
    }

    /// 拉取单股单日三张表，内置 429 退避。
    pub async fn stock_day(&self, date: &str, code: &str) -> Result<DayFiles> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let resp = self
                .http
                .get(format!("{}/d5/l2plus/stock", self.base))
                .query(&[("date", date), ("code", code), ("format", "csv")])
                .send()
                .await
                .context("请求 D5 失败")?;
            let status = resp.status();
            if status.as_u16() == 429 {
                let wait = 1 + attempt.min(4);
                tracing::warn!(code, date, attempt, "D5 限流，{}s 后重试", wait);
                tokio::time::sleep(Duration::from_secs(wait)).await;
                if attempt > 12 {
                    return Err(anyhow!("D5 限流重试超限"));
                }
                continue;
            }
            if !status.is_success() {
                let text = resp.text().await.unwrap_or_default();
                return Err(anyhow!("D5 返回 {status}: {}", text.chars().take(200).collect::<String>()));
            }
            let body: serde_json::Value = resp.json().await.context("解析 D5 响应失败")?;
            if body.get("code").and_then(|c| c.as_i64()) != Some(200) {
                return Err(anyhow!("D5 业务错误: {}", body.get("message").cloned().unwrap_or_default()));
            }
            let files = body.get("files").ok_or_else(|| anyhow!("D5 无 files 字段"))?;
            let get = |k: &str| {
                files
                    .get(k)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string()
            };
            let out = DayFiles {
                snapshots: get("行情.csv"),
                entrusts: get("逐笔委托.csv"),
                trades: get("逐笔成交.csv"),
            };
            if out.snapshots.is_empty() && out.trades.is_empty() {
                return Err(anyhow!("D5 返回空数据（该日该股可能未归档）"));
            }
            return Ok(out);
        }
    }
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct IngestStatus {
    pub running: bool,
    pub date: String,
    pub total: usize,
    pub done: usize,
    pub current: String,
    pub last_error: Option<String>,
    pub finished_at: i64,
}

/// CSV 导入器：D5 → 解析 → QuestDB（幂等，按 (date, code) 覆盖）。
pub struct Ingestor {
    d5: D5Client,
    qdb: QuestDb,
    pub status: Arc<Mutex<IngestStatus>>,
    /// 导入时是否先清理该日该股旧数据
    force: Arc<std::sync::atomic::AtomicBool>,
}

struct Csv {
    idx: HashMap<String, usize>,
    rows: Vec<Vec<String>>,
}

impl Csv {
    fn parse(text: &str) -> Csv {
        let mut idx = HashMap::new();
        let mut rows = Vec::new();
        let mut lines = text.lines();
        if let Some(head) = lines.next() {
            for (i, name) in head.trim_end_matches(',').split(',').enumerate() {
                idx.insert(name.trim().to_string(), i);
            }
        }
        for line in lines {
            let line = line.trim_end_matches(',');
            if line.is_empty() {
                continue;
            }
            rows.push(line.split(',').map(|s| s.to_string()).collect());
        }
        Csv { idx, rows }
    }

    fn col<'a>(&self, row: &'a [String], name: &str) -> &'a str {
        self.idx.get(name).and_then(|i| row.get(*i)).map(|s| s.as_str()).unwrap_or("")
    }

    fn i64(&self, row: &[String], name: &str) -> i64 {
        self.col(row, name).trim().parse().unwrap_or(0)
    }

    fn f64(&self, row: &[String], name: &str) -> f64 {
        self.col(row, name).trim().parse().unwrap_or(0.0)
    }
}

/// 万分之一元 → 厘
fn px(v: i64) -> i64 {
    v / 10
}

impl Ingestor {
    pub fn new(d5: D5Client, qdb: QuestDb, force: bool) -> Self {
        Ingestor {
            d5,
            qdb,
            status: Arc::new(Mutex::new(IngestStatus::default())),
            force: Arc::new(std::sync::atomic::AtomicBool::new(force)),
        }
    }

    pub fn set_force(&self, v: bool) {
        self.force.store(v, std::sync::atomic::Ordering::Relaxed);
    }

    pub async fn d5_dates(&self) -> Result<Vec<String>> {
        self.d5.dates().await
    }

    pub fn status(&self) -> IngestStatus {
        self.status.lock().map(|s| s.clone()).unwrap_or_default()
    }

    /// 导入多只股票单日数据，串行执行（D5 盘中限流 1 QPS）。
    pub async fn import_day(&self, date: &str, codes: &[String]) -> Result<()> {
        {
            let mut st = self.status.lock().unwrap();
            *st = IngestStatus {
                running: true,
                date: date.to_string(),
                total: codes.len(),
                ..Default::default()
            };
        }
        let result = self.import_day_inner(date, codes).await;
        let mut st = self.status.lock().unwrap();
        st.running = false;
        st.finished_at = crate::model::now_ms();
        if let Err(e) = &result {
            st.last_error = Some(e.to_string());
        }
        result
    }

    async fn import_day_inner(&self, date: &str, codes: &[String]) -> Result<()> {
        for code in codes {
            self.status.lock().unwrap().current = code.clone();
            match self.import_one(date, code).await {
                Ok((s, e, t)) => {
                    tracing::info!(date, code, snaps = s, entrusts = e, trades = t, "导入完成");
                    let mut st = self.status.lock().unwrap();
                    st.done += 1;
                }
                Err(e) => {
                    tracing::warn!(date, code, error = %e, "导入失败");
                    let mut st = self.status.lock().unwrap();
                    st.last_error = Some(format!("{code}: {e}"));
                    st.done += 1;
                }
            }
        }
        Ok(())
    }

    /// 单只导入（供回放前自动补数据调用）。
    pub async fn import_one_public(&self, date: &str, code: &str) -> Result<(usize, usize, usize)> {
        self.import_one(date, code).await
    }

    async fn import_one(&self, date: &str, code: &str) -> Result<(usize, usize, usize)> {
        let files = self.d5.stock_day(date, code).await?;
        let snaps = self.write_snapshots(date, code, &files.snapshots).await?;
        let ents = self.write_entrusts(date, code, &files.entrusts).await?;
        let trds = self.write_trades(date, code, &files.trades).await?;
        // 幂等：DEDUP 以 (ts, date, code) 为键，重复导入覆盖同一行
        let key_ts = crate::tsdb::day_start_us(date);
        self.qdb
            .write_ilp(&format!(
                "replay_imports,date={date},code={code} snaps={snaps}i,entrusts={ents}i,trades={trds}i,imported_at={}i {key_ts}
",
                crate::model::now_ms()
            ))
            .await?;
        Ok((snaps, ents, trds))
    }

    async fn write_snapshots(&self, date: &str, code: &str, csv_text: &str) -> Result<usize> {
        let csv = Csv::parse(csv_text);
        let mut buf = String::with_capacity(1 << 20);
        let mut n = 0usize;
        let mut lines = 0usize;
        for row in &csv.rows {
            let t = csv.i64(row, "时间");
            if t <= 0 {
                continue;
            }
            let last = px(csv.i64(row, "成交价"));
            let b1 = px(csv.i64(row, "申买价1"));
            let a1 = px(csv.i64(row, "申卖价1"));
            if last == 0 && b1 == 0 && a1 == 0 {
                continue;
            }
            let ts = to_us(date, t);
            let mut line = format!(
                "snapshots,code={code} last={}i,cum_vol={}i,cum_amt={}i,open_p={}i,high_p={}i,low_p={}i,prev_close={}i",
                last,
                csv.i64(row, "当日累计成交量"),
                (csv.f64(row, "当日成交额") * 1000.0).round() as i64,
                px(csv.i64(row, "开盘价")),
                px(csv.i64(row, "最高价")),
                px(csv.i64(row, "最低价")),
                px(csv.i64(row, "前收盘")),
            );
            for i in 1..=10 {
                line.push_str(&format!(",bid{i}={}i", px(csv.i64(row, &format!("申买价{i}")))));
            }
            for i in 1..=10 {
                line.push_str(&format!(",bidv{i}={}i", csv.i64(row, &format!("申买量{i}"))));
            }
            for i in 1..=10 {
                line.push_str(&format!(",ask{i}={}i", px(csv.i64(row, &format!("申卖价{i}")))));
            }
            for i in 1..=10 {
                line.push_str(&format!(",askv{i}={}i", csv.i64(row, &format!("申卖量{i}"))));
            }
            line.push_str(&format!(" {ts}\n"));
            buf.push_str(&line);
            n += 1;
            lines += 1;
            if lines >= 20_000 {
                self.qdb.write_ilp(&buf).await?;
                buf.clear();
                lines = 0;
            }
        }
        self.qdb.write_ilp(&buf).await?;
        Ok(n)
    }

    async fn write_entrusts(&self, date: &str, code: &str, csv_text: &str) -> Result<usize> {
        let csv = Csv::parse(csv_text);
        let mut buf = String::with_capacity(1 << 20);
        let mut n = 0usize;
        let mut lines = 0usize;
        for row in &csv.rows {
            let t = csv.i64(row, "时间");
            if t <= 0 {
                continue;
            }
            let ts = to_us(date, t);
            let typ = csv.col(row, "委托类型").trim();
            let side = csv.col(row, "委托代码").trim();
            if side.is_empty() {
                continue;
            }
            buf.push_str(&format!(
                "entrusts,code={code} seq={}i,typ=\"{}\",side=\"{}\",price={}i,qty={}i {ts}\n",
                csv.i64(row, "交易所委托号"),
                if typ.is_empty() { "0" } else { typ },
                side,
                px(csv.i64(row, "委托价格")),
                csv.i64(row, "委托数量"),
            ));
            n += 1;
            lines += 1;
            if lines >= 20_000 {
                self.qdb.write_ilp(&buf).await?;
                buf.clear();
                lines = 0;
            }
        }
        self.qdb.write_ilp(&buf).await?;
        Ok(n)
    }

    async fn write_trades(&self, date: &str, code: &str, csv_text: &str) -> Result<usize> {
        let csv = Csv::parse(csv_text);
        let mut buf = String::with_capacity(1 << 20);
        let mut n = 0usize;
        let mut lines = 0usize;
        for row in &csv.rows {
            let t = csv.i64(row, "时间");
            if t <= 0 {
                continue;
            }
            let ts = to_us(date, t);
            let flag = csv.col(row, "成交代码").trim();
            let side = csv.col(row, "BS标志").trim();
            buf.push_str(&format!(
                "trades_l2,code={code} trade_no={}i,flag=\"{}\",side=\"{}\",price={}i,qty={}i,ask_seq={}i,bid_seq={}i {ts}\n",
                csv.i64(row, "成交编号"),
                if flag.is_empty() { "0" } else { flag },
                if side.is_empty() { "N" } else { side },
                px(csv.i64(row, "成交价格")),
                csv.i64(row, "成交数量"),
                csv.i64(row, "叫卖序号"),
                csv.i64(row, "叫买序号"),
            ));
            n += 1;
            lines += 1;
            if lines >= 20_000 {
                self.qdb.write_ilp(&buf).await?;
                buf.clear();
                lines = 0;
            }
        }
        self.qdb.write_ilp(&buf).await?;
        Ok(n)
    }

    /// 已导入的交易日（升序）与股票数。
    pub async fn imported_days(&self) -> Result<Vec<(String, i64)>> {
        let rows = self
            .qdb
            .exec("SELECT date, count() FROM replay_imports ORDER BY date")
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| {
                (
                    r.first().and_then(|v| v.as_str()).unwrap_or("").to_string(),
                    r.get(1).and_then(|v| v.as_i64()).unwrap_or(0),
                )
            })
            .filter(|(d, _)| !d.is_empty())
            .collect())
    }

    pub async fn imported_codes(&self, date: &str) -> Result<Vec<String>> {
        let rows = self
            .qdb
            .exec(&format!(
                "SELECT code FROM replay_imports WHERE date = '{date}' ORDER BY code"
            ))
            .await?;
        Ok(rows
            .into_iter()
            .filter_map(|r| r.first().and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect())
    }

    /// 某日某股在库中的数据条数。
    pub async fn counts(&self, date: &str, code: &str) -> Result<(i64, i64, i64)> {
        let day = format!("{}-{}-{}", &date[0..4], &date[4..6], &date[6..8]);
        let q = |t: &str| {
            format!(
                "SELECT count() FROM {t} WHERE code = '{code}' AND ts IN '{day}'"
            )
        };
        let one = |rows: Vec<Vec<serde_json::Value>>| {
            rows.first()
                .and_then(|r| r.first())
                .and_then(|v| v.as_i64())
                .unwrap_or(0)
        };
        let s = one(self.qdb.exec(&q("snapshots")).await?);
        let e = one(self.qdb.exec(&q("entrusts")).await?);
        let t = one(self.qdb.exec(&q("trades_l2")).await?);
        Ok((s, e, t))
    }

    /// 打印某日数据范围，便于自检。
    pub async fn describe(&self, date: &str, code: &str) -> Result<String> {
        let day = format!("{}-{}-{}", &date[0..4], &date[4..6], &date[6..8]);
        let rows = self
            .qdb
            .exec(&format!(
                "SELECT min(ts), max(ts), count() FROM trades_l2 WHERE code='{code}' AND ts IN '{day}'"
            ))
            .await?;
        if let Some(r) = rows.first() {
            let min_t = r.first().and_then(|v| v.as_str()).unwrap_or("");
            let max_t = r.get(1).and_then(|v| v.as_str()).unwrap_or("");
            let n = r.get(2).and_then(|v| v.as_i64()).unwrap_or(0);
            return Ok(format!("{code} {date}: {n} 笔成交，{min_t} ~ {max_t}"));
        }
        Ok(format!("{code} {date}: 无数据"))
    }

    pub fn clock_hint(date: &str, hhmmss: i64) -> String {
        fmt_clock(to_us(date, hhmmss * 1000))
    }
}
