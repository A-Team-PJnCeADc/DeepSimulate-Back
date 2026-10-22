use anyhow::{anyhow, Context, Result};
use serde_json::Value;

/// QuestDB HTTP 接口封装：/exec 执行 SQL，/write 走 InfluxDB Line Protocol，/exp 导出 CSV。
#[derive(Clone)]
pub struct QuestDb {
    base: String,
    http: reqwest::Client,
}

pub const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS snapshots (
        ts TIMESTAMP, code SYMBOL, last LONG, cum_vol LONG, cum_amt LONG,
        open_p LONG, high_p LONG, low_p LONG, prev_close LONG,
        bid1 LONG, bid2 LONG, bid3 LONG, bid4 LONG, bid5 LONG,
        bid6 LONG, bid7 LONG, bid8 LONG, bid9 LONG, bid10 LONG,
        bidv1 LONG, bidv2 LONG, bidv3 LONG, bidv4 LONG, bidv5 LONG,
        bidv6 LONG, bidv7 LONG, bidv8 LONG, bidv9 LONG, bidv10 LONG,
        ask1 LONG, ask2 LONG, ask3 LONG, ask4 LONG, ask5 LONG,
        ask6 LONG, ask7 LONG, ask8 LONG, ask9 LONG, ask10 LONG,
        askv1 LONG, askv2 LONG, askv3 LONG, askv4 LONG, askv5 LONG,
        askv6 LONG, askv7 LONG, askv8 LONG, askv9 LONG, askv10 LONG
    ) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, code)",
    "CREATE TABLE IF NOT EXISTS entrusts (
        ts TIMESTAMP, code SYMBOL, seq LONG, typ SYMBOL, side SYMBOL, price LONG, qty LONG
    ) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, code, seq)",
    "CREATE TABLE IF NOT EXISTS trades_l2 (
        ts TIMESTAMP, code SYMBOL, trade_no LONG, flag SYMBOL, side SYMBOL,
        price LONG, qty LONG, ask_seq LONG, bid_seq LONG
    ) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, code, trade_no)",
    "CREATE TABLE IF NOT EXISTS replay_imports (
        ts TIMESTAMP, date SYMBOL, code SYMBOL, snaps LONG, entrusts LONG, trades LONG, imported_at LONG
    ) TIMESTAMP(ts) PARTITION BY MONTH WAL DEDUP UPSERT KEYS(ts, date, code)",
];

impl QuestDb {
    pub fn new(base: &str) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(120))
            .pool_max_idle_per_host(4)
            .build()
            .context("构建 QuestDB HTTP 客户端失败")?;
        Ok(QuestDb {
            base: base.trim_end_matches('/').to_string(),
            http,
        })
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    pub async fn ensure_schema(&self) -> Result<()> {
        for ddl in SCHEMA {
            self.exec(ddl).await.with_context(|| format!("建表失败: {}", &ddl[..40.min(ddl.len())]))?;
        }
        Ok(())
    }

    /// 执行 SQL，返回 dataset（行 × 列的值）。
    pub async fn exec(&self, sql: &str) -> Result<Vec<Vec<Value>>> {
        let resp = self
            .http
            .get(format!("{}/exec", self.base))
            .query(&[("query", sql)])
            .send()
            .await
            .context("请求 QuestDB /exec 失败")?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(anyhow!("QuestDB 执行失败({status}): {}", trim_err(&text)));
        }
        let body: Value = serde_json::from_str(&text).context("解析 QuestDB 响应失败")?;
        if let Some(err) = body.get("error").and_then(|v| v.as_str()) {
            return Err(anyhow!("QuestDB 执行失败: {err}"));
        }
        let rows = body
            .get("dataset")
            .and_then(|d| d.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|r| r.as_array().cloned().unwrap_or_default())
                    .collect()
            })
            .unwrap_or_default();
        Ok(rows)
    }

    /// 执行无需返回结果的 SQL。
    pub async fn exec_batch(&self, sql: &str) -> Result<()> {
        self.exec(sql).await.map(|_| ())
    }

    /// 逗号分隔的 CSV 导出，用于大结果集（比 JSON 快一个量级）。
    pub async fn export_csv(&self, sql: &str) -> Result<String> {
        let resp = self
            .http
            .get(format!("{}/exp", self.base))
            .query(&[("query", sql)])
            .send()
            .await
            .context("请求 QuestDB /exp 失败")?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(anyhow!("QuestDB 导出失败({status}): {}", trim_err(&text)));
        }
        Ok(text)
    }

    /// 写入 InfluxDB Line Protocol（微秒时间戳）。
    pub async fn write_ilp(&self, body: &str) -> Result<()> {
        if body.is_empty() {
            return Ok(());
        }
        let resp = self
            .http
            .post(format!("{}/write", self.base))
            .query(&[("precision", "us")])
            .header(reqwest::header::CONTENT_TYPE, "text/plain; charset=utf-8")
            .body(body.to_string())
            .send()
            .await
            .context("写入 QuestDB 失败")?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!("QuestDB 写入失败({status}): {}", trim_err(&text)));
        }
        Ok(())
    }

    pub async fn healthy(&self) -> bool {
        self.exec("SELECT 1").await.is_ok()
    }

}

fn trim_err(s: &str) -> String {
    let t = s.replace('\n', " ");
    t.chars().take(300).collect()
}

/// 交易日 "YYYYMMDD" 的 00:00:00 的 Unix 微秒（把交易所挂钟时间当作 UTC 处理）。
pub fn day_start_us(date: &str) -> i64 {
    let y: i64 = date[0..4].parse().unwrap_or(1970);
    let m: i64 = date[4..6].parse().unwrap_or(1);
    let d: i64 = date[6..8].parse().unwrap_or(1);
    days_from_civil(y, m, d) * 86_400_000_000
}

/// HHMMSSmmm（D5 时间字段）转成当日微秒偏移。
pub fn hhmmssmmm_to_offset_us(t: i64) -> i64 {
    let ms = t % 1000;
    let rest = t / 1000;
    let ss = rest % 100;
    let rest = rest / 100;
    let mm = rest % 100;
    let hh = rest / 100;
    ((hh * 3600 + mm * 60 + ss) * 1_000_000) + ms * 1000
}

pub fn to_us(date: &str, hhmmssmmm: i64) -> i64 {
    day_start_us(date) + hhmmssmmm_to_offset_us(hhmmssmmm)
}

/// 微秒时间戳转 HH:MM:SS。
pub fn fmt_clock(us: i64) -> String {
    let ofs = us.rem_euclid(86_400_000_000);
    let secs = ofs / 1_000_000;
    format!("{:02}:{:02}:{:02}", secs / 3600, (secs % 3600) / 60, secs % 60)
}

/// 微秒时间戳转 HH:MM:SS.mmm
pub fn fmt_clock_ms(us: i64) -> String {
    let ofs = us.rem_euclid(86_400_000_000);
    let secs = ofs / 1_000_000;
    let ms = (ofs % 1_000_000) / 1000;
    format!(
        "{:02}:{:02}:{:02}.{:03}",
        secs / 3600,
        (secs % 3600) / 60,
        secs % 60,
        ms
    )
}

pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn 交易日零点与时钟换算自洽() {
        let base = day_start_us("20260930");
        assert_eq!(fmt_clock(base), "00:00:00");
        let t = to_us("20260930", 93000123);
        assert_eq!(fmt_clock_ms(t), "09:30:00.123");
        assert_eq!(t - base, hhmmssmmm_to_offset_us(93000123));
    }

    #[test]
    fn 时间偏移解析() {
        assert_eq!(hhmmssmmm_to_offset_us(80000000), 8 * 3600 * 1_000_000);
        assert_eq!(hhmmssmmm_to_offset_us(145959500), ((14 * 3600 + 59 * 60 + 59) as i64) * 1_000_000 + 500_000);
    }
}
