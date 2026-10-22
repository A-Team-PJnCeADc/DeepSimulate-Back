use crate::model::{Price, Qty, StockMeta, Tick, TickSide};
use anyhow::{anyhow, Context, Result};
use serde::Deserialize;

const HAND: i64 = 100;

#[derive(Debug, Deserialize)]
struct ApiResp<T> {
    code: i64,
    #[serde(default)]
    message: String,
    data: Option<T>,
}

#[derive(Debug, Deserialize)]
struct RawK {
    #[serde(rename = "Last")]
    last: Price,
    #[serde(rename = "Open")]
    open: Price,
    #[serde(rename = "High")]
    high: Price,
    #[serde(rename = "Low")]
    low: Price,
    #[serde(rename = "Close")]
    close: Price,
}

#[derive(Debug, Deserialize)]
struct RawLevel {
    #[serde(rename = "Price")]
    price: Price,
    #[serde(rename = "Number")]
    number: Qty,
}

#[derive(Debug, Deserialize)]
struct RawQuote {
    #[serde(rename = "Code")]
    code: String,
    #[serde(rename = "K")]
    k: RawK,
    #[serde(rename = "TotalHand", default)]
    total_hand: i64,
    #[serde(rename = "Amount", default)]
    amount: i64,
    #[serde(rename = "BuyLevel", default)]
    buy_levels: Vec<RawLevel>,
    #[serde(rename = "SellLevel", default)]
    sell_levels: Vec<RawLevel>,
}

/// 行情源原始五档。价格单位厘，档位挂单量已由手换算为股。
#[derive(Debug, Clone)]
pub struct RawSnapshot {
    pub code: String,
    /// 昨收
    pub prev_close: Price,
    pub open: Price,
    pub high: Price,
    pub low: Price,
    /// 最新价
    pub last: Price,
    /// 成交量（手）
    pub total_hand: i64,
    /// 成交额（元）
    pub amount: i64,
    pub bids: Vec<(Price, Qty)>,
    pub asks: Vec<(Price, Qty)>,
}

#[derive(Debug, Deserialize)]
struct RawTickList {
    #[serde(rename = "List", default)]
    list: Vec<RawTick>,
}

#[derive(Debug, Deserialize)]
struct RawTick {
    #[serde(rename = "Time", default)]
    time: String,
    #[serde(rename = "Price")]
    price: Price,
    #[serde(rename = "Volume", default)]
    volume: i64,
    #[serde(default)]
    status: i64,
    #[serde(rename = "Number", default)]
    number: i64,
}

#[derive(Debug, Deserialize)]
struct RawMinuteList {
    #[serde(rename = "List", default)]
    list: Vec<RawMinute>,
}

#[derive(Debug, Deserialize)]
struct RawMinute {
    #[serde(rename = "Time", default)]
    time: String,
    #[serde(rename = "Price")]
    price: Price,
    #[serde(rename = "Number", default)]
    number: i64,
}

#[derive(Debug, Deserialize)]
struct RawCodes {
    #[serde(default)]
    codes: Vec<RawCode>,
}

#[derive(Debug, Deserialize)]
struct RawCode {
    code: String,
    name: String,
    exchange: String,
}

/// 通达信行情源 HTTP 客户端。对应 tdx-api 的 /api/quote、/api/trade、/api/minute、/api/codes。
#[derive(Clone)]
pub struct TdxClient {
    http: reqwest::Client,
    base: String,
}

impl TdxClient {
    pub fn new(base: &str) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(12))
            .pool_max_idle_per_host(8)
            .build()
            .context("构建 HTTP 客户端失败")?;
        Ok(TdxClient {
            http,
            base: base.trim_end_matches('/').to_string(),
        })
    }

    async fn get<T: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<T> {
        let url = format!("{}{}", self.base, path);
        let resp = self.http.get(&url).query(query).send().await?;
        let body: ApiResp<T> = resp
            .json()
            .await
            .with_context(|| format!("解析 {path} 响应失败"))?;
        if body.code != 0 {
            return Err(anyhow!("行情源返回错误 {}: {}", body.code, body.message));
        }
        body.data.ok_or_else(|| anyhow!("行情源 {path} 无 data"))
    }

    /// 批量五档。codes 以逗号拼接。
    pub async fn quotes(&self, codes: &[String]) -> Result<Vec<RawSnapshot>> {
        if codes.is_empty() {
            return Ok(vec![]);
        }
        let joined = codes.join(",");
        let rows: Vec<RawQuote> = self.get("/api/quote", &[("code", &joined)]).await?;
        Ok(rows
            .into_iter()
            .map(|r| RawSnapshot {
                code: r.code,
                prev_close: if r.k.last > 0 { r.k.last } else { r.k.close },
                open: r.k.open,
                high: r.k.high,
                low: r.k.low,
                last: r.k.close,
                total_hand: r.total_hand,
                amount: r.amount,
                bids: r
                    .buy_levels
                    .into_iter()
                    .map(|l| (l.price, l.number * HAND))
                    .collect(),
                asks: r
                    .sell_levels
                    .into_iter()
                    .map(|l| (l.price, l.number * HAND))
                    .collect(),
            })
            .collect())
    }

    /// 当日逐笔成交，返回时间正序（旧的在前）。
    pub async fn ticks(&self, code: &str) -> Result<Vec<Tick>> {
        let data: RawTickList = self.get("/api/trade", &[("code", code)]).await?;
        let mut ticks: Vec<Tick> = data
            .list
            .into_iter()
            .filter(|t| t.price > 0 && t.volume > 0)
            .map(|t| Tick {
                time: t.time,
                price: t.price,
                volume: t.volume * HAND,
                side: TickSide::from_status(t.status),
                number: t.number,
            })
            .collect();
        // 接口返回最新在前，翻转为时间正序便于增量比较
        ticks.reverse();
        Ok(ticks)
    }

    /// 当日分时，返回时间正序。
    pub async fn minute(&self, code: &str) -> Result<Vec<(String, Price, i64)>> {
        let data: RawMinuteList = self.get("/api/minute", &[("code", code)]).await?;
        Ok(data
            .list
            .into_iter()
            .map(|m| (m.time, m.price, m.number))
            .collect())
    }

    /// 全市场代码与名称表。
    pub async fn codes(&self) -> Result<Vec<StockMeta>> {
        let data: RawCodes = self.get("/api/codes", &[]).await?;
        Ok(data
            .codes
            .into_iter()
            .map(|c| StockMeta {
                code: c.code,
                name: c.name,
                exchange: c.exchange,
            })
            .collect())
    }
}
