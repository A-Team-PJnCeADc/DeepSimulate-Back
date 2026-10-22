use serde::{Deserialize, Serialize};

/// 价格单位：厘（1 元 = 1000 厘）。A 股最小价格变动 0.01 元 = 10 厘。
pub type Price = i64;
/// 金额单位：厘。
pub type Money = i64;
/// 数量单位：股。
pub type Qty = i64;

pub const PRICE_TICK: Price = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn as_str(self) -> &'static str {
        match self {
            Side::Buy => "buy",
            Side::Sell => "sell",
        }
    }
    pub fn parse(s: &str) -> Option<Side> {
        match s {
            "buy" => Some(Side::Buy),
            "sell" => Some(Side::Sell),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TickSide {
    /// 主动买入（外盘）
    Buy,
    /// 主动卖出（内盘）
    Sell,
    /// 中性
    Neutral,
}

impl TickSide {
    pub fn from_status(status: i64) -> TickSide {
        match status {
            0 => TickSide::Buy,
            1 => TickSide::Sell,
            _ => TickSide::Neutral,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Level {
    pub price: Price,
    pub volume: Qty,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuoteSnapshot {
    pub code: String,
    pub name: String,
    pub last: Price,
    pub prev_close: Price,
    pub open: Price,
    pub high: Price,
    pub low: Price,
    /// 成交量（手）
    pub total_hand: i64,
    /// 成交额（元）
    pub amount: i64,
    /// 买一到买五，index 0 为买一
    pub bids: Vec<Level>,
    /// 卖一到卖五，index 0 为卖一
    pub asks: Vec<Level>,
    pub limit_up: Price,
    pub limit_down: Price,
    pub is_st: bool,
    /// 服务端物理时钟（毫秒）
    pub ts: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Tick {
    /// 交易所时间字符串原样透传
    pub time: String,
    pub price: Price,
    pub volume: Qty,
    pub side: TickSide,
    /// 该笔成交包含的成交单数
    pub number: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderStatus {
    /// 已报（含排队中）
    Pending,
    /// 部分成交
    Partial,
    /// 全部成交
    Filled,
    /// 已撤
    Cancelled,
    /// 废单
    Rejected,
}

impl OrderStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            OrderStatus::Pending => "pending",
            OrderStatus::Partial => "partial",
            OrderStatus::Filled => "filled",
            OrderStatus::Cancelled => "cancelled",
            OrderStatus::Rejected => "rejected",
        }
    }
    pub fn is_active(self) -> bool {
        matches!(self, OrderStatus::Pending | OrderStatus::Partial)
    }
}

/// 委托。价格与金额均为厘，数量为股。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: String,
    pub user_id: i64,
    pub code: String,
    pub name: String,
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
    pub filled: Qty,
    pub status: OrderStatus,
    pub reason: Option<String>,
    /// 撮合序号，同价位按此 FIFO
    pub seq: u64,
    /// 前方排队量（股），归零后可成交
    pub queue_ahead: Qty,
    /// 买单剩余冻结资金（厘）
    pub frozen: Money,
    /// 已成交金额累计（厘），与 filled 一起决定累计佣金与印花税
    pub amount_paid: Money,
    pub created_at: i64,
    pub updated_at: i64,
}

impl Order {
    pub fn remaining(&self) -> Qty {
        (self.qty - self.filled).max(0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fill {
    pub trade_id: String,
    pub order_id: String,
    pub user_id: i64,
    pub code: String,
    pub name: String,
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
    pub amount: Money,
    pub commission: Money,
    pub stamp_tax: Money,
    pub ts: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    pub id: String,
    pub order_id: String,
    pub user_id: i64,
    pub code: String,
    pub name: String,
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
    pub amount: Money,
    pub commission: Money,
    pub stamp_tax: Money,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub code: String,
    pub name: String,
    /// 总持仓
    pub total: Qty,
    /// 可卖数量（T+1，当日买入不计入）
    pub available: Qty,
    /// 买入成本价（厘，含费用摊薄）
    pub cost: Price,
    pub last: Price,
    pub market_value: Money,
    pub pnl: Money,
    pub pnl_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountView {
    pub user_id: i64,
    pub username: String,
    pub cash_available: Money,
    pub cash_frozen: Money,
    pub market_value: Money,
    pub total_asset: Money,
    pub initial: Money,
    pub total_pnl: Money,
    pub positions: Vec<Position>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StockMeta {
    pub code: String,
    pub name: String,
    pub exchange: String,
}

/// 时钟：毫秒
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 交易日字符串 YYYY-MM-DD（东八区）
pub fn today_cn() -> String {
    use chrono::Utc;
    let ts = Utc::now() + chrono::Duration::hours(8);
    ts.format("%Y-%m-%d").to_string()
}

/// 价格四舍五入到最小变动价位
pub fn round_price(p: Price) -> Price {
    ((p + PRICE_TICK / 2) / PRICE_TICK) * PRICE_TICK
}

/// 涨跌停价：按昨收 × 比例后四舍五入到分
pub fn limit_prices(prev_close: Price, is_st: bool) -> (Price, Price) {
    let pct = if is_st { 5 } else { 10 };
    let up = round_price(prev_close * (100 + pct) / 100);
    let down = round_price(prev_close * (100 - pct) / 100);
    (up, down)
}

/// 民用历 ↔ 天数（Howard Hinnant 算法），用于练手日推进的日期换算
pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

pub fn civil_from_days(z: i64) -> (i64, i64, i64) {
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

/// 毫秒时间戳 → 东八区 YYYYMMDD
pub fn ymd_from_ms(ms: i64) -> String {
    let days = (ms + 8 * 3_600_000).div_euclid(86_400_000);
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}{m:02}{d:02}")
}
