use crate::model::{Level, Price, Qty, QuoteSnapshot, Side, Tick, TickSide};

pub const DEPTH: usize = 5;

/// 模拟 L2 盘口。
///
/// 推算逻辑：以交易所五档快照为基准，用逐笔成交的方向与数量扣减对应档位挂单量，
/// 使快照间隔内盘口呈现流动性消耗的流动感；新快照到达时以真实数据直接覆盖校准。
///
/// 局限：
/// 1. 五档快照只暴露总挂单量，不暴露队列构成，本模型无法区分同一价位上的真实委托；
/// 2. 逐笔数据的时间精度为交易所回报时间，同一时刻的多笔成交只能按顺序扣减；
/// 3. 逐笔成交价若落在两个档位之间（快照已过期），扣减目标是最近档位，存在偏差；
/// 4. 本模型是对真实盘口的合理推测，不是真实 L2 队列，也不代表真实的可成交量。
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct L2Book {
    pub code: String,
    /// index 0 = 买一
    pub bids: Vec<Level>,
    /// index 0 = 卖一
    pub asks: Vec<Level>,
    pub last: Price,
    /// 已应用快照次数
    pub snapshots: u64,
    /// 已应用逐笔笔数
    pub ticks_applied: u64,
}

#[allow(dead_code)]
impl L2Book {
    pub fn from_quote(q: &QuoteSnapshot) -> Self {
        let mut b = L2Book {
            code: q.code.clone(),
            bids: vec![],
            asks: vec![],
            last: q.last,
            snapshots: 0,
            ticks_applied: 0,
        };
        b.apply_snapshot(q);
        b
    }

    pub fn empty(code: &str) -> Self {
        L2Book {
            code: code.to_string(),
            bids: vec![],
            asks: vec![],
            last: 0,
            snapshots: 0,
            ticks_applied: 0,
        }
    }

    /// 快照覆盖：直接替换五档，纠正推算误差。
    pub fn apply_snapshot(&mut self, q: &QuoteSnapshot) {
        self.bids = normalize(&q.bids);
        self.asks = normalize(&q.asks);
        self.last = q.last;
        self.snapshots += 1;
    }

    pub fn best_bid(&self) -> Option<Level> {
        self.bids.first().copied().filter(|l| l.price > 0)
    }
    pub fn best_ask(&self) -> Option<Level> {
        self.asks.first().copied().filter(|l| l.price > 0)
    }

    fn levels_of(&self, side: Side) -> &Vec<Level> {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }

    fn levels_of_mut(&mut self, side: Side) -> &mut Vec<Level> {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    /// 新委托挂入档位：仅当价格落在已展示档位内时增加挂单量。
    pub fn add_volume(&mut self, side: Side, price: Price, qty: Qty) -> bool {
        if let Some(l) = self
            .levels_of_mut(side)
            .iter_mut()
            .find(|l| l.price == price)
        {
            l.volume += qty;
            return true;
        }
        false
    }

    pub fn volume_at(&self, side: Side, price: Price) -> Qty {
        self.levels_of(side)
            .iter()
            .find(|l| l.price == price)
            .map(|l| l.volume)
            .unwrap_or(0)
    }

    /// 扣减指定方向、指定价格的挂单量，返回实际扣减量。价格无对应档位时扣减最近档位。
    pub fn deduct(&mut self, side: Side, price: Price, qty: Qty) -> Qty {
        if qty <= 0 {
            return 0;
        }
        let levels = self.levels_of_mut(side);
        let idx = match levels.iter().position(|l| l.price == price) {
            Some(i) => i,
            None => {
                let mut best: Option<(usize, i64)> = None;
                for (i, l) in levels.iter().enumerate() {
                    if l.price <= 0 {
                        continue;
                    }
                    let d = (l.price - price).abs();
                    if best.map(|(_, bd)| d < bd).unwrap_or(true) {
                        best = Some((i, d));
                    }
                }
                match best {
                    Some((i, _)) => i,
                    None => return 0,
                }
            }
        };
        let take = levels[idx].volume.min(qty);
        levels[idx].volume -= take;
        take
    }

    /// 用一笔逐笔成交扣减盘口。主动卖出吃买盘，主动买入吃卖盘，中性单按价格就近选择。
    pub fn apply_tick(&mut self, t: &Tick) -> Qty {
        let side = match t.side {
            TickSide::Sell => Some(Side::Buy),
            TickSide::Buy => Some(Side::Sell),
            TickSide::Neutral => {
                let bid_d = self
                    .best_bid()
                    .map(|l| (l.price - t.price).abs())
                    .unwrap_or(i64::MAX);
                let ask_d = self
                    .best_ask()
                    .map(|l| (l.price - t.price).abs())
                    .unwrap_or(i64::MAX);
                if bid_d == i64::MAX && ask_d == i64::MAX {
                    None
                } else if bid_d <= ask_d {
                    Some(Side::Buy)
                } else {
                    Some(Side::Sell)
                }
            }
        };
        let Some(side) = side else { return 0 };
        let applied = self.deduct(side, t.price, t.volume);
        if applied > 0 {
            self.ticks_applied += 1;
        }
        applied
    }
}

fn normalize(levels: &[Level]) -> Vec<Level> {
    let mut v: Vec<Level> = levels
        .iter()
        .copied()
        .filter(|l| l.price > 0)
        .take(DEPTH)
        .collect();
    while v.len() < DEPTH {
        v.push(Level {
            price: 0,
            volume: 0,
        });
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quote() -> QuoteSnapshot {
        QuoteSnapshot {
            code: "000001".into(),
            name: "平安银行".into(),
            last: 11_320,
            prev_close: 11_410,
            open: 11_410,
            high: 11_420,
            low: 11_280,
            total_hand: 0,
            amount: 0,
            bids: (0..5)
                .map(|i| Level {
                    price: 11_310 - i * 10,
                    volume: 10_000 * (i as i64 + 1),
                })
                .collect(),
            asks: (0..5)
                .map(|i| Level {
                    price: 11_320 + i * 10,
                    volume: 8_000 * (i as i64 + 1),
                })
                .collect(),
            limit_up: 12_550,
            limit_down: 10_270,
            is_st: false,
            ts: 0,
        }
    }

    fn tick(price: Price, volume: Qty, side: TickSide) -> Tick {
        Tick {
            time: "10:00:00".into(),
            price,
            volume,
            side,
            number: 1,
        }
    }

    #[test]
    fn 主动卖出扣减买盘() {
        let mut b = L2Book::from_quote(&quote());
        let applied = b.apply_tick(&tick(11_310, 3_000, TickSide::Sell));
        assert_eq!(applied, 3_000);
        assert_eq!(b.volume_at(Side::Buy, 11_310), 7_000);
        // 卖盘不受影响
        assert_eq!(b.volume_at(Side::Sell, 11_320), 8_000);
    }

    #[test]
    fn 主动买入扣减卖盘() {
        let mut b = L2Book::from_quote(&quote());
        b.apply_tick(&tick(11_320, 2_500, TickSide::Buy));
        assert_eq!(b.volume_at(Side::Sell, 11_320), 5_500);
    }

    #[test]
    fn 快照覆盖纠正推算误差() {
        let mut b = L2Book::from_quote(&quote());
        for _ in 0..100 {
            b.apply_tick(&tick(11_310, 20_000, TickSide::Sell));
        }
        assert_eq!(b.volume_at(Side::Buy, 11_310), 0);
        b.apply_snapshot(&quote());
        assert_eq!(b.volume_at(Side::Buy, 11_310), 10_000);
    }

    #[test]
    fn 扣减不会为负() {
        let mut b = L2Book::from_quote(&quote());
        b.apply_tick(&tick(11_310, 999_999, TickSide::Sell));
        assert_eq!(b.volume_at(Side::Buy, 11_310), 0);
        let applied = b.apply_tick(&tick(11_310, 100, TickSide::Sell));
        assert_eq!(applied, 0);
    }

    #[test]
    fn 价格无对应档位时扣减最近档() {
        let mut b = L2Book::from_quote(&quote());
        b.apply_tick(&tick(11_305, 1_000, TickSide::Sell));
        assert_eq!(b.volume_at(Side::Buy, 11_310), 9_000);
    }
}
