use crate::error::DomainError;
use crate::model::{Fill, Level, Order, Price, Qty, QuoteSnapshot, Side, Tick};
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub mod book;
pub mod fees;
pub mod l2;
pub mod reject;
pub mod validate;

pub use book::Book;

pub use reject::Reject;

/// 一次撮合产生的成交与状态变更。
#[derive(Debug, Default)]
pub struct MatchBatch {
    pub fills: Vec<Fill>,
    pub orders: Vec<Order>,
}

/// 撮合引擎：每只股票一个委托簿，按代码加锁，互不阻塞。
pub struct Engine {
    books: DashMap<String, Arc<Mutex<Book>>>,
    seq: AtomicU64,
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}

impl Engine {
    pub fn new() -> Self {
        Engine {
            books: DashMap::new(),
            seq: AtomicU64::new(1),
        }
    }

    pub fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    pub fn book(&self, code: &str, name: &str) -> Arc<Mutex<Book>> {
        if let Some(b) = self.books.get(code) {
            return b.clone();
        }
        self.books
            .entry(code.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(Book::new(code, name))))
            .clone()
    }

    pub fn book_ref(&self, code: &str) -> Option<Arc<Mutex<Book>>> {
        self.books.get(code).map(|b| b.clone())
    }

    pub fn pending_count(&self) -> usize {
        self.books
            .iter()
            .map(|b| b.value().lock().map(|x| x.active_count()).unwrap_or(0))
            .sum()
    }

    /// 行情快照更新：覆盖盘口并对转向可成交的委托撮合。
    pub fn on_snapshot(&self, q: &QuoteSnapshot) -> MatchBatch {
        let mut batch = MatchBatch::default();
        let book = self.book(&q.code, &q.name);
        let mut b = match book.lock() {
            Ok(b) => b,
            Err(poisoned) => poisoned.into_inner(),
        };
        b.on_snapshot(q, crate::model::now_ms(), &mut batch);
        batch
    }

    /// 逐笔成交更新：扣减推演盘口后消耗排队量。
    pub fn on_ticks(&self, code: &str, ticks: &[Tick]) -> MatchBatch {
        let Some(book) = self.book_ref(code) else {
            return MatchBatch::default();
        };
        let mut batch = MatchBatch::default();
        let ts = crate::model::now_ms();
        let mut b = match book.lock() {
            Ok(b) => b,
            Err(poisoned) => poisoned.into_inner(),
        };
        for t in ticks {
            b.on_tick(t, ts, &mut batch);
        }
        batch
    }

    /// 逐笔委托：新报单挂入档位。
    pub fn on_entrust_add(&self, code: &str, side: Side, price: Price, qty: Qty) {
        let Some(book) = self.book_ref(code) else { return };
        let mut b = match book.lock() {
            Ok(b) => b,
            Err(p) => p.into_inner(),
        };
        b.on_entrust_add(side, price, qty);
    }

    /// 逐笔撤单：扣减档位并压缩排队量。
    pub fn on_entrust_cancel(&self, code: &str, side: Side, price: Price, qty: Qty) -> MatchBatch {
        let Some(book) = self.book_ref(code) else {
            return MatchBatch::default();
        };
        let mut batch = MatchBatch::default();
        let mut b = match book.lock() {
            Ok(b) => b,
            Err(p) => p.into_inner(),
        };
        b.on_entrust_cancel(side, price, qty, crate::model::now_ms(), &mut batch);
        batch
    }

    /// 委托入簿并尝试撮合。
    pub fn place(&self, order: Order) -> (Order, MatchBatch) {
        let mut batch = MatchBatch::default();
        let book = self.book(&order.code, "");
        let mut b = match book.lock() {
            Ok(b) => b,
            Err(poisoned) => poisoned.into_inner(),
        };
        let placed = b.place(order, crate::model::now_ms(), &mut batch);
        (placed, batch)
    }

    /// 撤单。委托已完成或不存在时返回错误。
    pub fn cancel(&self, code: &str, id: &str) -> Result<Order, DomainError> {
        let Some(book) = self.book_ref(code) else {
            return Err(DomainError::OrderNotFound);
        };
        let mut b = match book.lock() {
            Ok(b) => b,
            Err(poisoned) => poisoned.into_inner(),
        };
        b.cancel(id, crate::model::now_ms())
            .ok_or(DomainError::OrderNotCancellable)
    }

    pub fn l2_levels(&self, code: &str) -> Option<(Vec<Level>, Vec<Level>)> {
        let book = self.book_ref(code)?;
        let b = match book.lock() {
            Ok(b) => b,
            Err(poisoned) => poisoned.into_inner(),
        };
        Some((b.l2.bids.clone(), b.l2.asks.clone()))
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Level, OrderStatus, Price, Qty, Side, TickSide};

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
                    volume: 10_000,
                })
                .collect(),
            asks: (0..5)
                .map(|i| Level {
                    price: 11_320 + i * 10,
                    volume: 8_000,
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

    fn order(id: &str, side: Side, price: Price, qty: Qty) -> Order {
        Order {
            id: id.into(),
            user_id: 1,
            code: "000001".into(),
            name: "平安银行".into(),
            side,
            price,
            qty,
            filled: 0,
            status: OrderStatus::Pending,
            reason: None,
            seq: 1,
            queue_ahead: 0,
            frozen: 0,
            amount_paid: 0,
            created_at: 0,
            updated_at: 0,
        }
    }

    fn book() -> Book {
        let mut b = Book::new("000001", "平安银行");
        let mut batch = MatchBatch::default();
        b.on_snapshot(&quote(), 0, &mut batch);
        b
    }

    #[test]
    fn 市价可成交的买单按卖一价成交() {
        let mut b = book();
        let mut batch = MatchBatch::default();
        let o = order("o1", Side::Buy, 11_320, 1_000);
        let placed = b.place(o, 1, &mut batch);
        assert_eq!(placed.status, OrderStatus::Filled);
        assert_eq!(placed.filled, 1_000);
        assert_eq!(batch.fills.len(), 1);
        assert_eq!(batch.fills[0].price, 11_320, "成交价取卖一价而非委托价");
        assert_eq!(batch.fills[0].qty, 1_000);
        assert_eq!(b.l2.volume_at(Side::Sell, 11_320), 7_000);
    }

    #[test]
    fn 部分成交且剩余留在队列() {
        let mut b = book();
        let mut batch = MatchBatch::default();
        // 卖一仅 8000 股，买 10000 股：卖一被吃光后按卖二价继续成交
        let o = order("o1", Side::Buy, 11_330, 12_000);
        let placed = b.place(o, 1, &mut batch);
        assert_eq!(placed.status, OrderStatus::Filled);
        assert_eq!(batch.fills.len(), 2);
        assert_eq!(batch.fills[0].price, 11_320);
        assert_eq!(batch.fills[0].qty, 8_000);
        assert_eq!(batch.fills[1].price, 11_330);
        assert_eq!(batch.fills[1].qty, 4_000);

        // 限价只到卖一：剩余留在队列
        let mut b = book();
        let mut batch = MatchBatch::default();
        let o = order("o2", Side::Buy, 11_320, 10_000);
        let placed = b.place(o, 1, &mut batch);
        assert_eq!(placed.status, OrderStatus::Partial);
        assert_eq!(placed.filled, 8_000);
        assert_eq!(placed.remaining(), 2_000);
        assert_eq!(b.active_count(), 1);
    }

    #[test]
    fn 排队量消耗完才能成交() {
        let mut b = book();
        let mut batch = MatchBatch::default();
        // 买一挂 10000 股，委托排在后面
        let o = order("o1", Side::Buy, 11_310, 1_000);
        let placed = b.place(o, 1, &mut batch);
        assert_eq!(placed.status, OrderStatus::Pending);
        assert_eq!(placed.queue_ahead, 10_000);
        assert!(batch.fills.is_empty());

        // 逐笔成交 4000 股：只消耗排队量
        b.on_tick(&tick(11_310, 4_000, TickSide::Sell), 2, &mut batch);
        let cur = b.get("o1").cloned().unwrap();
        assert_eq!(cur.queue_ahead, 6_000);
        assert_eq!(cur.filled, 0);

        // 再成交 6000 股：排队量归零
        b.on_tick(&tick(11_310, 6_000, TickSide::Sell), 3, &mut batch);
        let cur = b.get("o1").cloned().unwrap();
        assert_eq!(cur.queue_ahead, 0);
        assert_eq!(cur.filled, 0);

        // 排队量归零后同一价位的成交开始回补委托
        b.on_tick(&tick(11_310, 600, TickSide::Sell), 4, &mut batch);
        let cur = b.get("o1").cloned().unwrap();
        assert_eq!(cur.filled, 600);
        assert_eq!(cur.status, OrderStatus::Partial);

        b.on_tick(&tick(11_310, 1_000, TickSide::Sell), 5, &mut batch);
        assert!(b.get("o1").is_none());
        assert_eq!(batch.fills.iter().map(|f| f.qty).sum::<Qty>(), 1_000);
    }

    #[test]
    fn 同价位先到先成交() {
        let mut b = book();
        let mut batch = MatchBatch::default();
        let mut a = order("a", Side::Buy, 11_300, 500);
        a.seq = 1;
        let mut c = order("c", Side::Buy, 11_300, 500);
        c.seq = 2;
        b.place(a, 1, &mut batch);
        b.place(c, 2, &mut batch);
        // 买二档 10000 股在前，先消耗完才轮到 a
        assert_eq!(b.get("a").map(|o| o.queue_ahead), Some(10_000));
        assert_eq!(b.get("c").map(|o| o.queue_ahead), Some(10_000));

        b.on_tick(&tick(11_300, 10_400, TickSide::Sell), 3, &mut batch);
        let a = b.get("a").cloned();
        let c = b.get("c").cloned();
        assert!(a.is_some(), "a 只成交 400 股，未完成");
        assert_eq!(a.unwrap().filled, 400);
        assert_eq!(c.unwrap().filled, 0);
    }

    #[test]
    fn 卖单排队与成交价取买一() {
        let mut b = book();
        let mut batch = MatchBatch::default();
        let o = order("s1", Side::Sell, 11_310, 1_000);
        let placed = b.place(o, 1, &mut batch);
        assert_eq!(placed.status, OrderStatus::Filled);
        assert_eq!(batch.fills[0].price, 11_310);
        // 卖出佣金 5 元 + 印花税千分之一
        assert_eq!(batch.fills[0].commission, 5_000);
        assert_eq!(batch.fills[0].stamp_tax, 11_310 * 1_000 / 1_000);
    }

    #[test]
    fn 撤单释放剩余() {
        let mut b = book();
        let mut batch = MatchBatch::default();
        let o = order("o1", Side::Buy, 11_320, 10_000);
        b.place(o, 1, &mut batch);
        assert_eq!(b.active_count(), 1);
        let cancelled = b.cancel("o1", 2).unwrap();
        assert_eq!(cancelled.status, OrderStatus::Cancelled);
        assert_eq!(cancelled.filled, 8_000);
        assert_eq!(b.active_count(), 0);
        assert!(b.cancel("o1", 3).is_none());
    }

    #[test]
    fn 快照更新后可成交的挂单被触发() {
        let mut b = book();
        let mut batch = MatchBatch::default();
        // 挂在买一 11.31 排队
        let o = order("o1", Side::Buy, 11_310, 1_000);
        b.place(o, 1, &mut batch);
        // 价格下跌，卖一降到 11.31 以下：该委托转为可成交
        let mut q = quote();
        q.asks = (0..5)
            .map(|i| Level {
                price: 11_300 - i * 10,
                volume: 5_000,
            })
            .collect();
        b.on_snapshot(&q, 2, &mut batch);
        assert!(b.get("o1").is_none(), "应按新的卖一价全部成交");
        let f = batch.fills.last().unwrap();
        assert_eq!(f.price, 11_300);
    }
}
