use crate::model::{Fill, Order, OrderStatus, Price, Qty, QuoteSnapshot, Side, Tick, TickSide};
use std::collections::{BTreeMap, HashMap, VecDeque};

use super::fees;
use super::l2::{L2Book, DEPTH};
use super::MatchBatch;

/// 单只股票的委托簿。买单按价格降序、卖单按价格升序，同价位按 seq 先进先出。
#[derive(Debug)]
pub struct Book {
    pub code: String,
    pub name: String,
    pub l2: L2Book,
    buys: BTreeMap<Price, VecDeque<String>>,
    sells: BTreeMap<Price, VecDeque<String>>,
    orders: HashMap<String, Order>,
}

impl Book {
    pub fn new(code: &str, name: &str) -> Self {
        Book {
            code: code.to_string(),
            name: name.to_string(),
            l2: L2Book::empty(code),
            buys: BTreeMap::new(),
            sells: BTreeMap::new(),
            orders: HashMap::new(),
        }
    }

    pub fn active_count(&self) -> usize {
        self.orders.len()
    }

    #[allow(dead_code)]
    pub fn get(&self, id: &str) -> Option<&Order> {
        self.orders.get(id)
    }

    #[allow(dead_code)]
    pub fn active_orders(&self) -> impl Iterator<Item = &Order> {
        self.orders.values()
    }

    fn queue_of(&mut self, side: Side) -> &mut BTreeMap<Price, VecDeque<String>> {
        match side {
            Side::Buy => &mut self.buys,
            Side::Sell => &mut self.sells,
        }
    }

    fn insert_resting(&mut self, order: &Order) {
        self.queue_of(order.side)
            .entry(order.price)
            .or_default()
            .push_back(order.id.clone());
    }

    fn remove_resting(&mut self, side: Side, price: Price, id: &str) {
        let q = self.queue_of(side);
        if let Some(dq) = q.get_mut(&price) {
            if let Some(pos) = dq.iter().position(|x| x == id) {
                dq.remove(pos);
            }
            if dq.is_empty() {
                q.remove(&price);
            }
        }
    }

    /// 新快照到达：覆盖五档后，对转向可成交的挂单执行吃单撮合。
    pub fn on_snapshot(&mut self, q: &QuoteSnapshot, ts: i64, batch: &mut MatchBatch) {
        self.name = q.name.clone();
        self.l2.apply_snapshot(q);
        self.taker_pass(Side::Buy, ts, batch);
        self.taker_pass(Side::Sell, ts, batch);
    }

    /// 逐笔成交到达：先扣减推演盘口，再消耗该价位排队量并撮合。
    pub fn on_tick(&mut self, t: &Tick, ts: i64, batch: &mut MatchBatch) {
        self.l2.apply_tick(t);
        let sides: &[Side] = match t.side {
            TickSide::Sell => &[Side::Buy],
            TickSide::Buy => &[Side::Sell],
            TickSide::Neutral => &[Side::Buy, Side::Sell],
        };
        for &side in sides {
            self.consume_queue(side, t.price, t.volume, ts, batch);
        }
    }

    /// 排队消耗：同价位按 seq 依次扣减前方排队量，前方归零后剩余成交量成交。
    fn consume_queue(
        &mut self,
        side: Side,
        price: Price,
        volume: Qty,
        ts: i64,
        batch: &mut MatchBatch,
    ) {
        let ids: Vec<String> = match side {
            Side::Buy => self.buys.get(&price).cloned().unwrap_or_default(),
            Side::Sell => self.sells.get(&price).cloned().unwrap_or_default(),
        }
        .into_iter()
        .collect();
        let mut vol = volume;
        for id in ids {
            if vol <= 0 {
                break;
            }
            let Some(o) = self.orders.get(&id) else {
                continue;
            };
            let qa = o.queue_ahead;
            let rem = o.remaining();
            let advance = qa.min(vol);
            if advance > 0 {
                if let Some(o) = self.orders.get_mut(&id) {
                    o.queue_ahead -= advance;
                }
                vol -= advance;
            }
            if qa - advance == 0 && vol > 0 && rem > 0 {
                let f = vol.min(rem);
                self.apply_fill(&id, price, f, ts, batch);
                vol -= f;
            }
        }
    }

    /// 逐笔委托：新报单挂入档位（排在已有委托之后，不影响用户前方排队量）。
    pub fn on_entrust_add(&mut self, side: Side, price: Price, qty: Qty) {
        if qty > 0 {
            self.l2.add_volume(side, price, qty);
        }
    }

    /// 逐笔撤单：扣减档位挂单量，并按比例压缩该价位的用户前方排队量。
    pub fn on_entrust_cancel(&mut self, side: Side, price: Price, qty: Qty, ts: i64, batch: &mut MatchBatch) {
        let before = self.l2.volume_at(side, price);
        if before <= 0 || qty <= 0 {
            return;
        }
        self.l2.deduct(side, price, qty);
        let after = self.l2.volume_at(side, price);
        let ids: Vec<String> = match side {
            Side::Buy => self.buys.get(&price).cloned().unwrap_or_default().into_iter().collect(),
            Side::Sell => self.sells.get(&price).cloned().unwrap_or_default().into_iter().collect(),
        };
        for id in ids {
            if let Some(o) = self.orders.get_mut(&id) {
                if o.queue_ahead > 0 {
                    let scaled = ((o.queue_ahead as f64) * (after as f64) / (before as f64)).round() as i64;
                    if scaled != o.queue_ahead {
                        o.queue_ahead = scaled.max(0);
                        o.updated_at = ts;
                        batch.orders.push(o.clone());
                    }
                }
            }
        }
    }

    /// 委托入簿：计算前方排队量，可立即成交的部分按对手档位价成交。
    pub fn place(&mut self, mut order: Order, ts: i64, batch: &mut MatchBatch) -> Order {
        order.updated_at = ts;
        order.status = OrderStatus::Pending;
        let id = order.id.clone();
        let side = order.side;
        let price = order.price;
        let entry = order.clone();
        self.orders.insert(id.clone(), order);
        self.insert_resting(&entry);
        self.taker_pass(side, ts, batch);
        // 吃单后仍有剩余：排在当前档位已有挂单量之后
        if let Some(o) = self.orders.get_mut(&id) {
            o.queue_ahead = self.l2.volume_at(side, price);
            o.updated_at = ts;
            let snap = o.clone();
            batch.orders.push(snap.clone());
            return snap;
        }
        batch
            .orders
            .iter()
            .rev()
            .find(|o| o.id == id)
            .cloned()
            .unwrap_or(entry)
    }

    /// 撤销未成交部分。
    pub fn cancel(&mut self, id: &str, ts: i64) -> Option<Order> {
        let mut o = self.orders.remove(id)?;
        self.remove_resting(o.side, o.price, id);
        o.status = OrderStatus::Cancelled;
        o.queue_ahead = 0;
        o.updated_at = ts;
        Some(o)
    }

    /// 吃单撮合：价格为限价约束，成交价取被吃档位价，单次成交量不超过该档位挂单量。
    pub fn taker_pass(&mut self, side: Side, ts: i64, batch: &mut MatchBatch) {
        let candidates = self.marketable_ids(side);
        for id in candidates {
            self.sweep(&id, ts, batch);
        }
    }

    fn marketable_ids(&self, side: Side) -> Vec<String> {
        let mut out = Vec::new();
        match side {
            Side::Buy => {
                let Some(ask) = self.l2.best_ask() else {
                    return out;
                };
                for (price, q) in self.buys.iter().rev() {
                    if *price < ask.price {
                        break;
                    }
                    out.extend(q.iter().cloned());
                }
            }
            Side::Sell => {
                let Some(bid) = self.l2.best_bid() else {
                    return out;
                };
                for (price, q) in self.sells.iter() {
                    if *price > bid.price {
                        break;
                    }
                    out.extend(q.iter().cloned());
                }
            }
        }
        out
    }

    fn sweep(&mut self, id: &str, ts: i64, batch: &mut MatchBatch) {
        let Some(order) = self.orders.get(id) else {
            return;
        };
        let side = order.side;
        let limit = order.price;
        let mut rem = order.remaining();
        if rem <= 0 {
            return;
        }
        for i in 0..DEPTH {
            if rem == 0 {
                break;
            }
            let (lp, lv) = match side {
                Side::Buy => (self.l2.asks[i].price, self.l2.asks[i].volume),
                Side::Sell => (self.l2.bids[i].price, self.l2.bids[i].volume),
            };
            if lp == 0 || lv <= 0 {
                continue;
            }
            let crosses = match side {
                Side::Buy => lp <= limit,
                Side::Sell => lp >= limit,
            };
            if !crosses {
                break;
            }
            let take = rem.min(lv);
            match side {
                Side::Buy => self.l2.asks[i].volume -= take,
                Side::Sell => self.l2.bids[i].volume -= take,
            }
            self.apply_fill(id, lp, take, ts, batch);
            rem -= take;
        }
    }

    fn apply_fill(&mut self, id: &str, price: Price, qty: Qty, ts: i64, batch: &mut MatchBatch) {
        let Some(o) = self.orders.get_mut(id) else {
            return;
        };
        let prior_amount = o.amount_paid;
        let f = fees::compute_fill(
            price,
            qty,
            o.side,
            prior_amount,
            fees::commission_on(prior_amount),
            fees::stamp_tax_on(prior_amount, o.side),
        );
        o.filled += qty;
        o.amount_paid = prior_amount + f.amount;
        o.updated_at = ts;
        if o.side == Side::Buy {
            o.frozen = (o.frozen - f.net(Side::Buy)).max(0);
        }
        o.status = if o.filled >= o.qty {
            OrderStatus::Filled
        } else {
            OrderStatus::Partial
        };
        let done = o.filled >= o.qty;
        let snap = o.clone();
        let side = o.side;
        let price_level = o.price;
        let trade_id = format!("{}-{}", snap.id, snap.filled);
        batch.fills.push(Fill {
            trade_id,
            order_id: snap.id.clone(),
            user_id: snap.user_id,
            code: self.code.clone(),
            name: self.name.clone(),
            side,
            price,
            qty,
            amount: f.amount,
            commission: f.commission,
            stamp_tax: f.stamp_tax,
            ts,
        });
        batch.orders.push(snap.clone());
        if done {
            self.orders.remove(id);
            self.remove_resting(side, price_level, id);
        }
    }
}
