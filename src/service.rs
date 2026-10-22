use crate::account::AccountService;
use crate::config::Config;
use crate::db::{Db, PersistEvent};
use crate::engine::validate::validate;
use crate::engine::{Engine, MatchBatch, Reject};
use crate::error::{AppError, DomainError};
use crate::model::{
    AccountView, Money, Order, OrderStatus, Price, Qty, QuoteSnapshot, Side, TradeRecord,
};
use crate::ws::WsHub;
use anyhow::Result;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// 行情读取口，供账户估值使用，避免 Core 反向依赖行情模块的具体实现。
pub trait QuoteSource: Send + Sync {
    /// 返回 (名称, 最新价)，无行情时返回 None。
    fn l1(&self, code: &str) -> Option<(String, Price)>;
}

/// 交易核心：撮合引擎 + 账户 + 推送 + 落盘。行情模块与 API 层共用同一实例。
pub struct Core {
    pub engine: Arc<Engine>,
    pub accounts: Arc<AccountService>,
    pub ws: Arc<WsHub>,
    pub db: Db,
    pub quotes: Arc<dyn QuoteSource>,
    usernames: DashMap<i64, String>,
    /// 最近委托内存副本：落盘有 50ms 批量窗口，读取委托列表时与库合并
    recent_orders: DashMap<i64, Vec<Order>>,
    /// 活动委托 id -> 股票代码
    order_index: DashMap<String, String>,
    order_seq: AtomicU64,
    /// 每只股票最近一次撮合耗时（微秒）
    pub match_latency_us: DashMap<String, u64>,
}

#[derive(Debug, serde::Deserialize)]
pub struct OrderRequest {
    pub code: String,
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
}

impl Core {
    pub fn new(
        _cfg: Arc<Config>,
        engine: Arc<Engine>,
        accounts: Arc<AccountService>,
        ws: Arc<WsHub>,
        db: Db,
        quotes: Arc<dyn QuoteSource>,
    ) -> Self {
        Core {
            engine,
            accounts,
            ws,
            db,
            quotes,
            usernames: DashMap::new(),
            recent_orders: DashMap::new(),
            order_index: DashMap::new(),
            order_seq: AtomicU64::new(1),
            match_latency_us: DashMap::new(),
        }
    }

    pub fn cache_username(&self, user_id: i64, name: &str) {
        self.usernames.insert(user_id, name.to_string());
    }

    fn username(&self, user_id: i64) -> String {
        self.usernames
            .get(&user_id)
            .map(|n| n.clone())
            .unwrap_or_else(|| format!("user{user_id}"))
    }

    fn next_order_id(&self) -> String {
        let n = self.order_seq.fetch_add(1, Ordering::Relaxed);
        format!("O{}{:05}", crate::model::now_ms(), n)
    }

    /// 首次访问时从 SQLite 载入账户与持仓，并按交易日水位做 T+1 结算。
    pub async fn ensure_loaded(&self, user_id: i64) -> Result<(), AppError> {
        if !self.accounts.has(user_id) {
            let Some((acc, last_settle)) = self.db.load_account(user_id).await? else {
                return Err(DomainError::AccountMissing.into());
            };
            let positions = self.db.load_positions(user_id).await?;
            self.accounts.load_account(acc);
            for p in positions {
                self.accounts.load_position(user_id, p);
            }
            if let Some(d) = last_settle {
                self.accounts.load_settle_date(user_id, d);
            }
        }
        let today = crate::model::today_cn();
        if self.accounts.settle_date_of(user_id).as_deref() != Some(today.as_str()) {
            self.accounts.settle_t1(user_id, &today);
            self.db.set_settle_date(user_id, &today).await?;
            self.record_snapshot(user_id).await;
            tracing::info!(user_id, date = %today, "T+1 结算完成");
        }
        Ok(())
    }

    pub async fn record_snapshot(&self, user_id: i64) {
        let Some(view) = self.account_view(user_id) else {
            return;
        };
        let _ = self
            .db
            .record_snapshot(
                user_id,
                &crate::model::today_cn(),
                view.total_asset,
                view.market_value,
                view.cash_available,
            )
            .await;
    }

    pub fn account_view(&self, user_id: i64) -> Option<AccountView> {
        let q = self.quotes.clone();
        self.accounts
            .view(user_id, &self.username(user_id), move |code| {
                q.l1(code).unwrap_or_else(|| (code.to_string(), 0))
            })
    }

    pub fn push_account(&self, user_id: i64) {
        if let Some(v) = self.account_view(user_id) {
            self.ws.push_account(&v);
        }
    }

    /// 下单：校验 → 冻结 → 入簿撮合 → 成交入账与推送。校验失败落废单并返回。
    pub fn submit(
        &self,
        user_id: i64,
        req: OrderRequest,
        quote: &QuoteSnapshot,
    ) -> Result<Order, AppError> {
        let now = crate::model::now_ms();
        let sellable = self.accounts.sellable(user_id, &req.code);
        let cash = self.accounts.cash_available(user_id);
        let mut order = Order {
            id: self.next_order_id(),
            user_id,
            code: req.code.clone(),
            name: quote.name.clone(),
            side: req.side,
            price: req.price,
            qty: req.qty,
            filled: 0,
            status: OrderStatus::Pending,
            reason: None,
            seq: self.engine.next_seq(),
            queue_ahead: 0,
            frozen: 0,
            amount_paid: 0,
            created_at: now,
            updated_at: now,
        };

        let checked = validate(req.side, req.price, req.qty, quote, cash, sellable);
        let reserve: Money = match checked {
            Ok(r) => r,
            Err(reject) => {
                order.status = OrderStatus::Rejected;
                order.reason = Some(reject.message());
                self.finish_order(&order);
                return Ok(order);
            }
        };

        if req.side == Side::Buy {
            if self.accounts.reserve_buy(user_id, reserve).is_err() {
                order.status = OrderStatus::Rejected;
                order.reason = Some(
                    Reject::InsufficientFunds {
                        need: reserve,
                        available: cash,
                    }
                    .message(),
                );
                self.finish_order(&order);
                return Ok(order);
            }
            order.frozen = reserve;
        } else if let Err(e) = self.accounts.reserve_sell(user_id, &req.code, req.qty) {
            order.status = OrderStatus::Rejected;
            order.reason = Some(e.to_string());
            self.finish_order(&order);
            return Ok(order);
        }

        self.order_index
            .insert(order.id.clone(), order.code.clone());
        let (placed, batch) = self.engine.place(order);
        self.dispatch(batch);
        self.finish_order(&placed);
        self.push_account(user_id);
        Ok(placed)
    }

    /// 撤单：撤掉剩余部分，解冻资金或持仓。
    pub fn cancel(&self, user_id: i64, order_id: &str) -> Result<Order, AppError> {
        let Some(code) = self.order_index.get(order_id).map(|c| c.clone()) else {
            return Err(DomainError::OrderNotFound.into());
        };
        let cancelled = self.engine.cancel(&code, order_id)?;
        if cancelled.user_id != user_id {
            // 理论上不可达：委托索引与用户一致
            return Err(DomainError::OrderNotFound.into());
        }
        self.order_index.remove(order_id);
        self.release(&cancelled);
        self.finish_order(&cancelled);
        self.ws.push_order(&cancelled);
        self.push_account(user_id);
        Ok(cancelled)
    }

    /// 撤销所有未完成委托（回放跳转/停止时调用）。
    /// 只撤某个用户的活动委托（转世重生用，避免误伤其他用户）
    pub fn cancel_active_of(&self, user_id: i64) {
        let ids: Vec<String> = self
            .recent_orders
            .get(&user_id)
            .map(|v| {
                v.iter()
                    .filter(|o| o.status == crate::model::OrderStatus::Pending || o.status == crate::model::OrderStatus::Partial)
                    .map(|o| o.id.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for id in ids {
            let Some(code) = self.order_index.get(&id).map(|c| c.clone()) else { continue };
            match self.engine.cancel(&code, &id) {
                Ok(o) => {
                    self.release(&o);
                    self.finish_order(&o);
                    self.push_account(o.user_id);
                }
                Err(_) => {
                    self.order_index.remove(&id);
                }
            }
        }
    }

    pub fn cancel_all_active(&self) {
        let ids: Vec<String> = self.order_index.iter().map(|e| e.key().clone()).collect();
        for id in ids {
            let Some(code) = self.order_index.get(&id).map(|c| c.clone()) else { continue };
            match self.engine.cancel(&code, &id) {
                Ok(o) => {
                    self.release(&o);
                    self.finish_order(&o);
                    self.push_account(o.user_id);
                }
                Err(_) => {
                    self.order_index.remove(&id);
                }
            }
        }
    }

    /// 释放委托剩余冻结。
    fn release(&self, o: &Order) {
        match o.side {
            Side::Buy => self.accounts.release_buy(o.user_id, o.frozen),
            Side::Sell => self
                .accounts
                .release_sell(o.user_id, &o.code, o.remaining()),
        }
    }

    /// 成交入账、推送与落盘。
    pub fn dispatch(&self, batch: MatchBatch) {
        let mut touched = Vec::new();
        for fill in batch.fills {
            if let Err(e) = self.accounts.apply_fill(&fill) {
                tracing::error!(error = %e, "成交入账失败");
                continue;
            }
            let rec = TradeRecord {
                id: fill.trade_id.clone(),
                order_id: fill.order_id.clone(),
                user_id: fill.user_id,
                code: fill.code.clone(),
                name: fill.name.clone(),
                side: fill.side,
                price: fill.price,
                qty: fill.qty,
                amount: fill.amount,
                commission: fill.commission,
                stamp_tax: fill.stamp_tax,
                created_at: fill.ts,
            };
            self.persist(PersistEvent::Trade(Box::new(rec.clone())));
            self.ws.push_trade(&rec);
            if !touched.contains(&fill.user_id) {
                touched.push(fill.user_id);
            }
        }
        for o in batch.orders {
            if !o.status.is_active() {
                self.order_index.remove(&o.id);
                // 委托终结：退还按限价预留、实际未用掉的冻结资金
                if o.status == OrderStatus::Filled && o.side == Side::Buy && o.frozen > 0 {
                    self.accounts.release_buy(o.user_id, o.frozen);
                }
            }
            self.remember(&o);
            self.persist(PersistEvent::Order(Box::new(o.clone())));
            self.ws.push_order(&o);
        }
        for uid in touched {
            self.push_account(uid);
        }
    }

    pub fn finish_order(&self, o: &Order) {
        if !o.status.is_active() {
            self.order_index.remove(&o.id);
        }
        self.remember(o);
        self.persist(PersistEvent::Order(Box::new(o.clone())));
        self.ws.push_order(o);
    }

    /// 记录委托最新状态，供读取路径与库数据合并。
    fn remember(&self, o: &Order) {
        let mut slot = self.recent_orders.entry(o.user_id).or_default();
        match slot.iter_mut().find(|x| x.id == o.id) {
            Some(x) => *x = o.clone(),
            None => {
                slot.push(o.clone());
                if slot.len() > 300 {
                    let drop_n = slot.len() - 300;
                    slot.drain(0..drop_n);
                }
            }
        }
    }

    pub fn recent_orders(&self, user_id: i64, limit: usize) -> Vec<Order> {
        self.recent_orders
            .get(&user_id)
            .map(|v| {
                let mut list: Vec<Order> = v.iter().cloned().collect();
                list.sort_by(|a, b| b.created_at.cmp(&a.created_at).then(b.seq.cmp(&a.seq)));
                list.truncate(limit);
                list
            })
            .unwrap_or_default()
    }

    fn persist(&self, ev: PersistEvent) {
        self.accounts.emit_external(ev);
    }

    /// 启动恢复：内存引擎为空，把库里遗留的活动委托全部撤销并解冻。
    pub async fn recover_open_orders(&self) -> Result<()> {
        let open = self.db.load_open_orders(None).await?;
        if open.is_empty() {
            return Ok(());
        }
        tracing::info!(count = open.len(), "恢复：撤销遗留活动委托");
        for mut o in open {
            self.ensure_loaded(o.user_id).await.ok();
            o.status = OrderStatus::Cancelled;
            o.reason = Some("系统重启，委托已撤销".into());
            o.queue_ahead = 0;
            o.updated_at = crate::model::now_ms();
            match o.side {
                Side::Buy => self.accounts.release_buy(o.user_id, o.frozen),
                Side::Sell => self
                    .accounts
                    .release_sell(o.user_id, &o.code, o.remaining()),
            }
            self.persist(PersistEvent::Order(Box::new(o)));
        }
        Ok(())
    }
}
