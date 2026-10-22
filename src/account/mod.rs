use crate::db::PersistEvent;
use crate::error::DomainError;
use crate::model::{AccountView, Fill, Money, Position, Price, Qty, Side};
use dashmap::DashMap;
use std::sync::Mutex;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct Account {
    pub user_id: i64,
    pub cash_available: Money,
    pub cash_frozen: Money,
    pub initial: Money,
}

#[derive(Debug, Clone)]
pub struct PosState {
    pub code: String,
    /// 总持仓
    pub total: Qty,
    /// 可卖数量（T+1）
    pub available: Qty,
    /// 卖出委托冻结数量
    pub frozen: Qty,
    /// 持仓总成本（含费用）
    pub cost_amount: Money,
}

impl PosState {
    pub fn cost_per_share(&self) -> Price {
        if self.total <= 0 {
            0
        } else {
            self.cost_amount / self.total
        }
    }
    pub fn sellable(&self) -> Qty {
        self.available.max(0)
    }
}

/// 资金与持仓。全部在内存中变更，异步批量落盘。
pub struct AccountService {
    accounts: DashMap<i64, Account>,
    positions: DashMap<(i64, String), PosState>,
    /// user_id -> 最近一次 T+1 结算日期
    settle_date: DashMap<i64, String>,
    events: Mutex<Option<mpsc::Sender<PersistEvent>>>,
}

impl Default for AccountService {
    fn default() -> Self {
        Self::new()
    }
}

impl AccountService {
    pub fn new() -> Self {
        AccountService {
            accounts: DashMap::new(),
            positions: DashMap::new(),
            settle_date: DashMap::new(),
            events: Mutex::new(None),
        }
    }

    pub fn set_event_sender(&self, tx: mpsc::Sender<PersistEvent>) {
        if let Ok(mut slot) = self.events.lock() {
            *slot = Some(tx);
        }
    }

    fn emit(&self, ev: PersistEvent) {
        if let Ok(slot) = self.events.lock() {
            if let Some(tx) = slot.as_ref() {
                if tx.try_send(ev).is_err() {
                    tracing::warn!("落盘事件队列已满，事件被丢弃");
                }
            }
        }
    }

    pub fn has(&self, user_id: i64) -> bool {
        self.accounts.contains_key(&user_id)
    }

    pub fn load_account(&self, acc: Account) {
        self.accounts.insert(acc.user_id, acc);
    }

    pub fn load_position(&self, user_id: i64, pos: PosState) {
        self.positions.insert((user_id, pos.code.clone()), pos);
    }

    pub fn load_settle_date(&self, user_id: i64, date: String) {
        self.settle_date.insert(user_id, date);
    }

    pub fn account(&self, user_id: i64) -> Option<Account> {
        self.accounts.get(&user_id).map(|a| a.clone())
    }

    pub fn cash_available(&self, user_id: i64) -> Money {
        self.accounts
            .get(&user_id)
            .map(|a| a.cash_available)
            .unwrap_or(0)
    }

    pub fn position(&self, user_id: i64, code: &str) -> Option<PosState> {
        self.positions
            .get(&(user_id, code.to_string()))
            .map(|p| p.clone())
    }

    pub fn sellable(&self, user_id: i64, code: &str) -> Qty {
        self.position(user_id, code)
            .map(|p| p.sellable())
            .unwrap_or(0)
    }

    /// 建仓前重设初始资金（同时更新内存状态，避免要等重启才生效）
    pub fn reset_initial(&self, user_id: i64, cash: crate::model::Money) {
        if let Some(mut a) = self.accounts.get_mut(&user_id) {
            a.cash_available = cash;
            a.cash_frozen = 0;
            a.initial = cash;
        }
    }

    /// 转世重生：清空持仓与冻结，资金回到 initial
    pub fn reset_user(&self, user_id: i64) {
        let codes: Vec<String> = self
            .positions
            .iter()
            .filter(|e| e.key().0 == user_id)
            .map(|e| e.key().1.clone())
            .collect();
        for c in codes {
            self.positions.remove(&(user_id, c));
        }
        let initial = self.accounts.get(&user_id).map(|a| a.initial).unwrap_or(0);
        if initial > 0 {
            self.reset_initial(user_id, initial);
        }
    }

    pub fn positions_of(&self, user_id: i64) -> Vec<PosState> {
        self.positions
            .iter()
            .filter(|e| e.key().0 == user_id)
            .map(|e| e.value().clone())
            .collect()
    }

    pub fn user_ids(&self) -> Vec<i64> {
        self.accounts.iter().map(|e| *e.key()).collect()
    }

    /// T+1 结算：当日买入的持仓在下一个交易日开盘前转为可卖。
    /// 以日期水位驱动，账户首次访问或定时任务触发，重复调用无副作用。
    pub fn settle_t1(&self, user_id: i64, today: &str) -> bool {
        let last = self.settle_date.get(&user_id).map(|d| d.clone());
        if last.as_deref() == Some(today) {
            return false;
        }
        let mut changed = false;
        let keys: Vec<(i64, String)> = self
            .positions
            .iter()
            .filter(|e| e.key().0 == user_id)
            .map(|e| e.key().clone())
            .collect();
        for key in keys {
            if let Some(mut p) = self.positions.get_mut(&key) {
                let target = (p.total - p.frozen).max(0);
                if p.available != target {
                    p.available = target;
                    changed = true;
                    let snapshot = p.clone();
                    drop(p);
                    self.emit(PersistEvent::Position {
                        user_id,
                        code: key.1.clone(),
                        total: snapshot.total,
                        available: snapshot.available,
                        frozen: snapshot.frozen,
                        cost_amount: snapshot.cost_amount,
                    });
                }
            }
        }
        self.settle_date.insert(user_id, today.to_string());
        changed
    }

    pub fn settle_date_of(&self, user_id: i64) -> Option<String> {
        self.settle_date.get(&user_id).map(|d| d.clone())
    }

    /// 由上层（成交推送等）写入任意落盘事件。
    pub fn emit_external(&self, ev: PersistEvent) {
        self.emit(ev);
    }

    /// 买入冻结：可用资金转入冻结。
    pub fn reserve_buy(&self, user_id: i64, amount: Money) -> Result<(), DomainError> {
        let mut acc = self
            .accounts
            .get_mut(&user_id)
            .ok_or(DomainError::AccountMissing)?;
        if acc.cash_available < amount {
            return Err(DomainError::BadRequest("可用资金不足".into()));
        }
        acc.cash_available -= amount;
        acc.cash_frozen += amount;
        let snap = acc.clone();
        drop(acc);
        self.emit_account(&snap);
        Ok(())
    }

    /// 撤单退还冻结资金。
    pub fn release_buy(&self, user_id: i64, amount: Money) {
        if amount <= 0 {
            return;
        }
        if let Some(mut acc) = self.accounts.get_mut(&user_id) {
            let refund = amount.min(acc.cash_frozen);
            acc.cash_frozen -= refund;
            acc.cash_available += refund;
            let snap = acc.clone();
            drop(acc);
            self.emit_account(&snap);
        }
    }

    /// 卖出冻结：可卖数量转入冻结。
    pub fn reserve_sell(&self, user_id: i64, code: &str, qty: Qty) -> Result<(), DomainError> {
        let key = (user_id, code.to_string());
        let mut p = self
            .positions
            .get_mut(&key)
            .ok_or_else(|| DomainError::BadRequest("无持仓，无法卖出".into()))?;
        if p.available < qty {
            return Err(DomainError::BadRequest("可卖数量不足".into()));
        }
        p.available -= qty;
        p.frozen += qty;
        let snap = p.clone();
        drop(p);
        self.emit_pos(user_id, &snap);
        Ok(())
    }

    pub fn release_sell(&self, user_id: i64, code: &str, qty: Qty) {
        if qty <= 0 {
            return;
        }
        let key = (user_id, code.to_string());
        if let Some(mut p) = self.positions.get_mut(&key) {
            let back = qty.min(p.frozen);
            p.frozen -= back;
            p.available += back;
            let snap = p.clone();
            drop(p);
            self.emit_pos(user_id, &snap);
        }
    }

    /// 成交回报入账。买单从冻结资金扣除实际成本与费用；卖单增加可用资金并扣减持仓。
    pub fn apply_fill(&self, fill: &Fill) -> Result<(), DomainError> {
        match fill.side {
            Side::Buy => {
                let mut acc = self
                    .accounts
                    .get_mut(&fill.user_id)
                    .ok_or(DomainError::AccountMissing)?;
                let cost = fill.amount + fill.commission;
                let from_frozen = cost.min(acc.cash_frozen);
                acc.cash_frozen -= from_frozen;
                let rest = cost - from_frozen;
                acc.cash_available = (acc.cash_available - rest).max(0);
                let acc_snap = acc.clone();
                drop(acc);
                self.emit_account(&acc_snap);

                let key = (fill.user_id, fill.code.clone());
                let mut p = self
                    .positions
                    .entry(key.clone())
                    .or_insert_with(|| PosState {
                        code: fill.code.clone(),
                        total: 0,
                        available: 0,
                        frozen: 0,
                        cost_amount: 0,
                    });
                p.total += fill.qty;
                p.cost_amount += cost;
                let snap = p.clone();
                drop(p);
                self.emit_pos(fill.user_id, &snap);
            }
            Side::Sell => {
                let mut acc = self
                    .accounts
                    .get_mut(&fill.user_id)
                    .ok_or(DomainError::AccountMissing)?;
                let net = fill.amount - fill.commission - fill.stamp_tax;
                acc.cash_available += net;
                let acc_snap = acc.clone();
                drop(acc);
                self.emit_account(&acc_snap);

                let key = (fill.user_id, fill.code.clone());
                if let Some(mut p) = self.positions.get_mut(&key) {
                    let unit_cost = if p.total > 0 {
                        p.cost_amount / p.total
                    } else {
                        0
                    };
                    let released_cost = unit_cost * fill.qty;
                    p.total = (p.total - fill.qty).max(0);
                    p.frozen = (p.frozen - fill.qty).max(0);
                    p.cost_amount = (p.cost_amount - released_cost).max(0);
                    if p.total == 0 {
                        p.cost_amount = 0;
                        p.frozen = 0;
                        p.available = 0;
                    }
                    let snap = p.clone();
                    drop(p);
                    if snap.total == 0 {
                        self.positions.remove(&key);
                    } else {
                        self.emit_pos(fill.user_id, &snap);
                    }
                }
            }
        }
        Ok(())
    }

    fn emit_account(&self, acc: &Account) {
        self.emit(PersistEvent::Account {
            user_id: acc.user_id,
            cash_available: acc.cash_available,
            cash_frozen: acc.cash_frozen,
            initial: acc.initial,
        });
    }

    fn emit_pos(&self, user_id: i64, p: &PosState) {
        self.emit(PersistEvent::Position {
            user_id,
            code: p.code.clone(),
            total: p.total,
            available: p.available,
            frozen: p.frozen,
            cost_amount: p.cost_amount,
        });
    }

    /// 组装账户视图。`quote` 返回 (名称, 最新价)。
    pub fn view<F>(&self, user_id: i64, username: &str, quote: F) -> Option<AccountView>
    where
        F: Fn(&str) -> (String, Price),
    {
        let acc = self.account(user_id)?;
        let mut positions = Vec::new();
        let mut market_value = 0;
        for p in self.positions_of(user_id) {
            let (name, last) = quote(&p.code);
            let price = if last > 0 { last } else { p.cost_per_share() };
            let mv = price * p.total;
            let pnl = mv - p.cost_amount;
            let pnl_pct = if p.cost_amount > 0 {
                pnl as f64 / p.cost_amount as f64 * 100.0
            } else {
                0.0
            };
            market_value += mv;
            positions.push(Position {
                code: p.code.clone(),
                name,
                total: p.total,
                available: p.sellable(),
                cost: p.cost_per_share(),
                last: price,
                market_value: mv,
                pnl,
                pnl_pct,
            });
        }
        positions.sort_by(|a, b| b.market_value.cmp(&a.market_value));
        let total_asset = acc.cash_available + acc.cash_frozen + market_value;
        Some(AccountView {
            user_id,
            username: username.to_string(),
            cash_available: acc.cash_available,
            cash_frozen: acc.cash_frozen,
            market_value,
            total_asset,
            initial: acc.initial,
            total_pnl: total_asset - acc.initial,
            positions,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fill(side: Side, price: Price, qty: Qty, commission: Money, stamp: Money) -> Fill {
        Fill {
            trade_id: "t1".into(),
            order_id: "o1".into(),
            user_id: 1,
            code: "000001".into(),
            name: "平安银行".into(),
            side,
            price,
            qty,
            amount: price * qty,
            commission,
            stamp_tax: stamp,
            ts: 0,
        }
    }

    fn service() -> AccountService {
        let s = AccountService::new();
        s.load_account(Account {
            user_id: 1,
            cash_available: 1_000_000_000,
            cash_frozen: 0,
            initial: 1_000_000_000,
        });
        s
    }

    #[test]
    fn 当日买入不计入可卖() {
        let s = service();
        s.reserve_buy(1, 11_315 * 100 + 5_000).unwrap();
        s.apply_fill(&fill(Side::Buy, 11_315, 100, 5_000, 0))
            .unwrap();
        let p = s.position(1, "000001").unwrap();
        assert_eq!(p.total, 100);
        assert_eq!(p.available, 0);
        assert_eq!(s.sellable(1, "000001"), 0);
        // 次日结算后可卖
        assert!(s.settle_t1(1, "2026-10-23"));
        assert_eq!(s.sellable(1, "000001"), 100);
        // 同一天重复结算无副作用
        assert!(!s.settle_t1(1, "2026-10-23"));
    }

    #[test]
    fn 买入冻结与成交扣减() {
        let s = service();
        let before = s.cash_available(1);
        s.reserve_buy(1, 11_320 * 1_000 + 5_000).unwrap();
        assert_eq!(s.cash_available(1), before - (11_320 * 1_000 + 5_000));
        s.apply_fill(&fill(Side::Buy, 11_320, 1_000, 5_000, 0))
            .unwrap();
        let acc = s.account(1).unwrap();
        assert_eq!(acc.cash_frozen, 0);
        assert_eq!(acc.cash_available, before - 11_320 * 1_000 - 5_000);
    }

    #[test]
    fn 卖出所得扣除费用() {
        let s = service();
        s.apply_fill(&fill(Side::Buy, 10_000, 1_000, 5_000, 0))
            .unwrap();
        s.settle_t1(1, "2026-10-23");
        let before = s.cash_available(1);
        s.reserve_sell(1, "000001", 1_000).unwrap();
        assert_eq!(s.sellable(1, "000001"), 0);
        s.apply_fill(&fill(Side::Sell, 11_000, 1_000, 5_000, 10_000))
            .unwrap();
        assert_eq!(s.cash_available(1), before + 11_000 * 1_000 - 15_000);
        assert!(s.position(1, "000001").is_none());
    }

    #[test]
    fn 撤单退还冻结资金与持仓() {
        let s = service();
        let before = s.cash_available(1);
        s.reserve_buy(1, 5_000_000).unwrap();
        s.release_buy(1, 5_000_000);
        assert_eq!(s.cash_available(1), before);

        s.apply_fill(&fill(Side::Buy, 10_000, 500, 5_000, 0))
            .unwrap();
        s.settle_t1(1, "2026-10-24");
        s.reserve_sell(1, "000001", 500).unwrap();
        s.release_sell(1, "000001", 500);
        assert_eq!(s.sellable(1, "000001"), 500);
    }

    #[test]
    fn 成本价随加仓摊薄() {
        let s = service();
        s.apply_fill(&fill(Side::Buy, 10_000, 1_000, 5_000, 0))
            .unwrap();
        s.apply_fill(&fill(Side::Buy, 12_000, 1_000, 6_000, 0))
            .unwrap();
        let p = s.position(1, "000001").unwrap();
        assert_eq!(p.total, 2_000);
        // (10000000 + 5000 + 12000000 + 6000) / 2000 = 11005.5 → 11005
        assert_eq!(p.cost_per_share(), 11_005);
    }
}
