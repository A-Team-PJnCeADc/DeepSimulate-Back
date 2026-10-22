use crate::model::{AccountView, Level, Order, QuoteSnapshot, Tick, TradeRecord};
use serde_json::json;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

const CLIENT_BUFFER: usize = 256;

pub struct ClientHandle {
    pub id: u64,
    pub user_id: i64,
    pub subs: HashSet<String>,
    tx: mpsc::Sender<Arc<str>>,
}

/// WebSocket 推送中心。行情按订阅广播，委托与成交只推送给所属用户。
pub struct WsHub {
    clients: Mutex<Vec<ClientHandle>>,
    next_id: AtomicU64,
}

impl Default for WsHub {
    fn default() -> Self {
        Self::new()
    }
}

impl WsHub {
    pub fn new() -> Self {
        WsHub {
            clients: Mutex::new(Vec::new()),
            next_id: AtomicU64::new(1),
        }
    }

    pub fn register(&self, user_id: i64) -> (u64, mpsc::Receiver<Arc<str>>) {
        let (tx, rx) = mpsc::channel(CLIENT_BUFFER);
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut list) = self.clients.lock() {
            list.push(ClientHandle {
                id,
                user_id,
                subs: HashSet::new(),
                tx,
            });
        }
        (id, rx)
    }

    pub fn unregister(&self, id: u64) {
        if let Ok(mut list) = self.clients.lock() {
            list.retain(|c| c.id != id);
        }
    }

    pub fn set_subs(&self, id: u64, codes: HashSet<String>) {
        if let Ok(mut list) = self.clients.lock() {
            if let Some(c) = list.iter_mut().find(|c| c.id == id) {
                c.subs = codes;
            }
        }
    }

    pub fn client_count(&self) -> usize {
        self.clients.lock().map(|l| l.len()).unwrap_or(0)
    }

    fn send_to<F: Fn(&ClientHandle) -> bool>(&self, filter: F, payload: Arc<str>) {
        let Ok(list) = self.clients.lock() else {
            return;
        };
        for c in list.iter() {
            if filter(c) {
                // 队列满表示客户端消费不过来，行情类消息直接丢弃
                let _ = c.tx.try_send(payload.clone());
            }
        }
    }

    /// 回放行情：独立消息类型 rquote，只有回放界面订阅，避免与实时行情混淆
    pub fn push_replay_quote(&self, q: &QuoteSnapshot, bids: &[Level], asks: &[Level]) {
        self.push_quote_inner(q, bids, asks, "rquote");
    }

    pub fn push_quote(&self, q: &QuoteSnapshot, bids: &[Level], asks: &[Level]) {
        self.push_quote_inner(q, bids, asks, "quote");
    }

    fn push_quote_inner(&self, q: &QuoteSnapshot, bids: &[Level], asks: &[Level], kind: &str) {
        let payload: Arc<str> = Arc::from(
            json!({
                "type": kind,
                "data": {
                    "code": q.code,
                    "name": q.name,
                    "last": q.last,
                    "prev_close": q.prev_close,
                    "open": q.open,
                    "high": q.high,
                    "low": q.low,
                    "total_hand": q.total_hand,
                    "amount": q.amount,
                    "limit_up": q.limit_up,
                    "limit_down": q.limit_down,
                    "is_st": q.is_st,
                    "bids": bids,
                    "asks": asks,
                    "ts": q.ts,
                }
            })
            .to_string(),
        );
        self.send_to(|c| c.subs.contains(&q.code), payload);
    }

    pub fn push_ticks(&self, code: &str, ticks: &[Tick]) {
        if ticks.is_empty() {
            return;
        }
        let payload: Arc<str> = Arc::from(
            json!({
                "type": "tick",
                "data": { "code": code, "ticks": ticks }
            })
            .to_string(),
        );
        self.send_to(|c| c.subs.contains(code), payload);
    }

    pub fn push_order(&self, o: &Order) {
        let payload: Arc<str> = Arc::from(json!({ "type": "order", "data": o }).to_string());
        let uid = o.user_id;
        self.send_to(|c| c.user_id == uid, payload);
    }

    pub fn push_trade(&self, t: &TradeRecord) {
        let payload: Arc<str> = Arc::from(json!({ "type": "trade", "data": t }).to_string());
        let uid = t.user_id;
        self.send_to(|c| c.user_id == uid, payload);
    }

    pub fn push_account(&self, view: &AccountView) {
        let payload: Arc<str> = Arc::from(json!({ "type": "account", "data": view }).to_string());
        let uid = view.user_id;
        self.send_to(|c| c.user_id == uid, payload);
    }

    /// 只发给单个连接（握手回执、订阅确认等）。
    pub fn send_to_client(&self, id: u64, value: serde_json::Value) {
        let payload: Arc<str> = Arc::from(value.to_string());
        let Ok(list) = self.clients.lock() else {
            return;
        };
        if let Some(c) = list.iter().find(|c| c.id == id) {
            let _ = c.tx.try_send(payload);
        }
    }
}
