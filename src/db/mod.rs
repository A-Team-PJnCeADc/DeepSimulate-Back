use crate::account::{Account, PosState};
use crate::model::{Money, Order, OrderStatus, Qty, Side, TradeRecord};
use anyhow::{Context, Result};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Row, SqlitePool};
use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::mpsc;

/// 落盘事件。写线程按窗口合并，同一实体的多次变更只保留最终值。
#[derive(Debug)]
pub enum PersistEvent {
    Account {
        user_id: i64,
        cash_available: Money,
        cash_frozen: Money,
        initial: Money,
    },
    Position {
        user_id: i64,
        code: String,
        total: Qty,
        available: Qty,
        frozen: Qty,
        cost_amount: Money,
    },
    Order(Box<Order>),
    Trade(Box<TradeRecord>),
}

#[derive(Clone)]
pub struct Db {
    pool: SqlitePool,
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS users (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    username      TEXT    NOT NULL UNIQUE,
    password_hash TEXT    NOT NULL,
    created_at    INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS accounts (
    user_id        INTEGER PRIMARY KEY,
    cash_available INTEGER NOT NULL,
    cash_frozen    INTEGER NOT NULL,
    initial        INTEGER NOT NULL,
    updated_at     INTEGER NOT NULL,
    last_settle    TEXT
);
CREATE TABLE IF NOT EXISTS positions (
    user_id     INTEGER NOT NULL,
    code        TEXT    NOT NULL,
    total       INTEGER NOT NULL,
    available   INTEGER NOT NULL,
    frozen      INTEGER NOT NULL,
    cost_amount INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL,
    PRIMARY KEY (user_id, code)
);
CREATE TABLE IF NOT EXISTS orders (
    id          TEXT    PRIMARY KEY,
    user_id     INTEGER NOT NULL,
    code        TEXT    NOT NULL,
    name        TEXT    NOT NULL DEFAULT '',
    side        TEXT    NOT NULL,
    price       INTEGER NOT NULL,
    qty         INTEGER NOT NULL,
    filled      INTEGER NOT NULL,
    status      TEXT    NOT NULL,
    reason      TEXT,
    seq         INTEGER NOT NULL,
    queue_ahead INTEGER NOT NULL,
    frozen      INTEGER NOT NULL,
    amount_paid INTEGER NOT NULL DEFAULT 0,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_orders_user ON orders(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_active ON orders(status);
CREATE TABLE IF NOT EXISTS trades (
    id         TEXT    PRIMARY KEY,
    order_id   TEXT    NOT NULL,
    user_id    INTEGER NOT NULL,
    code       TEXT    NOT NULL,
    name       TEXT    NOT NULL DEFAULT '',
    side       TEXT    NOT NULL,
    price      INTEGER NOT NULL,
    qty        INTEGER NOT NULL,
    amount     INTEGER NOT NULL,
    commission INTEGER NOT NULL,
    stamp_tax  INTEGER NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_trades_user ON trades(user_id, created_at DESC);
CREATE TABLE IF NOT EXISTS daily_snapshots (
    user_id      INTEGER NOT NULL,
    date         TEXT    NOT NULL,
    total_asset  INTEGER NOT NULL,
    market_value INTEGER NOT NULL,
    cash_available INTEGER NOT NULL,
    PRIMARY KEY (user_id, date)
);
CREATE TABLE IF NOT EXISTS user_settings (
    user_id      INTEGER PRIMARY KEY,
    commission_bp INTEGER NOT NULL DEFAULT 25,
    min_commission INTEGER NOT NULL DEFAULT 5000,
    stamp_bp     INTEGER NOT NULL DEFAULT 10,
    start_date   TEXT    NOT NULL DEFAULT '',
    initial_date TEXT    NOT NULL DEFAULT '',
    practice_clock TEXT  NOT NULL DEFAULT '09:15:00',
    updated_at   INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS sessions (
    token      TEXT    PRIMARY KEY,
    user_id    INTEGER NOT NULL,
    username   TEXT    NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS watchlist (
    user_id INTEGER NOT NULL,
    code    TEXT    NOT NULL,
    PRIMARY KEY (user_id, code)
);
"#;

impl Db {
    pub async fn connect(url: &str) -> Result<Db> {
        let opts = SqliteConnectOptions::from_str(url)
            .context("数据库连接串非法")?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(opts)
            .await
            .context("打开 SQLite 失败")?;
        let db = Db { pool };
        db.add_columns().await;
        db.migrate().await?;
        Ok(db)
    }

    /// 给老库补列（CREATE TABLE IF NOT EXISTS 不会新增字段）
    async fn add_columns(&self) {
        for sql in [
            "ALTER TABLE user_settings ADD COLUMN initial_date TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE user_settings ADD COLUMN practice_clock TEXT NOT NULL DEFAULT '09:15:00'",
        ] {
            let _ = sqlx::query(sql).execute(&self.pool).await;
        }
    }

    async fn migrate(&self) -> Result<()> {
        for stmt in SCHEMA.split(';') {
            let s = stmt.trim();
            if s.is_empty() {
                continue;
            }
            sqlx::query(s).execute(&self.pool).await?;
        }
        Ok(())
    }

    /// 登录令牌落库：服务重启后会话仍有效，避免前端被踢回登录页
    pub async fn save_session(&self, token: &str, user_id: i64, username: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO sessions (token, user_id, username, created_at) VALUES (?, ?, ?, ?)
             ON CONFLICT(token) DO UPDATE SET user_id = excluded.user_id",
        )
        .bind(token)
        .bind(user_id)
        .bind(username)
        .bind(crate::model::now_ms())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete_session(&self, token: &str) -> Result<()> {
        sqlx::query("DELETE FROM sessions WHERE token = ?")
            .bind(token)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn load_sessions(&self) -> Result<Vec<(String, i64, String)>> {
        let rows: Vec<(String, i64, String)> =
            sqlx::query_as("SELECT token, user_id, username FROM sessions")
                .fetch_all(&self.pool)
                .await?;
        Ok(rows)
    }

    /// 用户参数：佣金(基点)、最低佣金(厘)、印花税(基点)、起始日期
    pub async fn load_user_settings(&self, user_id: i64) -> Result<Option<(i64, i64, i64, String)>> {
        let row: Option<(i64, i64, i64, String)> = sqlx::query_as(
            "SELECT commission_bp, min_commission, stamp_bp, start_date FROM user_settings WHERE user_id = ?",
        )
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    /// 练手日的初始值、当前值与分时进度（服务端维护，刷新后仍准确）
    pub async fn load_practice_state(&self, user_id: i64) -> Result<Option<(String, String, String)>> {
        let row: Option<(String, String, String)> = sqlx::query_as(
            "SELECT COALESCE(initial_date,''), COALESCE(start_date,''), COALESCE(practice_clock,'09:15:00')
             FROM user_settings WHERE user_id = ?",
        )
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    pub async fn save_practice_clock(&self, user_id: i64, clock: &str) -> Result<()> {
        sqlx::query("UPDATE user_settings SET practice_clock = ? WHERE user_id = ?")
            .bind(clock)
            .bind(user_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn save_user_settings(
        &self,
        user_id: i64,
        commission_bp: i64,
        min_commission: i64,
        stamp_bp: i64,
        start_date: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO user_settings (user_id, commission_bp, min_commission, stamp_bp, start_date, initial_date, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(user_id) DO UPDATE SET
               commission_bp = excluded.commission_bp,
               min_commission = excluded.min_commission,
               stamp_bp = excluded.stamp_bp,
               start_date = excluded.start_date,
               initial_date = CASE WHEN user_settings.initial_date = '' THEN excluded.initial_date ELSE user_settings.initial_date END,
               updated_at = excluded.updated_at",
        )
        .bind(user_id)
        .bind(commission_bp)
        .bind(min_commission)
        .bind(stamp_bp)
        .bind(start_date)
        .bind(start_date)
        .bind(crate::model::now_ms())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// 设置初始资金（只在账户尚未建仓时允许改）
    pub async fn set_initial_cash(&self, user_id: i64, cash: i64) -> Result<()> {
        sqlx::query("UPDATE accounts SET cash_available = ?, initial = ?, updated_at = ? WHERE user_id = ?")
            .bind(cash)
            .bind(cash)
            .bind(crate::model::now_ms())
            .bind(user_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// 练手日推进的锚点日期（上次保存设置的真实日期，东八区）
    pub async fn settings_anchor(&self, user_id: i64) -> Result<String> {
        let ms: Option<i64> = sqlx::query_scalar("SELECT updated_at FROM user_settings WHERE user_id = ?")
            .bind(user_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(ms.map(crate::model::ymd_from_ms).unwrap_or_default())
    }

    /// 清空该用户的交易记录（持仓/委托/成交），账户资金由调用方重置
    pub async fn clear_trading(&self, user_id: i64) -> Result<()> {
        for sql in [
            "DELETE FROM positions WHERE user_id = ?",
            "DELETE FROM orders WHERE user_id = ?",
            "DELETE FROM trades WHERE user_id = ?",
            "DELETE FROM user_settings WHERE user_id = ?",
        ] {
            sqlx::query(sql).bind(user_id).execute(&self.pool).await?;
        }
        Ok(())
    }

    pub async fn create_user(&self, username: &str, hash: &str, initial: Money) -> Result<i64> {
        let now = crate::model::now_ms();
        let mut tx = self.pool.begin().await?;
        let id: i64 = sqlx::query(
            "INSERT INTO users (username, password_hash, created_at) VALUES (?, ?, ?) RETURNING id",
        )
        .bind(username)
        .bind(hash)
        .bind(now)
        .fetch_one(&mut *tx)
        .await
        .context("写入用户失败")?
        .get(0);
        sqlx::query(
            "INSERT INTO accounts (user_id, cash_available, cash_frozen, initial, updated_at, last_settle)
             VALUES (?, ?, 0, ?, ?, ?)",
        )
        .bind(id)
        .bind(initial)
        .bind(initial)
        .bind(now)
        .bind(crate::model::today_cn())
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(id)
    }

    pub async fn find_user(&self, username: &str) -> Result<Option<(i64, String, String)>> {
        let row = sqlx::query("SELECT id, username, password_hash FROM users WHERE username = ?")
            .bind(username)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| (r.get(0), r.get(1), r.get(2))))
    }

    pub async fn load_account(&self, user_id: i64) -> Result<Option<(Account, Option<String>)>> {
        let row = sqlx::query(
            "SELECT cash_available, cash_frozen, initial, last_settle FROM accounts WHERE user_id = ?",
        )
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| {
            (
                Account {
                    user_id,
                    cash_available: r.get(0),
                    cash_frozen: r.get(1),
                    initial: r.get(2),
                },
                r.get::<Option<String>, _>(3),
            )
        }))
    }

    pub async fn load_positions(&self, user_id: i64) -> Result<Vec<PosState>> {
        let rows = sqlx::query(
            "SELECT code, total, available, frozen, cost_amount FROM positions WHERE user_id = ?",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| PosState {
                code: r.get(0),
                total: r.get(1),
                available: r.get(2),
                frozen: r.get(3),
                cost_amount: r.get(4),
            })
            .collect())
    }

    pub async fn load_open_orders(&self, user_id: Option<i64>) -> Result<Vec<Order>> {
        let rows =
            match user_id {
                Some(uid) => sqlx::query(
                    "SELECT id, user_id, code, name, side, price, qty, filled, status, reason, seq,
                            queue_ahead, frozen, amount_paid, created_at, updated_at
                     FROM orders WHERE status IN ('pending','partial') AND user_id = ?",
                )
                .bind(uid)
                .fetch_all(&self.pool)
                .await?,
                None => sqlx::query(
                    "SELECT id, user_id, code, name, side, price, qty, filled, status, reason, seq,
                            queue_ahead, frozen, amount_paid, created_at, updated_at
                     FROM orders WHERE status IN ('pending','partial')",
                )
                .fetch_all(&self.pool)
                .await?,
            };
        Ok(rows.into_iter().map(row_to_order).collect())
    }

    pub async fn orders_of(&self, user_id: i64, limit: i64) -> Result<Vec<Order>> {
        let rows = sqlx::query(
            "SELECT id, user_id, code, name, side, price, qty, filled, status, reason, seq,
                    queue_ahead, frozen, amount_paid, created_at, updated_at
             FROM orders WHERE user_id = ? ORDER BY created_at DESC, seq DESC LIMIT ?",
        )
        .bind(user_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(row_to_order).collect())
    }

    pub async fn trades_of(&self, user_id: i64, limit: i64) -> Result<Vec<TradeRecord>> {
        let rows = sqlx::query(
            "SELECT id, user_id, order_id, code, name, side, price, qty, amount, commission, stamp_tax, created_at
             FROM trades WHERE user_id = ? ORDER BY created_at DESC LIMIT ?",
        )
        .bind(user_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| TradeRecord {
                id: r.get(0),
                user_id: r.get(1),
                order_id: r.get(2),
                code: r.get(3),
                name: r.get(4),
                side: Side::parse(r.get::<String, _>(5).as_str()).unwrap_or(Side::Buy),
                price: r.get(6),
                qty: r.get(7),
                amount: r.get(8),
                commission: r.get(9),
                stamp_tax: r.get(10),
                created_at: r.get(11),
            })
            .collect())
    }

    pub async fn watchlist(&self, user_id: i64) -> Result<Vec<String>> {
        let rows =
            sqlx::query("SELECT w.code FROM watchlist w WHERE w.user_id = ? ORDER BY w.code")
                .bind(user_id)
                .fetch_all(&self.pool)
                .await?;
        Ok(rows.into_iter().map(|r| r.get(0)).collect())
    }

    pub async fn add_watch(&self, user_id: i64, code: &str) -> Result<()> {
        sqlx::query("INSERT OR IGNORE INTO watchlist (user_id, code) VALUES (?, ?)")
            .bind(user_id)
            .bind(code)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn remove_watch(&self, user_id: i64, code: &str) -> Result<()> {
        sqlx::query("DELETE FROM watchlist WHERE user_id = ? AND code = ?")
            .bind(user_id)
            .bind(code)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn record_snapshot(
        &self,
        user_id: i64,
        date: &str,
        total_asset: Money,
        market_value: Money,
        cash_available: Money,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO daily_snapshots (user_id, date, total_asset, market_value, cash_available)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(user_id, date) DO UPDATE SET
                total_asset = excluded.total_asset,
                market_value = excluded.market_value,
                cash_available = excluded.cash_available",
        )
        .bind(user_id)
        .bind(date)
        .bind(total_asset)
        .bind(market_value)
        .bind(cash_available)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_settle_date(&self, user_id: i64, date: &str) -> Result<()> {
        sqlx::query("UPDATE accounts SET last_settle = ?, updated_at = ? WHERE user_id = ?")
            .bind(date)
            .bind(crate::model::now_ms())
            .bind(user_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

fn row_to_order(r: sqlx::sqlite::SqliteRow) -> Order {
    Order {
        id: r.get(0),
        user_id: r.get(1),
        code: r.get(2),
        name: r.get(3),
        side: Side::parse(r.get::<String, _>(4).as_str()).unwrap_or(Side::Buy),
        price: r.get(5),
        qty: r.get(6),
        filled: r.get(7),
        status: parse_status(&r.get::<String, _>(8)),
        reason: r.get(9),
        seq: r.get(10),
        queue_ahead: r.get(11),
        frozen: r.get(12),
        amount_paid: r.get(13),
        created_at: r.get(14),
        updated_at: r.get(15),
    }
}

pub fn parse_status(s: &str) -> OrderStatus {
    match s {
        "pending" => OrderStatus::Pending,
        "partial" => OrderStatus::Partial,
        "filled" => OrderStatus::Filled,
        "cancelled" => OrderStatus::Cancelled,
        _ => OrderStatus::Rejected,
    }
}

/// 批量落盘。每 50ms 或攒满 512 条事件执行一次，按主键合并后单事务写入。
pub fn spawn_writer(db: Db, mut rx: mpsc::Receiver<PersistEvent>) {
    tokio::spawn(async move {
        let mut accounts: HashMap<i64, PersistEvent> = HashMap::new();
        let mut positions: HashMap<(i64, String), PersistEvent> = HashMap::new();
        let mut orders: HashMap<String, PersistEvent> = HashMap::new();
        let mut trades: HashMap<String, PersistEvent> = HashMap::new();

        loop {
            let mut stop = false;
            match rx.recv().await {
                Some(ev) => push_event(
                    &mut accounts,
                    &mut positions,
                    &mut orders,
                    &mut trades,
                    ev,
                ),
                None => stop = true,
            }
            // 收集窗口
            let deadline = tokio::time::Instant::now() + Duration::from_millis(50);
            while !stop {
                let total = accounts.len() + positions.len() + orders.len() + trades.len();
                if total >= 512 {
                    break;
                }
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(Some(ev)) => {
                        push_event(&mut accounts, &mut positions, &mut orders, &mut trades, ev)
                    }
                    Ok(None) => {
                        stop = true;
                    }
                    Err(_) => break,
                }
            }

            if !accounts.is_empty()
                || !positions.is_empty()
                || !orders.is_empty()
                || !trades.is_empty()
            {
                if let Err(e) =
                    flush(&db, &mut accounts, &mut positions, &mut orders, &mut trades).await
                {
                    tracing::error!(error = %e, "批量落盘失败");
                }
            }
            if stop {
                break;
            }
        }
        tracing::info!("落盘线程退出");
    });
}

fn push_event(
    accounts: &mut HashMap<i64, PersistEvent>,
    positions: &mut HashMap<(i64, String), PersistEvent>,
    orders: &mut HashMap<String, PersistEvent>,
    trades: &mut HashMap<String, PersistEvent>,

    ev: PersistEvent,
) {
    match &ev {
        PersistEvent::Account { user_id, .. } => {
            accounts.insert(*user_id, ev);
        }
        PersistEvent::Position { user_id, code, .. } => {
            positions.insert((*user_id, code.clone()), ev);
        }
        PersistEvent::Order(o) => {
            orders.insert(o.id.clone(), ev);
        }
        PersistEvent::Trade(t) => {
            trades.insert(t.id.clone(), ev);
        }
    }
}

async fn flush(
    db: &Db,
    accounts: &mut HashMap<i64, PersistEvent>,
    positions: &mut HashMap<(i64, String), PersistEvent>,
    orders: &mut HashMap<String, PersistEvent>,
    trades: &mut HashMap<String, PersistEvent>,
) -> Result<()> {
    let mut tx = db.pool.begin().await?;
    let now = crate::model::now_ms();

    for ev in accounts.drain().map(|(_, v)| v) {
        if let PersistEvent::Account {
            user_id,
            cash_available,
            cash_frozen,
            initial,
        } = ev
        {
            sqlx::query(
                "INSERT INTO accounts (user_id, cash_available, cash_frozen, initial, updated_at)
                 VALUES (?, ?, ?, ?, ?)
                 ON CONFLICT(user_id) DO UPDATE SET
                    cash_available = excluded.cash_available,
                    cash_frozen = excluded.cash_frozen,
                    initial = excluded.initial,
                    updated_at = excluded.updated_at",
            )
            .bind(user_id)
            .bind(cash_available)
            .bind(cash_frozen)
            .bind(initial)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }
    }

    for ev in positions.drain().map(|(_, v)| v) {
        if let PersistEvent::Position {
            user_id,
            code,
            total,
            available,
            frozen,
            cost_amount,
        } = ev
        {
            if total == 0 {
                sqlx::query("DELETE FROM positions WHERE user_id = ? AND code = ?")
                    .bind(user_id)
                    .bind(&code)
                    .execute(&mut *tx)
                    .await?;
            } else {
                sqlx::query(
                    "INSERT INTO positions (user_id, code, total, available, frozen, cost_amount, updated_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(user_id, code) DO UPDATE SET
                        total = excluded.total,
                        available = excluded.available,
                        frozen = excluded.frozen,
                        cost_amount = excluded.cost_amount,
                        updated_at = excluded.updated_at",
                )
                .bind(user_id)
                .bind(&code)
                .bind(total)
                .bind(available)
                .bind(frozen)
                .bind(cost_amount)
                .bind(now)
                .execute(&mut *tx)
                .await?;
            }
        }
    }

    for ev in orders.drain().map(|(_, v)| v) {
        if let PersistEvent::Order(o) = ev {
            sqlx::query(
                "INSERT INTO orders (id, user_id, code, name, side, price, qty, filled, status, reason, seq,
                                     queue_ahead, frozen, amount_paid, created_at, updated_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT(id) DO UPDATE SET
                    filled = excluded.filled,
                    status = excluded.status,
                    reason = excluded.reason,
                    queue_ahead = excluded.queue_ahead,
                    frozen = excluded.frozen,
                    amount_paid = excluded.amount_paid,
                    updated_at = excluded.updated_at",
            )
            .bind(&o.id)
            .bind(o.user_id)
            .bind(&o.code)
            .bind(&o.name)
            .bind(o.side.as_str())
            .bind(o.price)
            .bind(o.qty)
            .bind(o.filled)
            .bind(o.status.as_str())
            .bind(&o.reason)
            .bind(o.seq as i64)
            .bind(o.queue_ahead)
            .bind(o.frozen)
            .bind(o.amount_paid)
            .bind(o.created_at)
            .bind(o.updated_at)
            .execute(&mut *tx)
            .await?;
        }
    }

    for ev in trades.drain().map(|(_, v)| v) {
        if let PersistEvent::Trade(t) = ev {
            sqlx::query(
                "INSERT OR IGNORE INTO trades (id, order_id, user_id, code, name, side, price, qty,
                                               amount, commission, stamp_tax, created_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&t.id)
            .bind(&t.order_id)
            .bind(t.user_id)
            .bind(&t.code)
            .bind(&t.name)
            .bind(t.side.as_str())
            .bind(t.price)
            .bind(t.qty)
            .bind(t.amount)
            .bind(t.commission)
            .bind(t.stamp_tax)
            .bind(t.created_at)
            .execute(&mut *tx)
            .await?;
        }
    }

    tx.commit().await?;
    Ok(())
}
