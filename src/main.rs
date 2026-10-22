mod account;
mod api;
mod ingest;
mod config;
mod db;
mod engine;
mod error;
mod market;
mod model;
mod replay;
mod service;
mod tsdb;
mod ws;

use crate::account::AccountService;
use crate::api::AppState;
use crate::config::Config;
use crate::db::{spawn_writer, Db};
use crate::engine::Engine;
use crate::ingest::{D5Client, Ingestor};
use crate::replay::Replay;
use crate::tsdb::QuestDb;
use crate::market::Market;
use crate::service::Core;
use crate::ws::WsHub;
use axum::Router;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "deepsimulate_back=info,tower_http=warn".into()),
        )
        .init();

    let cfg = Arc::new(Config::from_env());
    tracing::info!(bind = %cfg.bind, tdx = %cfg.tdx_base, "启动模拟交易服务");

    let db = Db::connect(&cfg.database_url).await?;
    let (tx, rx) = mpsc::channel(8192);
    let accounts = Arc::new(AccountService::new());
    accounts.set_event_sender(tx);
    spawn_writer(db.clone(), rx);

    let ws = Arc::new(WsHub::new());
    let market = Market::new(cfg.clone())?;
    let engine = Arc::new(Engine::new());
    let core = Arc::new(Core::new(
        cfg.clone(),
        engine,
        accounts,
        ws,
        db.clone(),
        market.clone(),
    ));

    core.recover_open_orders().await?;

    // 时序库：回放模式下必须可用；不可用时只告警，实时模式不受影响
    let qdb = QuestDb::new(&cfg.questdb_url)?;
    match qdb.ensure_schema().await {
        Ok(_) => tracing::info!(url = %cfg.questdb_url, "QuestDB 就绪"),
        Err(e) => tracing::warn!(url = %cfg.questdb_url, error = %e, "QuestDB 不可用，回放功能停用"),
    }
    let ingestor = Arc::new(Ingestor::new(D5Client::new(&cfg.d5_base)?, qdb.clone(), true));
    let replay = Arc::new(Replay::new(qdb));
    // 回放前若缺数据，自动向 D5 拉取并写入时序库
    replay.set_ingestor(ingestor.clone());

    // 代码表决定名称与 ST 涨跌幅限制，必须先就绪再对外服务
    market.load_meta_with_retry(5).await;

    let persisted = db.load_sessions().await.unwrap_or_default();
    let state = AppState::new(
        db.clone(),
        cfg.clone(),
        market.clone(),
        core.clone(),
        replay.clone(),
        ingestor.clone(),
        persisted,
    );
    let app = Router::new()
        .merge(api::handlers::router(state.clone()))
        .merge(api::ext::router(state.clone()))
        .merge(api::live::router(state.clone()))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .layer(TraceLayer::new_for_http());

    if cfg.mode == "replay" {
        tracing::info!("回放模式：不启动实时行情轮询，通过 /api/replay/start 驱动");
    } else {
        tokio::spawn(market::run(market.clone(), core.clone()));
    }
    tokio::spawn(settle_loop(core.clone()));

    let listener = TcpListener::bind(&cfg.bind).await?;
    tracing::info!("监听 {}", cfg.bind);
    axum::serve(listener, app).await?;
    Ok(())
}

/// 每分钟检查交易日水位，跨日后对已加载账户执行 T+1 解锁并记录日终快照。
async fn settle_loop(core: Arc<Core>) {
    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
        let today = model::today_cn();
        for uid in core.accounts.user_ids() {
            if core.accounts.settle_date_of(uid).as_deref() != Some(today.as_str()) {
                if let Err(e) = core.ensure_loaded(uid).await {
                    tracing::warn!(user_id = uid, error = %e, "跨日结算失败");
                }
            }
        }
    }
}
