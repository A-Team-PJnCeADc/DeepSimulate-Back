use crate::model::Money;

/// 运行配置，全部可由环境变量覆盖。
#[derive(Debug, Clone)]
pub struct Config {
    pub bind: String,
    /// live=实时轮询行情源，replay=历史 L2 回放
    pub mode: String,
    pub tdx_base: String,
    /// QuestDB HTTP 地址
    pub questdb_url: String,
    /// data_interface 代理地址（D5 归档）
    pub d5_base: String,
    pub database_url: String,
    /// 新用户初始资金（厘）
    pub initial_cash: Money,
    /// 五档轮询间隔
    pub quote_interval_ms: u64,
    /// 逐笔轮询间隔
    pub tick_interval_ms: u64,
    /// 每轮逐笔轮询的股票数（逐笔接口无批量查询，轮转拉取）
    pub tick_codes_per_cycle: usize,
    /// 逐笔列表只保留最近多少笔给前端
    pub tick_keep: usize,
    /// 行情源不可用时的重试间隔
    pub retry_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            bind: "127.0.0.1:9010".into(),
            mode: "replay".into(),
            tdx_base: "http://127.0.0.1:8080".into(),
            questdb_url: "http://127.0.0.1:9000".into(),
            d5_base: "http://127.0.0.1:8081".into(),
            database_url: "sqlite://deepsim.db".into(),
            initial_cash: 1_000_000_000,
            quote_interval_ms: 700,
            tick_interval_ms: 1_000,
            tick_codes_per_cycle: 4,
            tick_keep: 120,
            retry_ms: 3_000,
        }
    }
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

impl Config {
    pub fn from_env() -> Self {
        let d = Config::default();
        Config {
            bind: std::env::var("DEEPSIM_BIND").unwrap_or(d.bind),
            mode: std::env::var("DEEPSIM_MODE").unwrap_or(d.mode),
            tdx_base: std::env::var("DEEPSIM_TDX_BASE").unwrap_or(d.tdx_base),
            questdb_url: std::env::var("DEEPSIM_QUESTDB").unwrap_or(d.questdb_url),
            d5_base: std::env::var("DEEPSIM_D5_BASE").unwrap_or(d.d5_base),
            database_url: std::env::var("DEEPSIM_DB").unwrap_or(d.database_url),
            initial_cash: std::env::var("DEEPSIM_INITIAL_CASH")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(d.initial_cash),
            quote_interval_ms: env_u64("DEEPSIM_QUOTE_MS", d.quote_interval_ms),
            tick_interval_ms: env_u64("DEEPSIM_TICK_MS", d.tick_interval_ms),
            tick_codes_per_cycle: env_u64("DEEPSIM_TICK_BATCH", d.tick_codes_per_cycle as u64)
                as usize,
            tick_keep: env_u64("DEEPSIM_TICK_KEEP", d.tick_keep as u64) as usize,
            retry_ms: env_u64("DEEPSIM_RETRY_MS", d.retry_ms),
        }
    }
}
