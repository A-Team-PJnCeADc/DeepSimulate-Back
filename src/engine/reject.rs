use crate::model::{Money, Price, Qty};

/// 废单原因。命中任意一条即拒绝委托，委托仍会入库并展示给用户。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reject {
    PriceNotTick {
        price: Price,
    },
    QuantityZero,
    QuantityNotRoundLot {
        qty: Qty,
    },
    OddLotNotFullPosition {
        qty: Qty,
        available: Qty,
    },
    PriceOutOfLimit {
        price: Price,
        up: Price,
        down: Price,
    },
    InsufficientFunds {
        need: Money,
        available: Money,
    },
    InsufficientPosition {
        need: Qty,
        available: Qty,
    },
}

impl Reject {
    pub fn message(&self) -> String {
        match self {
            Reject::PriceNotTick { price } => {
                format!("委托价 {} 不符合最小变动价位 0.01 元", fmt_money(*price))
            }
            Reject::QuantityZero => "委托数量必须大于 0".to_string(),
            Reject::QuantityNotRoundLot { qty } => {
                format!("买入数量必须为 100 股整数倍（当前 {qty} 股）")
            }
            Reject::OddLotNotFullPosition { qty, available } => {
                format!("零股只能一次性全部卖出（委托 {qty} 股，可卖 {available} 股）")
            }
            Reject::PriceOutOfLimit { price, up, down } => format!(
                "委托价 {} 超出涨跌幅限制区间 [{}, {}]",
                fmt_money(*price),
                fmt_money(*down),
                fmt_money(*up)
            ),
            Reject::InsufficientFunds { need, available } => format!(
                "可用资金不足：需 {}，可用 {}",
                fmt_money(*need),
                fmt_money(*available)
            ),
            Reject::InsufficientPosition { need, available } => {
                format!("可卖数量不足：需 {need} 股，可卖 {available} 股")
            }
        }
    }
}

/// 厘转元，保留两位小数
pub fn fmt_money(v: Money) -> String {
    let neg = v < 0;
    let a = v.abs();
    let yuan = a / 1000;
    let cents = (a % 1000) / 10;
    let sign = if neg { "-" } else { "" };
    format!("{sign}{yuan}.{cents:02}")
}
