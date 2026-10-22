use crate::model::{Money, Price, Qty, QuoteSnapshot, Side};

use super::fees;
use super::reject::Reject;

/// 委托前置校验。通过时返回买入需要冻结的资金（卖出为 0）。
///
/// `sellable` 是账户可卖数量（T+1 已扣除当日买入），为零即当日买入不可卖。
pub fn validate(
    side: Side,
    price: Price,
    qty: Qty,
    quote: &QuoteSnapshot,
    available_cash: Money,
    sellable: Qty,
) -> Result<Money, Reject> {
    if qty <= 0 {
        return Err(Reject::QuantityZero);
    }
    if price <= 0 || price % crate::model::PRICE_TICK != 0 {
        return Err(Reject::PriceNotTick { price });
    }
    if price < quote.limit_down || price > quote.limit_up {
        return Err(Reject::PriceOutOfLimit {
            price,
            up: quote.limit_up,
            down: quote.limit_down,
        });
    }
    match side {
        Side::Buy => {
            if qty % 100 != 0 {
                return Err(Reject::QuantityNotRoundLot { qty });
            }
            let reserve = fees::buy_reserve(price, qty);
            if reserve > available_cash {
                return Err(Reject::InsufficientFunds {
                    need: reserve,
                    available: available_cash,
                });
            }
            Ok(reserve)
        }
        Side::Sell => {
            if qty > sellable {
                return Err(Reject::InsufficientPosition {
                    need: qty,
                    available: sellable,
                });
            }
            // 零股只能一次性全部卖出
            if qty % 100 != 0 && qty != sellable {
                return Err(Reject::OddLotNotFullPosition {
                    qty,
                    available: sellable,
                });
            }
            Ok(0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Level;

    fn quote() -> QuoteSnapshot {
        QuoteSnapshot {
            code: "000001".into(),
            name: "平安银行".into(),
            last: 11_320,
            prev_close: 11_400,
            open: 11_400,
            high: 11_400,
            low: 11_400,
            total_hand: 0,
            amount: 0,
            bids: vec![Level {
                price: 11_310,
                volume: 1_000,
            }],
            asks: vec![Level {
                price: 11_320,
                volume: 1_000,
            }],
            limit_up: 12_540,
            limit_down: 10_260,
            is_st: false,
            ts: 0,
        }
    }

    #[test]
    fn t加一当日买入不可卖() {
        // 总持仓 1000 股但可卖 0（当日买入）
        let err = validate(Side::Sell, 11_310, 1_000, &quote(), 0, 0).unwrap_err();
        assert!(matches!(err, Reject::InsufficientPosition { .. }));
        assert!(err.message().contains("可卖数量不足"));
    }

    #[test]
    fn 涨跌停之外废单() {
        let q = quote();
        let up = validate(Side::Buy, q.limit_up + 10, 100, &q, i64::MAX, 0).unwrap_err();
        assert!(matches!(up, Reject::PriceOutOfLimit { .. }));
        let down = validate(Side::Sell, q.limit_down - 10, 100, &q, i64::MAX, 1_000).unwrap_err();
        assert!(matches!(down, Reject::PriceOutOfLimit { .. }));
        // 涨停价本身可以买
        assert!(validate(Side::Buy, q.limit_up, 100, &q, i64::MAX, 0).is_ok());
        // 跌停价本身可以卖
        assert!(validate(Side::Sell, q.limit_down, 100, &q, i64::MAX, 1_000).is_ok());
    }

    #[test]
    fn 买入必须整手() {
        let err = validate(Side::Buy, 11_310, 150, &quote(), i64::MAX, 0).unwrap_err();
        assert!(matches!(err, Reject::QuantityNotRoundLot { .. }));
        assert!(validate(Side::Buy, 11_310, 200, &quote(), i64::MAX, 0).is_ok());
    }

    #[test]
    fn 零股只能一次性全部卖出() {
        // 可卖 1000 股，卖 150 股不允许
        let err = validate(Side::Sell, 11_310, 150, &quote(), 0, 1_000).unwrap_err();
        assert!(matches!(err, Reject::OddLotNotFullPosition { .. }));
        // 可卖 150 股，一次卖完允许
        assert!(validate(Side::Sell, 11_310, 150, &quote(), 0, 150).is_ok());
        // 可卖 1150 股，卖 150 股仍不允许（不是全部）
        assert!(validate(Side::Sell, 11_310, 150, &quote(), 0, 1_150).is_err());
    }

    #[test]
    fn 资金不足废单() {
        // 100 股 × 11.31 元 = 1131 元，加佣金 5 元
        let need = 11_310 * 100 + 5_000;
        let ok = validate(Side::Buy, 11_310, 100, &quote(), need, 0).unwrap();
        assert_eq!(ok, need);
        let err = validate(Side::Buy, 11_310, 100, &quote(), need - 10, 0).unwrap_err();
        assert!(matches!(err, Reject::InsufficientFunds { .. }));
    }

    #[test]
    fn 价格必须是最小变动价位整数倍() {
        let err = validate(Side::Buy, 11_315, 100, &quote(), i64::MAX, 0).unwrap_err();
        assert!(matches!(err, Reject::PriceNotTick { .. }));
    }
}
