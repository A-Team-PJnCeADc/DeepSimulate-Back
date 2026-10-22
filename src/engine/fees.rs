use crate::model::{Money, Price, Qty, Side};

/// 佣金费率万分之 2.5
pub const COMMISSION_NUM: Money = 25;
pub const COMMISSION_DEN: Money = 100_000;
/// 佣金最低 5 元（5000 厘）
pub const MIN_COMMISSION: Money = 5_000;
/// 印花税千分之一，仅卖出收取
pub const STAMP_NUM: Money = 1;
pub const STAMP_DEN: Money = 1_000;

/// 金额四舍五入到分（10 厘）
pub fn round_to_cent(v: Money) -> Money {
    if v <= 0 {
        0
    } else {
        ((v + 5) / 10) * 10
    }
}

/// 累计成交额对应的佣金总额，四舍五入到分，最低 5 元。零成交额不计佣金。
pub fn commission_on(cum_amount: Money) -> Money {
    if cum_amount <= 0 {
        return 0;
    }
    round_to_cent(cum_amount * COMMISSION_NUM / COMMISSION_DEN).max(MIN_COMMISSION)
}

/// 印花税：卖出成交额千分之一，四舍五入到分。
pub fn stamp_tax_on(cum_amount: Money, side: Side) -> Money {
    match side {
        Side::Sell => round_to_cent(cum_amount * STAMP_NUM / STAMP_DEN),
        Side::Buy => 0,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fees {
    pub amount: Money,
    pub commission: Money,
    pub stamp_tax: Money,
}

impl Fees {
    /// 买入总扣款 / 卖出净得款
    pub fn net(&self, side: Side) -> Money {
        match side {
            Side::Buy => self.amount + self.commission,
            Side::Sell => self.amount - self.commission - self.stamp_tax,
        }
    }
    pub fn total_fee(&self) -> Money {
        self.commission + self.stamp_tax
    }
}

/// 单笔成交费用。佣金按委托累计成交额计算后逐笔补差，因此同一委托多次部分成交
/// 只承担一次最低佣金；印花税按本次成交额线性收取。
pub fn compute_fill(
    price: Price,
    qty: Qty,
    side: Side,
    prior_amount: Money,
    prior_commission: Money,
    prior_stamp: Money,
) -> Fees {
    let amount = price * qty;
    let cum_amount = prior_amount + amount;
    let commission = (commission_on(cum_amount) - prior_commission).max(0);
    let stamp_tax = (stamp_tax_on(cum_amount, side) - prior_stamp).max(0);
    Fees {
        amount,
        commission,
        stamp_tax,
    }
}

/// 下单前的费用预估（假定一次全部成交）。
pub fn compute(price: Price, qty: Qty, side: Side) -> Fees {
    compute_fill(price, qty, side, 0, 0, 0)
}

/// 买入委托需要的冻结金额：成交金额 + 佣金。
pub fn buy_reserve(price: Price, qty: Qty) -> Money {
    price * qty + commission_on(price * qty)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn 佣金按万分之二点五且不低于五元() {
        assert_eq!(commission_on(10_000_000), 5_000);
        assert_eq!(commission_on(100_000_000), 25_000);
        assert_eq!(commission_on(1_000_000_000), 250_000);
    }

    #[test]
    fn 佣金四舍五入到分() {
        // 30000 元 → 7.5 元
        assert_eq!(commission_on(30_000_000), 7_500);
        // 44100 元 → 11.025 元 → 11.03 元
        assert_eq!(commission_on(44_100_000), 11_030);
        // 3999 元 → 0.99975 元 → 低于最低佣金
        assert_eq!(commission_on(3_999_000), 5_000);
    }

    #[test]
    fn 印花税仅卖出收取() {
        assert_eq!(stamp_tax_on(10_000_000, Side::Buy), 0);
        assert_eq!(stamp_tax_on(10_000_000, Side::Sell), 10_000);
    }

    #[test]
    fn 买入卖出净额方向相反() {
        let f = compute(10_000, 1000, Side::Buy);
        assert_eq!(f.amount, 10_000_000);
        assert_eq!(f.commission, 5_000);
        assert_eq!(f.stamp_tax, 0);
        assert_eq!(f.net(Side::Buy), 10_005_000);

        let s = compute(10_000, 1000, Side::Sell);
        assert_eq!(s.net(Side::Sell), 9_985_000);
        assert_eq!(s.total_fee(), 15_000);
    }

    #[test]
    fn 部分成交累计只收一次最低佣金() {
        let big = compute(10_000, 10_000, Side::Sell);
        assert_eq!(big.commission, 25_000);
        // 分十次各 1000 股成交，佣金合计仍为 25 元
        let mut prior_amount = 0;
        let mut prior_commission = 0;
        let mut prior_stamp = 0;
        let mut total = 0;
        for _ in 0..10 {
            let f = compute_fill(
                10_000,
                1_000,
                Side::Sell,
                prior_amount,
                prior_commission,
                prior_stamp,
            );
            total += f.commission;
            prior_amount += f.amount;
            prior_commission += f.commission;
            prior_stamp += f.stamp_tax;
        }
        assert_eq!(total, 25_000);
    }

    #[test]
    fn 冻结金额覆盖成交金额与佣金() {
        assert_eq!(buy_reserve(10_000, 1000), 10_005_000);
        assert_eq!(buy_reserve(10_000, 100), 1_005_000);
    }
}
