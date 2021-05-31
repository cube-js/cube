use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
/// This it not a general-purpose decimal implementation. We use it inside [TableValue] to cement
/// data format of decimals in CubeStore.
pub struct Decimal {
    // Users see only i64, we keep i128 so serialization does not change when we switch to i128.
    raw_value: i128,
}

impl Decimal {
    pub fn new(raw_value: i64) -> Decimal {
        Decimal {
            raw_value: raw_value as i128,
        }
    }

    pub fn raw_value(&self) -> i64 {
        self.raw_value as i64
    }

    pub fn negate(&self) -> Decimal {
        Decimal::new(-self.raw_value())
    }

    pub fn to_string(&self, scale: u8) -> String {
        let n = 10_i64.pow(scale as u32);
        let v = self.raw_value();
        let integral = v / n;
        let fractional = (v % n).abs();
        format!("{}.{}", integral, fractional)
    }
}
