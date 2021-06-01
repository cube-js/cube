use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
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

impl Serialize for Decimal {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        // Flexbuffers do not support i128.
        let v = self.raw_value as u128;
        ((v >> 64) as u64, v as u64).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Decimal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        type SerTuple = (u64, u64);
        let (high, low) = SerTuple::deserialize(deserializer)?;
        let v: u128 = (high as u128) << 64 | low as u128;
        Ok(Decimal {
            raw_value: v as i128,
        })
    }
}
