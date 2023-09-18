use deepsize::DeepSizeOf;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, DeepSizeOf)]
#[repr(transparent)]
///Wrapper for Int96
pub struct Int96 {
    // Users see only i64, we keep i128 so serialization does not change when we switch to i128.
    raw_value: i128,
}

impl Int96 {
    pub fn new(raw_value: i128) -> Int96 {
        Int96 { raw_value }
    }

    pub fn raw_value(&self) -> i128 {
        self.raw_value as i128
    }

    pub fn to_string(&self) -> String {
        self.raw_value.to_string()
    }
}

impl Serialize for Int96 {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        // Flexbuffers do not support i128.
        let v = self.raw_value as u128;
        ((v >> 64) as u64, v as u64).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Int96 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        type SerTuple = (u64, u64);
        let (high, low) = SerTuple::deserialize(deserializer)?;
        let v: u128 = (high as u128) << 64 | low as u128;
        Ok(Int96 {
            raw_value: v as i128,
        })
    }
}
