/// V1TimeDimensionGranularity : A granularity for a time dimension. If you pass `null` to the granularity, Cube.js will only perform filtering by a specified time dimension, without grouping.

/// A granularity for a time dimension. If you pass `null` to the granularity, Cube.js will only perform filtering by a specified time dimension, without grouping.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum V1TimeDimensionGranularity {
    #[serde(rename = "second")]
    Second,
    #[serde(rename = "minute")]
    Minute,
    #[serde(rename = "hour")]
    Hour,
    #[serde(rename = "day")]
    Day,
    #[serde(rename = "week")]
    Week,
    #[serde(rename = "month")]
    Month,
    #[serde(rename = "quarter")]
    Quarter,
    #[serde(rename = "year")]
    Year,
    #[serde(rename = "null")]
    Null,
}

impl ToString for V1TimeDimensionGranularity {
    fn to_string(&self) -> String {
        match self {
            Self::Second => String::from("second"),
            Self::Minute => String::from("minute"),
            Self::Hour => String::from("hour"),
            Self::Day => String::from("day"),
            Self::Week => String::from("week"),
            Self::Month => String::from("month"),
            Self::Quarter => String::from("quarter"),
            Self::Year => String::from("year"),
            Self::Null => String::from("null"),
        }
    }
}

impl Default for V1TimeDimensionGranularity {
    fn default() -> V1TimeDimensionGranularity {
        Self::Second
    }
}
