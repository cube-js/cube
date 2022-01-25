/// V1FilterOperatorInDateRange : The operator `inDateRange` is used to filter a time dimension into a specific date range. The values must be an array of dates with the following format `YYYY-MM-DD`. If only one date is specified, the filter would be set exactly to this date. There is a convenient way to use date filters with grouping - [learn more about timeDimensions query property here](#time-dimensions-format) - Dimension types: `time`.

/// The operator `inDateRange` is used to filter a time dimension into a specific date range. The values must be an array of dates with the following format `YYYY-MM-DD`. If only one date is specified, the filter would be set exactly to this date. There is a convenient way to use date filters with grouping - [learn more about timeDimensions query property here](#time-dimensions-format) - Dimension types: `time`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum V1FilterOperatorInDateRange {
    #[serde(rename = "inDateRange")]
    InDateRange,
}

impl ToString for V1FilterOperatorInDateRange {
    fn to_string(&self) -> String {
        match self {
            Self::InDateRange => String::from("inDateRange"),
        }
    }
}

impl Default for V1FilterOperatorInDateRange {
    fn default() -> V1FilterOperatorInDateRange {
        Self::InDateRange
    }
}
