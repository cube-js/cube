#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct V1LoadRequestQueryTimeDimension {
    /// The name of the time dimension.
    #[serde(rename = "dimension")]
    pub dimension: String,
    #[serde(rename = "granularity", skip_serializing_if = "Option::is_none")]
    pub granularity: Option<crate::models::V1TimeDimensionGranularity>,
    /// An array of dates with the following format `YYYY-MM-DD` or in `YYYY-MM-DDTHH:mm:ss.SSS` format. Values should always be local and in query `timezone`. Dates in `YYYY-MM-DD` format are also accepted. Such dates are padded to the start and end of the day if used in start and end of date range interval accordingly. If only one date is specified it's equivalent to passing two of the same dates as a date range. You can also set a relative `dateRange` with a string instead of an array e.g. `today`, `yesterday`, `tomorrow`, `last quarter`, `last year`, `next month`, `last 6 months` or `last 360 days`. > Be aware that e.g. `Last 7 days` or `Next 2 weeks` do not include > the current date. If you need the current date also you can use > `from N days ago to now` or `from now to N days from now`.
    #[serde(rename = "dateRange", skip_serializing_if = "Option::is_none")]
    pub date_range: Option<serde_json::Value>,
    /// An array of date ranges to compare a measure change over a previous period. You can use compare date range queries when you want to see, for example, how a metric performed over a period in the past and how it performs now. You can pass two or more date ranges where each of them is in the same format as a `dateRange`.
    #[serde(rename = "compareDateRange", skip_serializing_if = "Option::is_none")]
    pub compare_date_range: Option<Vec<String>>,
}

impl V1LoadRequestQueryTimeDimension {
    pub fn new(dimension: String) -> V1LoadRequestQueryTimeDimension {
        V1LoadRequestQueryTimeDimension {
            dimension,
            granularity: None,
            date_range: None,
            compare_date_range: None,
        }
    }
}
