/// V1LoadRequestQueryFilterItem : Filters are applied differently to dimensions and measures. When you filter on a dimension, you are restricting the raw data before any calculations are made. When you filter on a measure, you are restricting the results after the measure has been calculated. Only some operators are available for measures. For dimensions, the available operators depend on the [type of the dimension](/schema/reference/types-and-formats#types). Filters can also contain `or` and `and` logical operators. > **Note:** You can not put dimensions and measures filters in the same logical > operator.

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct V1LoadRequestQueryFilterItem {
    /// The dimension or measure to be used in the filter, for example: `Stories.isDraft`.
    #[serde(rename = "member", skip_serializing_if = "Option::is_none")]
    pub member: Option<String>,
    #[serde(rename = "operator", skip_serializing_if = "Option::is_none")]
    pub operator: Option<Box<crate::models::V1FilterOperator>>,
    /// An array of values for the filter. Values must be of type `string`. If you need to pass a date, pass it as a string in `YYYY-MM-DD` format.
    #[serde(rename = "values", skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<String>>,
    #[serde(rename = "or", skip_serializing_if = "Option::is_none")]
    pub or: Option<Vec<serde_json::Value>>,
    #[serde(rename = "and", skip_serializing_if = "Option::is_none")]
    pub and: Option<Vec<serde_json::Value>>,
}

impl V1LoadRequestQueryFilterItem {
    /// Filters are applied differently to dimensions and measures. When you filter on a dimension, you are restricting the raw data before any calculations are made. When you filter on a measure, you are restricting the results after the measure has been calculated. Only some operators are available for measures. For dimensions, the available operators depend on the [type of the dimension](/schema/reference/types-and-formats#types). Filters can also contain `or` and `and` logical operators. > **Note:** You can not put dimensions and measures filters in the same logical > operator.
    pub fn new() -> V1LoadRequestQueryFilterItem {
        V1LoadRequestQueryFilterItem {
            member: None,
            operator: None,
            values: None,
            or: None,
            and: None,
        }
    }
}
