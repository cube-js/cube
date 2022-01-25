#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct V1LoadRequestQueryFilterBase {
    /// The dimension or measure to be used in the filter, for example: `Stories.isDraft`.
    #[serde(rename = "member", skip_serializing_if = "Option::is_none")]
    pub member: Option<String>,
    #[serde(rename = "operator", skip_serializing_if = "Option::is_none")]
    pub operator: Option<Box<crate::models::V1FilterOperator>>,
    /// An array of values for the filter. Values must be of type `string`. If you need to pass a date, pass it as a string in `YYYY-MM-DD` format.
    #[serde(rename = "values", skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<String>>,
}

impl V1LoadRequestQueryFilterBase {
    pub fn new() -> V1LoadRequestQueryFilterBase {
        V1LoadRequestQueryFilterBase {
            member: None,
            operator: None,
            values: None,
        }
    }
}
