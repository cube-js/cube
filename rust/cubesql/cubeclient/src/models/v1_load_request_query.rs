/// V1LoadRequestQuery : A query sent to the Cube API. Cube queries are plain JSON objects, describing an analytics query. The basic elements of a query (query members) are `measures`, `dimensions`, and `segments`.

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct V1LoadRequestQuery {
    /// An array of measures. The query member format name is `<CUBE_NAME>.<MEMBER_NAME>`.
    #[serde(rename = "measures", skip_serializing_if = "Option::is_none")]
    pub measures: Option<Vec<String>>,
    /// An array of dimensions. The query member format name is `<CUBE_NAME>.<MEMBER_NAME>`. In the case of dimension of type `time`, a granularity can optionally be added to the name, in the following format: `<CUBE_NAME>.<TIME_DIMENSION_NAME>.<GRANULARITY>` e.g. `Stories.time.month`.
    #[serde(rename = "dimensions", skip_serializing_if = "Option::is_none")]
    pub dimensions: Option<Vec<String>>,
    /// An array of segments. A segment is a named filter created in a Data Schema.
    #[serde(rename = "segments", skip_serializing_if = "Option::is_none")]
    pub segments: Option<Vec<String>>,
    /// A convenient way to specify a time dimension with a filter. It is an array of objects in the [timeDimension format](/query-format#time-dimensions-format). Since grouping and filtering by a time dimension is quite a common use-case, Cube provides a convenient shortcut to pass a dimension and a filter as a `timeDimension` property.
    #[serde(rename = "timeDimensions", skip_serializing_if = "Option::is_none")]
    pub time_dimensions: Option<Vec<crate::models::V1LoadRequestQueryTimeDimension>>,
    /// If the `order` property is not specified in the query, Cube.js sorts results by default using the following rules: - The first time dimension with granularity, ascending. If no time dimension with granularity exists... - The first measure, descending. If no measure exists... - The first dimension, ascending.
    #[serde(rename = "order", skip_serializing_if = "Option::is_none")]
    pub order: Option<Vec<Vec<String>>>,
    /// A row limit for your query.
    #[serde(rename = "limit", skip_serializing_if = "Option::is_none")]
    pub limit: Option<i32>,
    /// The number of initial rows to be skipped for your query.
    #[serde(rename = "offset", skip_serializing_if = "Option::is_none")]
    pub offset: Option<i32>,
    /// A list of filters to apply to the query. Learn more about [the filters format](/query-format#filters-format). Filters are applied differently to dimensions and measures. When you filter on a dimension, you are restricting the raw data before any calculations are made. When you filter on a measure, you are restricting the results after the measure has been calculated.
    #[serde(rename = "filters", skip_serializing_if = "Option::is_none")]
    pub filters: Option<Vec<crate::models::V1LoadRequestQueryFilterItem>>,
    /// All time based calculations performed within Cube.js are timezone-aware. This property is applied to all time dimensions during aggregation and filtering. It isn't applied to the time dimension referenced in a dimensions query property unless granularity or date filter is specified. Using this property you can set your desired timezone in TZ Database Name format, e.g.: `America/Los_Angeles`.
    #[serde(rename = "timezone", skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    /// If `renewQuery` is set to `true`, Cube.js will renew all `refreshKey` for queries and query results in the foreground. However if the `refreshKey` doesn't indicate that there's a need for an update then this setting has no effect. > **NOTE**: Cube.js provides only eventual consistency guarantee. Using too > small `refreshKey` values together with `renewQuery` in order to achieve > immediate consistency can lead to endless refresh loops and > overall system instability.
    #[serde(rename = "renewQuery", skip_serializing_if = "Option::is_none")]
    pub renew_query: Option<bool>,
    /// If set to `true`, no `GROUP BY` statement will be added to the query. Instead, the raw results after filtering and joining will be returned without grouping. By default `ungrouped` queries require a primary key as a dimension of every cube involved in the query for security purposes. In case of `ungrouped` query measures will be rendered as underlying `sql` of measures without aggregation and time dimensions will be truncated as usual but not grouped by.
    #[serde(rename = "ungrouped", skip_serializing_if = "Option::is_none")]
    pub ungrouped: Option<bool>,
}

impl V1LoadRequestQuery {
    /// A query sent to the Cube API. Cube queries are plain JSON objects, describing an analytics query. The basic elements of a query (query members) are `measures`, `dimensions`, and `segments`.
    pub fn new() -> V1LoadRequestQuery {
        V1LoadRequestQuery {
            measures: None,
            dimensions: None,
            segments: None,
            time_dimensions: None,
            order: None,
            limit: None,
            offset: None,
            filters: None,
            timezone: None,
            renew_query: None,
            ungrouped: None,
        }
    }
}
