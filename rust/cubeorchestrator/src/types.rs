use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DBResponsePrimitive {
    Null,
    Boolean(bool),
    Number(f64),
    String(String),
}

#[derive(Debug, Clone)]
pub enum DBResponseValue {
    DateTime(DateTime<Utc>),
    Primitive(DBResponsePrimitive),
    // TODO: Is this variant still used?
    Object { value: DBResponsePrimitive },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ResultType {
    #[serde(rename = "default")]
    Default,
    #[serde(rename = "compact")]
    Compact,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum QueryType {
    #[serde(rename = "regularQuery")]
    RegularQuery,
    #[serde(rename = "compareDateRangeQuery")]
    CompareDateRangeQuery,
    #[serde(rename = "blendingQuery")]
    BlendingQuery,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum MemberType {
    #[serde(rename = "measures")]
    Measures,
    #[serde(rename = "dimensions")]
    Dimensions,
    #[serde(rename = "segments")]
    Segments,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum FilterOperator {
    #[serde(rename = "equals")]
    Equals,
    #[serde(rename = "notEquals")]
    NotEquals,
    #[serde(rename = "contains")]
    Contains,
    #[serde(rename = "notContains")]
    NotContains,
    #[serde(rename = "in")]
    In,
    #[serde(rename = "notIn")]
    NotIn,
    #[serde(rename = "gt")]
    Gt,
    #[serde(rename = "gte")]
    Gte,
    #[serde(rename = "lt")]
    Lt,
    #[serde(rename = "lte")]
    Lte,
    #[serde(rename = "set")]
    Set,
    #[serde(rename = "notSet")]
    NotSet,
    #[serde(rename = "inDateRange")]
    InDateRange,
    #[serde(rename = "notInDateRange")]
    NotInDateRange,
    #[serde(rename = "onTheDate")]
    OnTheDate,
    #[serde(rename = "beforeDate")]
    BeforeDate,
    #[serde(rename = "beforeOrOnDate")]
    BeforeOrOnDate,
    #[serde(rename = "afterDate")]
    AfterDate,
    #[serde(rename = "afterOrOnDate")]
    AfterOrOnDate,
    #[serde(rename = "measureFilter")]
    MeasureFilter,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryFilter {
    pub member: String,
    pub operator: FilterOperator,
    pub values: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GroupingSet {
    pub group_type: String,
    pub id: u32,
    pub sub_id: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ParsedMemberExpression {
    pub expression: Vec<String>,
    #[serde(rename = "cubeName")]
    pub cube_name: String,
    pub name: String,
    #[serde(rename = "expressionName")]
    pub expression_name: String,
    pub definition: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "groupingSet")]
    pub grouping_set: Option<GroupingSet>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryTimeDimension {
    pub dimension: String,
    pub date_range: Option<Vec<String>>,
    pub compare_date_range: Option<Vec<String>>,
    pub granularity: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AliasToMemberMap {
    pub map: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GranularityMeta {
    pub name: String,
    pub title: String,
    pub interval: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConfigItem {
    pub title: String,
    pub short_title: String,
    pub description: String,
    #[serde(rename = "type")]
    pub member_type: String,
    pub format: String,
    pub meta: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drill_members: Option<Vec<serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drill_members_grouped: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub granularities: Option<Vec<GranularityMeta>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Order {
    pub id: String,
    pub desc: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NormalizedQueryFilter {
    pub member: String,
    pub operator: FilterOperator,
    pub values: Option<Vec<String>>,
    pub dimension: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum MemberOrMemberExpression {
    Member(String),
    MemberExpression(ParsedMemberExpression),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LogicalAndFilter {
    pub and: Vec<LogicalFilter>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LogicalOrFilter {
    pub or: Vec<LogicalFilter>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum QueryFilterOrLogicalFilter {
    QueryFilter(QueryFilter),
    LogicalAndFilter(LogicalAndFilter),
    LogicalOrFilter(LogicalOrFilter),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum LogicalFilter {
    QueryFilter(QueryFilter),
    LogicalAndFilter(LogicalAndFilter),
    LogicalOrFilter(LogicalOrFilter),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Query {
    pub measures: Vec<MemberOrMemberExpression>,
    pub dimensions: Option<Vec<MemberOrMemberExpression>>,
    pub filters: Option<Vec<LogicalFilter>>,
    #[serde(rename = "timeDimensions")]
    pub time_dimensions: Option<Vec<QueryTimeDimension>>,
    pub segments: Option<Vec<MemberOrMemberExpression>>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    pub total: Option<bool>,
    #[serde(rename = "totalQuery")]
    pub total_query: Option<bool>,
    pub order: Option<Value>,
    pub timezone: Option<String>,
    #[serde(rename = "renewQuery")]
    pub renew_query: Option<bool>,
    pub ungrouped: Option<bool>,
    #[serde(rename = "responseFormat")]
    pub response_format: Option<ResultType>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NormalizedQuery {
    pub measures: Vec<MemberOrMemberExpression>,
    pub dimensions: Option<Vec<MemberOrMemberExpression>>,
    #[serde(rename = "timeDimensions")]
    pub time_dimensions: Option<Vec<QueryTimeDimension>>,
    pub segments: Option<Vec<MemberOrMemberExpression>>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    pub total: Option<bool>,
    #[serde(rename = "totalQuery")]
    pub total_query: Option<bool>,
    pub timezone: Option<String>,
    #[serde(rename = "renewQuery")]
    pub renew_query: Option<bool>,
    pub ungrouped: Option<bool>,
    #[serde(rename = "responseFormat")]
    pub response_format: Option<ResultType>,
    pub filters: Option<Vec<NormalizedQueryFilter>>,
    #[serde(rename = "rowLimit")]
    pub row_limit: Option<u32>,
    pub order: Option<Vec<Order>>,
}
