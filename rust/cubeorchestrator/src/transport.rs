use crate::query_result_transform::DBResponsePrimitive;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, fmt::Display};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ResultType {
    Default,
    Compact,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum QueryType {
    #[default]
    RegularQuery,
    CompareDateRangeQuery,
    BlendingQuery,
}

impl Display for QueryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = serde_json::to_value(self)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        write!(f, "{}", str)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MemberType {
    Measures,
    Dimensions,
    Segments,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FilterOperator {
    Equals,
    NotEquals,
    Contains,
    NotContains,
    In,
    NotIn,
    Gt,
    Gte,
    Lt,
    Lte,
    Set,
    NotSet,
    InDateRange,
    NotInDateRange,
    OnTheDate,
    BeforeDate,
    BeforeOrOnDate,
    AfterDate,
    AfterOrOnDate,
    MeasureFilter,
    EndsWith,
    NotEndsWith,
    StartsWith,
    NotStartsWith,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryFilter {
    pub member: String,
    pub operator: FilterOperator,
    pub values: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GroupingSet {
    pub group_type: String,
    pub id: u32,
    pub sub_id: Option<u32>,
}

// We can do nothing with JS functions here,
// but to keep DTOs in sync with reality, let's keep it.
pub type JsFunction = String;

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MemberExpression {
    // Made as Option and JsValueDeserializer set's it to None.
    pub expression: Option<JsFunction>,
    pub cube_name: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub definition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grouping_set: Option<GroupingSet>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ParsedMemberExpression {
    pub expression: Vec<String>,
    pub cube_name: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub definition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grouping_set: Option<GroupingSet>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryTimeDimension {
    pub dimension: String,
    pub date_range: Option<Vec<String>>,
    pub compare_date_range: Option<Vec<String>>,
    pub granularity: Option<String>,
}

pub type AliasToMemberMap = HashMap<String, String>;

pub type MembersMap = HashMap<String, String>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GranularityMeta {
    pub name: String,
    pub title: String,
    pub interval: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigItem {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub short_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub member_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drill_members: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drill_members_grouped: Option<DrillMembersGrouped>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub granularities: Option<Vec<GranularityMeta>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrillMembersGrouped {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measures: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dimensions: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnnotatedConfigItem {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub short_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub member_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drill_members: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drill_members_grouped: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub granularity: Option<GranularityMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: String,
    pub desc: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedQueryFilter {
    pub member: String,
    pub operator: FilterOperator,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<DBResponsePrimitive>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dimension: Option<String>,
}

// TODO: Not used, as all members are made as Strings for now
// XXX: Omitted function variant
#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
#[serde(untagged)]
pub enum MemberOrMemberExpression {
    Member(String),
    ParsedMemberExpression(ParsedMemberExpression),
    MemberExpression(MemberExpression),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicalAndFilter {
    pub and: Vec<QueryFilterOrLogicalFilter>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicalOrFilter {
    pub or: Vec<QueryFilterOrLogicalFilter>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum QueryFilterOrLogicalFilter {
    QueryFilter(QueryFilter),
    LogicalAndFilter(LogicalAndFilter),
    LogicalOrFilter(LogicalOrFilter),
    NormalizedQueryFilter(NormalizedQueryFilter),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measures: Option<Vec<MemberOrMemberExpression>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dimensions: Option<Vec<MemberOrMemberExpression>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_dimensions: Option<Vec<QueryTimeDimension>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segments: Option<Vec<MemberOrMemberExpression>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_query: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub renew_query: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ungrouped: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_format: Option<ResultType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<Vec<QueryFilterOrLogicalFilter>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order: Option<Vec<Order>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_type: Option<QueryType>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransformDataRequest {
    pub alias_to_member_name_map: HashMap<String, String>,
    pub annotation: HashMap<String, ConfigItem>,
    pub query: NormalizedQuery,
    pub query_type: Option<QueryType>,
    pub res_type: Option<ResultType>,
}

pub type JsRawData = Vec<HashMap<String, DBResponsePrimitive>>;
