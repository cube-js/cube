use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, fmt::Display};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResultType {
    #[serde(rename = "default")]
    Default,
    #[serde(rename = "compact")]
    Compact,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub enum QueryType {
    #[serde(rename = "regularQuery")]
    #[default]
    RegularQuery,
    #[serde(rename = "compareDateRangeQuery")]
    CompareDateRangeQuery,
    #[serde(rename = "blendingQuery")]
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
pub enum MemberType {
    #[serde(rename = "measures")]
    Measures,
    #[serde(rename = "dimensions")]
    Dimensions,
    #[serde(rename = "segments")]
    Segments,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct GroupingSet {
    pub group_type: String,
    pub id: u32,
    pub sub_id: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
pub struct ConfigItem {
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub short_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "type")]
    pub member_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "drillMembers")]
    pub drill_members: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "drillMembersGrouped")]
    pub drill_members_grouped: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub granularities: Option<Vec<GranularityMeta>>,
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
    pub values: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dimension: Option<String>,
}

// TODO: Not used, as all members are made as Strings for now
// XXX: Omitted function variant
#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
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
    // pub measures: Vec<MemberOrMemberExpression>,
    pub measures: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    // pub dimensions: Option<Vec<MemberOrMemberExpression>>,
    pub dimensions: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<Vec<LogicalFilter>>,
    #[serde(rename = "timeDimensions")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_dimensions: Option<Vec<QueryTimeDimension>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    // pub segments: Option<Vec<MemberOrMemberExpression>>,
    pub segments: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "totalQuery")]
    pub total_query: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    #[serde(rename = "renewQuery")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub renew_query: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ungrouped: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "responseFormat")]
    pub response_format: Option<ResultType>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedQuery {
    // pub measures: Vec<MemberOrMemberExpression>,
    pub measures: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    // pub dimensions: Option<Vec<MemberOrMemberExpression>>,
    pub dimensions: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "timeDimensions")]
    pub time_dimensions: Option<Vec<QueryTimeDimension>>,
    // pub segments: Option<Vec<MemberOrMemberExpression>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segments: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "totalQuery")]
    pub total_query: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "renewQuery")]
    pub renew_query: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ungrouped: Option<bool>,
    #[serde(rename = "responseFormat")]
    pub response_format: Option<ResultType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<Vec<NormalizedQueryFilter>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "rowLimit")]
    pub row_limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order: Option<Vec<Order>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "queryType")]
    pub query_type: Option<QueryType>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TransformDataRequest {
    #[serde(rename = "aliasToMemberNameMap")]
    pub alias_to_member_name_map: HashMap<String, String>,
    pub annotation: HashMap<String, ConfigItem>,
    pub query: NormalizedQuery,
    #[serde(rename = "queryType")]
    pub query_type: Option<QueryType>,
    #[serde(rename = "resType")]
    pub res_type: Option<ResultType>,
}
