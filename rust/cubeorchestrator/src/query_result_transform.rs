use crate::{
    query_message_parser::QueryResult,
    transport::{
        AnnotatedConfigItem, ConfigItem, MemberOrMemberExpression, MembersMap, NormalizedQuery,
        QueryTimeDimension, QueryType, ResultType, TransformDataRequest,
    },
};
use anyhow::{bail, Context, Result};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use itertools::multizip;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    sync::Arc,
};

pub const COMPARE_DATE_RANGE_FIELD: &str = "compareDateRange";
pub const COMPARE_DATE_RANGE_SEPARATOR: &str = " - ";
pub const BLENDING_QUERY_KEY_PREFIX: &str = "time.";
pub const BLENDING_QUERY_RES_SEPARATOR: &str = ".";
pub const MEMBER_SEPARATOR: &str = ".";

/// Transform specified `value` with specified `type` to the network protocol type.
pub fn transform_value(value: DBResponseValue, type_: &str) -> DBResponsePrimitive {
    match value {
        DBResponseValue::DateTime(dt) if type_ == "time" || type_.is_empty() => {
            DBResponsePrimitive::String(
                dt.with_timezone(&Utc)
                    .format("%Y-%m-%dT%H:%M:%S%.3f")
                    .to_string(),
            )
        }
        DBResponseValue::Primitive(DBResponsePrimitive::String(ref s)) if type_ == "time" => {
            let formatted = DateTime::parse_from_rfc3339(s)
                .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S%.3f").to_string())
                .or_else(|_| {
                    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.3f").map(|dt| {
                        Utc.from_utc_datetime(&dt)
                            .format("%Y-%m-%dT%H:%M:%S%.3f")
                            .to_string()
                    })
                })
                .or_else(|_| {
                    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").map(|dt| {
                        Utc.from_utc_datetime(&dt)
                            .format("%Y-%m-%dT%H:%M:%S%.3f")
                            .to_string()
                    })
                })
                .or_else(|_| {
                    NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").map(|dt| {
                        Utc.from_utc_datetime(&dt)
                            .format("%Y-%m-%dT%H:%M:%S%.3f")
                            .to_string()
                    })
                })
                .or_else(|_| {
                    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.3f %Z").map(|dt| {
                        Utc.from_utc_datetime(&dt)
                            .format("%Y-%m-%dT%H:%M:%S%.3f")
                            .to_string()
                    })
                })
                .or_else(|_| {
                    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.3f %:z").map(|dt| {
                        Utc.from_utc_datetime(&dt)
                            .format("%Y-%m-%dT%H:%M:%S%.3f")
                            .to_string()
                    })
                })
                .unwrap_or_else(|_| s.clone());
            DBResponsePrimitive::String(formatted)
        }
        DBResponseValue::Primitive(p) => p,
        DBResponseValue::Object { value } => value,
        _ => DBResponsePrimitive::Null,
    }
}

/// Parse date range value from time dimension.
pub fn get_date_range_value(
    time_dimensions: Option<&Vec<QueryTimeDimension>>,
) -> Result<DBResponsePrimitive> {
    let time_dimensions = match time_dimensions {
        Some(time_dimensions) => time_dimensions,
        None => bail!("QueryTimeDimension should be specified for the compare date range query."),
    };

    let dim = match time_dimensions.first() {
        Some(dim) => dim,
        None => bail!("No time dimension provided."),
    };

    let date_range: &Vec<String> = match &dim.date_range {
        Some(date_range) => date_range,
        None => bail!("Inconsistent QueryTimeDimension configuration: dateRange required."),
    };

    if date_range.len() == 1 {
        bail!(
            "Inconsistent dateRange configuration for the compare date range query: {}",
            date_range[0]
        );
    }

    Ok(DBResponsePrimitive::String(
        date_range.join(COMPARE_DATE_RANGE_SEPARATOR),
    ))
}

/// Parse blending query key from time dimension for query.
pub fn get_blending_query_key(time_dimensions: Option<&Vec<QueryTimeDimension>>) -> Result<String> {
    let dim = time_dimensions
        .and_then(|dims| dims.first().cloned())
        .context("QueryTimeDimension should be specified for the blending query.")?;

    let granularity = dim
        .granularity.clone()
        .context(format!(
            "Inconsistent QueryTimeDimension configuration for the blending query, granularity required: {:?}",
            dim
        ))?;

    Ok(format!("{}{}", BLENDING_QUERY_KEY_PREFIX, granularity))
}

/// Parse blending query key from time dimension for response.
pub fn get_blending_response_key(
    time_dimensions: Option<&Vec<QueryTimeDimension>>,
) -> Result<String> {
    let dim = time_dimensions
        .and_then(|dims| dims.first().cloned())
        .context("QueryTimeDimension should be specified for the blending query.")?;

    let granularity = dim
        .granularity.clone()
        .context(format!(
            "Inconsistent QueryTimeDimension configuration for the blending query, granularity required: {:?}",
            dim
        ))?;

    let dimension = dim.dimension.clone();

    Ok(format!(
        "{}{}{}",
        dimension, BLENDING_QUERY_RES_SEPARATOR, granularity
    ))
}

/// Parse member names from request/response.
pub fn get_members(
    query_type: &QueryType,
    query: &NormalizedQuery,
    db_data: &QueryResult,
    alias_to_member_name_map: &HashMap<String, String>,
    annotation: &HashMap<String, ConfigItem>,
) -> Result<(MembersMap, Vec<String>)> {
    let mut members_map: MembersMap = HashMap::new();
    // Hashmaps don't guarantee the order of the elements while iterating
    // this fires in get_compact_row because members map doesn't hold the members for
    // date range queries, which are added later and thus columns in final recordset are not
    // in sync with the order of members in members list.
    let mut members_arr: Vec<String> = vec![];

    if db_data.columns.is_empty() {
        return Ok((members_map, members_arr));
    }

    for column in db_data.columns.iter() {
        let member_name = alias_to_member_name_map
            .get(column)
            .context(format!("Member name not found for alias: '{}'", column))?;

        if !annotation.contains_key(member_name) {
            bail!(
                concat!(
                    "You requested hidden member: '{}'. Please make it visible using `shown: true`. ",
                    "Please note primaryKey fields are `shown: false` by default: ",
                    "https://cube.dev/docs/schema/reference/joins#setting-a-primary-key."
                ),
                column
            );
        }

        members_map.insert(member_name.clone(), column.clone());
        members_arr.push(member_name.clone());

        let path = member_name.split(MEMBER_SEPARATOR).collect::<Vec<&str>>();
        let calc_member = format!("{}{}{}", path[0], MEMBER_SEPARATOR, path[1]);

        if path.len() == 3
            && query.dimensions.as_ref().map_or(true, |dims| {
                !dims
                    .iter()
                    .any(|dim| *dim == MemberOrMemberExpression::Member(calc_member.clone()))
            })
        {
            members_map.insert(calc_member.clone(), column.clone());
            members_arr.push(calc_member);
        }
    }

    match query_type {
        QueryType::CompareDateRangeQuery => {
            members_map.insert(
                COMPARE_DATE_RANGE_FIELD.to_string(),
                QueryType::CompareDateRangeQuery.to_string(),
            );
            members_arr.push(COMPARE_DATE_RANGE_FIELD.to_string());
        }
        QueryType::BlendingQuery => {
            let blending_key = get_blending_query_key(query.time_dimensions.as_ref())
                .context("Failed to generate blending query key")?;
            if let Some(dim) = query
                .time_dimensions
                .as_ref()
                .and_then(|dims| dims.first().cloned())
            {
                let val = members_map.get(&dim.dimension).unwrap();
                members_map.insert(blending_key.clone(), val.clone());
                members_arr.push(blending_key);
            }
        }
        _ => {}
    }

    Ok((members_map, members_arr))
}

/// Convert DB response object to the compact output format.
pub fn get_compact_row(
    members_to_alias_map: &HashMap<String, String>,
    annotation: &HashMap<String, ConfigItem>,
    query_type: &QueryType,
    members: &[String],
    time_dimensions: Option<&Vec<QueryTimeDimension>>,
    db_row: &[DBResponseValue],
    columns_pos: &HashMap<String, usize>,
) -> Result<Vec<DBResponsePrimitive>> {
    let mut row: Vec<DBResponsePrimitive> = Vec::with_capacity(members.len());

    for m in members {
        if let Some(annotation_item) = annotation.get(m) {
            if let Some(alias) = members_to_alias_map.get(m) {
                if let Some(key) = columns_pos.get(alias) {
                    if let Some(value) = db_row.get(*key) {
                        let mtype = annotation_item.member_type.as_deref().unwrap_or("");
                        row.push(transform_value(value.clone(), mtype));
                    }
                }
            }
        }
    }

    match query_type {
        QueryType::CompareDateRangeQuery => {
            row.push(get_date_range_value(time_dimensions)?);
        }
        QueryType::BlendingQuery => {
            let blending_key = get_blending_response_key(time_dimensions)?;

            if let Some(alias) = members_to_alias_map.get(&blending_key) {
                if let Some(key) = columns_pos.get(alias) {
                    if let Some(value) = db_row.get(*key) {
                        let member_type = annotation.get(alias).map_or("", |annotation_item| {
                            annotation_item.member_type.as_deref().unwrap_or("")
                        });

                        row.push(transform_value(value.clone(), member_type));
                    }
                }
            }
        }
        _ => {}
    }

    Ok(row)
}

/// Convert DB response object to the vanilla output format.
pub fn get_vanilla_row(
    alias_to_member_name_map: &HashMap<String, String>,
    annotation: &HashMap<String, ConfigItem>,
    query_type: &QueryType,
    query: &NormalizedQuery,
    db_row: &[DBResponseValue],
    columns_pos: &HashMap<String, usize>,
) -> Result<HashMap<String, DBResponsePrimitive>> {
    let mut row = HashMap::new();

    for (alias, &index) in columns_pos {
        if let Some(value) = db_row.get(index) {
            let member_name = match alias_to_member_name_map.get(alias) {
                Some(m) => m,
                None => {
                    bail!("Missing member name for alias: {}", alias);
                }
            };

            let annotation_for_member = match annotation.get(member_name) {
                Some(am) => am,
                None => {
                    bail!(
                    concat!(
                        "You requested hidden member: '{}'. Please make it visible using `shown: true`. ",
                        "Please note primaryKey fields are `shown: false` by default: ",
                        "https://cube.dev/docs/schema/reference/joins#setting-a-primary-key."
                    ),
                    alias
                )
                }
            };

            let transformed_value = transform_value(
                value.clone(),
                annotation_for_member
                    .member_type
                    .as_ref()
                    .unwrap_or(&"".to_string()),
            );

            row.insert(member_name.clone(), transformed_value.clone());

            // Handle deprecated time dimensions without granularity
            let path: Vec<&str> = member_name.split(MEMBER_SEPARATOR).collect();
            let member_name_without_granularity =
                format!("{}{}{}", path[0], MEMBER_SEPARATOR, path[1]);
            if path.len() == 3
                && query.dimensions.as_ref().map_or(true, |dims| {
                    !dims.iter().any(|dim| {
                        *dim == MemberOrMemberExpression::Member(
                            member_name_without_granularity.clone(),
                        )
                    })
                })
            {
                row.insert(member_name_without_granularity, transformed_value);
            }
        }
    }

    match query_type {
        QueryType::CompareDateRangeQuery => {
            let date_range_value = get_date_range_value(query.time_dimensions.as_ref())?;
            row.insert("compareDateRange".to_string(), date_range_value);
        }
        QueryType::BlendingQuery => {
            let blending_key = get_blending_query_key(query.time_dimensions.as_ref())?;
            let response_key = get_blending_response_key(query.time_dimensions.as_ref())?;

            if let Some(value) = row.get(&response_key) {
                row.insert(blending_key, value.clone());
            }
        }
        _ => {}
    }

    Ok(row)
}

/// Helper to get a list if unique granularities from normalized queries
pub fn get_query_granularities(queries: &[&NormalizedQuery]) -> Vec<String> {
    queries
        .iter()
        .filter_map(|query| {
            query
                .time_dimensions
                .as_ref()
                .and_then(|tds| tds.first())
                .and_then(|td| td.granularity.clone())
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect()
}

/// Get Pivot Query for a list of queries
pub fn get_pivot_query(
    query_type: &QueryType,
    queries: &Vec<&NormalizedQuery>,
) -> Result<NormalizedQuery> {
    let mut pivot_query = queries
        .first()
        .copied()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Queries list cannot be empty"))?;

    match query_type {
        QueryType::BlendingQuery => {
            // Merge and deduplicate measures and dimensions across all queries
            let mut merged_measures = HashSet::new();
            let mut merged_dimensions = HashSet::new();

            for query in queries {
                if let Some(measures) = &query.measures {
                    merged_measures.extend(measures.iter().cloned());
                }
                if let Some(dimensions) = &query.dimensions {
                    merged_dimensions.extend(dimensions.iter().cloned());
                }
            }

            pivot_query.measures = if !merged_measures.is_empty() {
                Some(merged_measures.into_iter().collect())
            } else {
                None
            };
            pivot_query.dimensions = if !merged_dimensions.is_empty() {
                Some(merged_dimensions.into_iter().collect())
            } else {
                None
            };

            // Add time dimensions
            let granularities = get_query_granularities(queries);
            if !granularities.is_empty() {
                pivot_query.time_dimensions = Some(vec![QueryTimeDimension {
                    dimension: "time".to_string(),
                    date_range: None,
                    compare_date_range: None,
                    granularity: granularities.first().cloned(),
                }]);
            }
        }
        QueryType::CompareDateRangeQuery => {
            let mut dimensions = vec![MemberOrMemberExpression::Member(
                "compareDateRange".to_string(),
            )];
            if let Some(dims) = pivot_query.dimensions {
                dimensions.extend(dims.clone());
            }
            pivot_query.dimensions = Option::from(dimensions);
        }
        _ => {}
    }

    pivot_query.query_type = Option::from(query_type.clone());

    Ok(pivot_query)
}

pub fn get_final_cubestore_result_array(
    transform_requests: &[TransformDataRequest],
    cube_store_results: &[Arc<QueryResult>],
    result_data: &mut [RequestResultData],
) -> Result<()> {
    for (transform_data, cube_store_result, result) in multizip((
        transform_requests.iter(),
        cube_store_results.iter(),
        result_data.iter_mut(),
    )) {
        result.prepare_results(transform_data, cube_store_result)?;
    }

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum TransformedData {
    Compact {
        members: Vec<String>,
        dataset: Vec<Vec<DBResponsePrimitive>>,
    },
    Vanilla(Vec<HashMap<String, DBResponsePrimitive>>),
}

impl TransformedData {
    /// Transforms queried data array to the output format.
    pub fn transform(
        request_data: &TransformDataRequest,
        cube_store_result: &QueryResult,
    ) -> Result<Self> {
        let alias_to_member_name_map = &request_data.alias_to_member_name_map;
        let annotation = &request_data.annotation;
        let query = &request_data.query;
        let query_type = &request_data.query_type.clone().unwrap_or_default();
        let res_type = request_data.res_type.clone();

        let (members_to_alias_map, members) = get_members(
            query_type,
            query,
            cube_store_result,
            alias_to_member_name_map,
            annotation,
        )?;

        match res_type {
            Some(ResultType::Compact) => {
                let dataset: Vec<_> = cube_store_result
                    .rows
                    .iter()
                    .map(|row| {
                        get_compact_row(
                            &members_to_alias_map,
                            annotation,
                            query_type,
                            &members,
                            query.time_dimensions.as_ref(),
                            row,
                            &cube_store_result.columns_pos,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(TransformedData::Compact { members, dataset })
            }
            _ => {
                let dataset: Vec<_> = cube_store_result
                    .rows
                    .iter()
                    .map(|row| {
                        get_vanilla_row(
                            alias_to_member_name_map,
                            annotation,
                            query_type,
                            query,
                            row,
                            &cube_store_result.columns_pos,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(TransformedData::Vanilla(dataset))
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestResultDataMulti {
    pub query_type: QueryType,
    pub results: Vec<RequestResultData>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pivot_query: Option<NormalizedQuery>,
    pub slow_query: bool,
}

impl RequestResultDataMulti {
    /// Processes multiple results and populates the final `RequestResultDataMulti` structure
    /// which is sent to the client.
    pub fn prepare_results(
        &mut self,
        request_data: &[TransformDataRequest],
        cube_store_result: &[Arc<QueryResult>],
    ) -> Result<()> {
        for (transform_data, cube_store_result, result) in multizip((
            request_data.iter(),
            cube_store_result.iter(),
            self.results.iter_mut(),
        )) {
            result.prepare_results(transform_data, cube_store_result)?;
        }

        let normalized_queries = self
            .results
            .iter()
            .map(|result| &result.query)
            .collect::<Vec<_>>();

        self.pivot_query = Some(get_pivot_query(&self.query_type, &normalized_queries)?);

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestResultData {
    pub query: NormalizedQuery,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_refresh_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_key_values: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub used_pre_aggregations: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transformed_query: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    pub annotation: HashMap<String, HashMap<String, AnnotatedConfigItem>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub db_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ext_db_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external: Option<bool>,
    pub slow_query: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<TransformedData>,
}

impl RequestResultData {
    /// Populates the `RequestResultData` structure with the transformed Query result.
    pub fn prepare_results(
        &mut self,
        request_data: &TransformDataRequest,
        cube_store_result: &QueryResult,
    ) -> Result<()> {
        let transformed = TransformedData::transform(request_data, cube_store_result)?;
        self.data = Some(transformed);

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestResultArray {
    pub results: Vec<RequestResultData>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum DBResponsePrimitive {
    Null,
    Boolean(bool),
    Number(f64),
    String(String),
    Uncommon(Value),
}

impl Display for DBResponsePrimitive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            DBResponsePrimitive::Null => "null".to_string(),
            DBResponsePrimitive::Boolean(b) => b.to_string(),
            DBResponsePrimitive::Number(n) => n.to_string(),
            DBResponsePrimitive::String(s) => s.clone(),
            DBResponsePrimitive::Uncommon(v) => {
                serde_json::to_string(&v).unwrap_or_else(|_| v.to_string())
            }
        };
        write!(f, "{}", str)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub enum DBResponseValue {
    DateTime(DateTime<Utc>),
    Primitive(DBResponsePrimitive),
    // TODO: Is this variant still used?
    Object { value: DBResponsePrimitive },
}

impl Display for DBResponseValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            DBResponseValue::DateTime(dt) => dt.to_rfc3339(),
            DBResponseValue::Primitive(p) => p.to_string(),
            DBResponseValue::Object { value } => value.to_string(),
        };
        write!(f, "{}", str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::JsRawData;
    use anyhow::Result;
    use chrono::{TimeZone, Timelike, Utc};
    use serde_json::from_str;
    use std::{fmt, sync::LazyLock};

    type TestSuiteData = HashMap<String, TestData>;

    #[derive(Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct TestData {
        request: TransformDataRequest,
        query_result: JsRawData,
        final_result_default: Option<TransformedData>,
        final_result_compact: Option<TransformedData>,
    }

    const TEST_SUITE_JSON: &str = r#"
{
  "regular_discount_by_city": {
    "request": {
      "aliasToMemberNameMap": {
        "e_commerce_records_us2021__avg_discount": "ECommerceRecordsUs2021.avg_discount",
        "e_commerce_records_us2021__city": "ECommerceRecordsUs2021.city"
      },
      "annotation": {
        "ECommerceRecordsUs2021.avg_discount": {
          "title": "E Commerce Records Us2021 Avg Discount",
          "shortTitle": "Avg Discount",
          "type": "number",
          "drillMembers": [],
          "drillMembersGrouped": {
            "measures": [],
            "dimensions": []
          }
        },
        "ECommerceRecordsUs2021.city": {
          "title": "E Commerce Records Us2021 City",
          "shortTitle": "City",
          "type": "string"
        }
      },
      "query": {
        "dimensions": [
          "ECommerceRecordsUs2021.city"
        ],
        "measures": [
          "ECommerceRecordsUs2021.avg_discount"
        ],
        "limit": 2,
        "rowLimit": 2,
        "timezone": "UTC",
        "order": [],
        "filters": [],
        "timeDimensions": []
      },
      "queryType": "regularQuery"
    },
    "queryResult": [
      {
        "e_commerce_records_us2021__city": "Missouri City",
        "e_commerce_records_us2021__avg_discount": "0.80000000000000000000"
      },
      {
        "e_commerce_records_us2021__city": "Abilene",
        "e_commerce_records_us2021__avg_discount": "0.80000000000000000000"
      }
    ],
    "finalResultDefault": [
      {
        "ECommerceRecordsUs2021.city": "Missouri City",
        "ECommerceRecordsUs2021.avg_discount": "0.80000000000000000000"
      },
      {
        "ECommerceRecordsUs2021.city": "Abilene",
        "ECommerceRecordsUs2021.avg_discount": "0.80000000000000000000"
      }
    ],
    "finalResultCompact": {
      "members": [
        "ECommerceRecordsUs2021.city",
        "ECommerceRecordsUs2021.avg_discount"
      ],
      "dataset": [
        [
          "Missouri City",
          "0.80000000000000000000"
        ],
        [
          "Abilene",
          "0.80000000000000000000"
        ]
      ]
    }
  },
  "regular_profit_by_postal_code": {
    "request": {
      "aliasToMemberNameMap": {
        "e_commerce_records_us2021__avg_profit": "ECommerceRecordsUs2021.avg_profit",
        "e_commerce_records_us2021__postal_code": "ECommerceRecordsUs2021.postalCode"
      },
      "annotation": {
        "ECommerceRecordsUs2021.avg_profit": {
          "title": "E Commerce Records Us2021 Avg Profit",
          "shortTitle": "Avg Profit",
          "type": "number",
          "drillMembers": [],
          "drillMembersGrouped": {
            "measures": [],
            "dimensions": []
          }
        },
        "ECommerceRecordsUs2021.postalCode": {
          "title": "E Commerce Records Us2021 Postal Code",
          "shortTitle": "Postal Code",
          "type": "string"
        }
      },
      "query": {
        "dimensions": [
          "ECommerceRecordsUs2021.postalCode"
        ],
        "measures": [
          "ECommerceRecordsUs2021.avg_profit"
        ],
        "limit": 2,
        "rowLimit": 2,
        "timezone": "UTC",
        "order": [],
        "filters": [],
        "timeDimensions": []
      },
      "queryType": "regularQuery"
    },
    "queryResult": [
      {
        "e_commerce_records_us2021__postal_code": "95823",
        "e_commerce_records_us2021__avg_profit": "646.1258666666666667"
      },
      {
        "e_commerce_records_us2021__postal_code": "64055",
        "e_commerce_records_us2021__avg_profit": "487.8315000000000000"
      }
    ],
    "finalResultDefault": [
      {
        "ECommerceRecordsUs2021.postalCode": "95823",
        "ECommerceRecordsUs2021.avg_profit": "646.1258666666666667"
      },
      {
        "ECommerceRecordsUs2021.postalCode": "64055",
        "ECommerceRecordsUs2021.avg_profit": "487.8315000000000000"
      }
    ],
    "finalResultCompact": {
      "members": [
        "ECommerceRecordsUs2021.postalCode",
        "ECommerceRecordsUs2021.avg_profit"
      ],
      "dataset": [
        [
          "95823",
          "646.1258666666666667"
        ],
        [
          "64055",
          "487.8315000000000000"
        ]
      ]
    }
  },
  "compare_date_range_count_by_order_date": {
    "request": {
      "aliasToMemberNameMap": {
        "e_commerce_records_us2021__count": "ECommerceRecordsUs2021.count",
        "e_commerce_records_us2021__order_date_day": "ECommerceRecordsUs2021.orderDate.day"
      },
      "annotation": {
        "ECommerceRecordsUs2021.count": {
          "title": "E Commerce Records Us2021 Count",
          "shortTitle": "Count",
          "type": "number",
          "drillMembers": [
            "ECommerceRecordsUs2021.city",
            "ECommerceRecordsUs2021.country",
            "ECommerceRecordsUs2021.customerId",
            "ECommerceRecordsUs2021.orderId",
            "ECommerceRecordsUs2021.productId",
            "ECommerceRecordsUs2021.productName",
            "ECommerceRecordsUs2021.orderDate"
          ],
          "drillMembersGrouped": {
            "measures": [],
            "dimensions": [
              "ECommerceRecordsUs2021.city",
              "ECommerceRecordsUs2021.country",
              "ECommerceRecordsUs2021.customerId",
              "ECommerceRecordsUs2021.orderId",
              "ECommerceRecordsUs2021.productId",
              "ECommerceRecordsUs2021.productName",
              "ECommerceRecordsUs2021.orderDate"
            ]
          }
        },
        "ECommerceRecordsUs2021.orderDate.day": {
          "title": "E Commerce Records Us2021 Order Date",
          "shortTitle": "Order Date",
          "type": "time"
        },
        "ECommerceRecordsUs2021.orderDate": {
          "title": "E Commerce Records Us2021 Order Date",
          "shortTitle": "Order Date",
          "type": "time"
        }
      },
      "query": {
        "measures": [
          "ECommerceRecordsUs2021.count"
        ],
        "timeDimensions": [
          {
            "dimension": "ECommerceRecordsUs2021.orderDate",
            "granularity": "day",
            "dateRange": [
              "2020-01-01T00:00:00.000",
              "2020-01-31T23:59:59.999"
            ]
          }
        ],
        "limit": 2,
        "rowLimit": 2,
        "timezone": "UTC",
        "order": [],
        "filters": [],
        "dimensions": []
      },
      "queryType": "compareDateRangeQuery"
    },
    "queryResult": [
      {
        "e_commerce_records_us2021__order_date_day": "2020-01-01T00:00:00.000",
        "e_commerce_records_us2021__count": "10"
      },
      {
        "e_commerce_records_us2021__order_date_day": null,
        "e_commerce_records_us2021__count": null
      }
    ],
    "finalResultDefault": [
      {
        "ECommerceRecordsUs2021.orderDate.day": "2020-01-01T00:00:00.000",
        "ECommerceRecordsUs2021.orderDate": "2020-01-01T00:00:00.000",
        "ECommerceRecordsUs2021.count": "10",
        "compareDateRange": "2020-01-01T00:00:00.000 - 2020-01-31T23:59:59.999"
      },
      {
        "ECommerceRecordsUs2021.orderDate.day": null,
        "ECommerceRecordsUs2021.orderDate": null,
        "ECommerceRecordsUs2021.count": null,
        "compareDateRange": null
      }
    ],
    "finalResultCompact": {
      "members": [
        "ECommerceRecordsUs2021.orderDate.day",
        "ECommerceRecordsUs2021.orderDate",
        "ECommerceRecordsUs2021.count",
        "compareDateRange"
      ],
      "dataset": [
        [
          "2020-01-01T00:00:00.000",
          "2020-01-01T00:00:00.000",
          "10",
          "2020-01-01T00:00:00.000 - 2020-01-31T23:59:59.999"
        ],
        [
          null,
          null,
          null,
          "2020-01-01T00:00:00.000 - 2020-01-31T23:59:59.999"
        ]
      ]
    }
  },
  "compare_date_range_count_by_order_date2": {
    "request": {
      "aliasToMemberNameMap": {
        "e_commerce_records_us2021__count": "ECommerceRecordsUs2021.count",
        "e_commerce_records_us2021__order_date_day": "ECommerceRecordsUs2021.orderDate.day"
      },
      "annotation": {
        "ECommerceRecordsUs2021.count": {
          "title": "E Commerce Records Us2021 Count",
          "shortTitle": "Count",
          "type": "number",
          "drillMembers": [
            "ECommerceRecordsUs2021.city",
            "ECommerceRecordsUs2021.country",
            "ECommerceRecordsUs2021.customerId",
            "ECommerceRecordsUs2021.orderId",
            "ECommerceRecordsUs2021.productId",
            "ECommerceRecordsUs2021.productName",
            "ECommerceRecordsUs2021.orderDate"
          ],
          "drillMembersGrouped": {
            "measures": [],
            "dimensions": [
              "ECommerceRecordsUs2021.city",
              "ECommerceRecordsUs2021.country",
              "ECommerceRecordsUs2021.customerId",
              "ECommerceRecordsUs2021.orderId",
              "ECommerceRecordsUs2021.productId",
              "ECommerceRecordsUs2021.productName",
              "ECommerceRecordsUs2021.orderDate"
            ]
          }
        },
        "ECommerceRecordsUs2021.orderDate.day": {
          "title": "E Commerce Records Us2021 Order Date",
          "shortTitle": "Order Date",
          "type": "time"
        },
        "ECommerceRecordsUs2021.orderDate": {
          "title": "E Commerce Records Us2021 Order Date",
          "shortTitle": "Order Date",
          "type": "time"
        }
      },
      "query": {
        "measures": [
          "ECommerceRecordsUs2021.count"
        ],
        "timeDimensions": [
          {
            "dimension": "ECommerceRecordsUs2021.orderDate",
            "granularity": "day",
            "dateRange": [
              "2020-03-01T00:00:00.000",
              "2020-03-31T23:59:59.999"
            ]
          }
        ],
        "limit": 2,
        "rowLimit": 2,
        "timezone": "UTC",
        "order": [],
        "filters": [],
        "dimensions": []
      },
      "queryType": "compareDateRangeQuery"
    },
    "queryResult": [
      {
        "e_commerce_records_us2021__order_date_day": "2020-03-02T00:00:00.000",
        "e_commerce_records_us2021__count": "11"
      },
      {
        "e_commerce_records_us2021__order_date_day": "2020-03-03T00:00:00.000",
        "e_commerce_records_us2021__count": "7"
      }
    ],
    "finalResultDefault": [
      {
        "ECommerceRecordsUs2021.orderDate.day": "2020-03-02T00:00:00.000",
        "ECommerceRecordsUs2021.orderDate": "2020-03-02T00:00:00.000",
        "ECommerceRecordsUs2021.count": "11",
        "compareDateRange": "2020-03-01T00:00:00.000 - 2020-03-31T23:59:59.999"
      },
      {
        "ECommerceRecordsUs2021.orderDate.day": "2020-03-03T00:00:00.000",
        "ECommerceRecordsUs2021.orderDate": "2020-03-03T00:00:00.000",
        "ECommerceRecordsUs2021.count": "7",
        "compareDateRange": "2020-03-01T00:00:00.000 - 2020-03-31T23:59:59.999"
      }
    ],
    "finalResultCompact": {
      "members": [
        "ECommerceRecordsUs2021.orderDate.day",
        "ECommerceRecordsUs2021.orderDate",
        "ECommerceRecordsUs2021.count",
        "compareDateRange"
      ],
      "dataset": [
        [
          "2020-03-02T00:00:00.000",
          "2020-03-02T00:00:00.000",
          "11",
          "2020-03-01T00:00:00.000 - 2020-03-31T23:59:59.999"
        ],
        [
          "2020-03-03T00:00:00.000",
          "2020-03-03T00:00:00.000",
          "7",
          "2020-03-01T00:00:00.000 - 2020-03-31T23:59:59.999"
        ]
      ]
    }
  },
  "blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode": {
    "request": {
      "aliasToMemberNameMap": {
        "e_commerce_records_us2021__avg_discount": "ECommerceRecordsUs2021.avg_discount",
        "e_commerce_records_us2021__order_date_month": "ECommerceRecordsUs2021.orderDate.month"
      },
      "annotation": {
        "ECommerceRecordsUs2021.avg_discount": {
          "title": "E Commerce Records Us2021 Avg Discount",
          "shortTitle": "Avg Discount",
          "type": "number",
          "drillMembers": [],
          "drillMembersGrouped": {
            "measures": [],
            "dimensions": []
          }
        },
        "ECommerceRecordsUs2021.orderDate.month": {
          "title": "E Commerce Records Us2021 Order Date",
          "shortTitle": "Order Date",
          "type": "time"
        },
        "ECommerceRecordsUs2021.orderDate": {
          "title": "E Commerce Records Us2021 Order Date",
          "shortTitle": "Order Date",
          "type": "time"
        }
      },
      "query": {
        "measures": [
          "ECommerceRecordsUs2021.avg_discount"
        ],
        "timeDimensions": [
          {
            "dimension": "ECommerceRecordsUs2021.orderDate",
            "granularity": "month",
            "dateRange": [
              "2020-01-01T00:00:00.000",
              "2020-12-30T23:59:59.999"
            ]
          }
        ],
        "filters": [
          {
            "operator": "equals",
            "values": [
              "Standard Class"
            ],
            "member": "ECommerceRecordsUs2021.shipMode"
          }
        ],
        "limit": 2,
        "rowLimit": 2,
        "timezone": "UTC",
        "order": [],
        "dimensions": []
      },
      "queryType": "blendingQuery"
    },
    "queryResult": [
      {
        "e_commerce_records_us2021__order_date_month": "2020-01-01T00:00:00.000",
        "e_commerce_records_us2021__avg_discount": "0.15638297872340425532"
      },
      {
        "e_commerce_records_us2021__order_date_month": "2020-02-01T00:00:00.000",
        "e_commerce_records_us2021__avg_discount": "0.17573529411764705882"
      }
    ],
    "finalResultDefault": [
      {
        "ECommerceRecordsUs2021.orderDate.month": "2020-01-01T00:00:00.000",
        "ECommerceRecordsUs2021.orderDate": "2020-01-01T00:00:00.000",
        "ECommerceRecordsUs2021.avg_discount": "0.15638297872340425532",
        "time.month": "2020-01-01T00:00:00.000"
      },
      {
        "ECommerceRecordsUs2021.orderDate.month": "2020-02-01T00:00:00.000",
        "ECommerceRecordsUs2021.orderDate": "2020-02-01T00:00:00.000",
        "ECommerceRecordsUs2021.avg_discount": "0.17573529411764705882",
        "time.month": "2020-02-01T00:00:00.000"
      }
    ],
    "finalResultCompact": {
      "members": [
        "ECommerceRecordsUs2021.orderDate.month",
        "ECommerceRecordsUs2021.orderDate",
        "ECommerceRecordsUs2021.avg_discount",
        "time.month"
      ],
      "dataset": [
        [
          "2020-01-01T00:00:00.000",
          "2020-01-01T00:00:00.000",
          "0.15638297872340425532",
          "2020-01-01T00:00:00.000"
        ],
        [
          "2020-02-01T00:00:00.000",
          "2020-02-01T00:00:00.000",
          "0.17573529411764705882",
          "2020-02-01T00:00:00.000"
        ]
      ]
    }
  },
  "blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode2": {
    "request": {
      "aliasToMemberNameMap": {
        "e_commerce_records_us2021__avg_discount": "ECommerceRecordsUs2021.avg_discount",
        "e_commerce_records_us2021__order_date_month": "ECommerceRecordsUs2021.orderDate.month"
      },
      "annotation": {
        "ECommerceRecordsUs2021.avg_discount": {
          "title": "E Commerce Records Us2021 Avg Discount",
          "shortTitle": "Avg Discount",
          "type": "number",
          "drillMembers": [],
          "drillMembersGrouped": {
            "measures": [],
            "dimensions": []
          }
        },
        "ECommerceRecordsUs2021.orderDate.month": {
          "title": "E Commerce Records Us2021 Order Date",
          "shortTitle": "Order Date",
          "type": "time"
        },
        "ECommerceRecordsUs2021.orderDate": {
          "title": "E Commerce Records Us2021 Order Date",
          "shortTitle": "Order Date",
          "type": "time"
        }
      },
      "query": {
        "measures": [
          "ECommerceRecordsUs2021.avg_discount"
        ],
        "timeDimensions": [
          {
            "dimension": "ECommerceRecordsUs2021.orderDate",
            "granularity": "month",
            "dateRange": [
              "2020-01-01T00:00:00.000",
              "2020-12-30T23:59:59.999"
            ]
          }
        ],
        "filters": [
          {
            "operator": "equals",
            "values": [
              "First Class"
            ],
            "member": "ECommerceRecordsUs2021.shipMode"
          }
        ],
        "limit": 2,
        "rowLimit": 2,
        "timezone": "UTC",
        "order": [],
        "dimensions": []
      },
      "queryType": "blendingQuery"
    },
    "queryResult": [
      {
        "e_commerce_records_us2021__order_date_month": "2020-01-01T00:00:00.000",
        "e_commerce_records_us2021__avg_discount": "0.28571428571428571429"
      },
      {
        "e_commerce_records_us2021__order_date_month": "2020-02-01T00:00:00.000",
        "e_commerce_records_us2021__avg_discount": "0.21777777777777777778"
      }
    ],
    "finalResultDefault": [
      {
        "ECommerceRecordsUs2021.orderDate.month": "2020-01-01T00:00:00.000",
        "ECommerceRecordsUs2021.orderDate": "2020-01-01T00:00:00.000",
        "ECommerceRecordsUs2021.avg_discount": "0.28571428571428571429",
        "time.month": "2020-01-01T00:00:00.000"
      },
      {
        "ECommerceRecordsUs2021.orderDate.month": "2020-02-01T00:00:00.000",
        "ECommerceRecordsUs2021.orderDate": "2020-02-01T00:00:00.000",
        "ECommerceRecordsUs2021.avg_discount": "0.21777777777777777778",
        "time.month": "2020-02-01T00:00:00.000"
      }
    ],
    "finalResultCompact": {
      "members": [
        "ECommerceRecordsUs2021.orderDate.month",
        "ECommerceRecordsUs2021.orderDate",
        "ECommerceRecordsUs2021.avg_discount",
        "time.month"
      ],
      "dataset": [
        [
          "2020-01-01T00:00:00.000",
          "2020-01-01T00:00:00.000",
          "0.28571428571428571429",
          "2020-01-01T00:00:00.000"
        ],
        [
          "2020-02-01T00:00:00.000",
          "2020-02-01T00:00:00.000",
          "0.21777777777777777778",
          "2020-02-01T00:00:00.000"
        ]
      ]
    }
  }
}
    "#;

    static TEST_SUITE_DATA: LazyLock<TestSuiteData> =
        LazyLock::new(|| from_str(TEST_SUITE_JSON).unwrap());

    #[derive(Debug)]
    pub struct TestError(String);

    impl Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Error: {}", self.0)
        }
    }

    impl std::error::Error for TestError {}

    /// Smart comparator of datasets.
    /// Hashmaps don't guarantee the order of the elements while iterating,
    /// so it's not possible to simply compare generated one and the one from the json.
    fn compare_transformed_data(
        left: &TransformedData,
        right: &TransformedData,
    ) -> Result<(), TestError> {
        match (left, right) {
            (
                TransformedData::Compact {
                    members: left_members,
                    dataset: left_dataset,
                },
                TransformedData::Compact {
                    members: right_members,
                    dataset: right_dataset,
                },
            ) => {
                let mut left_sorted_members = left_members.clone();
                let mut right_sorted_members = right_members.clone();
                left_sorted_members.sort();
                right_sorted_members.sort();

                if left_sorted_members != right_sorted_members {
                    return Err(TestError("Members do not match after sorting".to_string()));
                }

                if left_dataset.len() != right_dataset.len() {
                    return Err(TestError("Datasets have different lengths".to_string()));
                }

                let mut member_index_map = HashMap::new();
                for (i, member) in left_members.iter().enumerate() {
                    if let Some(right_index) = right_members.iter().position(|x| x == member) {
                        member_index_map.insert(i, right_index);
                    } else {
                        return Err(TestError("Member not found in right object".to_string()));
                    }
                }

                for (i, left_row) in left_dataset.iter().enumerate() {
                    let right_row = &right_dataset[i];

                    for (j, left_value) in left_row.iter().enumerate() {
                        let mapped_index = *member_index_map.get(&j).unwrap();
                        let right_value = &right_row[mapped_index];
                        if left_value != right_value {
                            return Err(TestError(format!(
                                "Dataset values at row {} and column {} do not match: {} != {}",
                                i, j, left_value, right_value
                            )));
                        }
                    }
                }

                Ok(())
            }
            (TransformedData::Vanilla(left_dataset), TransformedData::Vanilla(right_dataset)) => {
                if left_dataset.len() != right_dataset.len() {
                    return Err(TestError(
                        "Vanilla datasets have different lengths".to_string(),
                    ));
                }

                for (i, (left_record, right_record)) in
                    left_dataset.iter().zip(right_dataset.iter()).enumerate()
                {
                    if left_record.len() != right_record.len() {
                        return Err(TestError(format!(
                            "Vanilla dataset records at index {} have different numbers of keys",
                            i
                        )));
                    }

                    for (key, left_value) in left_record {
                        if let Some(right_value) = right_record.get(key) {
                            if left_value != right_value {
                                return Err(TestError(format!(
                                    "Values at index {} for key '{}' do not match: {:?} != {:?}",
                                    i, key, left_value, right_value
                                )));
                            }
                        } else {
                            return Err(TestError(format!(
                                "Key '{}' not found in right record at index {}",
                                key, i
                            )));
                        }
                    }
                }

                Ok(())
            }
            _ => Err(TestError("Mismatched TransformedData types".to_string())),
        }
    }

    #[test]
    fn test_transform_value_datetime_to_time() {
        let dt = Utc
            .with_ymd_and_hms(2024, 1, 1, 12, 30, 15)
            .unwrap()
            .with_nanosecond(123_000_000)
            .unwrap();
        let value = DBResponseValue::DateTime(dt);
        let result = transform_value(value, "time");

        assert_eq!(
            result,
            DBResponsePrimitive::String("2024-01-01T12:30:15.123".to_string())
        );
    }

    #[test]
    fn test_transform_value_datetime_empty_type() {
        let dt = Utc
            .with_ymd_and_hms(2024, 1, 1, 12, 30, 15)
            .unwrap()
            .with_nanosecond(123_000_000)
            .unwrap();
        let value = DBResponseValue::DateTime(dt);
        let result = transform_value(value, "");

        assert_eq!(
            result,
            DBResponsePrimitive::String("2024-01-01T12:30:15.123".to_string())
        );
    }

    #[test]
    fn test_transform_value_string_to_time_valid_rfc3339() {
        let value = DBResponseValue::Primitive(DBResponsePrimitive::String(
            "2024-01-01T12:30:15.123".to_string(),
        ));
        let result = transform_value(value, "time");

        assert_eq!(
            result,
            DBResponsePrimitive::String("2024-01-01T12:30:15.123".to_string())
        );
    }

    #[test]
    fn test_transform_value_string_wo_t_to_time_valid_rfc3339() {
        let value = DBResponseValue::Primitive(DBResponsePrimitive::String(
            "2024-01-01 12:30:15.123".to_string(),
        ));
        let result = transform_value(value, "time");

        assert_eq!(
            result,
            DBResponsePrimitive::String("2024-01-01T12:30:15.123".to_string())
        );
    }

    #[test]
    fn test_transform_value_string_wo_mssec_to_time_valid_rfc3339() {
        let value = DBResponseValue::Primitive(DBResponsePrimitive::String(
            "2024-01-01 12:30:15".to_string(),
        ));
        let result = transform_value(value, "time");

        assert_eq!(
            result,
            DBResponsePrimitive::String("2024-01-01T12:30:15.000".to_string())
        );
    }

    #[test]
    fn test_transform_value_string_wo_mssec_w_t_to_time_valid_rfc3339() {
        let value = DBResponseValue::Primitive(DBResponsePrimitive::String(
            "2024-01-01T12:30:15".to_string(),
        ));
        let result = transform_value(value, "time");

        assert_eq!(
            result,
            DBResponsePrimitive::String("2024-01-01T12:30:15.000".to_string())
        );
    }

    #[test]
    fn test_transform_value_string_with_tz_offset_to_time_valid_rfc3339() {
        let value = DBResponseValue::Primitive(DBResponsePrimitive::String(
            "2024-01-01 12:30:15.123 +00:00".to_string(),
        ));
        let result = transform_value(value, "time");

        assert_eq!(
            result,
            DBResponsePrimitive::String("2024-01-01T12:30:15.123".to_string())
        );
    }

    #[test]
    fn test_transform_value_string_with_tz_to_time_valid_rfc3339() {
        let value = DBResponseValue::Primitive(DBResponsePrimitive::String(
            "2024-01-01 12:30:15.123 UTC".to_string(),
        ));
        let result = transform_value(value, "time");

        assert_eq!(
            result,
            DBResponsePrimitive::String("2024-01-01T12:30:15.123".to_string())
        );
    }

    #[test]
    fn test_transform_value_string_to_time_invalid_rfc3339() {
        let value =
            DBResponseValue::Primitive(DBResponsePrimitive::String("invalid-date".to_string()));
        let result = transform_value(value, "time");

        assert_eq!(
            result,
            DBResponsePrimitive::String("invalid-date".to_string())
        );
    }

    #[test]
    fn test_transform_value_primitive_string_type_not_time() {
        let value =
            DBResponseValue::Primitive(DBResponsePrimitive::String("some-string".to_string()));
        let result = transform_value(value, "other");

        assert_eq!(
            result,
            DBResponsePrimitive::String("some-string".to_string())
        );
    }

    #[test]
    fn test_transform_value_object() {
        let obj_value = DBResponsePrimitive::String("object-value".to_string());
        let value = DBResponseValue::Object {
            value: obj_value.clone(),
        };
        let result = transform_value(value, "time");

        assert_eq!(result, obj_value);
    }

    #[test]
    fn test_transform_value_fallback_to_null() {
        let value = DBResponseValue::DateTime(Utc::now());
        let result = transform_value(value, "unknown");

        assert_eq!(result, DBResponsePrimitive::Null);
    }

    #[test]
    fn test_get_date_range_value_valid_range() -> Result<()> {
        let time_dimensions = vec![QueryTimeDimension {
            dimension: "some-dim".to_string(),
            date_range: Some(vec![
                "2024-01-01T00:00:00Z".to_string(),
                "2024-01-31T23:59:59Z".to_string(),
            ]),
            compare_date_range: None,
            granularity: None,
        }];

        let result = get_date_range_value(Some(&time_dimensions))?;
        assert_eq!(
            result,
            DBResponsePrimitive::String("2024-01-01T00:00:00Z - 2024-01-31T23:59:59Z".to_string())
        );
        Ok(())
    }

    #[test]
    fn test_get_date_range_value_no_time_dimensions() {
        let result = get_date_range_value(None);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "QueryTimeDimension should be specified for the compare date range query."
        );
    }

    #[test]
    fn test_get_date_range_value_empty_time_dimensions() {
        let time_dimensions: Vec<QueryTimeDimension> = vec![];

        let result = get_date_range_value(Some(&time_dimensions));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "No time dimension provided."
        );
    }

    #[test]
    fn test_get_date_range_value_missing_date_range() {
        let time_dimensions = vec![QueryTimeDimension {
            dimension: "dim".to_string(),
            date_range: None,
            compare_date_range: None,
            granularity: None,
        }];

        let result = get_date_range_value(Some(&time_dimensions));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Inconsistent QueryTimeDimension configuration: dateRange required."
        );
    }

    #[test]
    fn test_get_date_range_value_single_date_range() {
        let time_dimensions = vec![QueryTimeDimension {
            dimension: "dim".to_string(),
            date_range: Some(vec!["2024-01-01T00:00:00Z".to_string()]),
            compare_date_range: None,
            granularity: None,
        }];

        let result = get_date_range_value(Some(&time_dimensions));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Inconsistent dateRange configuration for the compare date range query: 2024-01-01T00:00:00Z"
        );
    }

    #[test]
    fn test_get_blending_query_key_valid_granularity() -> Result<()> {
        let time_dimensions = vec![QueryTimeDimension {
            dimension: "dim".to_string(),
            granularity: Some("day".to_string()),
            date_range: None,
            compare_date_range: None,
        }];

        let result = get_blending_query_key(Some(&time_dimensions))?;
        assert_eq!(result, "time.day");
        Ok(())
    }

    #[test]
    fn test_get_blending_query_key_no_time_dimensions() {
        let result = get_blending_query_key(None);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "QueryTimeDimension should be specified for the blending query."
        );
    }

    #[test]
    fn test_get_blending_query_key_empty_time_dimensions() {
        let time_dimensions: Vec<QueryTimeDimension> = vec![];

        let result = get_blending_query_key(Some(&time_dimensions));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "QueryTimeDimension should be specified for the blending query."
        );
    }

    #[test]
    fn test_get_blending_query_key_missing_granularity() {
        let time_dimensions = vec![QueryTimeDimension {
            dimension: "dim".to_string(),
            granularity: None,
            date_range: None,
            compare_date_range: None,
        }];

        let result = get_blending_query_key(Some(&time_dimensions));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Inconsistent QueryTimeDimension configuration for the blending query, granularity required: {:?}",
                QueryTimeDimension {
                    dimension: "dim".to_string(),
                    granularity: None,
                    date_range: None,
                    compare_date_range: None,
                }
            )
        );
    }

    #[test]
    fn test_get_blending_response_key_valid_dimension_and_granularity() -> Result<()> {
        let time_dimensions = vec![QueryTimeDimension {
            dimension: "orders.created_at".to_string(),
            granularity: Some("day".to_string()),
            date_range: None,
            compare_date_range: None,
        }];

        let result = get_blending_response_key(Some(&time_dimensions))?;
        assert_eq!(result, "orders.created_at.day");
        Ok(())
    }

    #[test]
    fn test_get_blending_response_key_no_time_dimensions() {
        let result = get_blending_response_key(None);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "QueryTimeDimension should be specified for the blending query."
        );
    }

    #[test]
    fn test_get_blending_response_key_empty_time_dimensions() {
        let time_dimensions: Vec<QueryTimeDimension> = vec![];

        let result = get_blending_response_key(Some(&time_dimensions));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "QueryTimeDimension should be specified for the blending query."
        );
    }

    #[test]
    fn test_get_blending_response_key_missing_granularity() {
        let time_dimensions = vec![QueryTimeDimension {
            dimension: "orders.created_at".to_string(),
            granularity: None,
            date_range: None,
            compare_date_range: None,
        }];

        let result = get_blending_response_key(Some(&time_dimensions));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Inconsistent QueryTimeDimension configuration for the blending query, granularity required: {:?}",
                QueryTimeDimension {
                    dimension: "orders.created_at".to_string(),
                    granularity: None,
                    date_range: None,
                    compare_date_range: None,
                }
            )
        );
    }

    #[test]
    fn test_regular_profit_by_postal_code_compact() -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(&"regular_profit_by_postal_code".to_string())
            .unwrap()
            .clone();
        test_data.request.res_type = Some(ResultType::Compact);
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let transformed = TransformedData::transform(&test_data.request, &raw_data)?;
        compare_transformed_data(&transformed, &test_data.final_result_compact.unwrap())?;
        Ok(())
    }

    #[test]
    fn test_compare_date_range_count_by_order_date() -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(&"compare_date_range_count_by_order_date".to_string())
            .unwrap()
            .clone();
        test_data.request.res_type = Some(ResultType::Compact);
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let transformed = TransformedData::transform(&test_data.request, &raw_data)?;
        compare_transformed_data(&transformed, &test_data.final_result_compact.unwrap())?;
        Ok(())
    }

    #[test]
    fn test_compare_date_range_count_by_order_date2() -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(&"compare_date_range_count_by_order_date2".to_string())
            .unwrap()
            .clone();
        test_data.request.res_type = Some(ResultType::Compact);
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let transformed = TransformedData::transform(&test_data.request, &raw_data)?;
        compare_transformed_data(&transformed, &test_data.final_result_compact.unwrap())?;
        Ok(())
    }

    #[test]
    fn test_regular_discount_by_city() -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(&"regular_discount_by_city".to_string())
            .unwrap()
            .clone();
        test_data.request.res_type = Some(ResultType::Compact);
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let transformed = TransformedData::transform(&test_data.request, &raw_data)?;
        compare_transformed_data(&transformed, &test_data.final_result_compact.unwrap())?;
        Ok(())
    }

    #[test]
    fn test_regular_discount_by_city_to_fail() -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(&"regular_discount_by_city".to_string())
            .unwrap()
            .clone();
        test_data
            .request
            .alias_to_member_name_map
            .remove(&"e_commerce_records_us2021__avg_discount".to_string());
        test_data.request.res_type = Some(ResultType::Compact);
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        match TransformedData::transform(&test_data.request, &raw_data) {
            Ok(_) => Err(TestError("regular_discount_by_city should fail ".to_string()).into()),
            Err(_) => Ok(()), // Should throw an error
        }
    }

    #[test]
    fn test_blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode(
    ) -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(
                &"blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode"
                    .to_string(),
            )
            .unwrap()
            .clone();
        test_data.request.res_type = Some(ResultType::Compact);
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let transformed = TransformedData::transform(&test_data.request, &raw_data)?;
        compare_transformed_data(&transformed, &test_data.final_result_compact.unwrap())?;
        Ok(())
    }

    #[test]
    fn test_blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode2(
    ) -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(
                &"blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode"
                    .to_string(),
            )
            .unwrap()
            .clone();
        test_data.request.res_type = Some(ResultType::Compact);
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let transformed = TransformedData::transform(&test_data.request, &raw_data)?;
        compare_transformed_data(&transformed, &test_data.final_result_compact.unwrap())?;
        Ok(())
    }

    #[test]
    fn test_regular_discount_by_city_default_to_fail() -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(&"regular_discount_by_city".to_string())
            .unwrap()
            .clone();
        test_data
            .request
            .alias_to_member_name_map
            .remove(&"e_commerce_records_us2021__avg_discount".to_string());
        test_data.request.res_type = Some(ResultType::Default);
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        match TransformedData::transform(&test_data.request, &raw_data) {
            Ok(_) => Err(TestError("regular_discount_by_city should fail ".to_string()).into()),
            Err(_) => Ok(()), // Should throw an error
        }
    }

    #[test]
    fn test_regular_discount_by_city_default() -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(&"regular_discount_by_city".to_string())
            .unwrap()
            .clone();
        test_data.request.res_type = Some(ResultType::Default);
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let transformed = TransformedData::transform(&test_data.request, &raw_data)?;
        compare_transformed_data(&transformed, &test_data.final_result_default.unwrap())?;
        Ok(())
    }

    #[test]
    fn test_regular_profit_by_postal_code_default() -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(&"regular_discount_by_city".to_string())
            .unwrap()
            .clone();
        test_data.request.res_type = Some(ResultType::Default);
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let transformed = TransformedData::transform(&test_data.request, &raw_data)?;
        compare_transformed_data(&transformed, &test_data.final_result_default.unwrap())?;
        Ok(())
    }

    #[test]
    fn test_blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode_default(
    ) -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(
                &"blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode"
                    .to_string(),
            )
            .unwrap()
            .clone();
        test_data.request.res_type = Some(ResultType::Default);
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let transformed = TransformedData::transform(&test_data.request, &raw_data)?;
        compare_transformed_data(&transformed, &test_data.final_result_default.unwrap())?;
        Ok(())
    }

    #[test]
    fn test_blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode2_default(
    ) -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(
                &"blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode2"
                    .to_string(),
            )
            .unwrap()
            .clone();
        test_data.request.res_type = Some(ResultType::Default);
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let transformed = TransformedData::transform(&test_data.request, &raw_data)?;
        compare_transformed_data(&transformed, &test_data.final_result_default.unwrap())?;
        Ok(())
    }

    #[test]
    fn test_get_members_no_alias_to_member_name_map() -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(&"regular_profit_by_postal_code".to_string())
            .unwrap()
            .clone();
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        test_data.request.alias_to_member_name_map = HashMap::new();
        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = &test_data.request.query;
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();

        match get_members(
            query_type,
            query,
            &raw_data,
            alias_to_member_name_map,
            annotation,
        ) {
            Ok(_) => Err(TestError("get_members() should fail ".to_string()).into()),
            Err(err) => {
                assert!(err.to_string().contains("Member name not found for alias"));
                Ok(())
            }
        }
    }

    #[test]
    fn test_get_members_empty_dataset() -> Result<()> {
        let test_data = TEST_SUITE_DATA
            .get(&"regular_profit_by_postal_code".to_string())
            .unwrap()
            .clone();
        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = &test_data.request.query;
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();

        let (members_to_alias_map, members) = get_members(
            query_type,
            query,
            &QueryResult {
                columns: vec![],
                rows: vec![],
                columns_pos: HashMap::new(),
            },
            alias_to_member_name_map,
            annotation,
        )?;
        assert_eq!(members_to_alias_map.len(), 0);
        assert_eq!(members.len(), 0);
        Ok(())
    }

    #[test]
    fn test_get_members_filled_dataset() -> Result<()> {
        let test_data = TEST_SUITE_DATA
            .get(&"regular_profit_by_postal_code".to_string())
            .unwrap()
            .clone();
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = &test_data.request.query;
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();

        let (members_to_alias_map, members) = get_members(
            query_type,
            query,
            &raw_data,
            alias_to_member_name_map,
            annotation,
        )?;
        let members_map_expected: MembersMap = HashMap::from([
            (
                "ECommerceRecordsUs2021.postalCode".to_string(),
                "e_commerce_records_us2021__postal_code".to_string(),
            ),
            (
                "ECommerceRecordsUs2021.avg_profit".to_string(),
                "e_commerce_records_us2021__avg_profit".to_string(),
            ),
        ]);
        assert_eq!(members_to_alias_map, members_map_expected);
        assert_eq!(members.len(), 2);
        Ok(())
    }

    #[test]
    fn test_get_members_compare_date_range_empty_dataset() -> Result<()> {
        let test_data = TEST_SUITE_DATA
            .get(&"compare_date_range_count_by_order_date".to_string())
            .unwrap()
            .clone();
        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = &test_data.request.query;
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();

        let (members_to_alias_map, members) = get_members(
            query_type,
            query,
            &QueryResult {
                columns: vec![],
                rows: vec![],
                columns_pos: HashMap::new(),
            },
            alias_to_member_name_map,
            annotation,
        )?;
        assert_eq!(members_to_alias_map.len(), 0);
        assert_eq!(members.len(), 0);
        Ok(())
    }

    #[test]
    fn test_get_members_compare_date_range_filled_dataset() -> Result<()> {
        let test_data = TEST_SUITE_DATA
            .get(&"compare_date_range_count_by_order_date".to_string())
            .unwrap()
            .clone();
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = &test_data.request.query;
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();

        let (members_to_alias_map, members) = get_members(
            query_type,
            query,
            &raw_data,
            alias_to_member_name_map,
            annotation,
        )?;
        let members_map_expected: MembersMap = HashMap::from([
            (
                "ECommerceRecordsUs2021.orderDate.day".to_string(),
                "e_commerce_records_us2021__order_date_day".to_string(),
            ),
            (
                "ECommerceRecordsUs2021.orderDate".to_string(),
                "e_commerce_records_us2021__order_date_day".to_string(),
            ),
            (
                "ECommerceRecordsUs2021.count".to_string(),
                "e_commerce_records_us2021__count".to_string(),
            ),
            (
                "compareDateRange".to_string(),
                "compareDateRangeQuery".to_string(),
            ),
        ]);
        assert_eq!(members_to_alias_map, members_map_expected);
        assert_eq!(members.len(), 4);
        Ok(())
    }

    #[test]
    fn test_get_members_blending_query_empty_dataset() -> Result<()> {
        let test_data = TEST_SUITE_DATA
            .get(
                &"blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode"
                    .to_string(),
            )
            .unwrap()
            .clone();
        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = &test_data.request.query;
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();

        let (members_to_alias_map, members) = get_members(
            query_type,
            query,
            &QueryResult {
                columns: vec![],
                rows: vec![],
                columns_pos: HashMap::new(),
            },
            alias_to_member_name_map,
            annotation,
        )?;
        assert_eq!(members_to_alias_map.len(), 0);
        assert_eq!(members.len(), 0);
        Ok(())
    }

    #[test]
    fn test_get_members_blending_query_filled_dataset() -> Result<()> {
        let test_data = TEST_SUITE_DATA
            .get(
                &"blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode"
                    .to_string(),
            )
            .unwrap()
            .clone();
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = &test_data.request.query;
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();

        let (members_to_alias_map, members) = get_members(
            query_type,
            query,
            &raw_data,
            alias_to_member_name_map,
            annotation,
        )?;
        let members_map_expected: HashMap<String, String> = HashMap::from([
            (
                "ECommerceRecordsUs2021.orderDate.month".to_string(),
                "e_commerce_records_us2021__order_date_month".to_string(),
            ),
            (
                "ECommerceRecordsUs2021.orderDate".to_string(),
                "e_commerce_records_us2021__order_date_month".to_string(),
            ),
            (
                "ECommerceRecordsUs2021.avg_discount".to_string(),
                "e_commerce_records_us2021__avg_discount".to_string(),
            ),
            (
                "time.month".to_string(),
                "e_commerce_records_us2021__order_date_month".to_string(),
            ),
        ]);
        assert_eq!(members_to_alias_map, members_map_expected);
        assert_eq!(members.len(), 4);
        Ok(())
    }

    #[test]
    fn test_get_compact_row_regular_profit_by_postal_code() -> Result<()> {
        let test_data = TEST_SUITE_DATA
            .get(&"regular_profit_by_postal_code".to_string())
            .unwrap()
            .clone();
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = test_data.request.query.clone();
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();
        let time_dimensions = &test_data.request.query.time_dimensions.unwrap();

        let (members_to_alias_map, members) = get_members(
            query_type,
            &query,
            &raw_data,
            alias_to_member_name_map,
            annotation,
        )?;
        let res = get_compact_row(
            &members_to_alias_map,
            &annotation,
            &query_type,
            &members,
            Some(time_dimensions),
            &raw_data.rows[0],
            &raw_data.columns_pos,
        )?;

        let members_map_expected = HashMap::from([
            (
                "ECommerceRecordsUs2021.postalCode".to_string(),
                DBResponsePrimitive::String("95823".to_string()),
            ),
            (
                "ECommerceRecordsUs2021.avg_profit".to_string(),
                DBResponsePrimitive::String("646.1258666666666667".to_string()),
            ),
        ]);

        assert_eq!(res.len(), members_map_expected.len());
        for (i, val) in members.iter().enumerate() {
            assert_eq!(res[i], members_map_expected.get(val).unwrap().clone());
        }

        Ok(())
    }

    #[test]
    fn test_get_compact_row_regular_discount_by_city() -> Result<()> {
        let test_data = TEST_SUITE_DATA
            .get(&"regular_discount_by_city".to_string())
            .unwrap()
            .clone();
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = test_data.request.query.clone();
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();
        let time_dimensions = &test_data.request.query.time_dimensions.unwrap();

        let (members_to_alias_map, members) = get_members(
            query_type,
            &query,
            &raw_data,
            alias_to_member_name_map,
            annotation,
        )?;
        let res = get_compact_row(
            &members_to_alias_map,
            &annotation,
            &query_type,
            &members,
            Some(time_dimensions),
            &raw_data.rows[0],
            &raw_data.columns_pos,
        )?;

        let members_map_expected = HashMap::from([
            (
                "ECommerceRecordsUs2021.city".to_string(),
                DBResponsePrimitive::String("Missouri City".to_string()),
            ),
            (
                "ECommerceRecordsUs2021.avg_discount".to_string(),
                DBResponsePrimitive::String("0.80000000000000000000".to_string()),
            ),
        ]);

        assert_eq!(res.len(), members_map_expected.len());
        for (i, val) in members.iter().enumerate() {
            assert_eq!(res[i], members_map_expected.get(val).unwrap().clone());
        }

        Ok(())
    }

    #[test]
    fn test_get_compact_row_compare_date_range_count_by_order_date() -> Result<()> {
        let test_data = TEST_SUITE_DATA
            .get(&"compare_date_range_count_by_order_date".to_string())
            .unwrap()
            .clone();
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = test_data.request.query.clone();
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();
        let time_dimensions = &test_data.request.query.time_dimensions.unwrap();

        let (members_to_alias_map, members) = get_members(
            query_type,
            &query,
            &raw_data,
            alias_to_member_name_map,
            annotation,
        )?;
        let res = get_compact_row(
            &members_to_alias_map,
            &annotation,
            &query_type,
            &members,
            Some(time_dimensions),
            &raw_data.rows[0],
            &raw_data.columns_pos,
        )?;

        let members_map_expected = HashMap::from([
            (
                "ECommerceRecordsUs2021.orderDate.day".to_string(),
                DBResponsePrimitive::String("2020-01-01T00:00:00.000".to_string()),
            ),
            (
                "ECommerceRecordsUs2021.orderDate".to_string(),
                DBResponsePrimitive::String("2020-01-01T00:00:00.000".to_string()),
            ),
            (
                "ECommerceRecordsUs2021.count".to_string(),
                DBResponsePrimitive::String("10".to_string()),
            ),
            (
                "compareDateRange".to_string(),
                DBResponsePrimitive::String(
                    "2020-01-01T00:00:00.000 - 2020-01-31T23:59:59.999".to_string(),
                ),
            ),
        ]);

        assert_eq!(res.len(), members_map_expected.len());
        for (i, val) in members.iter().enumerate() {
            assert_eq!(res[i], members_map_expected.get(val).unwrap().clone());
        }

        let res = get_compact_row(
            &members_to_alias_map,
            &annotation,
            &query_type,
            &members,
            Some(time_dimensions),
            &raw_data.rows[1],
            &raw_data.columns_pos,
        )?;

        let members_map_expected = HashMap::from([
            (
                "ECommerceRecordsUs2021.orderDate.day".to_string(),
                DBResponsePrimitive::Null,
            ),
            (
                "ECommerceRecordsUs2021.orderDate".to_string(),
                DBResponsePrimitive::Null,
            ),
            (
                "ECommerceRecordsUs2021.count".to_string(),
                DBResponsePrimitive::Null,
            ),
            (
                "compareDateRange".to_string(),
                DBResponsePrimitive::String(
                    "2020-01-01T00:00:00.000 - 2020-01-31T23:59:59.999".to_string(),
                ),
            ),
        ]);

        assert_eq!(res.len(), members_map_expected.len());
        for (i, val) in members.iter().enumerate() {
            assert_eq!(res[i], members_map_expected.get(val).unwrap().clone());
        }

        Ok(())
    }

    #[test]
    fn test_get_compact_row_blending_query_avg_discount() -> Result<()> {
        let test_data = TEST_SUITE_DATA
            .get(
                &"blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode"
                    .to_string(),
            )
            .unwrap()
            .clone();
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = test_data.request.query.clone();
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();
        let time_dimensions = &test_data.request.query.time_dimensions.unwrap();

        let (members_to_alias_map, members) = get_members(
            query_type,
            &query,
            &raw_data,
            alias_to_member_name_map,
            annotation,
        )?;
        let res = get_compact_row(
            &members_to_alias_map,
            &annotation,
            &query_type,
            &members,
            Some(time_dimensions),
            &raw_data.rows[0],
            &raw_data.columns_pos,
        )?;

        let members_map_expected = HashMap::from([
            (
                "ECommerceRecordsUs2021.orderDate.month".to_string(),
                DBResponsePrimitive::String("2020-01-01T00:00:00.000".to_string()),
            ),
            (
                "ECommerceRecordsUs2021.orderDate".to_string(),
                DBResponsePrimitive::String("2020-01-01T00:00:00.000".to_string()),
            ),
            (
                "ECommerceRecordsUs2021.avg_discount".to_string(),
                DBResponsePrimitive::String("0.15638297872340425532".to_string()),
            ),
            (
                "time.month".to_string(),
                DBResponsePrimitive::String("2020-01-01T00:00:00.000".to_string()),
            ),
        ]);

        assert_eq!(res.len(), members_map_expected.len());
        for (i, val) in members.iter().enumerate() {
            assert_eq!(res[i], members_map_expected.get(val).unwrap().clone());
        }
        Ok(())
    }

    #[test]
    fn test_get_vanilla_row_regular_discount_by_city() -> Result<()> {
        let test_data = TEST_SUITE_DATA
            .get(&"regular_discount_by_city".to_string())
            .unwrap()
            .clone();
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = test_data.request.query.clone();
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();

        let res = get_vanilla_row(
            &alias_to_member_name_map,
            &annotation,
            &query_type,
            &query,
            &raw_data.rows[0],
            &raw_data.columns_pos,
        )?;
        let expected = HashMap::from([
            (
                "ECommerceRecordsUs2021.city".to_string(),
                DBResponsePrimitive::String("Missouri City".to_string()),
            ),
            (
                "ECommerceRecordsUs2021.avg_discount".to_string(),
                DBResponsePrimitive::String("0.80000000000000000000".to_string()),
            ),
        ]);
        assert_eq!(res, expected);
        Ok(())
    }

    #[test]
    fn test_get_vanilla_row_regular_discount_by_city_to_fail_member() -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(&"regular_discount_by_city".to_string())
            .unwrap()
            .clone();
        test_data
            .request
            .alias_to_member_name_map
            .remove(&"e_commerce_records_us2021__avg_discount".to_string());
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = test_data.request.query.clone();
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();

        match get_vanilla_row(
            &alias_to_member_name_map,
            &annotation,
            &query_type,
            &query,
            &raw_data.rows[0],
            &raw_data.columns_pos,
        ) {
            Ok(_) => Err(TestError("get_vanilla_row() should fail ".to_string()).into()),
            Err(err) => {
                assert!(err.to_string().contains("Missing member name for alias"));
                Ok(())
            }
        }
    }

    #[test]
    fn test_get_vanilla_row_regular_discount_by_city_to_fail_annotation() -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(&"regular_discount_by_city".to_string())
            .unwrap()
            .clone();
        test_data
            .request
            .annotation
            .remove(&"ECommerceRecordsUs2021.avg_discount".to_string());
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = test_data.request.query.clone();
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();

        match get_vanilla_row(
            &alias_to_member_name_map,
            &annotation,
            &query_type,
            &query,
            &raw_data.rows[0],
            &raw_data.columns_pos,
        ) {
            Ok(_) => Err(TestError("get_vanilla_row() should fail ".to_string()).into()),
            Err(err) => {
                assert!(err.to_string().contains("You requested hidden member"));
                Ok(())
            }
        }
    }
}
