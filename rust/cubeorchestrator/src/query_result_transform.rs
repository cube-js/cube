use crate::{
    query_message_parser::QueryResult,
    transport::{
        AnnotatedConfigItem, ConfigItem, MembersMap, NormalizedQuery, QueryTimeDimension,
        QueryType, ResultType, TransformDataRequest,
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
            DBResponsePrimitive::String(dt.with_timezone(&Utc).format("%Y-%m-%dT%H:%M:%S%.3f").to_string())
        }
        DBResponseValue::Primitive(DBResponsePrimitive::String(ref s)) if type_ == "time" => {
            let formatted = DateTime::parse_from_rfc3339(s)
                .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S%.3f").to_string())
                .or_else(|_| {
                    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.3f")
                        .map(|dt| Utc.from_utc_datetime(&dt).format("%Y-%m-%dT%H:%M:%S%.3f").to_string())
                })
                .or_else(|_| {
                    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.3f %Z")
                        .map(|dt| Utc.from_utc_datetime(&dt).format("%Y-%m-%dT%H:%M:%S%.3f").to_string())
                })
                .or_else(|_| {
                    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.3f %:z")
                        .map(|dt| Utc.from_utc_datetime(&dt).format("%Y-%m-%dT%H:%M:%S%.3f").to_string())
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
) -> Result<MembersMap> {
    let mut members: MembersMap = HashMap::new();

    if db_data.columns.is_empty() {
        return Ok(members);
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

        members.insert(member_name.clone(), column.clone());

        let path = member_name.split(MEMBER_SEPARATOR).collect::<Vec<&str>>();
        let calc_member = format!("{}{}{}", path[0], MEMBER_SEPARATOR, path[1]);

        if path.len() == 3
            && query
                .dimensions
                .as_ref()
                .map_or(true, |dims| !dims.iter().any(|dim| *dim == calc_member))
        {
            members.insert(calc_member, column.clone());
        }
    }

    match query_type {
        QueryType::CompareDateRangeQuery => {
            members.insert(
                COMPARE_DATE_RANGE_FIELD.to_string(),
                QueryType::CompareDateRangeQuery.to_string(),
            );
        }
        QueryType::BlendingQuery => {
            let blending_key = get_blending_query_key(query.time_dimensions.as_ref())
                .context("Failed to generate blending query key")?;
            if let Some(dim) = query
                .time_dimensions
                .as_ref()
                .and_then(|dims| dims.first().cloned())
            {
                members.insert(blending_key, dim.dimension.clone());
            }
        }
        _ => {}
    }

    Ok(members)
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
                    !dims
                        .iter()
                        .any(|dim| *dim == member_name_without_granularity)
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
            let mut dimensions = vec!["compareDateRange".to_string()];
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

        let members_to_alias_map = get_members(
            query_type,
            query,
            cube_store_result,
            alias_to_member_name_map,
            annotation,
        )?;
        let members: Vec<String> = members_to_alias_map.keys().cloned().collect();

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
    pub refresh_key_values: Option<Value>,
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
}

impl Display for DBResponsePrimitive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            DBResponsePrimitive::Null => "null".to_string(),
            DBResponsePrimitive::Boolean(b) => b.to_string(),
            DBResponsePrimitive::Number(n) => n.to_string(),
            DBResponsePrimitive::String(s) => s.clone(),
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
    use anyhow::Result;
    use chrono::{TimeZone, Timelike, Utc};

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
}
