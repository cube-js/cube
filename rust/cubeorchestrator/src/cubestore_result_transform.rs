use crate::types::{
    ConfigItem, DBResponsePrimitive, DBResponseValue, MemberOrMemberExpression, MembersMap,
    NormalizedQuery, QueryTimeDimension, QueryType, ResultType, TransformedData,
    BLENDING_QUERY_KEY_PREFIX, BLENDING_QUERY_RES_SEPARATOR, COMPARE_DATE_RANGE_FIELD,
    COMPARE_DATE_RANGE_SEPARATOR, MEMBER_SEPARATOR,
};
use anyhow::{bail, Context, Result};
use chrono::SecondsFormat;
use std::collections::HashMap;

/// Transform specified `value` with specified `type` to the network protocol type.
pub fn transform_value(value: DBResponseValue, type_: &str) -> DBResponsePrimitive {
    match value {
        DBResponseValue::DateTime(dt) if type_ == "time" || type_.is_empty() => {
            let formatted = dt.to_rfc3339_opts(SecondsFormat::Millis, true);
            DBResponsePrimitive::String(formatted)
        }
        DBResponseValue::Primitive(p) => p,
        DBResponseValue::Object { value } => value,
        _ => DBResponsePrimitive::Null,
    }
}

/// Parse date range value from time dimension.
pub fn get_date_range_value(time_dimensions: Option<&Vec<QueryTimeDimension>>) -> Result<String> {
    let time_dimensions = match time_dimensions {
        Some(time_dimensions) => time_dimensions,
        None => bail!("QueryTimeDimension should be specified for the compare date range query."),
    };

    let dim = match time_dimensions.get(0) {
        Some(dim) => dim,
        None => bail!("No time dimension provided."),
    };

    let date_range: &Vec<String> = match &dim.date_range {
        Some(date_range) => date_range.as_ref(),
        None => bail!("Inconsistent QueryTimeDimension configuration: dateRange required."),
    };

    if date_range.len() == 1 {
        bail!(
            "Inconsistent dateRange configuration for the compare date range query: {}",
            date_range[0]
        );
    }

    Ok(date_range.join(COMPARE_DATE_RANGE_SEPARATOR))
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
    db_data: &Vec<HashMap<String, DBResponseValue>>,
    alias_to_member_name_map: &HashMap<String, String>,
    annotation: &HashMap<String, ConfigItem>,
) -> Result<MembersMap> {
    let mut members: MembersMap = HashMap::new();

    if db_data.is_empty() {
        return Ok(members);
    }

    let columns = db_data[0].keys().collect::<Vec<_>>();

    for column in columns {
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
            && query.dimensions.as_ref().map_or(true, |dims| {
                !dims.iter().any(|dim| match dim {
                    MemberOrMemberExpression::Member(name) => *name == calc_member,
                    MemberOrMemberExpression::MemberExpression(expr) => expr.name == calc_member,
                })
            })
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
    db_row: &HashMap<String, DBResponseValue>,
) -> Result<Vec<DBResponsePrimitive>> {
    let mut row: Vec<DBResponsePrimitive> = Vec::with_capacity(members.len());

    for m in members {
        if let Some(annotation_item) = annotation.get(m) {
            if let Some(alias) = members_to_alias_map.get(m) {
                if let Some(value) = db_row.get(alias) {
                    row.push(transform_value(value.clone(), &annotation_item.member_type));
                }
            }
        }
    }

    match query_type {
        QueryType::CompareDateRangeQuery => {
            row.push(DBResponsePrimitive::String(get_date_range_value(
                time_dimensions,
            )?));
        }
        QueryType::BlendingQuery => {
            let blending_key = get_blending_response_key(time_dimensions)?;

            if let Some(alias) = members_to_alias_map.get(&blending_key) {
                if let Some(value) = db_row.get(alias) {
                    let member_type = annotation
                        .get(alias)
                        .map_or("", |annotation_item| &annotation_item.member_type);

                    row.push(transform_value(value.clone(), member_type));
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
    db_row: &HashMap<String, DBResponseValue>,
) -> Result<HashMap<String, DBResponsePrimitive>> {
    let mut row = HashMap::new();

    for (alias, value) in db_row {
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

        let transformed_value = transform_value(value.clone(), &annotation_for_member.member_type);

        // Handle deprecated time dimensions without granularity
        let path: Vec<&str> = member_name.split(MEMBER_SEPARATOR).collect();
        let member_name_without_granularity = format!("{}{}{}", path[0], MEMBER_SEPARATOR, path[1]);
        if path.len() == 3
            && query.dimensions.as_ref().map_or(true, |dims| {
                !dims.iter().any(|dim| match dim {
                    MemberOrMemberExpression::Member(name) => {
                        *name == member_name_without_granularity
                    }
                    MemberOrMemberExpression::MemberExpression(expr) => {
                        expr.name == member_name_without_granularity
                    }
                })
            })
        {
            row.insert(member_name_without_granularity, transformed_value);
        } else {
            row.insert(member_name.clone(), transformed_value);
        }
    }

    match query_type {
        QueryType::CompareDateRangeQuery => {
            let date_range_value = get_date_range_value(query.time_dimensions.as_ref())?;
            row.insert(
                "compareDateRange".to_string(),
                DBResponsePrimitive::String(date_range_value),
            );
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

/// Transforms queried data array to the output format.
pub fn transform_data(
    alias_to_member_name_map: &HashMap<String, String>,
    annotation: &HashMap<String, ConfigItem>,
    data: Vec<HashMap<String, DBResponseValue>>,
    query: &NormalizedQuery,
    query_type: &QueryType,
    res_type: Option<ResultType>,
) -> Result<TransformedData> {
    let members_to_alias_map = get_members(
        query_type,
        query,
        &data,
        alias_to_member_name_map,
        annotation,
    )?;
    let members: Vec<String> = members_to_alias_map.keys().cloned().collect();

    match res_type {
        Some(ResultType::Compact) => {
            let dataset: Vec<_> = data
                .into_iter()
                .map(|row| {
                    get_compact_row(
                        &members_to_alias_map,
                        annotation,
                        query_type,
                        &members,
                        query.time_dimensions.as_ref(),
                        &row,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(TransformedData::Compact { members, dataset })
        }
        _ => {
            let dataset: Vec<_> = data
                .into_iter()
                .map(|row| {
                    get_vanilla_row(
                        alias_to_member_name_map,
                        annotation,
                        query_type,
                        query,
                        &row,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(TransformedData::Vanilla(dataset))
        }
    }
}
