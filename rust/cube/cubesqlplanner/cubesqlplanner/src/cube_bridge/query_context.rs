use super::base_query_options::{BaseQueryOptions, FilterItem, FilterValue};
use super::options_member::OptionsMember;
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct QueryContextTimeDimension {
    pub dimension: String,
    pub granularity: Option<String>,
    pub date_range: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct QueryContextFilter {
    pub member: String,
    pub operator: String,
    pub values: Option<Vec<Option<String>>>,
}

/// The query a measure `case.when[].context` condition is evaluated against,
/// in the shape the JS `QueryContext` class reads: full `cube.member` paths,
/// filters as the leaves of the filter tree, a time dimension's date range on
/// the time dimension itself.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct QueryContextData {
    pub dimensions: Vec<String>,
    pub time_dimensions: Vec<QueryContextTimeDimension>,
    pub measures: Vec<String>,
    pub filters: Vec<QueryContextFilter>,
}

impl QueryContextData {
    /// Member expressions have no member path and are left out.
    pub fn from_query_options(options: &dyn BaseQueryOptions) -> Result<Self, CubeError> {
        let member_names = |members: Option<Vec<OptionsMember>>| -> Vec<String> {
            members
                .unwrap_or_default()
                .into_iter()
                .filter_map(|member| match member {
                    OptionsMember::MemberName(name) => Some(name),
                    OptionsMember::MemberExpression(_) => None,
                })
                .collect()
        };
        let static_data = options.static_data();
        let time_dimensions = static_data
            .time_dimensions
            .iter()
            .flatten()
            .map(|td| QueryContextTimeDimension {
                dimension: td.dimension.clone(),
                granularity: td.granularity.clone(),
                date_range: td.date_range.clone(),
            })
            .collect();
        let mut filters = Vec::new();
        for item in static_data.filters.iter().flatten() {
            Self::collect_filter_leaves(item, &mut filters);
        }
        Ok(Self {
            dimensions: member_names(options.dimensions()?),
            time_dimensions,
            measures: member_names(options.measures()?),
            filters,
        })
    }

    fn collect_filter_leaves(item: &FilterItem, leaves: &mut Vec<QueryContextFilter>) {
        for nested in item.and.iter().flatten().chain(item.or.iter().flatten()) {
            Self::collect_filter_leaves(nested, leaves);
        }
        let Some(member) = item.member() else {
            return;
        };
        leaves.push(QueryContextFilter {
            member: member.clone(),
            operator: item.operator.clone().unwrap_or_default(),
            values: item
                .values
                .as_ref()
                .map(|values| values.iter().map(FilterValue::to_param_string).collect()),
        });
    }
}
