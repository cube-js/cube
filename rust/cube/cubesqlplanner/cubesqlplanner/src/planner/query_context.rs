use crate::cube_bridge::base_query_options::{BaseQueryOptions, FilterItem, FilterValue};
use crate::cube_bridge::options_member::OptionsMember;
use crate::cube_bridge::query_context::{
    QueryContextData, QueryContextFilter, QueryContextTimeDimension,
};
use crate::planner::{ModelCubes, SymbolPath};
use cubenativeutils::CubeError;

impl QueryContextData {
    /// Member names are normalised to `cube.member` of the data model cube, so a join
    /// path or a granularity suffix in the query compares equal to a `CUBE.member`
    /// reference in a condition. Member expressions have no member path and are left out.
    pub fn from_query_options(
        model_cubes: &ModelCubes,
        options: &dyn BaseQueryOptions,
    ) -> Result<Self, CubeError> {
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
        let mut dimensions = Vec::new();
        let mut time_dimensions = Vec::new();
        for name in member_names(options.dimensions()?) {
            let (member, granularity) = member_path(model_cubes, &name)?;
            match granularity {
                Some(granularity) => time_dimensions.push(QueryContextTimeDimension {
                    dimension: member,
                    granularity: Some(granularity),
                    date_range: None,
                }),
                None => dimensions.push(member),
            }
        }
        let static_data = options.static_data();
        for td in static_data.time_dimensions.iter().flatten() {
            time_dimensions.push(QueryContextTimeDimension {
                dimension: member_path(model_cubes, &td.dimension)?.0,
                granularity: td.granularity.clone(),
                date_range: td.date_range.clone(),
            });
        }
        let measures = member_names(options.measures()?)
            .iter()
            .map(|name| Ok(member_path(model_cubes, name)?.0))
            .collect::<Result<Vec<_>, CubeError>>()?;
        let mut filters = Vec::new();
        for item in static_data.filters.iter().flatten() {
            collect_filter_leaves(model_cubes, item, &mut filters)?;
        }
        Ok(Self {
            dimensions,
            time_dimensions,
            measures,
            filters,
        })
    }
}

fn member_path(
    model_cubes: &ModelCubes,
    name: &str,
) -> Result<(String, Option<String>), CubeError> {
    let path = SymbolPath::parse(model_cubes, name)?;
    let member = format!("{}.{}", path.cube_id().target(), path.symbol_name());
    Ok((member, path.granularity().clone()))
}

// Only the `and` spine is walked: a filter inside an `or` group does not constrain a
// member on its own.
fn collect_filter_leaves(
    model_cubes: &ModelCubes,
    item: &FilterItem,
    leaves: &mut Vec<QueryContextFilter>,
) -> Result<(), CubeError> {
    for nested in item.and.iter().flatten() {
        collect_filter_leaves(model_cubes, nested, leaves)?;
    }
    let Some(member) = item.member() else {
        return Ok(());
    };
    leaves.push(QueryContextFilter {
        member: member_path(model_cubes, member)?.0,
        operator: item.operator.clone().unwrap_or_default(),
        values: item
            .values
            .as_ref()
            .map(|values| values.iter().map(FilterValue::to_param_string).collect()),
    });
    Ok(())
}
