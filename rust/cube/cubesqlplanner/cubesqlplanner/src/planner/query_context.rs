use crate::cube_bridge::base_query_options::{BaseQueryOptions, FilterItem, FilterValue};
use crate::cube_bridge::member_sql::{MemberSql, SqlTemplate};
use crate::cube_bridge::options_member::OptionsMember;
use crate::cube_bridge::query_context::{
    QueryContextData, QueryContextFilter, QueryContextTimeDimension,
};
use crate::planner::{Compiler, SqlCallArg, SymbolPath, SymbolPathType};
use cubenativeutils::CubeError;
use std::rc::Rc;

impl QueryContextData {
    /// Member names are normalised to `cube.member` of the data model cube, so a join
    /// path, a granularity suffix or a view member in the query compares equal to a
    /// `CUBE.member` reference in a condition. Member expressions have no member path and
    /// are left out.
    pub fn from_query_options(
        compiler: &Compiler,
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
            let (member, granularity) = member_path(compiler, &name)?;
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
                dimension: member_path(compiler, &td.dimension)?.0,
                granularity: td.granularity.clone(),
                date_range: td.date_range.clone(),
            });
        }
        let measures = member_names(options.measures()?)
            .iter()
            .map(|name| Ok(member_path(compiler, name)?.0))
            .collect::<Result<Vec<_>, CubeError>>()?;
        let mut filters = Vec::new();
        for item in static_data.filters.iter().flatten() {
            collect_filter_leaves(compiler, item, &mut filters)?;
        }
        Ok(Self {
            dimensions,
            time_dimensions,
            measures,
            filters,
        })
    }
}

fn member_path(compiler: &Compiler, name: &str) -> Result<(String, Option<String>), CubeError> {
    let path = SymbolPath::parse(compiler.model_cubes(), name)?;
    Ok((model_member(compiler, &path)?, path.granularity().clone()))
}

// A member a view includes from a cube is that cube's member. Its `sql` is the reference,
// read as a compiled template: building the symbol instead would need the query context
// for a measure with a context case, which is what is being built here.
fn model_member(compiler: &Compiler, path: &SymbolPath) -> Result<String, CubeError> {
    let member = format!("{}.{}", path.cube_id().target(), path.symbol_name());
    let evaluator = compiler.cube_evaluator();
    let (included, sql) = match path.path_type() {
        SymbolPathType::Measure => {
            let definition = evaluator.measure_by_path(member.clone())?;
            (definition.static_data().included, definition.sql()?)
        }
        SymbolPathType::Dimension => {
            let definition = evaluator.dimension_by_path(member.clone())?;
            (definition.static_data().included, definition.sql()?)
        }
        _ => return Ok(member),
    };
    let Some(sql) = sql.filter(|_| included.unwrap_or(false)) else {
        return Ok(member);
    };
    let Some(target) = direct_reference(compiler, sql)? else {
        return Ok(member);
    };
    let target = SymbolPath::parse_parts_joined_to(
        compiler.model_cubes(),
        Some(path.cube_id()),
        None,
        &target,
    )?;
    model_member(compiler, &target)
}

fn direct_reference(
    compiler: &Compiler,
    sql: Rc<dyn MemberSql>,
) -> Result<Option<Vec<String>>, CubeError> {
    let compiled = compiler.base_tools().compile_member_sql(
        sql.clone(),
        compiler.security_context().clone(),
        sql.args_names().clone(),
    )?;
    let is_direct = compiled.template == SqlTemplate::String(SqlCallArg::dependency(0));
    Ok(match compiled.args.symbol_paths.as_slice() {
        [target] if is_direct => Some(target.clone()),
        _ => None,
    })
}

// Only the `and` spine is walked: a filter inside an `or` group does not constrain a
// member on its own.
fn collect_filter_leaves(
    compiler: &Compiler,
    item: &FilterItem,
    leaves: &mut Vec<QueryContextFilter>,
) -> Result<(), CubeError> {
    for nested in item.and.iter().flatten() {
        collect_filter_leaves(compiler, nested, leaves)?;
    }
    let Some(member) = item.member() else {
        return Ok(());
    };
    leaves.push(QueryContextFilter {
        member: member_path(compiler, member)?.0,
        operator: item.operator.clone().unwrap_or_default(),
        values: item
            .values
            .as_ref()
            .map(|values| values.iter().map(FilterValue::to_param_string).collect()),
    });
    Ok(())
}
