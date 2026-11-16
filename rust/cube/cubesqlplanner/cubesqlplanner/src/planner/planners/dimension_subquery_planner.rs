use super::{CommonUtils, JoinTreeBuilder, QueryPlanner};
use crate::logical_plan::{DimensionInput, LogicalSchema};
use crate::planner::collectors::{
    collect_cube_names, collect_join_hints, collect_sub_query_dimensions,
};
use crate::planner::filter::FilterItem;
use crate::planner::planners::multi_stage::PlanningScope;
use crate::planner::state::State;
use crate::planner::{CubeId, QueryProperties};
use crate::planner::{MemberExpressionExpression, MemberExpressionSymbol, MemberSymbol};
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::rc::Rc;

/// Plans `sub_query: true` dimensions. Each one becomes an inline subquery
/// grouping the owning cube's primary keys with the dimension's expression as
/// a measure, read by the host query as a dimension input on those keys.
pub struct DimensionSubqueryPlanner {
    utils: CommonUtils,
    query_tools: Rc<State>,
    query_properties: Rc<QueryProperties>,
}

impl DimensionSubqueryPlanner {
    pub fn new(query_tools: Rc<State>, query_properties: Rc<QueryProperties>) -> Self {
        Self {
            utils: CommonUtils::new(query_tools.clone()),
            query_tools,
            query_properties,
        }
    }

    pub fn plan_queries(
        &self,
        dimensions: &Vec<Rc<MemberSymbol>>,
        scope: &mut PlanningScope,
    ) -> Result<Vec<DimensionInput>, CubeError> {
        let mut result = Vec::new();
        for subquery_dimension in dimensions.iter() {
            result.push(self.plan_query(subquery_dimension.clone(), scope)?)
        }
        Ok(result)
    }

    fn plan_query(
        &self,
        subquery_dimension: Rc<MemberSymbol>,
        scope: &mut PlanningScope,
    ) -> Result<DimensionInput, CubeError> {
        let dim_name = subquery_dimension.name();
        let cube_name = subquery_dimension.cube_id().clone();
        let dimension_symbol = subquery_dimension.as_dimension()?;

        let primary_keys_dimensions = self.utils.primary_keys_dimensions(&cube_name)?;

        let expression = if let Some(sql_call) = dimension_symbol.member_sql() {
            sql_call.clone()
        } else {
            return Err(CubeError::user(format!(
                "Subquery dimension {} must have `sql` field",
                subquery_dimension.full_name()
            )));
        };

        let cube_symbol = self
            .query_tools
            .compiler()
            .borrow_mut()
            .add_cube_table_evaluator(cube_name.clone(), vec![])?;
        let member_expression_symbol = MemberExpressionSymbol::try_new(
            cube_symbol,
            dim_name.clone(),
            MemberExpressionExpression::SqlCall(expression),
            None,
            None,
            vec![cube_name.clone()],
        )?;
        let measure = MemberSymbol::new_member_expression(member_expression_symbol);

        let (dimensions_filters, time_dimensions_filters) =
            if dimension_symbol.propagate_filters_to_sub_query() {
                self.propagated_filters(&subquery_dimension, &measure, &primary_keys_dimensions)?
            } else {
                (vec![], vec![])
            };

        let sub_query_properties = QueryProperties::builder()
            .query_tools(self.query_tools.clone())
            .measures(vec![measure])
            .dimensions(primary_keys_dimensions.clone())
            .time_dimensions_filters(time_dimensions_filters)
            .dimensions_filters(dimensions_filters)
            // Aggregated per primary key and joined back by it — order is
            // meaningless and a default one breaks targets that require
            // OFFSET/FETCH (e.g. MSSQL) when this subquery sits in a derived table.
            .order_by(vec![])
            .ignore_cumulative(true)
            .build()?;
        let query_planner = QueryPlanner::new(sub_query_properties, self.query_tools.clone());
        let sub_query = query_planner.plan(scope)?;

        let owner_alias = self.query_tools.alias_for_cube(&cube_name)?;
        let name = format!("{owner_alias}_{dim_name}_subquery");
        let schema = LogicalSchema::default()
            .set_dimensions(vec![subquery_dimension])
            .into_rc();
        Ok(DimensionInput {
            subquery: scope.add_inline(name, sub_query, schema)?,
            keys: primary_keys_dimensions,
        })
    }

    // Filters the sub-query can evaluate: none reads a sub-query dimension, and
    // none needs a join whose condition reads this very dimension.
    fn propagated_filters(
        &self,
        subquery_dimension: &Rc<MemberSymbol>,
        measure: &Rc<MemberSymbol>,
        primary_keys: &[Rc<MemberSymbol>],
    ) -> Result<(Vec<FilterItem>, Vec<FilterItem>), CubeError> {
        let mut dimensions_filters =
            self.filters_without_subqueries(self.query_properties.dimensions_filters())?;
        let mut time_dimensions_filters =
            self.filters_without_subqueries(self.query_properties.time_dimensions_filters())?;
        loop {
            let cyclic_cubes = self.cubes_joined_through(
                subquery_dimension,
                measure,
                primary_keys,
                dimensions_filters
                    .iter()
                    .chain(time_dimensions_filters.iter()),
            )?;
            if cyclic_cubes.is_empty() {
                break;
            }
            let count = dimensions_filters.len() + time_dimensions_filters.len();
            dimensions_filters = Self::filters_outside(dimensions_filters, &cyclic_cubes)?;
            time_dimensions_filters =
                Self::filters_outside(time_dimensions_filters, &cyclic_cubes)?;
            if dimensions_filters.len() + time_dimensions_filters.len() == count {
                break;
            }
        }
        Ok((dimensions_filters, time_dimensions_filters))
    }

    fn filters_without_subqueries(
        &self,
        filters: &Vec<FilterItem>,
    ) -> Result<Vec<FilterItem>, CubeError> {
        let mut result = vec![];
        for item in filters.iter() {
            let mut has_subqueries = false;
            for member in Self::filter_members(item) {
                has_subqueries |= !collect_sub_query_dimensions(&member)?.is_empty();
            }
            if !has_subqueries {
                result.push(item.clone());
            }
        }
        Ok(result)
    }

    fn filters_outside(
        filters: Vec<FilterItem>,
        cubes: &HashSet<CubeId>,
    ) -> Result<Vec<FilterItem>, CubeError> {
        let mut result = vec![];
        for item in filters {
            let mut inside = false;
            for member in Self::filter_members(&item) {
                inside |= collect_cube_names(&member)?
                    .iter()
                    .any(|cube| cubes.contains(cube));
            }
            if !inside {
                result.push(item);
            }
        }
        Ok(result)
    }

    // Cubes the sub-query's join reaches only through an edge whose condition
    // reads `subquery_dimension`.
    fn cubes_joined_through<'a>(
        &self,
        subquery_dimension: &Rc<MemberSymbol>,
        measure: &Rc<MemberSymbol>,
        primary_keys: &[Rc<MemberSymbol>],
        filters: impl Iterator<Item = &'a FilterItem>,
    ) -> Result<HashSet<CubeId>, CubeError> {
        let mut hints = collect_join_hints(measure)?;
        for key in primary_keys {
            hints.extend(&collect_join_hints(key)?);
        }
        for item in filters {
            for member in Self::filter_members(item) {
                hints.extend(&collect_join_hints(&member)?);
            }
        }
        let (_, join) = JoinTreeBuilder::new(self.query_tools.clone()).build_for_hints(&hints)?;

        let mut cubes = HashSet::new();
        for item in join.joins() {
            for dep in item.on_sql().get_dependencies() {
                if collect_sub_query_dimensions(&dep)?
                    .iter()
                    .any(|dim| dim.id() == subquery_dimension.id())
                {
                    cubes.insert(item.cube().cube_id().clone());
                }
            }
        }
        let mut grown = !cubes.is_empty();
        while grown {
            grown = false;
            for item in join.joins() {
                if cubes.contains(item.original_from()) {
                    grown |= cubes.insert(item.cube().cube_id().clone());
                }
            }
        }
        Ok(cubes)
    }

    fn filter_members(item: &FilterItem) -> Vec<Rc<MemberSymbol>> {
        match item {
            FilterItem::Group(group) => group.items.iter().flat_map(Self::filter_members).collect(),
            FilterItem::Item(filter_item) => vec![filter_item.member_evaluator().clone()],
            FilterItem::Segment(filter_item) => vec![filter_item.member_evaluator().clone()],
        }
    }
}
