use super::{CommonUtils, JoinTreeBuilder, QueryPlanner};
use crate::logical_plan::{DimensionInput, DimensionInputKeys, LogicalSchema};
use crate::planner::collectors::{
    collect_join_hints, collect_sub_query_dimensions, collect_sub_query_dimensions_from_symbols,
};
use crate::planner::filter::FilterItem;
use crate::planner::planners::multi_stage::PlanningScope;
use crate::planner::state::State;
use crate::planner::QueryProperties;
use crate::planner::{MemberExpressionExpression, MemberExpressionSymbol, MemberSymbol};
use cubenativeutils::CubeError;
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
                let dimensions_filters = self.extract_propagated_filters(
                    &subquery_dimension,
                    self.query_properties.dimensions_filters(),
                )?;
                let time_dimensions_filters = self.extract_propagated_filters(
                    &subquery_dimension,
                    self.query_properties.time_dimensions_filters(),
                )?;
                (dimensions_filters, time_dimensions_filters)
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
            .order_by(Some(vec![]))
            .ignore_cumulative(true)
            .disable_external_pre_aggregations(
                self.query_properties.disable_external_pre_aggregations(),
            )
            .build()?;
        let query_planner = QueryPlanner::new(sub_query_properties, self.query_tools.clone());
        let sub_query = query_planner.plan(scope)?;

        let owner_alias = if cube_name.is_joined() {
            self.query_tools.alias_for_cube(&cube_name)?
        } else {
            cube_name.to_string()
        };
        let name = format!("{owner_alias}_{dim_name}_subquery");
        let schema = LogicalSchema::default()
            .set_dimensions(vec![subquery_dimension])
            .into_rc();
        Ok(DimensionInput {
            subquery: scope.add_inline_query(name, sub_query, schema)?,
            keys: primary_keys_dimensions,
            keys_kind: DimensionInputKeys::PrimaryKeys,
        })
    }

    fn extract_propagated_filters(
        &self,
        subquery_dimension: &Rc<MemberSymbol>,
        filters: &Vec<FilterItem>,
    ) -> Result<Vec<FilterItem>, CubeError> {
        let mut result = vec![];
        for item in filters.iter() {
            if self.is_propagatable_filter(subquery_dimension, item)? {
                result.push(item.clone());
            }
        }

        Ok(result)
    }

    // A filter can be pushed into the sub-query unless it reads a sub-query
    // dimension or needs a join whose condition reads this very dimension.
    fn is_propagatable_filter(
        &self,
        subquery_dimension: &Rc<MemberSymbol>,
        filter_item: &FilterItem,
    ) -> Result<bool, CubeError> {
        let member = match filter_item {
            FilterItem::Group(group) => {
                for item in group.items.iter() {
                    if !self.is_propagatable_filter(subquery_dimension, item)? {
                        return Ok(false);
                    }
                }
                return Ok(true);
            }
            FilterItem::Item(filter_item) => filter_item.member_evaluator(),
            FilterItem::Segment(filter_item) => filter_item.member_evaluator(),
        };
        if !collect_sub_query_dimensions(&member)?.is_empty() {
            return Ok(false);
        }
        Ok(!self.join_reads_dimension(subquery_dimension, &member)?)
    }

    fn join_reads_dimension(
        &self,
        subquery_dimension: &Rc<MemberSymbol>,
        filter_member: &Rc<MemberSymbol>,
    ) -> Result<bool, CubeError> {
        let mut hints = collect_join_hints(subquery_dimension)?;
        hints.extend(&collect_join_hints(filter_member)?);
        let (_, join) = JoinTreeBuilder::new(self.query_tools.clone()).build_for_hints(&hints)?;
        let join_sub_query_dimensions = collect_sub_query_dimensions_from_symbols(&vec![], &join)?;
        Ok(join_sub_query_dimensions
            .iter()
            .any(|dim| dim.id() == subquery_dimension.id()))
    }
}
