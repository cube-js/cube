use super::{CommonUtils, QueryPlanner};
use crate::logical_plan::{
    DimensionInput, DimensionInputKeys, LogicalSchema, Subquery, SubqueryBody, SubqueryRef,
};
use crate::planner::collectors::collect_sub_query_dimensions;
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
            Some(subquery_dimension.alias()),
            vec![cube_name.clone()],
        )?;
        let measure = MemberSymbol::new_member_expression(member_expression_symbol);

        let (dimensions_filters, time_dimensions_filters) = if dimension_symbol
            .propagate_filters_to_sub_query()
        {
            let dimensions_filters = self
                .extract_filters_without_subqueries(self.query_properties.dimensions_filters())?;
            let time_dimensions_filters = self.extract_filters_without_subqueries(
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
        let id = scope.next_input_id();
        let subquery = Rc::new(Subquery::new_inline(
            id,
            name.clone(),
            SubqueryBody::LeafMeasure(sub_query),
        ));
        scope.add_member(subquery)?;

        let schema = LogicalSchema::default()
            .set_dimensions(vec![subquery_dimension])
            .into_rc();
        Ok(DimensionInput {
            subquery: Rc::new(
                SubqueryRef::builder()
                    .id(id)
                    .name(name)
                    .schema(schema)
                    .build(),
            ),
            keys: primary_keys_dimensions,
            keys_kind: DimensionInputKeys::PrimaryKeys,
        })
    }

    fn extract_filters_without_subqueries(
        &self,
        filters: &Vec<FilterItem>,
    ) -> Result<Vec<FilterItem>, CubeError> {
        let mut result = vec![];
        for item in filters.iter() {
            if self.is_filter_without_subqueries(item)? {
                result.push(item.clone());
            }
        }

        Ok(result)
    }

    fn is_filter_without_subqueries(&self, filter_item: &FilterItem) -> Result<bool, CubeError> {
        match filter_item {
            FilterItem::Group(group) => {
                for item in group.items.iter() {
                    if !self.is_filter_without_subqueries(item)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            FilterItem::Item(filter_item) => {
                Ok(collect_sub_query_dimensions(&filter_item.member_evaluator())?.is_empty())
            }
            FilterItem::Segment(filter_item) => {
                Ok(collect_sub_query_dimensions(&filter_item.member_evaluator())?.is_empty())
            }
        }
    }
}
