use super::{CommonUtils, QueryPlanner};
use crate::logical_plan::{
    LogicalMultiStageMember, MultiStageDimensionJoin, MultiStageDimensionRef, PlanNode,
};
use crate::planner::collectors::collect_sub_query_dimensions;
use crate::planner::filter::FilterItem;
use crate::planner::planners::multi_stage::CteState;
use crate::planner::query_tools::QueryTools;
use crate::planner::QueryProperties;
use crate::planner::{MemberExpressionExpression, MemberExpressionSymbol, MemberSymbol};
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

/// Plans `MultiStageDimensionRef` CTEs for `sub_query: true` dimensions.
/// Each subquery dimension becomes its own `LogicalPlan` over the owning
/// cube's primary keys plus the dimension's measure expression; the
/// reference carries an `OnPrimaryKeys` join descriptor so consumers can
/// stitch the CTE back into the host query.
pub struct DimensionSubqueryPlanner {
    utils: CommonUtils,
    query_tools: Rc<QueryTools>,
    query_properties: Rc<QueryProperties>,
    sub_query_dims: HashMap<String, Vec<Rc<MemberSymbol>>>,
}

impl DimensionSubqueryPlanner {
    /// Planner with no sub-query dimensions — used when the host
    /// query has none.
    pub fn empty(query_tools: Rc<QueryTools>, query_properties: Rc<QueryProperties>) -> Self {
        Self {
            sub_query_dims: HashMap::new(),
            utils: CommonUtils::new(query_tools.clone()),
            query_tools,
            query_properties,
        }
    }
    /// Builds a planner over the given sub-query dimensions, indexed
    /// by owning cube.
    pub fn try_new(
        dimensions: &Vec<Rc<MemberSymbol>>,
        query_tools: Rc<QueryTools>,
        query_properties: Rc<QueryProperties>,
    ) -> Result<Self, CubeError> {
        let mut sub_query_dims: HashMap<String, Vec<Rc<MemberSymbol>>> = HashMap::new();
        for subquery_dimension in dimensions.iter() {
            let cube_name = subquery_dimension.cube_name().clone();
            sub_query_dims
                .entry(cube_name.clone())
                .or_default()
                .push(subquery_dimension.clone());
        }

        Ok(Self {
            sub_query_dims,
            utils: CommonUtils::new(query_tools.clone()),
            query_tools,
            query_properties,
        })
    }

    /// Build a `MultiStageDimensionRef` per subquery dim and publish the
    /// body of each one as a `LogicalMultiStageMember` on `cte_state`.
    /// The caller stores returned refs on `Query.multi_stage_dimensions`
    /// of the Query that consumes them; the QueryProcessor reads them
    /// from there to wire CTE joins and render references.
    pub fn plan_queries(
        &self,
        dimensions: &Vec<Rc<MemberSymbol>>,
        cte_state: &mut CteState,
    ) -> Result<Vec<Rc<MultiStageDimensionRef>>, CubeError> {
        let mut result = Vec::new();
        for subquery_dimension in dimensions.iter() {
            result.push(self.plan_query(subquery_dimension.clone(), cte_state)?);
        }
        Ok(result)
    }

    fn plan_query(
        &self,
        subquery_dimension: Rc<MemberSymbol>,
        cte_state: &mut CteState,
    ) -> Result<Rc<MultiStageDimensionRef>, CubeError> {
        let dim_name = subquery_dimension.name();
        let cube_name = subquery_dimension.cube_name().clone();
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
            .evaluator_compiler()
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
        let body_column = MemberSymbol::new_member_expression(member_expression_symbol);

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
            .measures(vec![body_column.clone()])
            .dimensions(primary_keys_dimensions.clone())
            .time_dimensions_filters(time_dimensions_filters)
            .dimensions_filters(dimensions_filters)
            .ignore_cumulative(true)
            .disable_external_pre_aggregations(
                self.query_properties.disable_external_pre_aggregations(),
            )
            .build()?;
        let query_planner = QueryPlanner::new(sub_query_properties, self.query_tools.clone());
        // The DSQ body itself surfaces on the outer top-level WITH via
        // `cte_state.add_member` below. Any CTEs it produces internally
        // (multiplied-measure keys/measure/agg-multiplied bodies) stay
        // bundled inside its own `LogicalPlan` so the pre-agg optimizer
        // treats the DSQ body as one rewrite unit.
        let body = query_planner.plan()?;

        // CTE name uses only `(cube, dim)`. Top-level deduplication relies on
        // the assumption that within one outer query the same `(cube, dim)`
        // pair maps to one body — the only inputs to `sub_query_properties`
        // here come from the outer `query_properties`, which is constant for
        // every call site, plus `propagate_filters_to_sub_query` which is a
        // dimension-level setting. If a future caller starts varying body
        // semantics for the same pair (e.g. per-call-site `time_shifts`),
        // the name needs an extra discriminator.
        let cte_name = format!("{}_{}_dimension_subquery", cube_name, dim_name);
        let PlanNode::Query(root_query) = body.root() else {
            return Err(CubeError::internal(format!(
                "DSQ body root must be a Query, got {}",
                body.root().node_name()
            )));
        };
        let schema = root_query.schema().clone();
        cte_state.add_member(Rc::new(LogicalMultiStageMember {
            name: cte_name.clone(),
            body,
        }));

        Ok(Rc::new(MultiStageDimensionRef {
            name: cte_name,
            schema,
            join: MultiStageDimensionJoin::OnPrimaryKeys {
                cube_name,
                pk_dimensions: primary_keys_dimensions,
            },
            exposed: subquery_dimension,
            body_column,
        }))
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

    pub fn is_empty(&self) -> bool {
        self.sub_query_dims.is_empty()
    }
}
