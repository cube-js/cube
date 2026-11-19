use super::{CommonUtils, QueryPlanner};
use crate::logical_plan::{DimensionInput, LogicalSchema, Query};
use crate::planner::collectors::collect_sub_query_dimensions;
use crate::planner::filter::{FilterGroupOperator, FilterItem};
use crate::planner::planners::multi_stage::PlanningScope;
use crate::planner::state::State;
use crate::planner::QueryProperties;
use crate::planner::{MemberExpressionExpression, MemberExpressionSymbol, MemberSymbol};
use cubenativeutils::CubeError;
use itertools::Itertools;
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

        let mut filters = vec![];
        if dimension_symbol.propagate_filters_to_sub_query() {
            for item in
                self.extract_filters_without_subqueries(self.query_properties.dimensions_filters())?
            {
                filters.push(PropagatedFilter::Dimension(item));
            }
            for item in self.extract_filters_without_subqueries(
                self.query_properties.time_dimensions_filters(),
            )? {
                filters.push(PropagatedFilter::TimeDimension(item));
            }
        }

        let sub_query = self.plan_sub_query_without_cycles(
            &subquery_dimension,
            &measure,
            &primary_keys_dimensions,
            filters,
            scope,
        )?;

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

    // Plans the sub-query with every propagated filter. If that plans this very
    // dimension with the same filters again, some filter's join reads it: the
    // filters are then added one by one, keeping those the sub-query plans with.
    fn plan_sub_query_without_cycles(
        &self,
        subquery_dimension: &Rc<MemberSymbol>,
        measure: &Rc<MemberSymbol>,
        primary_keys: &[Rc<MemberSymbol>],
        filters: Vec<PropagatedFilter>,
        scope: &mut PlanningScope,
    ) -> Result<Rc<Query>, CubeError> {
        let checkpoint = scope.checkpoint();
        let (result, cyclic) =
            self.plan_sub_query(subquery_dimension, measure, primary_keys, &filters, scope);
        if !(result.is_err() && cyclic) {
            return result;
        }
        scope.restore(checkpoint);

        let mut kept = vec![];
        let mut pending = filters;
        loop {
            let count = pending.len();
            let mut skipped = vec![];
            for filter in pending {
                kept.push(filter);
                let checkpoint = scope.checkpoint();
                let (probe, _) =
                    self.plan_sub_query(subquery_dimension, measure, primary_keys, &kept, scope);
                scope.restore(checkpoint);
                if probe.is_err() {
                    skipped.extend(kept.pop());
                }
            }
            // A filter skipped alone may still plan next to one kept later.
            if skipped.is_empty() || skipped.len() == count {
                break;
            }
            pending = skipped;
        }

        let (result, cyclic) =
            self.plan_sub_query(subquery_dimension, measure, primary_keys, &kept, scope);
        if result.is_err() && cyclic {
            return Err(CubeError::user(format!(
                "Sub-query dimension {} can't be computed: its own sub-query needs a join that reads it",
                subquery_dimension.full_name()
            )));
        }
        result
    }

    // Also tells whether planning hit this dimension with the same filters.
    fn plan_sub_query(
        &self,
        subquery_dimension: &Rc<MemberSymbol>,
        measure: &Rc<MemberSymbol>,
        primary_keys: &[Rc<MemberSymbol>],
        filters: &[PropagatedFilter],
        scope: &mut PlanningScope,
    ) -> (Result<Rc<Query>, CubeError>, bool) {
        let mut dimensions_filters = vec![];
        let mut time_dimensions_filters = vec![];
        let mut keys = vec![];
        for filter in filters {
            match filter {
                PropagatedFilter::Dimension(item) => {
                    keys.push(format!("d{}", filter_key(item)));
                    dimensions_filters.push(item.clone());
                }
                PropagatedFilter::TimeDimension(item) => {
                    keys.push(format!("t{}", filter_key(item)));
                    time_dimensions_filters.push(item.clone());
                }
            }
        }
        scope.with_sub_query_dimension(subquery_dimension.id(), keys.join(";"), |scope| {
            let sub_query_properties = QueryProperties::builder()
                .query_tools(self.query_tools.clone())
                .measures(vec![measure.clone()])
                .dimensions(primary_keys.to_vec())
                .time_dimensions_filters(time_dimensions_filters)
                .dimensions_filters(dimensions_filters)
                // Aggregated per primary key and joined back by it — order is
                // meaningless and a default one breaks targets that require
                // OFFSET/FETCH (e.g. MSSQL) when this subquery sits in a derived table.
                .order_by(vec![])
                .ignore_cumulative(true)
                .build()?;
            QueryPlanner::new(sub_query_properties, self.query_tools.clone()).plan(scope)
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

enum PropagatedFilter {
    Dimension(FilterItem),
    TimeDimension(FilterItem),
}

fn filter_key(item: &FilterItem) -> String {
    match item {
        FilterItem::Group(group) => {
            let operator = match group.operator {
                FilterGroupOperator::And => "and",
                FilterGroupOperator::Or => "or",
            };
            format!(
                "{operator}({})",
                group.items.iter().map(filter_key).join(",")
            )
        }
        FilterItem::Item(filter) => format!(
            "{}:{:?}:{:?}",
            filter.member_evaluator().id(),
            filter.filter_operator(),
            filter.values()
        ),
        FilterItem::Segment(segment) => segment.id().to_string(),
    }
}
