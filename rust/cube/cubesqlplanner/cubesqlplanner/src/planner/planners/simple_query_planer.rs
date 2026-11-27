use super::DimensionSubqueryPlanner;
use crate::logical_plan::*;
use crate::planner::collectors::collect_sub_query_dimensions_from_symbols;
use crate::planner::planners::multi_stage::PlanningScope;
use crate::planner::state::State;
use crate::planner::{QueryAnalysis, QueryProperties};
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Plans a `Query` for the simple case: a single join of cubes as its
/// source, no multi-stage or multiplied subqueries. Sub-query dimensions
/// are still woven into the join.
pub struct SimpleQueryPlanner {
    query_tools: Rc<State>,
    analysis: Rc<QueryAnalysis>,
}
impl SimpleQueryPlanner {
    pub fn new(query_tools: Rc<State>, analysis: Rc<QueryAnalysis>) -> Self {
        Self {
            analysis,
            query_tools,
        }
    }

    pub fn try_new_analyzed(
        query_tools: Rc<State>,
        query_properties: Rc<QueryProperties>,
    ) -> Result<Self, CubeError> {
        Ok(Self::new(
            query_tools,
            QueryAnalysis::try_new(query_properties)?,
        ))
    }

    /// Builds the `Query` for a simple-case request.
    pub fn plan(&self, scope: &mut PlanningScope) -> Result<Rc<Query>, CubeError> {
        let source = self.source_and_subquery_dimensions(scope)?;

        let schema = LogicalSchema::default()
            .set_dimensions(self.analysis.query().dimensions().clone())
            .set_measures(self.analysis.select_measures().to_vec())
            .set_time_dimensions(self.analysis.query().time_dimensions().clone())
            .into_rc();
        let (from, join) = source
            .grid_input(scope.multi_stage_dimension_input(&schema)?)
            .build()?;
        let logical_filter = Rc::new(LogicalFilter {
            dimensions_filters: self.analysis.query().dimensions_filters().clone(),
            time_dimensions_filters: self.analysis.query().time_dimensions_filters().clone(),
            measures_filter: self.analysis.query().measures_filters().clone(),
            segments: self.analysis.query().segments().clone(),
        });
        Query::builder()
            .schema(schema)
            .filter(logical_filter)
            .modifers(Rc::new(LogicalQueryModifiers {
                offset: self.analysis.query().offset(),
                limit: self.analysis.query().row_limit(),
                ungrouped: self.analysis.query().ungrouped(),
                distinct: false,
                order_by: self.analysis.query().order_by().to_vec(),
            }))
            .from(from)
            .join(join)
            .build()
    }

    /// Resolves the query's join and the sub-query dimensions that
    /// plug into it.
    pub fn source_and_subquery_dimensions(
        &self,
        scope: &mut PlanningScope,
    ) -> Result<LogicalFromBuilder, CubeError> {
        let join = self.analysis.simple_query_join()?;
        let subquery_dimensions = if let Some(join) = &join {
            collect_sub_query_dimensions_from_symbols(
                &self
                    .analysis
                    .query()
                    .get_member_symbols(true, true, true, true, &vec![]),
                join,
            )?
        } else {
            vec![]
        };
        let dimension_inputs =
            DimensionSubqueryPlanner::new(self.query_tools.clone(), self.analysis.query().clone())
                .plan_queries(&subquery_dimensions, scope)?;
        // Query-level SQL-API sub-query joins (from `subqueryJoins`) extend the
        // FROM clause with opaque joined sub-queries.
        let subquery_joins = self.analysis.query().subquery_joins();
        // The ON conditions' members are join hints too, so a join is missing only
        // when they reference no cube member at all.
        if join.is_none() && !subquery_joins.is_empty() {
            return Err(CubeError::user(
                "Sub-query join requires its ON condition to reference at least one \
                 cube member so the base table can be resolved \
                 (e.g. `{cube.field} = alias.field`)"
                    .to_string(),
            ));
        }
        Ok(LogicalFrom::over_join(join)
            .dimension_inputs(dimension_inputs)
            .subquery_joins(subquery_joins.clone()))
    }
}
