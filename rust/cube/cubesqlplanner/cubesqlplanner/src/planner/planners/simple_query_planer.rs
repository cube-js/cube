use super::{DimensionSubqueryPlanner, JoinPlanner, RequestQuery};
use crate::logical_plan::*;
use crate::planner::collectors::{collect_join_hints, collect_sub_query_dimensions_from_symbols};
use crate::planner::join_hints::JoinHints;
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
    join_planner: JoinPlanner,
}
impl SimpleQueryPlanner {
    pub fn new(query_tools: Rc<State>, analysis: Rc<QueryAnalysis>) -> Self {
        Self {
            join_planner: JoinPlanner::new(query_tools.clone()),
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

        let request_query = RequestQuery::new(&self.analysis);
        let schema = request_query.schema();
        let source = match scope.multi_stage_dimension_input(&schema)? {
            Some(input) => source.add_dimension_inputs(vec![input]),
            None => source,
        };
        Ok(request_query.build(schema, source))
    }

    /// Resolves the query's join and the sub-query dimensions that
    /// plug into it.
    pub fn source_and_subquery_dimensions(
        &self,
        scope: &mut PlanningScope,
    ) -> Result<Rc<QuerySource>, CubeError> {
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
        let source = if let Some(join) = join {
            self.join_planner.make_query_inputs(join)
        } else if !subquery_joins.is_empty() {
            // No selected member resolves the base cube, but the sub-query joins'
            // ON conditions reference it (e.g. `SELECT t.col FROM Cube JOIN (...) t
            // ON Cube.x = t.x`). Derive the join root from those ON references so
            // the base cube — and the sub-query joins below — are emitted.
            let mut hints = JoinHints::new();

            for subquery_join in subquery_joins {
                for dep in subquery_join.on_sql.get_dependencies() {
                    hints.extend(&collect_join_hints(&dep)?);
                }
            }

            if hints.is_empty() {
                return Err(CubeError::user(
                    "Sub-query join requires its ON condition to reference at least one \
                     cube member so the base table can be resolved \
                     (e.g. `{cube.field} = alias.field`)"
                        .to_string(),
                ));
            }

            self.join_planner.make_query_inputs_with_join_hints(hints)?
        } else {
            self.join_planner.make_empty_query_inputs()
        };
        let source = if subquery_joins.is_empty() {
            source
        } else {
            source.with_subquery_joins(subquery_joins.clone())
        };
        Ok(QuerySource::new(source).add_dimension_inputs(dimension_inputs))
    }
}
