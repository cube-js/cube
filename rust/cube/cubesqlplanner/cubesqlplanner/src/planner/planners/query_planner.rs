use super::{
    FullKeyAggregateQueryPlanner, MultiStageQueryPlanner, MultipliedMeasuresQueryPlanner,
    SimpleQueryPlanner,
};
use crate::logical_plan::*;
use crate::planner::planners::multi_stage::CteState;
use crate::planner::query_tools::QueryTools;
use crate::planner::QueryProperties;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Entry point of the logical-plan construction. Dispatches by
/// `QueryProperties::is_simple_query()`: simple queries go through
/// `SimpleQueryPlanner`; everything else is built up from
/// multi-stage and multiplied-measure CTEs (`MultiStageQueryPlanner`,
/// `MultipliedMeasuresQueryPlanner`) and stitched together by
/// `FullKeyAggregateQueryPlanner`.
pub struct QueryPlanner {
    query_tools: Rc<QueryTools>,
    request: Rc<QueryProperties>,
}

impl QueryPlanner {
    pub fn new(request: Rc<QueryProperties>, query_tools: Rc<QueryTools>) -> Self {
        Self {
            request,
            query_tools,
        }
    }

    /// Dispatches to `SimpleQueryPlanner` for simple queries; otherwise
    /// builds the multi-stage / multiplied CTEs and assembles them via
    /// `FullKeyAggregateQueryPlanner`.
    ///
    /// CTE members are registered into the caller-provided `cte_state`
    /// (one instance per plan, shared with nested planning scopes), so
    /// the returned `Query` is the query body only — the accumulated
    /// CTE list is attached at the top by `RootQuery`. The subquery
    /// refs the planners return are consumed right here by this
    /// query's `FullKeyAggregate`; they never leak between scopes.
    pub fn plan(&self, cte_state: &mut CteState) -> Result<Rc<Query>, CubeError> {
        if self.request.is_simple_query()? {
            let planner = SimpleQueryPlanner::new(self.query_tools.clone(), self.request.clone());
            planner.plan(cte_state)
        } else {
            let request = self.request.clone();
            let mut refs = Vec::new();

            let multi_stage_query_planner =
                MultiStageQueryPlanner::try_new(self.query_tools.clone(), request.clone())?;
            if self.request.allow_multi_stage() {
                refs.extend(multi_stage_query_planner.plan_queries(cte_state)?);
            }

            let multiplied_measures_query_planner =
                MultipliedMeasuresQueryPlanner::try_new(self.query_tools.clone(), request.clone())?;
            refs.extend(multiplied_measures_query_planner.plan_queries(cte_state)?);

            let full_key_aggregate_planner = FullKeyAggregateQueryPlanner::new(request.clone());
            let result = full_key_aggregate_planner.plan_logical_plan(refs)?;

            Ok(result)
        }
    }
}
