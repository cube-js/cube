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
    /// `FullKeyAggregateQueryPlanner`. Owns the local `CteState`,
    /// drives sub-planners into it and returns a `LogicalPlan` bundling
    /// the CTE bodies with the root Query.
    pub fn plan(&self) -> Result<Rc<LogicalPlan>, CubeError> {
        let mut cte_state = CteState::new();
        let root = if self.request.is_simple_query()? {
            let planner = SimpleQueryPlanner::new(self.query_tools.clone(), self.request.clone());
            planner.plan(&mut cte_state)?
        } else {
            let request = self.request.clone();
            let multi_stage_query_planner =
                MultiStageQueryPlanner::new(self.query_tools.clone(), request.clone());
            if self.request.allow_multi_stage() {
                multi_stage_query_planner.plan_queries(&mut cte_state)?;
            }

            let multiplied_measures_query_planner =
                MultipliedMeasuresQueryPlanner::try_new(self.query_tools.clone(), request.clone())?;
            multiplied_measures_query_planner.plan_queries(&mut cte_state)?;

            // Refs accumulated in this scope are the FK data inputs of the
            // root Query; members stay in `cte_state` and surface as the
            // LogicalPlan's CTE pool below.
            let all_refs = cte_state.drain_subquery_refs_from(0);

            let full_key_aggregate_planner = FullKeyAggregateQueryPlanner::new(request.clone());
            full_key_aggregate_planner.plan_logical_plan(all_refs)?
        };
        let (ctes, _) = cte_state.into_results();
        Ok(LogicalPlan::new(ctes, root))
    }
}
