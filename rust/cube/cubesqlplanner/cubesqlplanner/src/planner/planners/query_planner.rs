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

    /// Top-level entry: owns a fresh `CteState`, runs `plan_into`, and
    /// packs the accumulated CTE bodies with the produced root into a
    /// `LogicalPlan`.
    pub fn plan(&self) -> Result<Rc<LogicalPlan>, CubeError> {
        let mut cte_state = CteState::new();
        let root = self.plan_into(&mut cte_state)?;
        let (ctes, _) = cte_state.into_results();
        Ok(LogicalPlan::new(ctes, root))
    }

    /// Sub-plan entry: drives sub-planners into the caller-owned
    /// `cte_state` and returns just the root Query. Used when this
    /// plan's CTEs should flatten into the surrounding `LogicalPlan`'s
    /// pool (DSQ body, multi-stage leaf body).
    ///
    /// Subquery refs added during the call are pulled out via
    /// `drain_subquery_refs_from(baseline)` and become the FK data
    /// inputs of the returned root Query — they don't leak to the
    /// outer scope.
    pub fn plan_into(&self, cte_state: &mut CteState) -> Result<Rc<Query>, CubeError> {
        if self.request.is_simple_query()? {
            let planner = SimpleQueryPlanner::new(self.query_tools.clone(), self.request.clone());
            return planner.plan(cte_state);
        }
        let request = self.request.clone();
        let refs_baseline = cte_state.subquery_refs_len();

        let multi_stage_query_planner =
            MultiStageQueryPlanner::new(self.query_tools.clone(), request.clone());
        if self.request.allow_multi_stage() {
            multi_stage_query_planner.plan_queries(cte_state)?;
        }

        let multiplied_measures_query_planner =
            MultipliedMeasuresQueryPlanner::try_new(self.query_tools.clone(), request.clone())?;
        multiplied_measures_query_planner.plan_queries(cte_state)?;

        let all_refs = cte_state.drain_subquery_refs_from(refs_baseline);

        let full_key_aggregate_planner =
            FullKeyAggregateQueryPlanner::new(self.query_tools.clone(), request.clone());
        full_key_aggregate_planner.plan_logical_plan(all_refs, cte_state)
    }
}
