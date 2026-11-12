use super::{
    MergeQueryPlanner, MultiStageQueryPlanner, MultipliedGroupPlanner, RegularGroupPlanner,
    SimpleQueryPlanner,
};
use crate::logical_plan::*;
use crate::planner::planners::multi_stage::PlanningScope;
use crate::planner::state::State;
use crate::planner::{QueryAnalysis, QueryProperties};
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Entry point of the logical-plan construction. Analyzes the query once: a
/// simple query is planned by `SimpleQueryPlanner`, any other one as a merge of
/// the subqueries of its multi-stage measures and of its measure groups.
pub struct QueryPlanner {
    query_tools: Rc<State>,
    request: Rc<QueryProperties>,
}

impl QueryPlanner {
    pub fn new(request: Rc<QueryProperties>, query_tools: Rc<State>) -> Self {
        Self {
            request,
            query_tools,
        }
    }

    /// Subqueries are registered into the caller-provided `scope`
    /// (one instance per plan, shared with nested planning scopes), so
    /// the returned `Query` is the query body only — the accumulated
    /// subquery list is attached at the top by `RootQuery`. The subquery
    /// refs the planners return are consumed right here by this
    /// query's `Merge`; they never leak between scopes.
    pub fn plan(&self, scope: &mut PlanningScope) -> Result<Rc<Query>, CubeError> {
        let analysis = QueryAnalysis::try_new(self.request.clone())?;
        if analysis.is_simple() {
            let planner = SimpleQueryPlanner::new(self.query_tools.clone(), analysis);
            planner.plan(scope)
        } else {
            let mut refs = Vec::new();

            let multi_stage_query_planner =
                MultiStageQueryPlanner::try_new(self.query_tools.clone(), &analysis)?;
            if self.request.allow_multi_stage() {
                refs.extend(multi_stage_query_planner.plan_queries(scope)?);
            }

            let groups = analysis.measure_groups()?;
            let regular_planner =
                RegularGroupPlanner::new(self.query_tools.clone(), analysis.clone());
            for group in groups.regular() {
                refs.push(regular_planner.plan(group, scope)?);
            }
            let multiplied_planner =
                MultipliedGroupPlanner::new(self.query_tools.clone(), analysis.clone());
            for group in groups.multiplied() {
                refs.push(multiplied_planner.plan(group, scope)?);
            }

            let merge_planner = MergeQueryPlanner::new(analysis);
            let result = merge_planner.plan_logical_plan(refs, scope)?;

            Ok(result)
        }
    }
}
