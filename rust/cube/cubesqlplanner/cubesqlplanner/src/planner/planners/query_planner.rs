use super::{
    MergeQueryPlanner, MultiStageQueryPlanner, MultipliedMeasuresQueryPlanner, SimpleQueryPlanner,
};
use crate::logical_plan::*;
use crate::planner::planners::multi_stage::{EvaluationContext, PlanningScope};
use crate::planner::state::State;
use crate::planner::{QueryAnalysis, QueryProperties};
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Entry point of the logical-plan construction. Analyzes the query once and
/// dispatches by `QueryAnalysis::is_simple()`: simple queries go through
/// `SimpleQueryPlanner`; everything else is built up from
/// multi-stage and multiplied-measure subqueries (`MultiStageQueryPlanner`,
/// `MultipliedMeasuresQueryPlanner`) and stitched together by
/// `MergeQueryPlanner`.
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

    /// Dispatches to `SimpleQueryPlanner` for simple queries; otherwise
    /// builds the multi-stage / multiplied subqueries and assembles them via
    /// `MergeQueryPlanner`.
    ///
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
        } else if !analysis.override_legs().is_empty() {
            let refs = analysis
                .override_legs()
                .iter()
                .map(|leg| self.plan_override_leg(leg, scope))
                .collect::<Result<Vec<_>, _>>()?;
            MergeQueryPlanner::new(analysis).plan_logical_plan(refs, scope)
        } else {
            let mut refs = Vec::new();

            let multi_stage_query_planner =
                MultiStageQueryPlanner::try_new(self.query_tools.clone(), &analysis)?;
            if self.request.allow_multi_stage() {
                refs.extend(multi_stage_query_planner.plan_queries(scope)?);
            }

            let multiplied_measures_query_planner =
                MultipliedMeasuresQueryPlanner::new(self.query_tools.clone(), analysis.clone());
            refs.extend(multiplied_measures_query_planner.plan_queries(scope)?);

            let merge_planner = MergeQueryPlanner::new(analysis);
            let result = merge_planner.plan_logical_plan(refs, scope)?;

            Ok(result)
        }
    }

    fn plan_override_leg(
        &self,
        leg: &Rc<QueryProperties>,
        scope: &mut PlanningScope,
    ) -> Result<Rc<SubqueryRef>, CubeError> {
        let query = QueryPlanner::new(leg.clone(), self.query_tools.clone()).plan(scope)?;
        let (subquery_id, subquery_name) = scope.next_subquery();
        let member = Rc::new(Subquery::new(
            subquery_id,
            subquery_name.clone(),
            SubqueryBody::Query(query.with_evaluation_context(Some(EvaluationContext::default()))),
        ));
        scope.add_member(member)?;
        Ok(Rc::new(
            SubqueryRef::builder()
                .id(subquery_id)
                .name(subquery_name)
                .schema(query.schema().clone())
                .build(),
        ))
    }
}
