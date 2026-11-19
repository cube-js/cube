use super::{
    MergeQueryPlanner, MultiStageQueryPlanner, MultipliedMeasuresQueryPlanner, SimpleQueryPlanner,
};
use crate::logical_plan::*;
use crate::planner::planners::multi_stage::{EvaluationContext, PlanningScope};
use crate::planner::shared_dimensions::{bind_shared_dimensions, SharedDimensionBinding};
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
        match bind_shared_dimensions(&self.request)? {
            SharedDimensionBinding::None => self.plan_request(self.request.clone(), scope),
            SharedDimensionBinding::Single(request) => self.plan_request(request, scope),
            SharedDimensionBinding::Split(legs) => self.plan_legs(legs, scope),
        }
    }

    /// Plans each leg of a query whose measures read shared view dimensions
    /// through different paths as a subquery of its own, and merges them on
    /// the dimensions, which every leg names alike.
    fn plan_legs(
        &self,
        legs: Vec<Rc<QueryProperties>>,
        scope: &mut PlanningScope,
    ) -> Result<Rc<Query>, CubeError> {
        let mut refs = Vec::new();
        for leg in legs {
            let query = QueryPlanner::new(leg, self.query_tools.clone()).plan(scope)?;
            let (subquery_id, subquery_name) = scope.next_subquery();
            let leaf = query.with_evaluation_context(Some(EvaluationContext::default()));
            scope.add_member(Rc::new(Subquery::new(
                subquery_id,
                subquery_name.clone(),
                SubqueryBody::Query(leaf),
            )))?;
            refs.push(Rc::new(
                SubqueryRef::builder()
                    .id(subquery_id)
                    .name(subquery_name)
                    .schema(query.schema().clone())
                    .build(),
            ));
        }
        let analysis = QueryAnalysis::try_new(self.request.clone())?;
        MergeQueryPlanner::new(analysis).plan_logical_plan(refs, scope)
    }

    fn plan_request(
        &self,
        request: Rc<QueryProperties>,
        scope: &mut PlanningScope,
    ) -> Result<Rc<Query>, CubeError> {
        let analysis = QueryAnalysis::try_new(request.clone())?;
        if analysis.is_simple() {
            let planner = SimpleQueryPlanner::new(self.query_tools.clone(), analysis);
            planner.plan(scope)
        } else {
            let mut refs = Vec::new();

            let multi_stage_query_planner =
                MultiStageQueryPlanner::try_new(self.query_tools.clone(), &analysis)?;
            if request.allow_multi_stage() {
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
}
