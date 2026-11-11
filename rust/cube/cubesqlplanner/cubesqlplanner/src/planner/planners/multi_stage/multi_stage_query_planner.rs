use super::inode::InodeStagePlanner;
use super::leaf::LeafStagePlanner;
use super::rolling_bounds::RollingWindowBounds;
use super::rolling_window::RollingWindowStagePlanner;
use super::{check_multi_stage_stages, PlanningScope, StageRef, StageRegistry};
use crate::logical_plan::*;
use crate::planner::collectors::has_multi_stage_members;
use crate::planner::state::State;
use crate::planner::symbols::transforms::Specializer;
use crate::planner::MemberSymbol;
use crate::planner::{QueryAnalysis, QueryProperties};
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Plans the multi-stage subquery tree of a query. For every multi-stage
/// member it encounters in `all_used_symbols`, it recursively
/// plans the member and its dependencies into `Subquery`s, each as soon
/// as its inputs are planned. Subqueries are deduplicated by
/// `(member, state)` so the same multi-stage subquery isn't
/// emitted twice.
pub struct MultiStageQueryPlanner {
    query_tools: Rc<State>,
    query_properties: Rc<QueryProperties>,
    // The initial multi-stage subquery state. Shared immutably; any mutation goes
    // through `as_ref().clone()` on the consumer side. Used both as the entry
    // state for the recursive planner and as the reset target for `mode:
    // fixed` filter directives.
    root_state: Rc<QueryProperties>,
    // The query's symbols with a multi-stage part, in the order they are planned.
    multi_stage_members: Vec<Rc<MemberSymbol>>,
    bounds: RollingWindowBounds,
}

impl MultiStageQueryPlanner {
    pub fn try_new(query_tools: Rc<State>, analysis: &QueryAnalysis) -> Result<Self, CubeError> {
        let root_state = Self::build_root_state(&query_tools, analysis)?;
        let mut multi_stage_members = Vec::new();
        let ignore_cumulative = analysis.query().ignore_cumulative();
        for member in analysis.query().all_used_symbols()? {
            if analysis
                .classification()
                .has_multi_stage(&member, ignore_cumulative)?
            {
                multi_stage_members.push(member);
            }
        }
        Ok(Self {
            multi_stage_members,
            bounds: RollingWindowBounds::new(query_tools.clone()),
            query_tools,
            query_properties: analysis.query().clone(),
            root_state,
        })
    }

    // The subquery-side mirror of the request, minus `measures_filters`
    // (subqueries do not propagate them). Kept unspecialized: each stage
    // specializes its symbols against its own filters.
    fn build_root_state(
        query_tools: &Rc<State>,
        analysis: &QueryAnalysis,
    ) -> Result<Rc<QueryProperties>, CubeError> {
        let request = analysis.request();
        let specialized = analysis.query();
        QueryProperties::builder()
            .query_tools(query_tools.clone())
            .dimensions(Self::root_dimensions(
                request.dimensions(),
                specialized.dimensions(),
            )?)
            .time_dimensions(Self::root_dimensions(
                request.time_dimensions(),
                specialized.time_dimensions(),
            )?)
            .dimensions_filters(request.dimensions_filters().clone())
            .time_dimensions_filters(request.time_dimensions_filters().clone())
            .segments(request.segments().clone())
            .build()
    }

    // A dimension the query's filters left without multi-stage members is a
    // regular one to the query, and stages read it as the query planned it.
    fn root_dimensions(
        request: &[Rc<MemberSymbol>],
        specialized: &[Rc<MemberSymbol>],
    ) -> Result<Vec<Rc<MemberSymbol>>, CubeError> {
        // `map_symbols` specializes element by element, so the lists align.
        let mut result = Vec::with_capacity(request.len());
        for (requested, specialized) in request.iter().zip(specialized) {
            let collapsed = has_multi_stage_members(requested, true)?
                && !has_multi_stage_members(specialized, true)?;
            result.push(if collapsed { specialized } else { requested }.clone());
        }
        Ok(result)
    }

    pub(super) fn query_tools(&self) -> &Rc<State> {
        &self.query_tools
    }

    pub(super) fn query_properties(&self) -> &Rc<QueryProperties> {
        &self.query_properties
    }

    pub(super) fn root_state(&self) -> &Rc<QueryProperties> {
        &self.root_state
    }

    pub(super) fn bounds(&self) -> &RollingWindowBounds {
        &self.bounds
    }

    /// Populates `scope` with multi-stage subqueries for every
    /// multi-stage member used by the query and returns the subquery
    /// refs the caller's `Merge` joins over. No-op when
    /// the query has none.
    pub fn plan_queries(
        &self,
        scope: &mut PlanningScope,
    ) -> Result<Vec<Rc<SubqueryRef>>, CubeError> {
        if self.multi_stage_members.is_empty() {
            return Ok(vec![]);
        }

        let mut registry = StageRegistry::default();
        let state = self.root_state.clone();

        let mut subquery_refs = Vec::new();

        for member in self.multi_stage_members.iter() {
            let stage = self.plan_member(member.clone(), state.clone(), &mut registry, scope)?;
            if !stage.is_multi_stage_dimension() {
                subquery_refs.push(stage.subquery().clone());
            }
        }

        Ok(subquery_refs)
    }

    // Records a planned stage; its inputs are already in `scope`, so its
    // subquery lands after them.
    pub(super) fn add_stage(
        &self,
        member: &Rc<MemberSymbol>,
        state: Rc<QueryProperties>,
        stage: StageRef,
        subquery: Rc<Subquery>,
        reusable: bool,
        registry: &mut StageRegistry,
        scope: &mut PlanningScope,
    ) -> Result<StageRef, CubeError> {
        registry.add(member, state, stage.clone(), reusable);
        scope.add_member(subquery)?;
        Ok(stage)
    }

    // The stage computing `member` at the grain of `state`: reused when one was
    // already planned, otherwise planned by the kind of stage the member needs.
    pub(super) fn plan_member(
        &self,
        member: Rc<MemberSymbol>,
        state: Rc<QueryProperties>,
        registry: &mut StageRegistry,
        scope: &mut PlanningScope,
    ) -> Result<StageRef, CubeError> {
        let member = member.resolve_reference_chain();
        // `filter: include` lets a stage filter a member the query around it does
        // not, so the member is specialized against this stage's own set.
        let member =
            Specializer::for_stage_member(state.dimensions_filters(), &state.all_filter_items())
                .specialize(&member)?;
        let state = if member.is_dimension() {
            let mut new_state = state.as_ref().clone();
            new_state.remove_multistage_dimensions(registry.resolved_multi_stage_dimensions())?;
            Rc::new(new_state)
        } else {
            state
        };

        if let Some(exists) = registry.find(&member, &state) {
            return Ok(exists);
        };
        check_multi_stage_stages(
            registry.stage_count(),
            &member,
            self.query_tools.request_options().max_multi_stage_stages(),
        )?;

        if let Some(rolling_window_query) = RollingWindowStagePlanner::new(self).try_plan(
            member.clone(),
            state.clone(),
            registry,
            scope,
        )? {
            return Ok(rolling_window_query);
        }

        if !has_multi_stage_members(&member, false)? {
            return LeafStagePlanner::new(self).plan(member, state, registry, scope);
        }
        InodeStagePlanner::new(self).plan(member, state, registry, scope)
    }
}
