use super::stage::add_member_by_kind;
use super::{EvaluationContext, MultiStageQueryPlanner, PlanningScope, StageRef, StageRegistry};
use crate::logical_plan::transforms as logical_transforms;
use crate::logical_plan::*;
use crate::planner::filter::map_filter_items;
use crate::planner::planners::QueryPlanner;
use crate::planner::state::State;
use crate::planner::symbols::transforms;
use crate::planner::InputId;
use crate::planner::QueryProperties;
use crate::planner::{MeasureRenderModifier, MemberSymbol};
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Plans a stage that reads the underlying data directly: a member without
/// multi-stage dependencies, a dimension grid, or a rolling window's base.
pub(super) struct LeafStagePlanner<'a> {
    planner: &'a MultiStageQueryPlanner,
    query_tools: Rc<State>,
    query_properties: Rc<QueryProperties>,
}

impl<'a> LeafStagePlanner<'a> {
    pub(super) fn new(planner: &'a MultiStageQueryPlanner) -> Self {
        Self {
            query_tools: planner.query_tools().clone(),
            query_properties: planner.query_properties().clone(),
            planner,
        }
    }

    /// Plans `member`, which has no multi-stage members, as a leaf.
    pub(super) fn plan(
        &self,
        member: Rc<MemberSymbol>,
        state: Rc<QueryProperties>,
        registry: &mut StageRegistry,
        scope: &mut PlanningScope,
    ) -> Result<StageRef, CubeError> {
        let (id, alias) = scope.next_subquery();
        let stage = StageRef::new(id, alias.clone(), &state, &member, false, &[]);
        let subquery = self.plan_for_leaf_query(
            id,
            alias,
            &member,
            &state,
            self.query_properties.ungrouped(),
            false,
            false,
            scope,
        )?;
        self.planner
            .add_stage(&member, state, stage, subquery, true, registry, scope)
    }

    /// Plans the dimension grid of `state` for `member`, which computes
    /// nothing in its leaf (a `Rank` and similar).
    pub(super) fn plan_without_member(
        &self,
        member: Rc<MemberSymbol>,
        state: Rc<QueryProperties>,
        registry: &mut StageRegistry,
        scope: &mut PlanningScope,
    ) -> Result<StageRef, CubeError> {
        let (id, alias) = scope.next_subquery();
        let stage = StageRef::new(id, alias.clone(), &state, &member, false, &[]);
        let subquery = self.plan_for_leaf_query(
            id,
            alias,
            &member,
            &state,
            self.query_properties.ungrouped(),
            false,
            true,
            scope,
        )?;
        // It carries the member only to select its dimension grid, so a
        // `{member}` reference must never resolve to it.
        self.planner
            .add_stage(&member, state, stage, subquery, false, registry, scope)
    }

    /// Adds the leaf subquery that produces the base values for a
    /// rolling window — selects the requested dimensions plus the
    /// unrolled measure, marked `has_aggregates_on_top` so callers
    /// know an outer rolling computation will consume it.
    pub(super) fn plan_rolling_window_base(
        &self,
        member: Rc<MemberSymbol>,
        state: Rc<QueryProperties>,
        ungrouped: bool,
        registry: &mut StageRegistry,
        scope: &mut PlanningScope,
    ) -> Result<StageRef, CubeError> {
        let (id, alias) = scope.next_subquery();
        let stage = StageRef::new(id, alias.clone(), &state, &member, false, &[]);
        let subquery = self.plan_for_leaf_query(
            id,
            alias,
            &member,
            &state,
            self.query_properties.ungrouped() || ungrouped,
            true,
            false,
            scope,
        )?;
        self.planner
            .add_stage(&member, state, stage, subquery, true, registry, scope)
    }

    /// Builds the leaf subquery for a base measure — runs a `QueryPlanner`
    /// on the stage's state with `allow_multi_stage = false`,
    /// then wraps the result in a leaf subquery. The shared
    /// `scope` receives any subqueries the leaf planning produces
    /// (multiplied-measure subqueries), so they land in the root
    /// `WITH` list instead of nesting. `without_member` selects only the
    /// dimension grid, for cases like `Rank` that compute nothing in the leaf.
    /// `has_aggregates_on_top` materializes mergeable state for an outer
    /// rolling window.
    fn plan_for_leaf_query(
        &self,
        id: InputId,
        alias: String,
        member: &Rc<MemberSymbol>,
        state: &QueryProperties,
        is_ungrouped: bool,
        has_aggregates_on_top: bool,
        without_member: bool,
        scope: &mut PlanningScope,
    ) -> Result<Rc<Subquery>, CubeError> {
        // An aggregating stage on top (a rolling window) merges the leaf's
        // values, so measures with a mergeable state form must materialize
        // the state, not the final value.
        let leaf_as_state = has_aggregates_on_top;
        let member_node = if leaf_as_state {
            transforms::measures_as_state(member)?
        } else {
            member.clone()
        };
        let member_node = &member_node;
        let mut dimensions = state.dimensions().clone();
        let mut time_dimensions = state.time_dimensions().clone();
        let mut measures = vec![];
        if !without_member {
            add_member_by_kind(
                member_node,
                &mut dimensions,
                &mut time_dimensions,
                &mut measures,
            );
        }

        let measures_filters = if leaf_as_state {
            map_filter_items(state.measures_filters(), &transforms::measures_as_state)?
        } else {
            state.measures_filters().clone()
        };
        let order_by = QueryProperties::default_order(&dimensions, &time_dimensions, &measures);
        let subquery_properties = QueryProperties::builder()
            .query_tools(self.query_tools.clone())
            .measures(measures)
            .dimensions(dimensions)
            .time_dimensions(time_dimensions)
            .order_by(order_by)
            .time_dimensions_filters(state.time_dimensions_filters().clone())
            .dimensions_filters(state.dimensions_filters().clone())
            .measures_filters(measures_filters)
            .segments(state.segments().clone())
            .ignore_cumulative(true)
            .ungrouped(is_ungrouped)
            .query_join_hints(self.query_properties.query_join_hints().clone())
            .allow_multi_stage(false)
            .build()?;

        let query_planner =
            QueryPlanner::new(subquery_properties.clone(), self.query_tools.clone());
        // Subqueries hoisted out of this leaf (e.g. multiplied-measure
        // subqueries) must render under the same context the leaf
        // itself renders with.
        let evaluation_context = EvaluationContext {
            time_shifts: state.time_shifts().clone(),
            measure_for_ungrouped: is_ungrouped,
        };
        let query = scope.with_evaluation_context(evaluation_context.clone(), |scope| {
            query_planner.plan(scope)
        })?;
        // An ungrouped leaf feeds a stage that aggregates its rows, so its
        // measures render as row-level values.
        let query = if evaluation_context.measure_for_ungrouped {
            logical_transforms::measures_render_modifier_in_query(
                &query,
                &MeasureRenderModifier::RawValue,
            )?
        } else {
            query
        };
        let result = Subquery::new(
            id,
            alias,
            SubqueryBody::Query(query.with_evaluation_context(Some(evaluation_context))),
        );
        Ok(Rc::new(result))
    }
}
