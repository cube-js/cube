use super::grain_check;
use super::leaf::LeafStagePlanner;
use super::stage::{add_member_by_kind, query_order_by};
use super::{MultiStageQueryPlanner, PlanningScope, StageRef, StageRegistry};
use crate::cube_bridge::base_query_options::FilterValue;
use crate::logical_plan::*;
use crate::planner::filter::base_filter::FilterType;
use crate::planner::filter::tree_ops;
use crate::planner::filter::BaseFilter;
use crate::planner::filter::FilterItem;
use crate::planner::filter::FilterOperator;
use crate::planner::state::State;
use crate::planner::symbols::AggregationType;
use crate::planner::Case;
use crate::planner::CaseSwitchDefinition;
use crate::planner::CaseSwitchItem;
use crate::planner::InputId;
use crate::planner::MeasureKind;
use crate::planner::MeasureTimeShifts;
use crate::planner::MemberId;
use crate::planner::MemberSymbol;
use crate::planner::MultiStageFilter;
use crate::planner::MultiStageFilterMode;
use crate::planner::MultiStageGrain;
use crate::planner::QueryDateTimeHelper;
use crate::planner::QueryProperties;
use crate::planner::QueryTimeSeries;
use cubenativeutils::CubeError;
use indexmap::IndexMap;
use itertools::Itertools;
use std::rc::Rc;

/// Semantic shape of a non-leaf multi-stage subquery: a rank window,
/// an aggregate (possibly window-rendered), a non-aggregating
/// calculation, a dimension calculation.
#[derive(Clone)]
pub enum MultiStageInodeMemberType {
    Rank,
    Aggregate,
    Calculate,
    Dimension,
}

/// Non-leaf node in a multi-stage tree. Bundles the semantic
/// `inode_type` (Rank / Aggregate / Calculate / Dimension) with the partition-shaping `grain` carried over from
/// the measure's data-model directives and an optional `time_shift`.
#[derive(Clone)]
pub struct MultiStageInodeMember {
    inode_type: MultiStageInodeMemberType,
    grain: MultiStageGrain,
    time_shift: Option<MeasureTimeShifts>,
    /// Optimisation flag: this Aggregate inode is a safe candidate for
    /// the `window`-based render — single measure dep, additive identity
    /// rollup, no leaf-extending modifiers. When `true`, assembly skips
    /// the JOIN-model and the calculation renders a window function.
    /// Default `false`.
    use_window_path: bool,
}

impl MultiStageInodeMember {
    pub fn new(
        inode_type: MultiStageInodeMemberType,
        grain: MultiStageGrain,
        time_shift: Option<MeasureTimeShifts>,
    ) -> Self {
        Self {
            inode_type,
            grain,
            time_shift,
            use_window_path: false,
        }
    }

    pub fn with_use_window_path(mut self, value: bool) -> Self {
        self.use_window_path = value;
        self
    }

    pub fn use_window_path(&self) -> bool {
        self.use_window_path
    }

    pub fn inode_type(&self) -> &MultiStageInodeMemberType {
        &self.inode_type
    }

    pub fn grain(&self) -> &MultiStageGrain {
        &self.grain
    }

    pub fn time_shift(&self) -> &Option<MeasureTimeShifts> {
        &self.time_shift
    }
}

/// Plans a member with multi-stage members of its own: a measure calculation
/// (rank, aggregate, calculate) or a multi-stage dimension.
pub(super) struct InodeStagePlanner<'a> {
    planner: &'a MultiStageQueryPlanner,
    query_tools: Rc<State>,
    query_properties: Rc<QueryProperties>,
}

impl<'a> InodeStagePlanner<'a> {
    pub(super) fn new(planner: &'a MultiStageQueryPlanner) -> Self {
        Self {
            query_tools: planner.query_tools().clone(),
            query_properties: planner.query_properties().clone(),
            planner,
        }
    }

    /// Plans `member`, which has multi-stage members of its own, as a
    /// calculation over the stages it reads. Adjusts the state for any grain
    /// reshape, time-shift or per-member filter changes the inode demands.
    pub(super) fn plan(
        &self,
        member: Rc<MemberSymbol>,
        state: Rc<QueryProperties>,
        registry: &mut StageRegistry,
        scope: &mut PlanningScope,
    ) -> Result<StageRef, CubeError> {
        let member_id = member.id().clone();
        let (mut multi_stage_member, is_ungrupped) =
            self.create_multi_stage_inode_member(member.clone(), registry)?;

        let mut dimensions_to_add = multi_stage_member
            .grain()
            .include
            .clone()
            .unwrap_or_default();

        if let Some(case) = member.case() {
            if let Some(switch_dim) = case.case_switch_dimension() {
                dimensions_to_add.push(switch_dim);
            }
        }

        let directive_filter = multi_stage_filter_directive(&member);

        // new_state is the leaf grain on which children are computed.
        // For JOIN-model Aggregate inodes modifiers apply in this order:
        //   1. filter directive — pick `state` (Relative/None) or
        //      `root_state` (Fixed) as the base and apply exclude /
        //      keep_only / include against it.
        //   2. grain.exclude / grain.keep_only — shrink parent grain to
        //      the partition grain implied by the directive.
        //   3. grain.include — extend the result with extra leaf dims.
        //   4. time_shift / filter cleanup.
        // Step 2 must precede step 3: `keep_only` is an intersection and
        // would silently drop dims that step 3 needs to introduce.
        //
        // The window-path Aggregate inode skips step 2: the leaf stays
        // at the parent state plus any `include` extension, and the
        // window function does the `exclude` collapse at outer level.
        let filtered_state = {
            let mut filtered_state = match directive_filter.as_ref().map(|f| &f.mode) {
                Some(MultiStageFilterMode::Fixed) => self.planner.root_state().as_ref().clone(),
                Some(MultiStageFilterMode::Relative) | None => state.as_ref().clone(),
            };

            if let Some(filter) = &directive_filter {
                apply_filter_directive_to_state(filter, &mut filtered_state)?;
            }
            filtered_state
        };

        // Step 1 can drop a filter the query restricts the grid by. That is
        // the point of the directive — the aggregation input widens — but
        // the rows the inode *reports* must stay the query's, and only the
        // JOIN-model can hold the two apart: its keys side enumerates the
        // grid while its measure side spans the widened set. A window
        // expression has one row set serving both roles, so it reports the
        // widened rows too and values the query filtered out come back as
        // result rows. Hand such an inode to the JOIN-model.
        let query_filter_dropped =
            query_filters_dropped(self.planner.root_state(), &state, &filtered_state);
        if query_filter_dropped {
            multi_stage_member = multi_stage_member.with_use_window_path(false);
        }

        // Whether this inode can actually act on that, decided here so both
        // halves of the decision stay together — the keys side that carries
        // the query's rows is requested further down.
        //
        // A parent state with no dimensions is a single-row grid that no
        // filter change can widen, and an empty key-dimension list
        // degenerates into a cross join rather than being rejected. A
        // Dimension inode never reads `keys_input`, so building one leaves
        // unreferenced subqueries behind. And a Rank inode ranks within whatever
        // its source carries: handing it the keys side would shrink the
        // ranked population to the grid and collapse the ranks, trading a
        // widened row set for wrong values. Rank needs its rows restricted
        // *after* the window, which this assembly cannot express.
        let needs_query_grid = query_filter_dropped
            && (!state.dimensions().is_empty() || !state.time_dimensions().is_empty())
            && !matches!(
                multi_stage_member.inode_type(),
                MultiStageInodeMemberType::Dimension | MultiStageInodeMemberType::Rank
            );

        let use_window_path = multi_stage_member.use_window_path();
        let new_state = {
            let mut new_state = filtered_state;

            if !use_window_path
                && matches!(
                    multi_stage_member.inode_type(),
                    MultiStageInodeMemberType::Aggregate
                )
            {
                let grain = multi_stage_member.grain();
                let dims = grain.partition(new_state.dimensions());
                let time_dims = grain.partition(new_state.time_dimensions());
                new_state.set_dimensions(dims);
                new_state.set_time_dimensions(time_dims);
            }
            if !dimensions_to_add.is_empty() {
                new_state.add_dimensions(dimensions_to_add.clone());
            }
            if let Some(time_shift) = multi_stage_member.time_shift() {
                new_state.add_time_shifts(time_shift.clone())?;
            }
            if new_state.has_filters_for_member(&member_id) {
                new_state.remove_filter_for_member(&member_id);
            }
            Rc::new(new_state)
        };

        let mut input = vec![];
        self.make_childs(
            member.clone(),
            new_state.clone(),
            &state,
            &mut input,
            registry,
            scope,
        )?;

        // JOIN-model: when the measure side no longer enumerates the rows
        // this inode has to report — because new_state misses a dim that
        // was on the parent's `state`, or because a dropped query filter
        // widened it — we build keys-side stages per child on the
        // parent state, so the Merge broadcasts measure values
        // onto the query grain and only onto it. Window-path Aggregate
        // inodes (sum-of-sum / sum-of-count with no leaf-extending
        // `include`) handle broadcast via the window expression instead and
        // don't need keys_input.
        let mut keys_input: Vec<StageRef> = vec![];
        if !use_window_path {
            let new_state_has = |sym: &Rc<MemberSymbol>| {
                let sym_id = sym.clone().resolve_reference_chain().id().clone();
                new_state
                    .dimensions()
                    .iter()
                    .chain(new_state.time_dimensions().iter())
                    .any(|d| d.clone().resolve_reference_chain().id() == &sym_id)
            };
            let any_missing = state
                .dimensions()
                .iter()
                .chain(state.time_dimensions().iter())
                .any(|d| !new_state_has(d));
            // A dropped query filter needs the keys side for the same
            // reason a shrunk grain does — the measure side no longer
            // enumerates the query's rows — except here the grid keeps
            // every dimension and only the row count within it grows.
            if any_missing || needs_query_grid {
                // The member's reads were already checked against both grains
                // on the measure side; the keys side only enumerates the grid.
                self.make_childs(
                    member.clone(),
                    state.clone(),
                    &new_state,
                    &mut keys_input,
                    registry,
                    scope,
                )?;
            }
        }

        let (id, alias) = scope.next_subquery();
        let is_dimension = matches!(
            multi_stage_member.inode_type(),
            MultiStageInodeMemberType::Dimension
        );
        let stage = StageRef::new(id, alias.clone(), &state, &member, is_dimension, &input);
        let subquery = if is_dimension {
            self.plan_for_dimension_calculation(&stage, &member, &state, &input)?
        } else {
            self.plan_for_measure_calculation(
                id,
                alias,
                &member,
                &state,
                is_ungrupped,
                &multi_stage_member,
                &input,
                &keys_input,
            )?
        };
        self.planner
            .add_stage(&member, state, stage, subquery, true, registry, scope)
    }

    /// Classifies `base_member` into a `MultiStageInodeMember` — picks
    /// the inode kind (Rank / Aggregate / Calculate for a measure,
    /// Dimension for a dimension) and carries over the partition-shaping
    /// `grain` and optional `time_shift` from the data-model definition.
    /// Returns the inode together with the leaf's `is_ungrupped` flag.
    fn create_multi_stage_inode_member(
        &self,
        base_member: Rc<MemberSymbol>,
        registry: &mut StageRegistry,
    ) -> Result<(MultiStageInodeMember, bool), CubeError> {
        let inode = if let Ok(measure) = base_member.as_measure() {
            let member_type = match measure.kind() {
                MeasureKind::Rank => MultiStageInodeMemberType::Rank,
                MeasureKind::Calculated(_) => MultiStageInodeMemberType::Calculate,
                MeasureKind::Count(_)
                | MeasureKind::MultipliedCount(_)
                | MeasureKind::Aggregated(_)
                | MeasureKind::AggregatedState(_) => MultiStageInodeMemberType::Aggregate,
            };

            let time_shift = measure.time_shift().cloned();

            let is_ungrupped = match &member_type {
                MultiStageInodeMemberType::Rank | MultiStageInodeMemberType::Calculate => true,
                _ => self.query_properties.ungrouped(),
            };

            let grain = measure
                .multi_stage()
                .map(|ms| ms.grain.clone())
                .unwrap_or_default();
            // Of the `grain` lists only `include` disqualifies the window path
            // here: `exclude` and `keep_only` are realised through the window's
            // PARTITION BY at render time, while `include` extends the leaf
            // grain, which the JOIN-model is required for.
            //
            // Grain is not the only disqualifier. A `filter` directive that
            // drops a filter the query restricts the grid by also rules the
            // window path out. Detecting that needs the inherited state, which
            // only exists in `plan`, which revokes the flag.
            let has_include = grain.include.as_ref().is_some_and(|v| !v.is_empty());
            let use_window_path = matches!(member_type, MultiStageInodeMemberType::Aggregate)
                && !has_include
                && Self::is_window_path_eligible(&base_member);
            (
                MultiStageInodeMember::new(member_type, grain, time_shift)
                    .with_use_window_path(use_window_path),
                is_ungrupped,
            )
        } else {
            let grain = base_member
                .as_dimension()
                .ok()
                .and_then(|d| d.multi_stage().map(|ms| ms.grain.clone()))
                .unwrap_or_default();
            registry.add_resolved_multi_stage_dimension(
                base_member.clone().resolve_reference_chain().id().clone(),
            );
            (
                MultiStageInodeMember::new(MultiStageInodeMemberType::Dimension, grain, None),
                false,
            )
        };
        Ok(inode)
    }

    /// Plans the input stages of `member`'s inode. Switches to
    /// `try_make_childs_for_case_switch` when the member's body is a
    /// CASE-SWITCH expression; otherwise falls through to
    /// `default_make_childs`.
    fn make_childs(
        &self,
        member: Rc<MemberSymbol>,
        new_state: Rc<QueryProperties>,
        parent_state: &Rc<QueryProperties>,
        result: &mut Vec<StageRef>,
        registry: &mut StageRegistry,
        scope: &mut PlanningScope,
    ) -> Result<(), CubeError> {
        // The CASE-SWITCH path plans every branch dependency as its own subquery,
        // dimensions included, so each one is a column of the source rather than
        // something the stage grain has to carry. It deliberately skips the
        // reachability check `default_make_childs` applies.
        if let Some(Case::CaseSwitch(case_switch)) = member.case() {
            if self.try_make_childs_for_case_switch(
                case_switch,
                new_state.clone(),
                result,
                registry,
                scope,
            )? {
                return Ok(());
            }
        }
        self.default_make_childs(member, new_state, parent_state, result, registry, scope)
    }

    /// Aggregate inode is window-path eligible when it has exactly one
    /// measure dep, the outer aggregation is `sum`, and the inner
    /// aggregation rolls up as a sum (i.e. inner ∈ {sum, count}). This
    /// is the narrow subset where `sum(sum(x)) OVER (...)` is a faithful
    /// rollup — sum is associative and count rolls up as sum.
    fn is_window_path_eligible(base_member: &Rc<MemberSymbol>) -> bool {
        let Ok(outer) = base_member.as_measure() else {
            return false;
        };
        let outer_is_sum = matches!(
            outer.kind(),
            MeasureKind::Aggregated(a) if a.agg_type() == AggregationType::Sum
        );
        if !outer_is_sum {
            return false;
        }
        let deps = base_member.get_dependencies();
        let [dep] = deps.as_slice() else {
            return false;
        };
        let Ok(inner) = dep.clone().resolve_reference_chain().as_measure() else {
            return false;
        };
        match inner.kind() {
            MeasureKind::Count(_) => true,
            MeasureKind::Aggregated(a) => a.agg_type() == AggregationType::Sum,
            MeasureKind::MultipliedCount(_)
            | MeasureKind::AggregatedState(_)
            | MeasureKind::Calculated(_)
            | MeasureKind::Rank => false,
        }
    }

    /// Default child-generation path: for each measure or
    /// multi-stage-dimension dependency, recurses into
    /// `plan_member` and adds the result as an input
    /// subquery. If the member has no such deps (e.g. a `Rank` measure
    /// that only needs the dimension grid), produces a single
    /// "without-member" leaf instead.
    fn default_make_childs(
        &self,
        member: Rc<MemberSymbol>,
        new_state: Rc<QueryProperties>,
        parent_state: &Rc<QueryProperties>,
        result: &mut Vec<StageRef>,
        registry: &mut StageRegistry,
        scope: &mut PlanningScope,
    ) -> Result<(), CubeError> {
        // Checked before inputs are planned, so this error wins over any raised by an input.
        grain_check::check_dimension_reads(&self.query_tools, &member, &new_state, parent_state)?;
        let mut has_inputs = false;
        for dep in member.get_dependencies() {
            let dep = &dep.resolve_reference_chain();
            if dep.is_measure() || grain_check::is_multi_stage_dimension(dep)? {
                has_inputs = true;
                let stage =
                    self.planner
                        .plan_member(dep.clone(), new_state.clone(), registry, scope)?;
                if !stage.is_multi_stage_dimension() || member.as_dimension().is_ok() {
                    result.push(stage);
                }
            }
        }
        if !has_inputs {
            // Rank and similar members read only the dimension grid.
            result.push(
                LeafStagePlanner::new(self.planner)
                    .plan_without_member(member, new_state, registry, scope)?,
            );
        }
        Ok(())
    }

    /// Plans CASE-SWITCH dependencies: collects, per dependency, the
    /// union of switch values it covers and renders each dependency
    /// under a state with an equality filter on the switch member
    /// restricted to those values. An open `ELSE` branch makes the
    /// dependency unrestricted. Returns `false` when the switch is
    /// not a member reference, in which case the caller falls back
    /// to `default_make_childs`.
    fn try_make_childs_for_case_switch(
        &self,
        case: &CaseSwitchDefinition,
        new_state: Rc<QueryProperties>,
        result: &mut Vec<StageRef>,
        registry: &mut StageRegistry,
        scope: &mut PlanningScope,
    ) -> Result<bool, CubeError> {
        let CaseSwitchItem::Member(switch_member) = &case.switch else {
            return Ok(false);
        };

        // Collect, per dependency, the union of switch values that need it.
        // `None` marks an unrestricted (open ELSE) entry: such a dependency
        // must be processed without a prefilter on switch_member, since the
        // outer CASE will dispatch by value at row level.
        let mut deps: IndexMap<MemberId, (Rc<MemberSymbol>, Option<Vec<String>>)> = IndexMap::new();

        let mut record = |dep: Rc<MemberSymbol>, branch_values: Option<Vec<String>>| {
            let dep = dep.resolve_reference_chain();
            let entry = deps
                .entry(dep.id().clone())
                .or_insert_with(|| (dep.clone(), Some(Vec::new())));
            match (&mut entry.1, branch_values) {
                (None, _) => {} // already unrestricted
                (slot @ Some(_), None) => *slot = None,
                (Some(values), Some(branch)) => {
                    for v in branch {
                        if !values.contains(&v) {
                            values.push(v);
                        }
                    }
                }
            }
        };

        for itm in &case.items {
            for dep in itm.sql.get_dependencies() {
                record(dep, Some(vec![itm.value.clone()]));
            }
        }

        if let Some(else_sql) = &case.else_sql {
            let else_values = case.get_else_values();
            for dep in else_sql.get_dependencies() {
                record(dep.clone(), else_values.clone());
            }
        }

        for (_, (dep, values)) in deps {
            let mut state = new_state.as_ref().clone();
            if let Some(values) = values {
                if !values.is_empty() {
                    let filter = BaseFilter::try_new(
                        self.query_tools.query_tools().clone(),
                        switch_member.clone(),
                        FilterType::Dimension,
                        FilterOperator::Equal,
                        Some(values.into_iter().map(FilterValue::Str).collect_vec()),
                        None,
                    )?;
                    state.add_dimension_filter(FilterItem::Item(filter));
                }
            }
            let state = Rc::new(state);
            result.push(self.planner.plan_member(dep, state, registry, scope)?);
        }

        Ok(true)
    }

    /// Builds a measure-calculation subquery (Rank / Aggregate /
    /// Calculate). Wires the input subqueries into a `Merge`
    /// source; for the JOIN-based path (when the stage has keys inputs)
    /// also wires keys-side refs.
    fn plan_for_measure_calculation(
        &self,
        id: InputId,
        alias: String,
        member: &Rc<MemberSymbol>,
        state: &QueryProperties,
        is_ungrouped: bool,
        multi_stage_member: &MultiStageInodeMember,
        inputs: &[StageRef],
        keys_inputs: &[StageRef],
    ) -> Result<Rc<Subquery>, CubeError> {
        let partition_by = multi_stage_member.grain().partition(&all_dimensions(state));

        // Rank always uses a window function. Aggregate inodes are
        // routed through `Merge` by default; only the narrow
        // optimisation-eligible subset (planner sets `use_window_path`)
        // is emitted as a Window expression and additionally requires
        // partition_by to be a strict subset of all dimensions —
        // otherwise the window collapses into a plain group-by.
        let window_function_to_use = match multi_stage_member.inode_type() {
            MultiStageInodeMemberType::Rank => MultiStageCalculationWindowFunction::Rank,
            MultiStageInodeMemberType::Aggregate
                if multi_stage_member.use_window_path()
                    && partition_by.len() != all_dimensions(state).len() =>
            {
                MultiStageCalculationWindowFunction::Window
            }
            _ => MultiStageCalculationWindowFunction::None,
        };

        let measures = if member.is_measure() {
            vec![member.clone()]
        } else {
            vec![]
        };
        let schema = LogicalSchema::default()
            .set_dimensions(state.dimensions().clone())
            .set_time_dimensions(state.time_dimensions().clone())
            .set_measures(measures)
            .into_rc();

        let calculation_type = match multi_stage_member.inode_type() {
            MultiStageInodeMemberType::Rank => MultiStageCalculationType::Rank,
            MultiStageInodeMemberType::Aggregate => MultiStageCalculationType::Aggregate,
            MultiStageInodeMemberType::Calculate => MultiStageCalculationType::Calculate,
            _ => {
                return Err(CubeError::internal(format!(
                    "Wrong inode type for measure calculation"
                )))
            }
        };

        let result = MultiStageMeasureCalculation::builder()
            .schema(schema)
            .is_ungrouped(is_ungrouped)
            .calculation_type(calculation_type)
            .partition_by(partition_by)
            .window_function_to_use(window_function_to_use)
            .order_by(query_order_by(member, state))
            .source(Rc::new(
                Merge::builder()
                    .schema(input_schema(inputs))
                    .subqueries(input_subqueries(inputs))
                    .keys(input_subqueries(keys_inputs))
                    .build(),
            ))
            .build();

        let result = Subquery::new(
            id,
            alias,
            SubqueryBody::MultiStageMeasureCalculation(Rc::new(result)),
        );
        Ok(Rc::new(result))
    }

    /// Builds a dimension-calculation subquery for a multi-stage
    /// dimension. Includes the dimension itself plus every
    /// non-multi-stage dimension reachable from the subtree, so the
    /// resulting subquery can be joined back into the host query on
    /// those dimensions.
    fn plan_for_dimension_calculation(
        &self,
        stage: &StageRef,
        member: &Rc<MemberSymbol>,
        state: &QueryProperties,
        inputs: &[StageRef],
    ) -> Result<Rc<Subquery>, CubeError> {
        let mut dimensions = state.dimensions().clone();
        let mut time_dimensions = state.time_dimensions().clone();
        let mut measures = vec![];
        add_member_by_kind(member, &mut dimensions, &mut time_dimensions, &mut measures);
        // We add all non–multi-stage dimensions from the underlying states because
        // they’re needed to join a multi-stage dimension into the measure query
        let (all_dependend_dimensions, all_dependend_time_dimensions) =
            stage.non_multi_stage_dimensions()?;
        dimensions.extend(all_dependend_dimensions.iter().cloned());
        time_dimensions.extend(all_dependend_time_dimensions.iter().cloned());
        dimensions = dimensions
            .into_iter()
            .unique_by(|d| d.id().clone())
            .collect_vec();
        time_dimensions = time_dimensions
            .into_iter()
            .unique_by(|d| d.id().clone())
            .collect_vec();

        let schema = LogicalSchema::default()
            .set_dimensions(dimensions)
            .set_time_dimensions(time_dimensions)
            .set_measures(measures)
            .into_rc();

        let result = MultiStageDimensionCalculation::builder()
            .schema(schema)
            .order_by(query_order_by(member, state))
            .multi_stage_dimension(member.clone())
            .source(Rc::new(
                Merge::builder()
                    .schema(input_schema(inputs))
                    .subqueries(input_subqueries(inputs))
                    .build(),
            ))
            .build();

        let result = Subquery::new(
            stage.id(),
            stage.name().clone(),
            SubqueryBody::MultiStageDimensionCalculation(Rc::new(result)),
        );
        Ok(Rc::new(result))
    }
}

fn all_dimensions(state: &QueryProperties) -> Vec<Rc<MemberSymbol>> {
    state
        .dimensions()
        .iter()
        .cloned()
        .chain(state.time_dimensions().iter().cloned())
        .collect_vec()
}

fn input_schema(inputs: &[StageRef]) -> Rc<LogicalSchema> {
    let dimensions = inputs
        .iter()
        .flat_map(|input| input.schema().dimensions.iter().cloned())
        .unique_by(|dim| dim.id().clone())
        .collect_vec();
    let time_dimensions = inputs
        .iter()
        .flat_map(|input| input.schema().time_dimensions.iter().cloned())
        .unique_by(|dim| dim.id().clone())
        .collect_vec();

    LogicalSchema::default()
        .set_dimensions(dimensions)
        .set_time_dimensions(time_dimensions)
        .into_rc()
}

fn input_subqueries(inputs: &[StageRef]) -> Vec<Rc<SubqueryRef>> {
    inputs
        .iter()
        .map(|input| input.subquery().clone())
        .unique_by(|r| r.id())
        .collect_vec()
}

fn multi_stage_filter_directive(member: &Rc<MemberSymbol>) -> Option<MultiStageFilter> {
    if let Ok(measure) = member.as_measure() {
        return measure.multi_stage().and_then(|m| m.filter.clone());
    }
    if let Ok(dimension) = member.as_dimension() {
        return dimension.multi_stage().and_then(|m| m.filter.clone());
    }
    None
}

//
// TODO: known interaction gaps when `mode: fixed` resets to `root_state`
// in chains. Both manifest only when a multi-stage member with `mode: fixed`
// is computed as a dependency of another node that already mutated state.
//
// 1. Rolling window. `RollingWindowStagePlanner::try_plan` builds `base_rolling_state`
//    via `make_rolling_base_state` (extends date_range, swaps the time
//    dimension, prunes time-dim entries from `dimensions`). When a nested
//    multi-stage with `mode: fixed` is reached during recursion, it falls
//    back to `self.root_state`, dropping those rolling-window-specific
//    mutations — the leaf will read the original (narrow) date range while
//    the outer rolling frame expects the extended one.
//
// 2. Switch-case pruning. `apply_static_filter_to_symbol` runs at the top
//    of `plan_member` against `state.dimensions_filters()` —
//    the *inherited* filters, before this function. If the inherited set
//    restricts the switch dimension, case branches are pruned at symbol
//    level; the subsequent `mode: fixed` reset cannot un-prune them.
//
// True when `narrowed` lost a *query-level* filter that `base` restricts the
// grid by. Three conditions per filter: `base` has it, the query asked for it,
// and `narrowed` doesn't have it.
//
// The query membership check is what keeps the notion anchored to the result
// grid. A filter a parent multi-stage member introduced through its own
// `filter: include` narrows that parent's view, not the grid the query asked
// for; a child dropping it (`mode: fixed`) therefore cannot widen the grid
// past the query, and needs no keys side.
//
// Measure filters are absent from the comparison because they never reach a
// subquery state to begin with — `build_root_state` drops them — so there is no
// query-level measure filter for a directive to lose. Filters added on top of
// `base` don't count either: they shrink the grid, which is safe.
//
// The query-membership check compares whole filters, so a filter whose values
// were rewritten between the root state and `base` reads as one the query never
// asked for, and the drop goes undetected. That is deliberately out of scope: a
// path that rewrites filter values on the way down has to bound the row set by
// other means. The rolling-window date-range rewrite is the one such path, and
// it does — its rows come from the time series and its values through the frame
// condition, both built from the query's own range. A new rewriting path has to
// establish the same, or anchor this check on the member instead of the value.
fn query_filters_dropped(
    root: &QueryProperties,
    base: &QueryProperties,
    narrowed: &QueryProperties,
) -> bool {
    fn any_dropped(root: &[FilterItem], base: &[FilterItem], narrowed: &[FilterItem]) -> bool {
        base.iter().any(|item| {
            tree_ops::contains_with_member(root, item)
                && !tree_ops::contains_with_member(narrowed, item)
        })
    }

    any_dropped(
        root.dimensions_filters(),
        base.dimensions_filters(),
        narrowed.dimensions_filters(),
    ) || any_dropped(
        root.time_dimensions_filters(),
        base.time_dimensions_filters(),
        narrowed.time_dimensions_filters(),
    ) || any_dropped(root.segments(), base.segments(), narrowed.segments())
}

fn apply_filter_directive_to_state(
    filter: &MultiStageFilter,
    state: &mut QueryProperties,
) -> Result<(), CubeError> {
    if let Some(exclude) = &filter.exclude {
        let names: Vec<MemberId> = exclude.iter().map(|s| s.id().clone()).collect();
        state.remove_filters_for_members(&names);
    }
    if let Some(keep_only) = &filter.keep_only {
        let names: Vec<MemberId> = keep_only.iter().map(|s| s.id().clone()).collect();
        state.keep_only_filters_for_members(&names);
    }
    if !filter.include_dimension.is_empty() {
        state.add_dimension_filters(filter.include_dimension.clone());
    }
    if !filter.include_time_dimension.is_empty() {
        narrow_time_dimensions_to_include(&filter.include_time_dimension, state)?;
        state.add_time_dimension_filters(filter.include_time_dimension.clone());
    }
    if !filter.include_measure.is_empty() {
        state.add_measure_filters(filter.include_measure.clone());
    }
    Ok(())
}

/// An include date range on a time dimension the query groups by narrows that
/// dimension's date range, so the series has rows only inside the included period
/// while rolling windows still read back from it.
fn narrow_time_dimensions_to_include(
    include: &[FilterItem],
    state: &mut QueryProperties,
) -> Result<(), CubeError> {
    let mut time_dimensions = state.time_dimensions().clone();
    for item in include {
        let FilterItem::Item(filter) = item else {
            continue;
        };
        let [FilterValue::Str(from), FilterValue::Str(to)] = filter.values().as_slice() else {
            continue;
        };
        if !matches!(filter.filter_operator(), FilterOperator::InDateRange) {
            continue;
        }
        let base_id = filter
            .member_evaluator()
            .resolve_reference_chain()
            .id()
            .clone();
        // Bounds are compared normalized but kept as given, the form the series expects.
        let precision = QueryTimeSeries::MILLISECOND_PRECISION;
        let later_from = |a: &String, b: &String| -> Result<String, CubeError> {
            let (na, nb) = (
                QueryDateTimeHelper::format_from_date(a, precision)?,
                QueryDateTimeHelper::format_from_date(b, precision)?,
            );
            Ok(if na >= nb { a.clone() } else { b.clone() })
        };
        let earlier_to = |a: &String, b: &String| -> Result<String, CubeError> {
            let (na, nb) = (
                QueryDateTimeHelper::format_to_date(a, precision)?,
                QueryDateTimeHelper::format_to_date(b, precision)?,
            );
            Ok(if na <= nb { a.clone() } else { b.clone() })
        };
        for time_dimension in time_dimensions.iter_mut() {
            let Ok(symbol) = time_dimension.as_time_dimension() else {
                continue;
            };
            if symbol.base_symbol().clone().resolve_reference_chain().id() != &base_id {
                continue;
            }
            let range = match symbol.date_range_vec().as_deref() {
                Some([own_from, own_to]) => (later_from(own_from, from)?, earlier_to(own_to, to)?),
                _ => (from.clone(), to.clone()),
            };
            *time_dimension = MemberSymbol::new_time_dimension(symbol.with_date_range(Some(range)));
        }
    }
    state.set_time_dimensions(time_dimensions);
    Ok(())
}
