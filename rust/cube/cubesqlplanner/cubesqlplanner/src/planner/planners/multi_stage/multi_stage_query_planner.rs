use super::{
    MultiStageInodeMember, MultiStageInodeMemberType, MultiStageLeafMemberType, MultiStageMember,
    MultiStageMemberQueryPlanner, MultiStageMemberType, MultiStageQueryDescription, PlanningScope,
    RollingWindowDescription, TimeSeriesDescription,
};
use crate::cube_bridge::base_query_options::FilterValue;
use crate::cube_bridge::measure_definition::RollingWindow;
use crate::logical_plan::*;
use crate::planner::collectors::has_multi_stage_members;
use crate::planner::collectors::member_childs;
use crate::planner::filter::base_filter::FilterType;
use crate::planner::filter::tree_ops;
use crate::planner::filter::BaseFilter;
use crate::planner::filter::FilterItem;
use crate::planner::filter::FilterOperator;
use crate::planner::state::State;
use crate::planner::symbols::deps::{collect_cube_refs, collect_deps, SymbolDeps};
use crate::planner::symbols::transforms;
use crate::planner::symbols::AggregationType;
use crate::planner::Case;
use crate::planner::CaseSwitchDefinition;
use crate::planner::CaseSwitchItem;
use crate::planner::GranularityHelper;
use crate::planner::MeasureKind;
use crate::planner::MemberSymbol;
use crate::planner::MultiStageFilter;
use crate::planner::MultiStageFilterMode;
use crate::planner::MultiStageGrain;
use crate::planner::QueryProperties;
use cubenativeutils::CubeError;
use indexmap::IndexMap;
use itertools::Itertools;
use std::collections::HashSet;
use std::rc::Rc;

/// Plans the multi-stage CTE tree of a query. For every multi-stage
/// member it encounters in `all_used_symbols`, it recursively
/// produces `MultiStageQueryDescription`s for the member and its
/// dependencies, then asks `MultiStageMemberQueryPlanner` to render
/// each into a `LogicalMultiStageMember`. CTEs are deduplicated by
/// `(member, state)` so the same multi-stage subquery isn't
/// emitted twice.
pub struct MultiStageQueryPlanner {
    query_tools: Rc<State>,
    query_properties: Rc<QueryProperties>,
    // The initial multi-stage CTE state. Shared immutably; any mutation goes
    // through `as_ref().clone()` on the consumer side. Used both as the entry
    // state for the recursive planner and as the reset target for `mode:
    // fixed` filter directives.
    root_state: Rc<QueryProperties>,
}

impl MultiStageQueryPlanner {
    pub fn try_new(
        query_tools: Rc<State>,
        query_properties: Rc<QueryProperties>,
    ) -> Result<Self, CubeError> {
        let root_state = Self::build_root_state(&query_tools, &query_properties)?;
        Ok(Self {
            query_tools,
            query_properties,
            root_state,
        })
    }

    // The CTE-side mirror of `query_properties`: same dimensions/filters/
    // segments, but `measures_filters` are intentionally dropped (CTE queries
    // do not propagate them) and `order_by` is forced to an empty vec so the
    // builder skips default_order — this value is only ever used as a state
    // container, never planned directly.
    fn build_root_state(
        query_tools: &Rc<State>,
        query_properties: &Rc<QueryProperties>,
    ) -> Result<Rc<QueryProperties>, CubeError> {
        QueryProperties::builder()
            .query_tools(query_tools.clone())
            .dimensions(query_properties.dimensions().clone())
            .time_dimensions(query_properties.time_dimensions().clone())
            .dimensions_filters(query_properties.dimensions_filters().clone())
            .time_dimensions_filters(query_properties.time_dimensions_filters().clone())
            .segments(query_properties.segments().clone())
            .order_by(Some(vec![]))
            .build()
    }

    fn root_state(&self) -> &Rc<QueryProperties> {
        &self.root_state
    }

    /// Populates `scope` with multi-stage CTEs for every
    /// multi-stage member used by the query and returns the subquery
    /// refs the caller's `FullKeyAggregate` joins over. No-op when
    /// the query has none.
    pub fn plan_queries(
        &self,
        scope: &mut PlanningScope,
    ) -> Result<Vec<Rc<MultiStageSubqueryRef>>, CubeError> {
        let multi_stage_members = self
            .query_properties
            .all_used_symbols()?
            .into_iter()
            .filter_map(|memb| -> Option<Result<_, CubeError>> {
                match has_multi_stage_members(&memb, false) {
                    Ok(true) => Some(Ok(memb)),
                    Ok(false) => None,
                    Err(e) => Some(Err(e)),
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        if multi_stage_members.is_empty() {
            return Ok(vec![]);
        }

        let mut descriptions = Vec::new();
        let state = self.root_state.clone();

        let mut resolved_multi_stage_dimensions = HashSet::new();
        let mut subquery_refs = Vec::new();

        for member in multi_stage_members {
            let description = self.make_queries_descriptions(
                member.clone(),
                state.clone(),
                &mut descriptions,
                &mut resolved_multi_stage_dimensions,
                scope,
            )?;
            if !description.is_multi_stage_dimension() {
                let result = MultiStageSubqueryRef::builder()
                    .name(description.alias().clone())
                    .symbols(vec![description.member_node().clone()])
                    .schema(description.schema().clone())
                    .build();
                subquery_refs.push(Rc::new(result));
            }
        }

        for descr in descriptions.into_iter() {
            let planner = MultiStageMemberQueryPlanner::new(
                self.query_tools.clone(),
                self.query_properties.clone(),
                descr.clone(),
            );
            let member = planner.plan_logical_query(scope)?;
            scope.add_member(member);
        }

        Ok(subquery_refs)
    }

    /// Classifies `base_member` into a `MultiStageInodeMember` — picks
    /// the inode kind (Rank / Aggregate / Calculate for a measure,
    /// Dimension for a dimension) and carries over the partition-shaping
    /// `grain` and optional `time_shift` from the data-model definition.
    /// Returns the inode together with the leaf's `is_ungrupped` flag.
    fn create_multi_stage_inode_member(
        &self,
        base_member: Rc<MemberSymbol>,
        resolved_multi_stage_dimensions: &mut HashSet<String>,
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
            // only exists further down in `make_queries_descriptions` — the
            // flag is revoked there.
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
            resolved_multi_stage_dimensions
                .insert(base_member.clone().resolve_reference_chain().full_name());
            (
                MultiStageInodeMember::new(MultiStageInodeMemberType::Dimension, grain, None),
                false,
            )
        };
        Ok(inode)
    }

    /// Builds child descriptions for `member`'s inode. Switches to
    /// `try_make_childs_for_case_switch` when the member's body is a
    /// CASE-SWITCH expression; otherwise falls through to
    /// `default_make_childs`.
    fn make_childs(
        &self,
        member: Rc<MemberSymbol>,
        new_state: Rc<QueryProperties>,
        parent_state: &Rc<QueryProperties>,
        result: &mut Vec<Rc<MultiStageQueryDescription>>,
        descriptions: &mut Vec<Rc<MultiStageQueryDescription>>,
        resolved_multi_stage_dimensions: &mut HashSet<String>,
        scope: &mut PlanningScope,
    ) -> Result<(), CubeError> {
        // The CASE-SWITCH path plans every branch dependency as its own CTE,
        // dimensions included, so each one is a column of the source rather than
        // something the stage grain has to carry. It deliberately skips the
        // reachability check `default_make_childs` applies.
        if let Some(Case::CaseSwitch(case_switch)) = member.case() {
            if self.try_make_childs_for_case_switch(
                case_switch,
                new_state.clone(),
                result,
                descriptions,
                resolved_multi_stage_dimensions,
                scope,
            )? {
                return Ok(());
            }
        }
        self.default_make_childs(
            member,
            new_state,
            parent_state,
            result,
            descriptions,
            resolved_multi_stage_dimensions,
            scope,
        )
    }

    /// True if `member` is a dimension that has multi-stage members
    /// somewhere in its dependency tree.
    fn is_multi_stage_dimension(member: &Rc<MemberSymbol>) -> Result<bool, CubeError> {
        if member.is_dimension() {
            has_multi_stage_members(member, false)
        } else {
            Ok(false)
        }
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

    /// Applies the partition-shaping part of `grain` to a parent-state
    /// dimension list: `exclude` removes matching dims, then `keep_only`
    /// intersects what's left. `include` is appended outside this helper
    /// via `add_dimensions`.
    ///
    /// FIXME: merge with `MultiStageMemberQueryPlanner::member_partition_by_logical`
    /// — both apply the same grain reshape on different inputs; keeping two
    /// copies invites silent drift when only one is updated.
    fn partition_filter(
        dims: &Vec<Rc<MemberSymbol>>,
        grain: &MultiStageGrain,
    ) -> Vec<Rc<MemberSymbol>> {
        let dims: Vec<Rc<MemberSymbol>> = if let Some(exclude) = &grain.exclude {
            dims.iter()
                .filter(|d| !exclude.iter().any(|m| d.matches_grain_reference(m)))
                .cloned()
                .collect()
        } else {
            dims.clone()
        };
        if let Some(keep_only) = &grain.keep_only {
            dims.into_iter()
                .filter(|d| keep_only.iter().any(|m| d.matches_grain_reference(m)))
                .collect()
        } else {
            dims
        }
    }

    /// Default child-generation path: for each measure or
    /// multi-stage-dimension dependency, recurses into
    /// `make_queries_descriptions` and adds the result as an input
    /// CTE. If the member has no such deps (e.g. a `Rank` measure
    /// that only needs the dimension grid), produces a single
    /// "without-member" leaf instead.
    fn default_make_childs(
        &self,
        member: Rc<MemberSymbol>,
        new_state: Rc<QueryProperties>,
        parent_state: &Rc<QueryProperties>,
        result: &mut Vec<Rc<MultiStageQueryDescription>>,
        descriptions: &mut Vec<Rc<MultiStageQueryDescription>>,
        resolved_multi_stage_dimensions: &mut HashSet<String>,
        scope: &mut PlanningScope,
    ) -> Result<(), CubeError> {
        let is_masked = |m: &Rc<MemberSymbol>| {
            self.query_tools
                .query_tools()
                .is_member_masked(&m.full_name())
        };
        let rendered: HashSet<String> = rendered_dependencies(&member, &is_masked)
            .into_iter()
            .map(|d| d.resolve_reference_chain().full_name())
            .collect();
        let mut has_inputs = false;
        for dep in member.get_dependencies() {
            let dep = &dep.resolve_reference_chain();
            if dep.is_measure() || Self::is_multi_stage_dimension(dep)? {
                has_inputs = true;
                let description = self.make_queries_descriptions(
                    dep.clone(),
                    new_state.clone(),
                    descriptions,
                    resolved_multi_stage_dimensions,
                    scope,
                )?;
                if !description.is_multi_stage_dimension() || member.as_dimension().is_ok() {
                    result.push(description);
                }
            } else if dep.is_dimension() && rendered.contains(&dep.full_name()) {
                self.check_dimension_is_reachable(&member, dep, &new_state, parent_state)?;
            }
        }
        if !has_inputs {
            //Rank and similas cases

            let alias = scope.next_cte_name();
            let description = MultiStageQueryDescription::new(
                MultiStageMember::new_without_member_leaf(
                    MultiStageMemberType::Leaf(MultiStageLeafMemberType::Measure),
                    member.clone(),
                    self.query_properties.ungrouped(),
                    false,
                ),
                new_state.clone(),
                vec![],
                vec![],
                alias,
            );
            result.push(description.clone());
            descriptions.push(description.clone());
        }
        Ok(())
    }

    /// A dimension read by a multi-stage member's SQL is rendered against the
    /// CTE that computes the member, so it has to be a column of it. Two grains
    /// can supply one: the stage's own (`grain_state`) and, when the assembly
    /// broadcasts a narrowed measure back onto the query grid, the keys side
    /// built from `parent_state`. Beyond those the dimension has no rendering at
    /// all — the fallback is its own cube alias, and no cube is part of a CTE
    /// built from subqueries.
    fn check_dimension_is_reachable(
        &self,
        member: &Rc<MemberSymbol>,
        dimension: &Rc<MemberSymbol>,
        grain_state: &QueryProperties,
        parent_state: &QueryProperties,
    ) -> Result<(), CubeError> {
        if dimension_is_reachable(dimension, grain_state, parent_state, &|m| {
            self.query_tools
                .query_tools()
                .is_member_masked(&m.full_name())
        }) {
            return Ok(());
        }
        let grain = Self::grain_members(grain_state, parent_state);
        // A granularity of the very dimension the sql reads looks like a match in
        // the listing, so the one case where reading the grain is not enough to
        // tell them apart is spelled out.
        let target = dimension.clone().resolve_reference_chain().full_name();
        let hint = if grain
            .iter()
            .any(|m| Self::granular_time_dimension_base(m).as_ref() == Some(&target))
        {
            format!(
                " The grain carries {target} at a granularity, which is a value of its own and not \
                 the dimension itself."
            )
        } else {
            String::new()
        };
        let grain = if grain.is_empty() {
            "no dimensions".to_string()
        } else {
            grain
                .iter()
                .map(Self::describe_grain_member)
                .collect::<Vec<_>>()
                .join(", ")
        };
        Err(CubeError::user(format!(
            "Multi-stage member {member} reads dimension {dimension}, which is not part of the \
             grain it is computed at ({grain}).{hint} Add {dimension} to `grain.include` of \
             {member}, or remove it from the member's sql.",
            member = member.full_name(),
            dimension = dimension.full_name(),
        )))
    }

    // The grain the member is computed at: the stage's own dimensions and, when
    // the assembly broadcasts back onto the query grid, the keys side.
    fn grain_members(
        grain_state: &QueryProperties,
        parent_state: &QueryProperties,
    ) -> Vec<Rc<MemberSymbol>> {
        let mut members: Vec<Rc<MemberSymbol>> = Vec::new();
        for state in [grain_state, parent_state] {
            for dimension in state
                .dimensions()
                .iter()
                .chain(state.time_dimensions().iter())
            {
                let resolved = dimension.clone().resolve_reference_chain();
                if !members
                    .iter()
                    .any(|m| m.full_name() == resolved.full_name())
                {
                    members.push(resolved);
                }
            }
        }
        members
    }

    // A time dimension carries its granularity inside its name, which reads as a
    // member of its own; the granularity is named separately instead.
    fn describe_grain_member(member: &Rc<MemberSymbol>) -> String {
        match member.as_ref() {
            MemberSymbol::TimeDimension(time_dimension) => match time_dimension.granularity() {
                Some(granularity) => format!(
                    "{} ({})",
                    time_dimension
                        .base_symbol()
                        .clone()
                        .resolve_reference_chain()
                        .full_name(),
                    granularity
                ),
                None => member.full_name(),
            },
            _ => member.full_name(),
        }
    }

    fn granular_time_dimension_base(member: &Rc<MemberSymbol>) -> Option<String> {
        match member.as_ref() {
            MemberSymbol::TimeDimension(time_dimension) => {
                time_dimension.granularity().as_ref().map(|_| {
                    time_dimension
                        .base_symbol()
                        .clone()
                        .resolve_reference_chain()
                        .full_name()
                })
            }
            _ => None,
        }
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
        result: &mut Vec<Rc<MultiStageQueryDescription>>,
        descriptions: &mut Vec<Rc<MultiStageQueryDescription>>,
        resolved_multi_stage_dimensions: &mut HashSet<String>,
        scope: &mut PlanningScope,
    ) -> Result<bool, CubeError> {
        let CaseSwitchItem::Member(switch_member) = &case.switch else {
            return Ok(false);
        };

        // Collect, per dependency, the union of switch values that need it.
        // `None` marks an unrestricted (open ELSE) entry: such a dependency
        // must be processed without a prefilter on switch_member, since the
        // outer CASE will dispatch by value at row level.
        let mut deps: IndexMap<String, (Rc<MemberSymbol>, Option<Vec<String>>)> = IndexMap::new();

        let mut record = |dep: Rc<MemberSymbol>, branch_values: Option<Vec<String>>| {
            let dep = dep.resolve_reference_chain();
            let entry = deps
                .entry(dep.full_name())
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
            result.push(self.make_queries_descriptions(
                dep,
                state,
                descriptions,
                resolved_multi_stage_dimensions,
                scope,
            )?);
        }

        Ok(true)
    }

    /// Core recursive step. Given a `member` and the current
    /// `state`, resolves the reference chain, applies static filters
    /// from the dimensions filters, deduplicates against
    /// already-built descriptions, tries a rolling-window path
    /// (`try_plan_rolling_window`), and otherwise returns either a
    /// leaf `Measure` or an inode description whose children come
    /// from `make_childs`. Adjusts the state for any grain reshape,
    /// time-shift or per-member filter changes the inode demands.
    fn make_queries_descriptions(
        &self,
        member: Rc<MemberSymbol>,
        state: Rc<QueryProperties>,
        descriptions: &mut Vec<Rc<MultiStageQueryDescription>>,
        resolved_multi_stage_dimensions: &mut HashSet<String>,
        scope: &mut PlanningScope,
    ) -> Result<Rc<MultiStageQueryDescription>, CubeError> {
        let member = member.resolve_reference_chain();
        let member =
            transforms::apply_static_filter_to_symbol(&member, state.dimensions_filters())?;
        // `filter: include` lets a stage filter a member the query around it does
        // not, so activity is settled against this stage's own set.
        let member = transforms::apply_filter_params_activity_to_symbol(
            &member,
            &transforms::filter_params_activity_filters(&state.all_filter_items()),
        )?;
        let state = if member.is_dimension() {
            let mut new_state = state.as_ref().clone();
            new_state.remove_multistage_dimensions(resolved_multi_stage_dimensions)?;
            Rc::new(new_state)
        } else {
            state
        };

        let member_name = member.full_name();
        // Skip without-member leaves: they carry the rank/similar member's
        // own name only to select its dimension grid, so `(member, state)`
        // alone can't tell them apart from the member's real inode CTE. A
        // `{member}` reference must always resolve to the computing inode,
        // never to the bare-grid leaf.
        if let Some(exists) = descriptions.iter().find(|q| {
            !q.member().is_without_member_leaf() && q.is_match_member_and_state(&member, &state)
        }) {
            return Ok(exists.clone());
        };

        if let Some(rolling_window_query) = self.try_plan_rolling_window(
            member.clone(),
            state.clone(),
            descriptions,
            resolved_multi_stage_dimensions,
            scope,
        )? {
            return Ok(rolling_window_query);
        }

        let has_multi_stage_members = has_multi_stage_members(&member, false)?;
        let description = if !has_multi_stage_members {
            let alias = scope.next_cte_name();
            MultiStageQueryDescription::new(
                MultiStageMember::new(
                    MultiStageMemberType::Leaf(MultiStageLeafMemberType::Measure),
                    member.clone(),
                    self.query_properties.ungrouped(),
                    false,
                ),
                state.clone(),
                vec![],
                vec![],
                alias.clone(),
            )
        } else {
            let (mut multi_stage_member, is_ungrupped) = self
                .create_multi_stage_inode_member(member.clone(), resolved_multi_stage_dimensions)?;

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
                    Some(MultiStageFilterMode::Fixed) => self.root_state().as_ref().clone(),
                    Some(MultiStageFilterMode::Relative) | None => state.as_ref().clone(),
                };

                if let Some(filter) = &directive_filter {
                    apply_filter_directive_to_state(filter, &mut filtered_state);
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
                query_filters_dropped(self.root_state(), &state, &filtered_state);
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
            // unreferenced CTEs behind. And a Rank inode ranks within whatever
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
                    let dims = Self::partition_filter(new_state.dimensions(), grain);
                    let time_dims = Self::partition_filter(new_state.time_dimensions(), grain);
                    new_state.set_dimensions(dims);
                    new_state.set_time_dimensions(time_dims);
                }
                if !dimensions_to_add.is_empty() {
                    new_state.add_dimensions(dimensions_to_add.clone());
                }
                if let Some(time_shift) = multi_stage_member.time_shift() {
                    new_state.add_time_shifts(time_shift.clone())?;
                }
                if new_state.has_filters_for_member(&member_name) {
                    new_state.remove_filter_for_member(&member_name);
                }
                Rc::new(new_state)
            };

            let mut input = vec![];
            self.make_childs(
                member.clone(),
                new_state.clone(),
                &state,
                &mut input,
                descriptions,
                resolved_multi_stage_dimensions,
                scope,
            )?;

            // JOIN-model: when the measure side no longer enumerates the rows
            // this inode has to report — because new_state misses a dim that
            // was on the parent's `state`, or because a dropped query filter
            // widened it — we build keys-side descriptions per child on the
            // parent state, so the FullKeyAggregate broadcasts measure values
            // onto the query grain and only onto it. Window-path Aggregate
            // inodes (sum-of-sum / sum-of-count with no leaf-extending
            // `include`) handle broadcast via the window expression instead and
            // don't need keys_input.
            let mut keys_input: Vec<Rc<MultiStageQueryDescription>> = vec![];
            if !use_window_path {
                let new_state_has = |sym: &Rc<MemberSymbol>| {
                    let sym_name = sym.clone().resolve_reference_chain().full_name();
                    new_state
                        .dimensions()
                        .iter()
                        .chain(new_state.time_dimensions().iter())
                        .any(|d| d.clone().resolve_reference_chain().full_name() == sym_name)
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
                    self.make_childs(
                        member.clone(),
                        state.clone(),
                        &state,
                        &mut keys_input,
                        descriptions,
                        resolved_multi_stage_dimensions,
                        scope,
                    )?;
                }
            }

            let alias = scope.next_cte_name();
            MultiStageQueryDescription::new(
                MultiStageMember::new(
                    MultiStageMemberType::Inode(multi_stage_member),
                    member,
                    is_ungrupped,
                    false,
                ),
                state.clone(),
                input,
                keys_input,
                alias.clone(),
            )
        };

        descriptions.push(description.clone());
        Ok(description)
    }

    /// If `member` is a cumulative measure, plans the time-series
    /// and rolling-window CTEs and returns the rolling-window
    /// description. Returns `None` for other measures and for
    /// non-measure members.
    pub fn try_plan_rolling_window(
        &self,
        member: Rc<MemberSymbol>,
        state: Rc<QueryProperties>,
        descriptions: &mut Vec<Rc<MultiStageQueryDescription>>,
        resolved_multi_stage_dimensions: &mut HashSet<String>,
        scope: &mut PlanningScope,
    ) -> Result<Option<Rc<MultiStageQueryDescription>>, CubeError> {
        if let Ok(measure) = member.as_measure() {
            if measure.is_cumulative() {
                let rolling_window = if let Some(rolling_window) = measure.rolling_window() {
                    rolling_window.clone()
                } else {
                    RollingWindow {
                        trailing: None,
                        leading: None,
                        offset: None,
                        rolling_type: None,
                        granularity: None,
                    }
                };

                if !measure.is_multi_stage() {
                    let childs = member_childs(&member, true)?;
                    let measures = childs
                        .iter()
                        .filter(|s| s.as_measure().is_ok())
                        .collect_vec();
                    if !measures.is_empty() {
                        return Err(CubeError::user(
                            format!("Measure {} references another measures ({}). In this case, {} must have multi_stage: true defined",
                            member.full_name(),
                            measures.into_iter().map(|m| m.full_name()).join(", "),
                            member.full_name(),
                                        ),
                        ));
                    }
                }

                let ungrouped = measure.is_rolling_window() && !measure.is_additive();

                let mut time_dimensions = self
                    .query_properties
                    .time_dimensions()
                    .iter()
                    .map(|d| d.as_time_dimension())
                    .collect::<Result<Vec<_>, _>>()?;
                for dim in self.query_properties.dimensions() {
                    let dim = dim.clone().resolve_reference_chain();
                    if let Ok(time_dimension) = dim.as_time_dimension() {
                        time_dimensions.push(time_dimension);
                    }
                }

                let base_member = MemberSymbol::new_measure(transforms::unroll_rolling(&measure));

                if time_dimensions.is_empty() {
                    let base_state =
                        self.replace_date_range_for_rolling_window(&rolling_window, state.clone())?;
                    let rolling_base = if !measure.is_multi_stage() {
                        self.add_rolling_window_base(
                            base_member,
                            base_state,
                            false,
                            descriptions,
                            scope,
                        )?
                    } else {
                        self.make_queries_descriptions(
                            base_member,
                            base_state,
                            descriptions,
                            resolved_multi_stage_dimensions,
                            scope,
                        )?
                    };
                    return Ok(Some(rolling_base));
                }
                let uniq_time_dimensions = time_dimensions
                    .iter()
                    .unique_by(|a| (a.cube_name(), a.name(), a.date_range_vec()))
                    .collect_vec();
                if uniq_time_dimensions.len() != 1 {
                    return Err(CubeError::internal(
                        "Rolling window requires one time dimension and equal date ranges"
                            .to_string(),
                    ));
                }

                let time_dimension =
                    GranularityHelper::find_dimension_with_min_granularity(&time_dimensions)?;
                let time_dimension = MemberSymbol::new_time_dimension(time_dimension);

                let (base_rolling_state, base_time_dimension) = self.make_rolling_base_state(
                    time_dimension.clone(),
                    &rolling_window,
                    state.clone(),
                )?;

                let time_series =
                    self.add_time_series(time_dimension.clone(), state.clone(), descriptions)?;

                let rolling_base = if !measure.is_multi_stage() {
                    self.add_rolling_window_base(
                        base_member,
                        base_rolling_state,
                        ungrouped,
                        descriptions,
                        scope,
                    )?
                } else {
                    self.make_queries_descriptions(
                        base_member,
                        base_rolling_state,
                        descriptions,
                        resolved_multi_stage_dimensions,
                        scope,
                    )?
                };

                let input = vec![time_series, rolling_base];

                let alias = scope.next_cte_name();

                let rolling_window_descr = if let Some(granularity) =
                    self.get_to_date_rolling_granularity(&rolling_window)?
                {
                    RollingWindowDescription::new_to_date(
                        time_dimension,
                        base_time_dimension,
                        granularity,
                    )
                } else {
                    RollingWindowDescription::new_regular(
                        time_dimension,
                        base_time_dimension,
                        rolling_window.trailing.clone(),
                        rolling_window.leading.clone(),
                        rolling_window.offset.clone().unwrap_or("end".to_string()),
                    )
                };

                let inode_member = MultiStageInodeMember::new(
                    MultiStageInodeMemberType::RollingWindow(rolling_window_descr),
                    MultiStageGrain::default(),
                    None,
                );

                let description = MultiStageQueryDescription::new(
                    MultiStageMember::new(
                        MultiStageMemberType::Inode(inode_member),
                        member,
                        self.query_properties.ungrouped(),
                        false,
                    ),
                    state.clone(),
                    input,
                    vec![],
                    alias.clone(),
                );
                descriptions.push(description.clone());
                Ok(Some(description))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Adds (or reuses) the `time_series_get_range` leaf CTE — used
    /// by `add_time_series` when the requested time dimension has no
    /// explicit date range and the planner needs to compute one.
    fn add_time_series_get_range_query(
        &self,
        time_dimension: Rc<MemberSymbol>,
        state: Rc<QueryProperties>,
        descriptions: &mut Vec<Rc<MultiStageQueryDescription>>,
    ) -> Result<Rc<MultiStageQueryDescription>, CubeError> {
        let description = if let Some(description) = descriptions
            .iter()
            .find(|d| d.alias() == "time_series_get_range")
        {
            description.clone()
        } else {
            let time_series_get_range_node = MultiStageQueryDescription::new(
                MultiStageMember::new(
                    MultiStageMemberType::Leaf(MultiStageLeafMemberType::TimeSeriesGetRange(
                        time_dimension.clone(),
                    )),
                    time_dimension.clone(),
                    true,
                    false,
                ),
                state.clone(),
                vec![],
                vec![],
                "time_series_get_range".to_string(),
            );
            descriptions.push(time_series_get_range_node.clone());
            time_series_get_range_node
        };
        Ok(description)
    }

    /// Adds (or reuses) the `time_series` leaf CTE that drives a
    /// rolling window. When the time dimension has no `date_range`,
    /// also arranges for a sibling `time_series_get_range` CTE to
    /// feed it.
    fn add_time_series(
        &self,
        time_dimension: Rc<MemberSymbol>,
        state: Rc<QueryProperties>,
        descriptions: &mut Vec<Rc<MultiStageQueryDescription>>,
    ) -> Result<Rc<MultiStageQueryDescription>, CubeError> {
        let description = if let Some(description) =
            descriptions.iter().find(|d| d.alias() == "time_series")
        {
            description.clone()
        } else {
            let get_range_query_description = if time_dimension
                .as_time_dimension()?
                .date_range_vec()
                .is_some()
            {
                None
            } else {
                Some(self.add_time_series_get_range_query(
                    time_dimension.clone(),
                    state.clone(),
                    descriptions,
                )?)
            };
            let time_series_node = MultiStageQueryDescription::new(
                MultiStageMember::new(
                    MultiStageMemberType::Leaf(MultiStageLeafMemberType::TimeSeries(Rc::new(
                        TimeSeriesDescription {
                            time_dimension: time_dimension.clone(),
                            date_range_cte: get_range_query_description.map(|d| d.alias().clone()),
                        },
                    ))),
                    time_dimension.clone(),
                    true,
                    false,
                ),
                state.clone(),
                vec![],
                vec![],
                "time_series".to_string(),
            );
            descriptions.push(time_series_node.clone());
            time_series_node
        };
        Ok(description)
    }

    /// Adds the leaf CTE that produces the base values for a
    /// rolling window — selects the requested dimensions plus the
    /// unrolled measure, marked `has_aggregates_on_top` so callers
    /// know an outer rolling computation will consume it.
    fn add_rolling_window_base(
        &self,
        member: Rc<MemberSymbol>,
        state: Rc<QueryProperties>,
        ungrouped: bool,
        descriptions: &mut Vec<Rc<MultiStageQueryDescription>>,
        scope: &mut PlanningScope,
    ) -> Result<Rc<MultiStageQueryDescription>, CubeError> {
        let alias = scope.next_cte_name();
        let description = MultiStageQueryDescription::new(
            MultiStageMember::new(
                MultiStageMemberType::Leaf(MultiStageLeafMemberType::Measure),
                member,
                self.query_properties.ungrouped() || ungrouped,
                true,
            ),
            state,
            vec![],
            vec![],
            alias.clone(),
        );
        descriptions.push(description.clone());
        Ok(description)
    }

    /// Returns the granularity of a `to_date` rolling window. Errors
    /// if the window is declared as `to_date` without a granularity,
    /// and returns `None` for window kinds that don't carry one.
    fn get_to_date_rolling_granularity(
        &self,
        rolling_window: &RollingWindow,
    ) -> Result<Option<String>, CubeError> {
        let is_to_date = rolling_window
            .rolling_type
            .as_ref()
            .is_some_and(|tp| tp == "to_date");

        if is_to_date {
            if let Some(granularity) = &rolling_window.granularity {
                Ok(Some(granularity.clone()))
            } else {
                Err(CubeError::user(format!(
                    "Granularity required for to_date rolling window"
                )))
            }
        } else {
            Ok(None)
        }
    }

    /// Adjust date range filters for rolling window when there's no granularity.
    /// Without granularity there's no time_series CTE, so the InDateRange filter
    /// is rewritten into the rolling-window bounds (anchored by the window offset)
    /// applied directly to the base measure.
    fn replace_date_range_for_rolling_window(
        &self,
        rolling_window: &RollingWindow,
        state: Rc<QueryProperties>,
    ) -> Result<Rc<QueryProperties>, CubeError> {
        let mut new_state = state.as_ref().clone();
        for filter_item in state.time_dimensions_filters() {
            if let FilterItem::Item(filter) = filter_item {
                if matches!(filter.filter_operator(), FilterOperator::InDateRange) {
                    new_state.replace_date_range_for_rolling_window_without_granularity(
                        &filter.member_name(),
                        &rolling_window.trailing,
                        &rolling_window.leading,
                        rolling_window.offset.as_deref().unwrap_or("end"),
                    )?;
                }
            }
        }
        Ok(Rc::new(new_state))
    }

    /// Builds the state for a rolling-window base CTE: reduces the
    /// time dimension to the minimum granularity required by the
    /// window, drops other dimensions that resolve to time
    /// dimensions, and rewrites the time-dimension date-range
    /// filter to either a `to_date` bound or a regular trailing /
    /// leading bound.
    fn make_rolling_base_state(
        &self,
        time_dimension: Rc<MemberSymbol>,
        rolling_window: &RollingWindow,
        state: Rc<QueryProperties>,
    ) -> Result<(Rc<QueryProperties>, Rc<MemberSymbol>), CubeError> {
        let time_dimension_symbol = time_dimension.as_time_dimension()?;
        let time_dimension_base_name = time_dimension_symbol.base_symbol().full_name();
        let mut new_state = state.as_ref().clone();
        let trailing_granularity =
            GranularityHelper::granularity_from_interval(&rolling_window.trailing);
        let leading_granularity =
            GranularityHelper::granularity_from_interval(&rolling_window.leading);
        let window_granularity =
            GranularityHelper::min_granularity(&trailing_granularity, &leading_granularity)?;
        let result_granularity = GranularityHelper::min_granularity(
            &window_granularity,
            &time_dimension_symbol.resolved_granularity()?,
        )?;

        let new_time_dimension_symbol = time_dimension_symbol
            .change_granularity(self.query_tools.clone(), result_granularity.clone())?;
        let new_time_dimension = MemberSymbol::new_time_dimension(new_time_dimension_symbol);
        //We keep only one time_dimension in the leaf query because, even if time_dimension values have different granularity, in the leaf query we need to group by the lowest granularity.
        new_state.set_time_dimensions(vec![new_time_dimension.clone()]);

        let dimensions = new_state
            .dimensions()
            .clone()
            .into_iter()
            .filter(|d| {
                d.clone()
                    .resolve_reference_chain()
                    .as_time_dimension()
                    .is_err()
            })
            .collect_vec();
        new_state.set_dimensions(dimensions);

        if let Some(granularity) = self.get_to_date_rolling_granularity(rolling_window)? {
            new_state.replace_to_date_date_range_filter(&time_dimension_base_name, &granularity)?;
        } else {
            new_state.replace_regular_date_range_filter(
                &time_dimension_base_name,
                rolling_window.trailing.clone(),
                rolling_window.leading.clone(),
            )?;
        }

        Ok((Rc::new(new_state), new_time_dimension))
    }
}

// Mirrors how references are resolved when the CTE is rendered: a member the
// source exposes as a column stops the walk, and anything else is reachable only
// if every member its own SQL reads is. A dimension that also reads a raw cube
// column is unreachable whatever its member deps resolve to — that column renders
// against the cube alias, which such a CTE never has in scope.
//
// A leaf outside both grains is reported as well. `sql: category` and a constant
// `sql: "'x'"` are indistinguishable here — neither carries a cube ref — so the
// constant is reported too, and declaring it in `grain.include` resolves that the
// same way. The same ambiguity runs the other way: a bare identifier inside a
// larger expression (`UPPER({CUBE.status}) || category`) carries no cube ref
// either, so it passes and fails at the database instead.
fn dimension_is_reachable(
    dimension: &Rc<MemberSymbol>,
    grain_state: &QueryProperties,
    parent_state: &QueryProperties,
    is_masked: &dyn Fn(&Rc<MemberSymbol>) -> bool,
) -> bool {
    let target = dimension.full_name();
    let carries = |state: &QueryProperties| {
        state
            .dimensions()
            .iter()
            .chain(state.time_dimensions().iter())
            .any(|d| d.clone().resolve_reference_chain().full_name() == target)
    };
    if carries(grain_state) || carries(parent_state) {
        return true;
    }
    let deps = rendered_dependencies(dimension, is_masked);
    !deps.is_empty()
        && !reads_raw_cube_column(dimension, is_masked)
        && deps.iter().all(|dep| {
            dimension_is_reachable(
                &dep.clone().resolve_reference_chain(),
                grain_state,
                parent_state,
                is_masked,
            )
        })
}

// The slots of a member that reach its rendered SQL. `drill_filters` are carried
// by the symbol but never emitted, so they never put a column requirement on the
// CTE. A `mask` is emitted only for the members it applies to, so it is included
// for those and excluded otherwise. Both the member deps and the cube refs have to
// be read through this, or an excluded slot leaks back in through the side that
// isn't filtered.
//
// `iter_sql_calls` is the neighbouring accessor and is deliberately not reused:
// on the measure side it covers `kind` and `case` only, missing `measure_filters`
// and `measure_order_by`.
fn visit_rendered_slots(
    member: &Rc<MemberSymbol>,
    is_masked: &dyn Fn(&Rc<MemberSymbol>) -> bool,
    visit: &mut dyn FnMut(&dyn SymbolDeps),
) {
    // A mask reaches the SQL exactly for the members it is applied to, so it
    // counts as rendered only for those. The member's own slots below stay
    // required even then: an unconditional mask replaces the original render
    // rather than wrapping it, so strictly they are not emitted for a masked
    // member — but the same model is broken for every unmasked one, and the
    // narrower rule would only move where that surfaces.
    if is_masked(member) {
        if let Some(mask) = member.mask_sql() {
            visit(mask);
        }
    }
    if let Ok(measure) = member.as_measure() {
        visit(measure.kind());
        for filter in measure.measure_filters() {
            visit(filter);
        }
        for order_by in measure.measure_order_by() {
            visit(order_by);
        }
        if let Some(case) = measure.case() {
            visit(case);
        }
    } else if let Ok(dimension) = member.as_dimension() {
        visit(dimension.kind());
    } else if let Ok(time_dimension) = member.as_time_dimension() {
        // A time dimension is a view of its base: the granularity renders around
        // whatever the base renders, so both contribute.
        visit(time_dimension.granularity_obj());
        visit_rendered_slots(time_dimension.base_symbol(), is_masked, visit);
    } else {
        visit(member.as_ref());
    }
}

fn rendered_dependencies(
    member: &Rc<MemberSymbol>,
    is_masked: &dyn Fn(&Rc<MemberSymbol>) -> bool,
) -> Vec<Rc<MemberSymbol>> {
    let mut result = vec![];
    visit_rendered_slots(member, is_masked, &mut |slot| {
        result.extend(collect_deps(slot))
    });
    result
}

fn reads_raw_cube_column(
    member: &Rc<MemberSymbol>,
    is_masked: &dyn Fn(&Rc<MemberSymbol>) -> bool,
) -> bool {
    let mut found = false;
    visit_rendered_slots(member, is_masked, &mut |slot| {
        found = found || !collect_cube_refs(slot).is_empty()
    });
    found
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
// 1. Rolling window. `try_plan_rolling_window` builds `base_rolling_state`
//    via `make_rolling_base_state` (extends date_range, swaps the time
//    dimension, prunes time-dim entries from `dimensions`). When a nested
//    multi-stage with `mode: fixed` is reached during recursion, it falls
//    back to `self.root_state`, dropping those rolling-window-specific
//    mutations — the leaf will read the original (narrow) date range while
//    the outer rolling frame expects the extended one.
//
// 2. Switch-case pruning. `apply_static_filter_to_symbol` runs at the top
//    of `make_queries_descriptions` against `state.dimensions_filters()` —
//    the *inherited* filters, before this function. If the inherited set
//    restricts the switch dimension, case branches are pruned at symbol
//    level; the subsequent `mode: fixed` reset cannot un-prune them.
//
// `add_dimension_evaluator` wraps segment references into a `MemberExpression`
// whose `full_name()` is prefixed with `expr:` (e.g. `expr:orders.completed`).
// `BaseSegment::full_name()` carries the bare path (`orders.completed`). To make
// `exclude`/`keep_only` match both forms, return the symbol's `full_name()`
// alongside its `expr:`-stripped variant.
fn filter_directive_match_names(symbol: &Rc<MemberSymbol>) -> Vec<String> {
    let full = symbol.full_name();
    if let Some(stripped) = full.strip_prefix("expr:") {
        vec![full.clone(), stripped.to_string()]
    } else {
        vec![full]
    }
}

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
// CTE state to begin with — `build_root_state` drops them — so there is no
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

fn apply_filter_directive_to_state(filter: &MultiStageFilter, state: &mut QueryProperties) {
    if let Some(exclude) = &filter.exclude {
        let names: Vec<String> = exclude
            .iter()
            .flat_map(|s| filter_directive_match_names(s))
            .collect();
        state.remove_filters_for_members(&names);
    }
    if let Some(keep_only) = &filter.keep_only {
        let names: Vec<String> = keep_only
            .iter()
            .flat_map(|s| filter_directive_match_names(s))
            .collect();
        state.keep_only_filters_for_members(&names);
    }
    if !filter.include_dimension.is_empty() {
        state.add_dimension_filters(filter.include_dimension.clone());
    }
    if !filter.include_time_dimension.is_empty() {
        state.add_time_dimension_filters(filter.include_time_dimension.clone());
    }
    if !filter.include_measure.is_empty() {
        state.add_measure_filters(filter.include_measure.clone());
    }
}
