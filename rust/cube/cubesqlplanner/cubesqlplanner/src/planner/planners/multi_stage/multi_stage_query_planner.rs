use super::{
    CteState, MultiStageInodeMember, MultiStageInodeMemberType, MultiStageLeafMemberType,
    MultiStageMember, MultiStageMemberQueryPlanner, MultiStageMemberType,
    MultiStageQueryDescription, RollingWindowDescription, TimeSeriesDescription,
};
use crate::cube_bridge::measure_definition::RollingWindow;
use crate::logical_plan::*;
use crate::planner::apply_static_filter_to_symbol;
use crate::planner::collectors::has_multi_stage_members;
use crate::planner::collectors::member_childs;
use crate::planner::filter::base_filter::FilterType;
use crate::planner::filter::BaseFilter;
use crate::planner::filter::FilterItem;
use crate::planner::filter::FilterOperator;
use crate::planner::query_tools::QueryTools;
use crate::planner::symbols::AggregationType;
use crate::planner::Case;
use crate::planner::CaseSwitchDefinition;
use crate::planner::CaseSwitchItem;
use crate::planner::GranularityHelper;
use crate::planner::MeasureKind;
use crate::planner::MemberSymbol;
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
    query_tools: Rc<QueryTools>,
    query_properties: Rc<QueryProperties>,
}

impl MultiStageQueryPlanner {
    pub fn new(query_tools: Rc<QueryTools>, query_properties: Rc<QueryProperties>) -> Self {
        Self {
            query_tools,
            query_properties,
        }
    }

    /// Populates `cte_state` with multi-stage CTEs (and their
    /// subquery refs) for every multi-stage member used by the
    /// query. No-op when the query has none.
    pub fn plan_queries(&self, cte_state: &mut CteState) -> Result<(), CubeError> {
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
            return Ok(());
        }

        let mut descriptions = Vec::new();
        // Multi-stage CTE state: a query carrying the dimensions/filters of the
        // current node in the multi-stage tree. measures_filters are
        // intentionally dropped — CTE queries do not propagate them. order_by
        // is set to an empty vec so the builder skips default_order: this
        // value is used only as a state container, never planned directly.
        let state = QueryProperties::builder()
            .query_tools(self.query_tools.clone())
            .dimensions(self.query_properties.dimensions().clone())
            .time_dimensions(self.query_properties.time_dimensions().clone())
            .dimensions_filters(self.query_properties.dimensions_filters().clone())
            .time_dimensions_filters(self.query_properties.time_dimensions_filters().clone())
            .segments(self.query_properties.segments().clone())
            .order_by(Some(vec![]))
            .build()?;

        let mut resolved_multi_stage_dimensions = HashSet::new();

        for member in multi_stage_members {
            let description = self.make_queries_descriptions(
                member.clone(),
                state.clone(),
                &mut descriptions,
                &mut resolved_multi_stage_dimensions,
                cte_state,
            )?;
            if !description.is_multi_stage_dimension() {
                let result = MultiStageSubqueryRef::builder()
                    .name(description.alias().clone())
                    .symbols(vec![description.member_node().clone()])
                    .schema(description.schema().clone())
                    .build();
                cte_state.add_subquery_ref(Rc::new(result));
            }
        }

        for descr in descriptions.into_iter() {
            let planner = MultiStageMemberQueryPlanner::new(
                self.query_tools.clone(),
                self.query_properties.clone(),
                descr.clone(),
            );
            let member = planner.plan_logical_query()?;
            cte_state.add_member(member);
        }

        Ok(())
    }

    /// Classifies `base_member` into a `MultiStageInodeMember` — picks
    /// the inode kind (Rank / Aggregate / Calculate for a measure,
    /// Dimension for a dimension) and pulls the partition-shaping
    /// flags (`reduce_by`, `add_group_by`, `group_by`, `time_shift`)
    /// out of the data-model definition. Returns the inode together
    /// with the leaf's `is_ungrupped` flag.
    fn create_multi_stage_inode_member(
        &self,
        base_member: Rc<MemberSymbol>,
        resolved_multi_stage_dimensions: &mut HashSet<String>,
    ) -> Result<(MultiStageInodeMember, bool), CubeError> {
        let inode = if let Ok(measure) = base_member.as_measure() {
            let member_type = match measure.kind() {
                MeasureKind::Rank => MultiStageInodeMemberType::Rank,
                MeasureKind::Calculated(_) => MultiStageInodeMemberType::Calculate,
                _ => MultiStageInodeMemberType::Aggregate,
            };

            let time_shift = measure.time_shift().clone();

            let is_ungrupped = match &member_type {
                MultiStageInodeMemberType::Rank | MultiStageInodeMemberType::Calculate => true,
                _ => self.query_properties.ungrouped(),
            };

            let reduce_by = measure.reduce_by().clone().unwrap_or_default();
            let add_group_by = measure.add_group_by().clone().unwrap_or_default();
            let group_by = measure.group_by().clone();
            let use_window_path = matches!(member_type, MultiStageInodeMemberType::Aggregate)
                && add_group_by.is_empty()
                && Self::is_window_path_eligible(&base_member);
            (
                MultiStageInodeMember::new(
                    member_type,
                    reduce_by,
                    add_group_by,
                    group_by,
                    time_shift,
                )
                .with_use_window_path(use_window_path),
                is_ungrupped,
            )
        } else {
            let add_group_by = if let Ok(dimension) = base_member.as_dimension() {
                dimension.add_group_by().clone().unwrap_or_default()
            } else {
                vec![]
            };
            resolved_multi_stage_dimensions
                .insert(base_member.clone().resolve_reference_chain().full_name());
            (
                MultiStageInodeMember::new(
                    MultiStageInodeMemberType::Dimension,
                    vec![],
                    add_group_by,
                    None,
                    None,
                ),
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
        result: &mut Vec<Rc<MultiStageQueryDescription>>,
        descriptions: &mut Vec<Rc<MultiStageQueryDescription>>,
        resolved_multi_stage_dimensions: &mut HashSet<String>,
        cte_state: &mut CteState,
    ) -> Result<(), CubeError> {
        if let Some(Case::CaseSwitch(case_switch)) = member.case() {
            if self.try_make_childs_for_case_switch(
                case_switch,
                new_state.clone(),
                result,
                descriptions,
                resolved_multi_stage_dimensions,
                cte_state,
            )? {
                return Ok(());
            }
        }
        self.default_make_childs(
            member,
            new_state,
            result,
            descriptions,
            resolved_multi_stage_dimensions,
            cte_state,
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
            _ => false,
        }
    }

    /// Mirror of `MultiStageMemberQueryPlanner::member_partition_by_logical`:
    /// drops `reduce_by` dims and (when `group_by` is set) keeps only the
    /// dims explicitly listed. Used at planning time to decide whether
    /// reduce_by / group_by actually shrinks the partition vs the leaf
    /// grain.
    ///
    /// FIXME: merge with `MultiStageMemberQueryPlanner::member_partition_by_logical`
    /// — both apply the same reduce_by/group_by reshape on different inputs;
    /// keeping two copies invites silent drift when only one is updated.
    fn partition_filter(
        dims: &Vec<Rc<MemberSymbol>>,
        reduce_by: &Vec<Rc<MemberSymbol>>,
        group_by: &Option<Vec<Rc<MemberSymbol>>>,
    ) -> Vec<Rc<MemberSymbol>> {
        let dims: Vec<Rc<MemberSymbol>> = if !reduce_by.is_empty() {
            dims.iter()
                .filter(|d| !reduce_by.iter().any(|m| d.has_member_in_reference_chain(m)))
                .cloned()
                .collect()
        } else {
            dims.clone()
        };
        if let Some(group_by) = group_by {
            dims.into_iter()
                .filter(|d| group_by.iter().any(|m| d.has_member_in_reference_chain(m)))
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
        result: &mut Vec<Rc<MultiStageQueryDescription>>,
        descriptions: &mut Vec<Rc<MultiStageQueryDescription>>,
        resolved_multi_stage_dimensions: &mut HashSet<String>,
        cte_state: &mut CteState,
    ) -> Result<(), CubeError> {
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
                    cte_state,
                )?;
                if !description.is_multi_stage_dimension() || member.as_dimension().is_ok() {
                    result.push(description);
                }
            }
        }
        if !has_inputs {
            //Rank and similas cases

            let alias = cte_state.next_cte_name();
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
        cte_state: &mut CteState,
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
                        self.query_tools.clone(),
                        switch_member.clone(),
                        FilterType::Dimension,
                        FilterOperator::Equal,
                        Some(values.into_iter().map(Some).collect_vec()),
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
                cte_state,
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
    /// from `make_childs`. Adjusts the state for inodes with any
    /// `add_group_by`, time-shift or per-member filter changes the
    /// inode demands.
    fn make_queries_descriptions(
        &self,
        member: Rc<MemberSymbol>,
        state: Rc<QueryProperties>,
        descriptions: &mut Vec<Rc<MultiStageQueryDescription>>,
        resolved_multi_stage_dimensions: &mut HashSet<String>,
        cte_state: &mut CteState,
    ) -> Result<Rc<MultiStageQueryDescription>, CubeError> {
        let member = member.resolve_reference_chain();
        let member = apply_static_filter_to_symbol(&member, state.dimensions_filters())?;
        let state = if member.is_dimension() {
            let mut new_state = state.as_ref().clone();
            new_state.remove_multistage_dimensions(resolved_multi_stage_dimensions)?;
            Rc::new(new_state)
        } else {
            state
        };

        let member_name = member.full_name();
        if let Some(exists) = descriptions
            .iter()
            .find(|q| q.is_match_member_and_state(&member, &state))
        {
            return Ok(exists.clone());
        };

        if let Some(rolling_window_query) = self.try_plan_rolling_window(
            member.clone(),
            state.clone(),
            descriptions,
            resolved_multi_stage_dimensions,
            cte_state,
        )? {
            return Ok(rolling_window_query);
        }

        let has_multi_stage_members = has_multi_stage_members(&member, false)?;
        let description = if !has_multi_stage_members {
            let alias = cte_state.next_cte_name();
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
            let (multi_stage_member, is_ungrupped) = self
                .create_multi_stage_inode_member(member.clone(), resolved_multi_stage_dimensions)?;

            let mut dimensions_to_add = multi_stage_member.add_group_by_symbols().clone();

            if let Some(case) = member.case() {
                if let Some(switch_dim) = case.case_switch_dimension() {
                    dimensions_to_add.push(switch_dim);
                }
            }

            // new_state is the leaf grain on which children are computed.
            // For JOIN-model Aggregate inodes modifiers apply in this order:
            //   1. reduce_by / group_by — shrink parent grain to the
            //      partition grain implied by directives.
            //   2. add_group_by — extend the result with extra leaf dims.
            //   3. time_shift / filter cleanup.
            // Step 1 must precede step 2: `group_by` is a keep-only filter
            // and would silently drop dims that step 2 needs to introduce.
            //
            // The window-path Aggregate inode skips step 1: the leaf stays
            // at the parent state plus any add_group_by extension, and the
            // window function does the reduce_by collapse at outer level.
            let use_window_path = multi_stage_member.use_window_path();
            let new_state = {
                let mut new_state = state.as_ref().clone();
                if !use_window_path
                    && matches!(
                        multi_stage_member.inode_type(),
                        MultiStageInodeMemberType::Aggregate
                    )
                {
                    let reduce_by = multi_stage_member.reduce_by_symbols().clone();
                    let group_by = multi_stage_member.group_by_symbols().clone();
                    let dims =
                        Self::partition_filter(new_state.dimensions(), &reduce_by, &group_by);
                    let time_dims =
                        Self::partition_filter(new_state.time_dimensions(), &reduce_by, &group_by);
                    new_state.set_dimensions(dims);
                    new_state.set_time_dimensions(time_dims);
                }
                if !dimensions_to_add.is_empty() {
                    new_state.add_dimensions(dimensions_to_add.clone());
                }
                if let Some(time_shift) = multi_stage_member.time_shift() {
                    new_state.add_time_shifts(time_shift.clone())?;
                }
                if state.has_filters_for_member(&member_name) {
                    new_state.remove_filter_for_member(&member_name);
                }
                Rc::new(new_state)
            };

            let mut input = vec![];
            self.make_childs(
                member.clone(),
                new_state.clone(),
                &mut input,
                descriptions,
                resolved_multi_stage_dimensions,
                cte_state,
            )?;

            // JOIN-model: when new_state misses any dim that was on the
            // parent's `state`, this inode shrinks the parent grain. We
            // build keys-side descriptions per child on the parent state
            // so the FullKeyAggregate can broadcast measure values back
            // to the full query grain. Window-path Aggregate inodes
            // (sum-of-sum / sum-of-count without add_group_by) handle
            // broadcast via the window expression instead and don't need
            // keys_input.
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
                if any_missing {
                    self.make_childs(
                        member.clone(),
                        state.clone(),
                        &mut keys_input,
                        descriptions,
                        resolved_multi_stage_dimensions,
                        cte_state,
                    )?;
                }
            }

            let alias = cte_state.next_cte_name();
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
        cte_state: &mut CteState,
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

                let base_member = MemberSymbol::new_measure(measure.new_unrolling());

                if time_dimensions.is_empty() {
                    let base_state =
                        self.replace_date_range_for_rolling_window(&rolling_window, state.clone())?;
                    let rolling_base = if !measure.is_multi_stage() {
                        self.add_rolling_window_base(
                            base_member,
                            base_state,
                            false,
                            descriptions,
                            cte_state,
                        )?
                    } else {
                        self.make_queries_descriptions(
                            base_member,
                            base_state,
                            descriptions,
                            resolved_multi_stage_dimensions,
                            cte_state,
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
                        cte_state,
                    )?
                } else {
                    self.make_queries_descriptions(
                        base_member,
                        base_rolling_state,
                        descriptions,
                        resolved_multi_stage_dimensions,
                        cte_state,
                    )?
                };

                let input = vec![time_series, rolling_base];

                let alias = cte_state.next_cte_name();

                let rolling_window_descr = if measure.is_running_total() {
                    RollingWindowDescription::new_running_total(time_dimension, base_time_dimension)
                } else if let Some(granularity) =
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
                    vec![],
                    vec![],
                    None,
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
        cte_state: &mut CteState,
    ) -> Result<Rc<MultiStageQueryDescription>, CubeError> {
        let alias = cte_state.next_cte_name();
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
    /// Without granularity there's no time_series CTE, so we replace InDateRange
    /// with BeforeOrOnDate/AfterOrOnDate that use parameters directly.
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
