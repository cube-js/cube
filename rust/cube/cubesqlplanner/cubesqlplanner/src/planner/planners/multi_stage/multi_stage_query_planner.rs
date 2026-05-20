use super::member_query_planner::{
    build_for_cte_dimension_query, build_for_cte_query, build_for_leaf_cte_query,
    build_rolling_window_query, build_time_series_get_range_query, build_time_series_query,
    ref_for_member,
};
use super::{
    CteRole, CteState, MultiStageInodeMember, MultiStageInodeMemberType, MultiStageLeafMemberType,
    MultiStageMember, MultiStageMemberType, RollingWindowDescription, TimeSeriesDescription,
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

/// Routes a multi-stage CTE node to its `CteRole`: dimension-flavoured
/// members (DimensionSymbol / TimeDimensionSymbol) become
/// `MultiStageDimension`, everything else (measures, member-expression
/// aggregates, rolling-window inodes) becomes `MultiStageMeasure`.
fn role_for_multi_stage(member: &Rc<MemberSymbol>) -> CteRole {
    if member.is_dimension() {
        CteRole::MultiStageDimension
    } else {
        CteRole::MultiStageMeasure
    }
}

/// Per-planner cache for the singleton time-series leaves of a
/// rolling-window flow. Both the time-series axis and the
/// (optional) date-range computation are shared across every
/// rolling-window member in the same plan, so we register them
/// once and reuse the ref.
#[derive(Default)]
struct TimeSeriesCache {
    time_series: Option<Rc<MultiStageSubqueryRef>>,
    time_series_range: Option<Rc<MultiStageSubqueryRef>>,
}

/// Plans the multi-stage CTE tree of a query in a single bottom-up
/// pass. For each multi-stage member encountered in
/// `all_used_symbols`, `build_multi_stage_cte` recurses into the
/// member's dependencies, registers the resulting body in
/// `cte_state`, and returns a `MultiStageSubqueryRef`. The recursion
/// dedupes through `cte_state.find_matching` — the same
/// `(role, member, state)` is rendered once and reused via its cached
/// ref.
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

        // Multi-stage CTE state: carries the dimensions/filters of the
        // current node in the multi-stage tree. measures_filters are
        // intentionally dropped — CTE queries do not propagate them.
        // order_by is set to an empty vec so the builder skips
        // default_order: this value is used only as a state container,
        // never planned directly.
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
        let mut time_series_cache = TimeSeriesCache::default();

        for member in multi_stage_members {
            let cte_ref = self.build_multi_stage_cte(
                member.clone(),
                state.clone(),
                &mut resolved_multi_stage_dimensions,
                cte_state,
                &mut time_series_cache,
            )?;
            // Top-level non-dimension members surface as inputs of the
            // outer `FullKeyAggregate`. Dimension members are stitched
            // back as `MultiStageDimensionRef` through other channels.
            if !Self::is_multi_stage_dimension(&member)? {
                cte_state.add_subquery_ref(cte_ref);
            }
        }

        Ok(())
    }

    /// Core recursive step. Resolves the reference chain, applies
    /// static filters, dedupes through `cte_state`, tries a
    /// rolling-window path, and otherwise emits either a leaf-measure
    /// CTE or an inode CTE whose children come from `make_childs`.
    fn build_multi_stage_cte(
        &self,
        member: Rc<MemberSymbol>,
        state: Rc<QueryProperties>,
        resolved_multi_stage_dimensions: &mut HashSet<String>,
        cte_state: &mut CteState,
        time_series_cache: &mut TimeSeriesCache,
    ) -> Result<Rc<MultiStageSubqueryRef>, CubeError> {
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
        let role = role_for_multi_stage(&member);

        if let Some(existing) = cte_state.find_matching(role, &[member.clone()], &state) {
            return Ok(existing.cte_ref.clone());
        }

        if let Some(cte_ref) = self.try_build_rolling_window(
            member.clone(),
            state.clone(),
            resolved_multi_stage_dimensions,
            cte_state,
            time_series_cache,
        )? {
            return Ok(cte_ref);
        }

        let has_ms = has_multi_stage_members(&member, false)?;
        if !has_ms {
            // Leaf with no multi-stage deps — base measure CTE.
            let ms_member = MultiStageMember::new(
                MultiStageMemberType::Leaf(MultiStageLeafMemberType::Measure),
                member.clone(),
                self.query_properties.ungrouped(),
                false,
            );
            let alias = cte_state.next_cte_name(role);
            let body = build_for_leaf_cte_query(
                &self.query_tools,
                &self.query_properties,
                alias.clone(),
                &state,
                &ms_member,
                cte_state,
            )?;
            let cte_ref = ref_for_member(alias, &member, &state);
            cte_state.add_member(
                role,
                vec![member.clone()],
                state.clone(),
                body,
                cte_ref.clone(),
            );
            return Ok(cte_ref);
        }

        // Inode — `member` has multi-stage deps. Classify, adjust
        // state, recurse into children, then build the inode body.
        let (multi_stage_member, is_ungrupped) =
            self.create_multi_stage_inode_member(member.clone(), resolved_multi_stage_dimensions)?;

        let mut dimensions_to_add = multi_stage_member.add_group_by_symbols().clone();
        if let Some(case) = member.case() {
            if let Some(switch_dim) = case.case_switch_dimension() {
                dimensions_to_add.push(switch_dim);
            }
        }

        let new_state = if !dimensions_to_add.is_empty()
            || multi_stage_member.time_shift().is_some()
            || state.has_filters_for_member(&member_name)
        {
            let mut new_state = state.as_ref().clone();
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
        } else {
            state.clone()
        };

        let children = self.make_childs(
            member.clone(),
            new_state.clone(),
            resolved_multi_stage_dimensions,
            cte_state,
            time_series_cache,
        )?;

        let ms_member = MultiStageMember::new(
            MultiStageMemberType::Inode(multi_stage_member.clone()),
            member.clone(),
            is_ungrupped,
            false,
        );

        let alias = cte_state.next_cte_name(role);
        let body = match multi_stage_member.inode_type() {
            MultiStageInodeMemberType::Dimension => {
                build_for_cte_dimension_query(alias.clone(), &state, &ms_member, &children)?
            }
            _ => build_for_cte_query(
                alias.clone(),
                &state,
                &ms_member,
                &multi_stage_member,
                &children,
            )?,
        };
        let cte_ref = ref_for_member(alias, &member, &state);
        cte_state.add_member(
            role,
            vec![member.clone()],
            state.clone(),
            body,
            cte_ref.clone(),
        );
        Ok(cte_ref)
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
            (
                MultiStageInodeMember::new(
                    member_type,
                    reduce_by,
                    add_group_by,
                    group_by,
                    time_shift,
                ),
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

    /// Builds child CTE refs for `member`'s inode. Switches to
    /// `try_make_childs_for_case_switch` when the member's body is a
    /// CASE-SWITCH expression; otherwise falls through to
    /// `default_make_childs`.
    fn make_childs(
        &self,
        member: Rc<MemberSymbol>,
        new_state: Rc<QueryProperties>,
        resolved_multi_stage_dimensions: &mut HashSet<String>,
        cte_state: &mut CteState,
        time_series_cache: &mut TimeSeriesCache,
    ) -> Result<Vec<Rc<MultiStageSubqueryRef>>, CubeError> {
        if let Some(Case::CaseSwitch(case_switch)) = member.case() {
            if let Some(children) = self.try_make_childs_for_case_switch(
                case_switch,
                new_state.clone(),
                resolved_multi_stage_dimensions,
                cte_state,
                time_series_cache,
            )? {
                return Ok(children);
            }
        }
        self.default_make_childs(
            member,
            new_state,
            resolved_multi_stage_dimensions,
            cte_state,
            time_series_cache,
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

    /// Default child-generation path: for each measure or
    /// multi-stage-dimension dependency, recurses into
    /// `build_multi_stage_cte` and adds the result as a child ref.
    /// If the member has no such deps (e.g. a `Rank` measure that
    /// only needs the dimension grid), produces a single
    /// "without-member" leaf instead.
    fn default_make_childs(
        &self,
        member: Rc<MemberSymbol>,
        new_state: Rc<QueryProperties>,
        resolved_multi_stage_dimensions: &mut HashSet<String>,
        cte_state: &mut CteState,
        time_series_cache: &mut TimeSeriesCache,
    ) -> Result<Vec<Rc<MultiStageSubqueryRef>>, CubeError> {
        let mut result = vec![];
        let mut has_inputs = false;
        for dep in member.get_dependencies() {
            let dep = dep.resolve_reference_chain();
            if dep.is_measure() || Self::is_multi_stage_dimension(&dep)? {
                has_inputs = true;
                let child_ref = self.build_multi_stage_cte(
                    dep.clone(),
                    new_state.clone(),
                    resolved_multi_stage_dimensions,
                    cte_state,
                    time_series_cache,
                )?;
                // Multi-stage dimension children are joined back to
                // their measure consumers through MultiStageDimensionRef
                // — they should not surface as `FullKeyAggregate` data
                // inputs of a measure inode. The exception: if the
                // current inode is itself a dimension calc, a
                // multi-stage-dim child needs to be visible as input.
                let child_is_ms_dim = Self::is_multi_stage_dimension(&dep)?;
                if !child_is_ms_dim || member.as_dimension().is_ok() {
                    result.push(child_ref);
                }
            }
        }
        if !has_inputs {
            // Rank and similar cases: synthesize a "without-member"
            // leaf so the inode has a dimension grid to operate on.
            let role = role_for_multi_stage(&member);
            let ms_member = MultiStageMember::new_without_member_leaf(
                MultiStageMemberType::Leaf(MultiStageLeafMemberType::Measure),
                member.clone(),
                self.query_properties.ungrouped(),
                false,
            );
            let alias = cte_state.next_cte_name(role);
            let body = build_for_leaf_cte_query(
                &self.query_tools,
                &self.query_properties,
                alias.clone(),
                &new_state,
                &ms_member,
                cte_state,
            )?;
            let cte_ref = ref_for_member(alias, &member, &new_state);
            cte_state.add_member(
                role,
                vec![member.clone()],
                new_state.clone(),
                body,
                cte_ref.clone(),
            );
            result.push(cte_ref);
        }
        Ok(result)
    }

    /// Plans CASE-SWITCH dependencies: collects, per dependency, the
    /// union of switch values it covers and renders each dependency
    /// under a state with an equality filter on the switch member
    /// restricted to those values. An open `ELSE` branch makes the
    /// dependency unrestricted. Returns `None` when the switch is not
    /// a member reference, in which case the caller falls back to
    /// `default_make_childs`.
    fn try_make_childs_for_case_switch(
        &self,
        case: &CaseSwitchDefinition,
        new_state: Rc<QueryProperties>,
        resolved_multi_stage_dimensions: &mut HashSet<String>,
        cte_state: &mut CteState,
        time_series_cache: &mut TimeSeriesCache,
    ) -> Result<Option<Vec<Rc<MultiStageSubqueryRef>>>, CubeError> {
        let CaseSwitchItem::Member(switch_member) = &case.switch else {
            return Ok(None);
        };

        // Collect, per dependency, the union of switch values that need
        // it. `None` marks an unrestricted (open ELSE) entry: such a
        // dependency must be processed without a prefilter on
        // switch_member, since the outer CASE will dispatch by value at
        // row level.
        let mut deps: IndexMap<String, (Rc<MemberSymbol>, Option<Vec<String>>)> = IndexMap::new();

        let mut record = |dep: Rc<MemberSymbol>, branch_values: Option<Vec<String>>| {
            let dep = dep.resolve_reference_chain();
            let entry = deps
                .entry(dep.full_name())
                .or_insert_with(|| (dep.clone(), Some(Vec::new())));
            match (&mut entry.1, branch_values) {
                (None, _) => {}
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

        let mut result = vec![];
        for (_, (dep, values)) in deps {
            let mut dep_state = new_state.as_ref().clone();
            if let Some(values) = values {
                if !values.is_empty() {
                    let filter = BaseFilter::try_new(
                        self.query_tools.clone(),
                        switch_member.clone(),
                        FilterType::Dimension,
                        FilterOperator::Equal,
                        Some(values.into_iter().map(Some).collect_vec()),
                    )?;
                    dep_state.add_dimension_filter(FilterItem::Item(filter));
                }
            }
            let dep_state = Rc::new(dep_state);
            result.push(self.build_multi_stage_cte(
                dep,
                dep_state,
                resolved_multi_stage_dimensions,
                cte_state,
                time_series_cache,
            )?);
        }

        Ok(Some(result))
    }

    /// If `member` is a cumulative measure, plans the time-series
    /// and rolling-window CTEs and returns the rolling-window ref.
    /// Returns `None` for other measures and for non-measure members.
    fn try_build_rolling_window(
        &self,
        member: Rc<MemberSymbol>,
        state: Rc<QueryProperties>,
        resolved_multi_stage_dimensions: &mut HashSet<String>,
        cte_state: &mut CteState,
        time_series_cache: &mut TimeSeriesCache,
    ) -> Result<Option<Rc<MultiStageSubqueryRef>>, CubeError> {
        let Ok(measure) = member.as_measure() else {
            return Ok(None);
        };
        if !measure.is_cumulative() {
            return Ok(None);
        }
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
                return Err(CubeError::user(format!(
                    "Measure {} references another measures ({}). In this case, {} must have multi_stage: true defined",
                    member.full_name(),
                    measures.into_iter().map(|m| m.full_name()).join(", "),
                    member.full_name(),
                )));
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
                self.build_rolling_window_base(base_member, base_state, false, cte_state)?
            } else {
                self.build_multi_stage_cte(
                    base_member,
                    base_state,
                    resolved_multi_stage_dimensions,
                    cte_state,
                    time_series_cache,
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
                "Rolling window requires one time dimension and equal date ranges".to_string(),
            ));
        }

        let time_dimension =
            GranularityHelper::find_dimension_with_min_granularity(&time_dimensions)?;
        let time_dimension = MemberSymbol::new_time_dimension(time_dimension);

        let (base_rolling_state, base_time_dimension) =
            self.make_rolling_base_state(time_dimension.clone(), &rolling_window, state.clone())?;

        let time_series = self.add_time_series(
            time_dimension.clone(),
            state.clone(),
            cte_state,
            time_series_cache,
        )?;

        let rolling_base = if !measure.is_multi_stage() {
            self.build_rolling_window_base(base_member, base_rolling_state, ungrouped, cte_state)?
        } else {
            self.build_multi_stage_cte(
                base_member,
                base_rolling_state,
                resolved_multi_stage_dimensions,
                cte_state,
                time_series_cache,
            )?
        };

        let children = vec![time_series, rolling_base];

        let alias = cte_state.next_cte_name(CteRole::MultiStageMeasure);

        let rolling_window_descr = if measure.is_running_total() {
            RollingWindowDescription::new_running_total(time_dimension.clone(), base_time_dimension)
        } else if let Some(granularity) = self.get_to_date_rolling_granularity(&rolling_window)? {
            RollingWindowDescription::new_to_date(
                time_dimension.clone(),
                base_time_dimension,
                granularity,
            )
        } else {
            RollingWindowDescription::new_regular(
                time_dimension.clone(),
                base_time_dimension,
                rolling_window.trailing.clone(),
                rolling_window.leading.clone(),
                rolling_window.offset.clone().unwrap_or("end".to_string()),
            )
        };

        let inode_member = MultiStageInodeMember::new(
            MultiStageInodeMemberType::RollingWindow(rolling_window_descr.clone()),
            vec![],
            vec![],
            None,
            None,
        );

        let ms_member = MultiStageMember::new(
            MultiStageMemberType::Inode(inode_member),
            member.clone(),
            self.query_properties.ungrouped(),
            false,
        );

        let body = build_rolling_window_query(
            &self.query_tools,
            &self.query_properties,
            alias.clone(),
            &state,
            &ms_member,
            &rolling_window_descr,
            &children,
        )?;
        let cte_ref = ref_for_member(alias, &member, &state);
        cte_state.add_member(
            CteRole::MultiStageMeasure,
            vec![member.clone()],
            state.clone(),
            body,
            cte_ref.clone(),
        );
        Ok(Some(cte_ref))
    }

    /// Adds (or reuses, via `time_series_cache`) the
    /// `time_series_get_range` leaf CTE — used by `add_time_series`
    /// when the requested time dimension has no explicit date range.
    fn add_time_series_get_range_query(
        &self,
        time_dimension: Rc<MemberSymbol>,
        state: Rc<QueryProperties>,
        cte_state: &mut CteState,
        time_series_cache: &mut TimeSeriesCache,
    ) -> Result<Rc<MultiStageSubqueryRef>, CubeError> {
        if let Some(existing) = &time_series_cache.time_series_range {
            return Ok(existing.clone());
        }
        let alias = "time_series_get_range".to_string();
        let body = build_time_series_get_range_query(
            &self.query_tools,
            &self.query_properties,
            alias.clone(),
            time_dimension.clone(),
            cte_state,
        )?;
        let cte_ref = ref_for_member(alias, &time_dimension, &state);
        cte_state.add_member(
            CteRole::MultiStageMeasure,
            vec![time_dimension.clone()],
            state,
            body,
            cte_ref.clone(),
        );
        time_series_cache.time_series_range = Some(cte_ref.clone());
        Ok(cte_ref)
    }

    /// Adds (or reuses, via `time_series_cache`) the `time_series`
    /// leaf CTE that drives a rolling window. When the time dimension
    /// has no `date_range`, also arranges for a sibling
    /// `time_series_get_range` CTE to feed it.
    fn add_time_series(
        &self,
        time_dimension: Rc<MemberSymbol>,
        state: Rc<QueryProperties>,
        cte_state: &mut CteState,
        time_series_cache: &mut TimeSeriesCache,
    ) -> Result<Rc<MultiStageSubqueryRef>, CubeError> {
        if let Some(existing) = &time_series_cache.time_series {
            return Ok(existing.clone());
        }
        let get_range_cte = if time_dimension
            .as_time_dimension()?
            .date_range_vec()
            .is_some()
        {
            None
        } else {
            Some(self.add_time_series_get_range_query(
                time_dimension.clone(),
                state.clone(),
                cte_state,
                time_series_cache,
            )?)
        };
        let alias = "time_series".to_string();
        let body = build_time_series_query(
            alias.clone(),
            Rc::new(TimeSeriesDescription {
                time_dimension: time_dimension.clone(),
                date_range_cte: get_range_cte.map(|r| r.name().clone()),
            }),
        )?;
        let cte_ref = ref_for_member(alias, &time_dimension, &state);
        cte_state.add_member(
            CteRole::MultiStageMeasure,
            vec![time_dimension.clone()],
            state,
            body,
            cte_ref.clone(),
        );
        time_series_cache.time_series = Some(cte_ref.clone());
        Ok(cte_ref)
    }

    /// Adds the leaf CTE that produces the base values for a
    /// rolling window — selects the requested dimensions plus the
    /// unrolled measure, marked `has_aggregates_on_top` so callers
    /// know an outer rolling computation will consume it.
    fn build_rolling_window_base(
        &self,
        member: Rc<MemberSymbol>,
        state: Rc<QueryProperties>,
        ungrouped: bool,
        cte_state: &mut CteState,
    ) -> Result<Rc<MultiStageSubqueryRef>, CubeError> {
        let role = CteRole::MultiStageMeasure;
        let ms_member = MultiStageMember::new(
            MultiStageMemberType::Leaf(MultiStageLeafMemberType::Measure),
            member.clone(),
            self.query_properties.ungrouped() || ungrouped,
            true,
        );
        let alias = cte_state.next_cte_name(role);
        let body = build_for_leaf_cte_query(
            &self.query_tools,
            &self.query_properties,
            alias.clone(),
            &state,
            &ms_member,
            cte_state,
        )?;
        let cte_ref = ref_for_member(alias, &member, &state);
        cte_state.add_member(
            role,
            vec![member.clone()],
            state.clone(),
            body,
            cte_ref.clone(),
        );
        Ok(cte_ref)
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
                Err(CubeError::user(
                    "Granularity required for to_date rolling window".to_string(),
                ))
            }
        } else {
            Ok(None)
        }
    }

    /// Adjust date range filters for rolling window when there's no
    /// granularity. Without granularity there's no time_series CTE,
    /// so we replace InDateRange with BeforeOrOnDate/AfterOrOnDate
    /// that use parameters directly.
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
        // We keep only one time_dimension in the leaf query because,
        // even if time_dimension values have different granularity, in
        // the leaf query we need to group by the lowest granularity.
        new_state.set_time_dimensions(vec![new_time_dimension.clone()]);
        new_state.set_dimensions(
            state
                .dimensions()
                .iter()
                .filter(|d| {
                    let resolved = (*d).clone().resolve_reference_chain();
                    resolved.as_time_dimension().is_err()
                })
                .cloned()
                .collect_vec(),
        );

        if let Some(to_date_granularity) = self.get_to_date_rolling_granularity(rolling_window)? {
            new_state.replace_to_date_date_range_filter(
                &time_dimension_base_name,
                &to_date_granularity,
            )?;
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
