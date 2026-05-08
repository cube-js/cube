use super::query_tools::QueryTools;
use super::MemberSymbol;
use crate::cube_bridge::join_definition::JoinDefinition;
use crate::planner::collectors::{collect_multiplied_measures, has_multi_stage_members};
use crate::planner::filter::{Filter, FilterGroup, FilterItem, FilterOperator};
use crate::planner::join_hints::JoinHints;
use crate::planner::multi_fact_join_groups::{MeasuresJoinHints, MultiFactJoinGroups};
use crate::planner::planners::multi_stage::TimeShiftState;
use crate::planner::{
    apply_static_filter_to_filter_item, apply_static_filter_to_symbol, DimensionTimeShift,
    MeasureTimeShifts,
};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::cell::OnceCell;
use std::collections::HashSet;
use std::rc::Rc;
use typed_builder::TypedBuilder;

#[derive(Clone, Debug)]
pub struct OrderByItem {
    member_evaluator: Rc<MemberSymbol>,
    desc: bool,
}

impl OrderByItem {
    pub fn new(member_evaluator: Rc<MemberSymbol>, desc: bool) -> Self {
        Self {
            member_evaluator,
            desc,
        }
    }

    pub fn name(&self) -> String {
        self.member_evaluator.full_name()
    }

    pub fn member_symbol(&self) -> Rc<MemberSymbol> {
        self.member_evaluator.clone()
    }

    pub fn desc(&self) -> bool {
        self.desc
    }
}

impl PartialEq for OrderByItem {
    fn eq(&self, other: &Self) -> bool {
        self.desc == other.desc && member_chain_eq(&self.member_evaluator, &other.member_evaluator)
    }
}

/// Compare two member symbols by reference-chain-resolved name.
/// For root queries this is equivalent to `MemberSymbol::eq` (full_name compare);
/// for multi-stage states with CTE references it follows the chain to the resolved symbol.
fn member_chain_eq(a: &Rc<MemberSymbol>, b: &Rc<MemberSymbol>) -> bool {
    a.clone().resolve_reference_chain().full_name()
        == b.clone().resolve_reference_chain().full_name()
}

#[derive(Debug, Clone)]
pub struct MultipliedMeasure {
    measure: Rc<MemberSymbol>,
    cube_name: String, //May differ from cube_name of the measure for a member_expression that refers to a dimension.
}

impl MultipliedMeasure {
    pub fn new(measure: Rc<MemberSymbol>, cube_name: String) -> Rc<Self> {
        Rc::new(Self { measure, cube_name })
    }

    pub fn measure(&self) -> &Rc<MemberSymbol> {
        &self.measure
    }

    pub fn cube_name(&self) -> &String {
        &self.cube_name
    }
}

#[derive(Default, Clone, Debug)]
pub struct FullKeyAggregateMeasures {
    pub multiplied_measures: Vec<Rc<MultipliedMeasure>>,
    pub regular_measures: Vec<Rc<MemberSymbol>>,
    pub multi_stage_measures: Vec<Rc<MemberSymbol>>,
    pub rendered_as_multiplied_measures: HashSet<String>,
}

#[derive(Clone, TypedBuilder)]
#[builder(build_method(into = Result<Rc<QueryProperties>, CubeError>))]
pub struct QueryProperties {
    query_tools: Rc<QueryTools>,
    #[builder(default)]
    measures: Vec<Rc<MemberSymbol>>,
    #[builder(default)]
    dimensions: Vec<Rc<MemberSymbol>>,
    #[builder(default)]
    time_dimensions: Vec<Rc<MemberSymbol>>,
    #[builder(setter(skip), default)]
    time_shifts: TimeShiftState,
    #[builder(default)]
    dimensions_filters: Vec<FilterItem>,
    #[builder(default)]
    time_dimensions_filters: Vec<FilterItem>,
    #[builder(default)]
    measures_filters: Vec<FilterItem>,
    #[builder(default)]
    segments: Vec<FilterItem>,
    /// `None` = builder caller did not set order_by; `From` impl will fall
    /// back to `default_order`. `Some(vec)` is used as-is, including
    /// `Some(empty)` meaning "render no ORDER BY clause".
    #[builder(default)]
    order_by: Option<Vec<OrderByItem>>,
    #[builder(default)]
    row_limit: Option<usize>,
    #[builder(default)]
    offset: Option<usize>,
    #[builder(default)]
    ignore_cumulative: bool,
    #[builder(default)]
    ungrouped: bool,
    #[builder(default)]
    pre_aggregation_query: bool,
    #[builder(default)]
    total_query: bool,
    #[builder(default = Rc::new(JoinHints::new()))]
    query_join_hints: Rc<JoinHints>,
    #[builder(default = true)]
    allow_multi_stage: bool,
    #[builder(default)]
    disable_external_pre_aggregations: bool,
    #[builder(default)]
    pre_aggregation_id: Option<String>,
    #[builder(setter(skip), default)]
    multi_fact_join_groups: OnceCell<MultiFactJoinGroups>,
}

/// Finalize a QueryProperties built via the typed builder: materializes the
/// default order if the builder caller did not set `order_by`, and applies
/// static filters. `Some(empty)` is preserved as "no ORDER BY", `None`
/// triggers the default. Wired into `QueryProperties::builder().…build()`
/// via `build_method(into = …)`.
impl From<QueryProperties> for Result<Rc<QueryProperties>, CubeError> {
    fn from(mut qp: QueryProperties) -> Self {
        if qp.order_by.is_none() {
            qp.order_by = Some(QueryProperties::default_order(
                &qp.dimensions,
                &qp.time_dimensions,
                &qp.measures,
            ));
        }
        qp.apply_static_filters()?;
        Ok(Rc::new(qp))
    }
}

impl QueryProperties {
    pub fn allow_multi_stage(&self) -> bool {
        self.allow_multi_stage
    }

    fn apply_static_filters(&mut self) -> Result<(), CubeError> {
        let dimensions_filters = self.dimensions_filters.clone();
        for dim in self.dimensions.iter_mut() {
            *dim = apply_static_filter_to_symbol(dim, &dimensions_filters)?;
        }
        for dim in self.time_dimensions.iter_mut() {
            *dim = apply_static_filter_to_symbol(dim, &dimensions_filters)?;
        }
        for meas in self.measures.iter_mut() {
            *meas = apply_static_filter_to_symbol(meas, &dimensions_filters)?;
        }
        for filter_item in self.dimensions_filters.iter_mut() {
            *filter_item = apply_static_filter_to_filter_item(filter_item, &dimensions_filters)?;
        }
        for filter_item in self.measures_filters.iter_mut() {
            *filter_item = apply_static_filter_to_filter_item(filter_item, &dimensions_filters)?;
        }
        for filter_item in self.time_dimensions_filters.iter_mut() {
            *filter_item = apply_static_filter_to_filter_item(filter_item, &dimensions_filters)?;
        }
        for filter_item in self.segments.iter_mut() {
            *filter_item = apply_static_filter_to_filter_item(filter_item, &dimensions_filters)?;
        }
        for order_item in self.order_by.iter_mut().flatten() {
            order_item.member_evaluator =
                apply_static_filter_to_symbol(&order_item.member_evaluator, &dimensions_filters)?;
        }
        Ok(())
    }

    fn compute_multi_fact_join_groups(&self) -> Result<MultiFactJoinGroups, CubeError> {
        let measures_join_hints = MeasuresJoinHints::builder(&self.query_join_hints)
            .add_dimensions(&self.dimensions)
            .add_dimensions(&self.extract_dimensions_from_order())
            .add_dimensions(&self.time_dimensions)
            .add_filters(&self.time_dimensions_filters)
            .add_filters(&self.dimensions_filters)
            .add_filters(&self.segments)
            .build(&self.all_used_measures()?)?;
        MultiFactJoinGroups::try_new(self.query_tools.clone(), measures_join_hints)
    }

    fn multi_fact_join_groups(&self) -> Result<&MultiFactJoinGroups, CubeError> {
        if let Some(g) = self.multi_fact_join_groups.get() {
            return Ok(g);
        }
        let computed = self.compute_multi_fact_join_groups()?;
        Ok(self.multi_fact_join_groups.get_or_init(move || computed))
    }

    pub fn compute_join_multi_fact_groups_with_measures(
        &self,
        measures: &[Rc<MemberSymbol>],
    ) -> Result<MultiFactJoinGroups, CubeError> {
        self.multi_fact_join_groups()?.for_measures(measures)
    }

    pub fn is_total_query(&self) -> bool {
        self.total_query
    }

    fn extract_dimensions_from_order(&self) -> Vec<Rc<MemberSymbol>> {
        self.order_by
            .iter()
            .flatten()
            .filter_map(|order| {
                if order.member_evaluator.as_dimension().is_ok() {
                    Some(order.member_evaluator.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    fn is_multi_fact_join(&self) -> Result<bool, CubeError> {
        Ok(self.multi_fact_join_groups()?.is_multi_fact())
    }

    pub fn simple_query_join(&self) -> Result<Option<Rc<dyn JoinDefinition>>, CubeError> {
        self.multi_fact_join_groups()?.single_join()
    }

    pub fn measures(&self) -> &Vec<Rc<MemberSymbol>> {
        &self.measures
    }

    pub fn dimensions(&self) -> &Vec<Rc<MemberSymbol>> {
        &self.dimensions
    }

    pub fn time_dimensions(&self) -> &Vec<Rc<MemberSymbol>> {
        &self.time_dimensions
    }

    pub fn time_shifts(&self) -> &TimeShiftState {
        &self.time_shifts
    }

    pub fn time_dimensions_filters(&self) -> &Vec<FilterItem> {
        &self.time_dimensions_filters
    }

    pub fn dimensions_filters(&self) -> &Vec<FilterItem> {
        &self.dimensions_filters
    }

    pub fn measures_filters(&self) -> &Vec<FilterItem> {
        &self.measures_filters
    }

    pub fn row_limit(&self) -> Option<usize> {
        self.row_limit
    }

    pub fn query_join_hints(&self) -> &Rc<JoinHints> {
        &self.query_join_hints
    }

    pub fn offset(&self) -> Option<usize> {
        self.offset
    }

    pub fn order_by(&self) -> &[OrderByItem] {
        self.order_by.as_deref().unwrap_or(&[])
    }

    pub fn ungrouped(&self) -> bool {
        self.ungrouped
    }

    pub fn is_pre_aggregation_query(&self) -> bool {
        self.pre_aggregation_query
    }

    pub fn disable_external_pre_aggregations(&self) -> bool {
        self.disable_external_pre_aggregations
    }

    pub fn pre_aggregation_id(&self) -> Option<&str> {
        self.pre_aggregation_id.as_deref()
    }

    pub fn all_filters(&self) -> Option<Filter> {
        let items = self
            .time_dimensions_filters
            .iter()
            .chain(self.dimensions_filters.iter())
            .chain(self.segments.iter())
            .cloned()
            .collect_vec();
        if items.is_empty() {
            None
        } else {
            Some(Filter { items })
        }
    }

    pub fn segments(&self) -> &Vec<FilterItem> {
        &self.segments
    }

    pub fn all_members(&self, exclude_time_dimensions: bool) -> Vec<Rc<MemberSymbol>> {
        let dimensions = self.dimensions.iter().cloned();
        let measures = self.measures.iter().cloned();
        if exclude_time_dimensions {
            dimensions.chain(measures).collect_vec()
        } else {
            let time_dimensions = self.time_dimensions.iter().map(|d| d.clone());
            dimensions
                .chain(time_dimensions)
                .chain(measures)
                .collect_vec()
        }
    }

    pub fn all_used_symbols(&self) -> Result<Vec<Rc<MemberSymbol>>, CubeError> {
        let mut members = vec![];
        members.extend(self.time_dimensions.iter().cloned());
        members.extend(self.dimensions.iter().cloned());
        self.fill_all_filter_symbols(&mut members);
        members.extend(self.all_used_measures()?);

        let res = members
            .into_iter()
            .unique_by(|m| m.full_name())
            .collect_vec();
        Ok(res)
    }

    pub fn get_member_symbols(
        &self,
        include_time_dimensions: bool,
        include_dimensions: bool,
        include_measures: bool,
        include_filters: bool,
        additional_symbols: &Vec<Rc<MemberSymbol>>,
    ) -> Vec<Rc<MemberSymbol>> {
        let mut members = additional_symbols.clone();
        if include_time_dimensions {
            members.extend(self.time_dimensions.iter().cloned());
        }
        if include_dimensions {
            members.extend(self.dimensions.iter().cloned());
        }
        if include_measures {
            members.extend(self.measures.iter().cloned());
        }
        if include_filters {
            self.fill_all_filter_symbols(&mut members);
        }
        members
            .into_iter()
            .unique_by(|m| m.full_name())
            .collect_vec()
    }

    fn fill_all_filter_symbols(&self, members: &mut Vec<Rc<MemberSymbol>>) {
        if let Some(all_filters) = self.all_filters() {
            for filter_item in all_filters.items.iter() {
                filter_item.find_all_member_evaluators(members);
            }
        }
    }

    pub fn default_order(
        dimensions: &[Rc<MemberSymbol>],
        time_dimensions: &[Rc<MemberSymbol>],
        measures: &[Rc<MemberSymbol>],
    ) -> Vec<OrderByItem> {
        let mut result = Vec::new();
        if let Some(granularity_dim) = time_dimensions.iter().find(|d| {
            if let Ok(td) = d.as_time_dimension() {
                td.has_granularity()
            } else {
                false
            }
        }) {
            result.push(OrderByItem::new(granularity_dim.clone(), false));
        } else if !measures.is_empty() && !dimensions.is_empty() {
            result.push(OrderByItem::new(measures[0].clone(), true));
        } else if !dimensions.is_empty() {
            result.push(OrderByItem::new(dimensions[0].clone(), false));
        }
        result
    }

    pub fn is_simple_query(&self) -> Result<bool, CubeError> {
        let full_aggregate_measure = self.full_key_aggregate_measures()?;
        if full_aggregate_measure.multiplied_measures.is_empty()
            && (full_aggregate_measure.multi_stage_measures.is_empty() || !self.allow_multi_stage)
            && !self.is_multi_fact_join()?
            && (!self.has_multi_stage_dimensions()? || !self.allow_multi_stage)
        {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn has_multi_stage_dimensions(&self) -> Result<bool, CubeError> {
        for dim in self.dimensions.iter() {
            if has_multi_stage_members(dim, true)? {
                return Ok(true);
            }
        }
        for dim in self.time_dimensions.iter() {
            if has_multi_stage_members(dim, true)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn full_key_aggregate_measures(&self) -> Result<FullKeyAggregateMeasures, CubeError> {
        let mut result = FullKeyAggregateMeasures::default();
        let measures = self.all_used_measures()?;
        for m in measures.iter() {
            if has_multi_stage_members(m, self.ignore_cumulative || self.pre_aggregation_query)? {
                result.multi_stage_measures.push(m.clone())
            } else {
                let join = self
                    .compute_join_multi_fact_groups_with_measures(std::slice::from_ref(m))?
                    .single_join()?
                    .expect("No join groups returned for single measure multi-fact join group");
                for item in collect_multiplied_measures(m, join)? {
                    if item.multiplied {
                        result
                            .rendered_as_multiplied_measures
                            .insert(item.measure.full_name());
                    }
                    let is_multiplied_measure = if item.multiplied {
                        if let Ok(measure) = item.measure.as_measure() {
                            if measure.can_used_as_addictive_in_multplied() {
                                false
                            } else {
                                true
                            }
                        } else {
                            true
                        }
                    } else {
                        false
                    };
                    if is_multiplied_measure {
                        result
                            .multiplied_measures
                            .push(MultipliedMeasure::new(item.measure.clone(), item.cube_name));
                    } else {
                        result.regular_measures.push(item.measure.clone());
                    }
                }
            }
        }
        result.multi_stage_measures = result
            .multi_stage_measures
            .into_iter()
            .unique_by(|itm| itm.full_name())
            .collect();
        result.regular_measures = result
            .regular_measures
            .into_iter()
            .unique_by(|itm| itm.full_name())
            .collect();
        result.multiplied_measures = result
            .multiplied_measures
            .into_iter()
            .unique_by(|itm| itm.measure.full_name())
            .collect();

        Ok(result)
    }

    fn all_used_measures(&self) -> Result<Vec<Rc<MemberSymbol>>, CubeError> {
        let mut measures = self.measures.clone();
        for item in self.measures_filters.iter() {
            self.fill_missed_measures_from_filter(item, &mut measures)?;
        }
        for item in self.order_by.iter().flatten() {
            if let Ok(measure) = item.member_evaluator.as_measure() {
                if !measures
                    .iter()
                    .any(|m| m.full_name() == measure.full_name())
                {
                    measures.push(item.member_evaluator.clone());
                }
            }
        }
        Ok(measures)
    }

    fn fill_missed_measures_from_filter(
        &self,
        item: &FilterItem,
        measures: &mut Vec<Rc<MemberSymbol>>,
    ) -> Result<(), CubeError> {
        match item {
            FilterItem::Group(group) => {
                for item in group.items.iter() {
                    self.fill_missed_measures_from_filter(item, measures)?
                }
            }
            FilterItem::Item(item) => {
                let item_member_name = item.member_name();
                if !measures.iter().any(|m| m.full_name() == item_member_name) {
                    measures.push(item.member_evaluator().clone());
                }
            }
            FilterItem::Segment(_) => {}
        }
        Ok(())
    }

    /// Compare two member sequences by reference-chain-resolved name.
    /// Required for multi-stage state dedup, where dimensions can carry CTE
    /// references; for root queries it degenerates to plain full_name compare.
    fn members_equivalent(a: &[Rc<MemberSymbol>], b: &[Rc<MemberSymbol>]) -> bool {
        a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| member_chain_eq(x, y))
    }

    // --- multi-stage state mutations ---
    //
    // The methods below mutate fields that participate in
    // `compute_multi_fact_join_groups`, so they invalidate the lazy cache.
    // They originally lived on `MultiStageAppliedState`; that type was folded
    // into `QueryProperties` so the multi-stage planner can carry node state
    // as a regular query.

    pub fn set_dimensions(&mut self, dimensions: Vec<Rc<MemberSymbol>>) {
        self.dimensions = dimensions;
        self.invalidate_join_groups_cache();
    }

    pub fn set_time_dimensions(&mut self, time_dimensions: Vec<Rc<MemberSymbol>>) {
        self.time_dimensions = time_dimensions;
        self.invalidate_join_groups_cache();
    }

    /// Merge new dimensions into the existing list, deduplicating by
    /// reference-chain-resolved full name (so a dimension reachable through
    /// a CTE chain is treated as equal to its resolved counterpart).
    pub fn add_dimensions(&mut self, dimensions: Vec<Rc<MemberSymbol>>) {
        self.dimensions = self
            .dimensions
            .iter()
            .cloned()
            .chain(dimensions.into_iter())
            .unique_by(|d| d.clone().resolve_reference_chain().full_name())
            .collect_vec();
        self.invalidate_join_groups_cache();
    }

    pub fn add_dimension_filter(&mut self, filter: FilterItem) {
        self.dimensions_filters.push(filter);
        self.invalidate_join_groups_cache();
    }

    /// Drop multi-stage dimensions that are not yet resolved at the current
    /// stage. A dimension stays if it has no multi-stage members or if it has
    /// already been resolved upstream.
    pub fn remove_multistage_dimensions(
        &mut self,
        resolved_dimensions: &HashSet<String>,
    ) -> Result<(), CubeError> {
        let mut filtered = Vec::new();
        for d in &self.dimensions {
            if resolved_dimensions.contains(&d.clone().resolve_reference_chain().full_name())
                || !has_multi_stage_members(d, true)?
            {
                filtered.push(d.clone());
            }
        }
        self.dimensions = filtered;
        let mut filtered = Vec::new();
        for d in &self.time_dimensions {
            if resolved_dimensions.contains(&d.clone().resolve_reference_chain().full_name())
                || !has_multi_stage_members(d, true)?
            {
                filtered.push(d.clone());
            }
        }
        self.time_dimensions = filtered;
        self.invalidate_join_groups_cache();
        Ok(())
    }

    pub fn add_time_shifts(&mut self, time_shifts: MeasureTimeShifts) -> Result<(), CubeError> {
        let resolved_shifts = match time_shifts {
            MeasureTimeShifts::Dimensions(dimensions) => dimensions,
            MeasureTimeShifts::Common(interval) => self
                .all_time_members()
                .into_iter()
                .map(|m| DimensionTimeShift {
                    interval: Some(interval.clone()),
                    dimension: m,
                    name: None,
                })
                .collect_vec(),
            MeasureTimeShifts::Named(named_shift) => self
                .all_time_members()
                .into_iter()
                .map(|m| DimensionTimeShift {
                    interval: None,
                    dimension: m,
                    name: Some(named_shift.clone()),
                })
                .collect_vec(),
        };
        for ts in resolved_shifts.into_iter() {
            if let Some(exists) = self
                .time_shifts
                .dimensions_shifts
                .get_mut(&ts.dimension.full_name())
            {
                if let Some(interval) = exists.interval.clone() {
                    if let Some(new_interval) = ts.interval {
                        exists.interval = Some(interval + new_interval);
                    } else {
                        return Err(CubeError::internal(format!(
                            "Cannot use both named ({}) and interval ({}) shifts for the same dimension: {}.",
                            ts.name.clone().unwrap_or("-".to_string()),
                            interval.to_sql(),
                            ts.dimension.full_name(),
                        )));
                    }
                } else if let Some(named_shift) = exists.name.clone() {
                    return if let Some(new_interval) = ts.interval {
                        Err(CubeError::internal(format!(
                            "Cannot use both named ({}) and interval ({}) shifts for the same dimension: {}.",
                            named_shift,
                            new_interval.to_sql(),
                            ts.dimension.full_name(),
                        )))
                    } else {
                        Err(CubeError::internal(format!(
                            "Cannot use more than one named shifts ({}, {}) for the same dimension: {}.",
                            ts.name.clone().unwrap_or("-".to_string()),
                            named_shift,
                            ts.dimension.full_name(),
                        )))
                    };
                }
            } else {
                self.time_shifts
                    .dimensions_shifts
                    .insert(ts.dimension.full_name(), ts);
            }
        }
        Ok(())
    }

    fn all_time_members(&self) -> Vec<Rc<MemberSymbol>> {
        let mut filter_symbols: Vec<Rc<MemberSymbol>> = self
            .dimensions
            .iter()
            .cloned()
            .chain(self.time_dimensions.iter().cloned())
            .collect();
        for filter_item in self
            .time_dimensions_filters
            .iter()
            .chain(self.dimensions_filters.iter())
            .chain(self.segments.iter())
        {
            filter_item.find_all_member_evaluators(&mut filter_symbols);
        }
        filter_symbols
            .into_iter()
            .filter_map(|m| {
                let symbol = if let Ok(time_dim) = m.as_time_dimension() {
                    time_dim.base_symbol().clone().resolve_reference_chain()
                } else {
                    m.resolve_reference_chain()
                };
                if let Ok(dim) = symbol.as_dimension() {
                    if dim.is_time() {
                        Some(symbol)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .unique_by(|s| s.full_name())
            .collect_vec()
    }

    pub fn remove_filter_for_member(&mut self, member_name: &str) {
        self.time_dimensions_filters =
            Self::extract_filters_exclude_member(member_name, &self.time_dimensions_filters);
        self.dimensions_filters =
            Self::extract_filters_exclude_member(member_name, &self.dimensions_filters);
        self.measures_filters =
            Self::extract_filters_exclude_member(member_name, &self.measures_filters);
        self.invalidate_join_groups_cache();
    }

    fn extract_filters_exclude_member(
        member_name: &str,
        filters: &[FilterItem],
    ) -> Vec<FilterItem> {
        let mut result = Vec::new();
        for item in filters.iter() {
            match item {
                FilterItem::Group(group) => {
                    let new_group = FilterItem::Group(Rc::new(FilterGroup::new(
                        group.operator.clone(),
                        Self::extract_filters_exclude_member(member_name, &group.items),
                    )));
                    result.push(new_group);
                }
                FilterItem::Item(itm) => {
                    if itm.member_name() != member_name {
                        result.push(FilterItem::Item(itm.clone()));
                    }
                }
                FilterItem::Segment(_) => {}
            }
        }
        result
    }

    pub fn has_filters_for_member(&self, member_name: &str) -> bool {
        Self::has_filters_for_member_impl(member_name, &self.time_dimensions_filters)
            || Self::has_filters_for_member_impl(member_name, &self.dimensions_filters)
            || Self::has_filters_for_member_impl(member_name, &self.measures_filters)
    }

    fn has_filters_for_member_impl(member_name: &str, filters: &[FilterItem]) -> bool {
        for item in filters.iter() {
            match item {
                FilterItem::Group(group) => {
                    if Self::has_filters_for_member_impl(member_name, &group.items) {
                        return true;
                    }
                }
                FilterItem::Item(itm) => {
                    if itm.member_name() == member_name {
                        return true;
                    }
                }
                FilterItem::Segment(_) => {}
            }
        }
        false
    }

    /// Replace InDateRange filter with bounded version for rolling window without granularity.
    /// Unlike `replace_regular_date_range_filter` which uses time_series CTE references,
    /// this keeps parameter-based filters suitable for queries without a time_series CTE.
    pub fn replace_date_range_for_rolling_window_without_granularity(
        &mut self,
        member_name: &str,
        trailing: &Option<String>,
        leading: &Option<String>,
    ) -> Result<(), CubeError> {
        let trailing_unbounded = trailing.as_deref() == Some("unbounded");
        let leading_unbounded = leading.as_deref() == Some("unbounded");

        if !trailing_unbounded && !leading_unbounded {
            return Ok(());
        }

        if trailing_unbounded && leading_unbounded {
            // Both unbounded — remove the date range filter entirely
            self.time_dimensions_filters.retain(|item| match item {
                FilterItem::Item(itm) => {
                    !(itm.member_name() == member_name
                        && matches!(itm.filter_operator(), FilterOperator::InDateRange))
                }
                _ => true,
            });
        } else if trailing_unbounded {
            // Remove lower bound: InDateRange(from, to) → BeforeOrOnDate(to)
            let mut new_filters = Vec::new();
            for item in self.time_dimensions_filters.iter() {
                match item {
                    FilterItem::Item(itm)
                        if itm.member_name() == member_name
                            && matches!(itm.filter_operator(), FilterOperator::InDateRange) =>
                    {
                        let values = itm.values();
                        let to_value = if values.len() >= 2 {
                            vec![values[1].clone()]
                        } else {
                            values.clone()
                        };
                        new_filters.push(FilterItem::Item(itm.change_operator(
                            FilterOperator::BeforeOrOnDate,
                            to_value,
                            itm.use_raw_values(),
                        )?));
                    }
                    other => new_filters.push(other.clone()),
                }
            }
            self.time_dimensions_filters = new_filters;
        } else {
            // leading unbounded: remove upper bound: InDateRange(from, to) → AfterOrOnDate(from)
            let mut new_filters = Vec::new();
            for item in self.time_dimensions_filters.iter() {
                match item {
                    FilterItem::Item(itm)
                        if itm.member_name() == member_name
                            && matches!(itm.filter_operator(), FilterOperator::InDateRange) =>
                    {
                        let values = itm.values();
                        let from_value = if !values.is_empty() {
                            vec![values[0].clone()]
                        } else {
                            values.clone()
                        };
                        new_filters.push(FilterItem::Item(itm.change_operator(
                            FilterOperator::AfterOrOnDate,
                            from_value,
                            itm.use_raw_values(),
                        )?));
                    }
                    other => new_filters.push(other.clone()),
                }
            }
            self.time_dimensions_filters = new_filters;
        }
        self.invalidate_join_groups_cache();
        Ok(())
    }

    pub fn replace_regular_date_range_filter(
        &mut self,
        member_name: &str,
        left_interval: Option<String>,
        right_interval: Option<String>,
    ) -> Result<(), CubeError> {
        let operator = FilterOperator::RegularRollingWindowDateRange;
        let values = vec![left_interval.clone(), right_interval.clone()];
        self.time_dimensions_filters = self.change_date_range_filter_impl(
            member_name,
            &self.time_dimensions_filters,
            &operator,
            None,
            &values,
            &None,
        )?;
        self.invalidate_join_groups_cache();
        Ok(())
    }

    pub fn replace_to_date_date_range_filter(
        &mut self,
        member_name: &str,
        granularity: &String,
    ) -> Result<(), CubeError> {
        let operator = FilterOperator::ToDateRollingWindowDateRange;
        let values = vec![Some(granularity.clone())];
        self.time_dimensions_filters = self.change_date_range_filter_impl(
            member_name,
            &self.time_dimensions_filters,
            &operator,
            None,
            &values,
            &None,
        )?;
        self.invalidate_join_groups_cache();
        Ok(())
    }

    pub fn replace_range_in_date_filter(
        &mut self,
        member_name: &str,
        new_from: String,
        new_to: String,
    ) -> Result<(), CubeError> {
        let operator = FilterOperator::InDateRange;
        let replacement_values = vec![Some(new_from), Some(new_to)];
        self.time_dimensions_filters = self.change_date_range_filter_impl(
            member_name,
            &self.time_dimensions_filters,
            &operator,
            None,
            &vec![],
            &Some(replacement_values),
        )?;
        self.invalidate_join_groups_cache();
        Ok(())
    }

    pub fn replace_range_to_subquery_in_date_filter(
        &mut self,
        member_name: &str,
        new_from: String,
        new_to: String,
    ) -> Result<(), CubeError> {
        let operator = FilterOperator::InDateRange;
        let replacement_values = vec![Some(new_from), Some(new_to)];
        self.time_dimensions_filters = self.change_date_range_filter_impl(
            member_name,
            &self.time_dimensions_filters,
            &operator,
            Some(true),
            &vec![],
            &Some(replacement_values),
        )?;
        self.invalidate_join_groups_cache();
        Ok(())
    }

    fn change_date_range_filter_impl(
        &self,
        member_name: &str,
        filters: &[FilterItem],
        operator: &FilterOperator,
        use_raw_values: Option<bool>,
        additional_values: &Vec<Option<String>>,
        replacement_values: &Option<Vec<Option<String>>>,
    ) -> Result<Vec<FilterItem>, CubeError> {
        let mut result = Vec::new();
        for item in filters.iter() {
            match item {
                FilterItem::Group(group) => {
                    let new_group = FilterItem::Group(Rc::new(FilterGroup::new(
                        group.operator.clone(),
                        self.change_date_range_filter_impl(
                            member_name,
                            &group.items,
                            operator,
                            use_raw_values,
                            additional_values,
                            replacement_values,
                        )?,
                    )));
                    result.push(new_group);
                }
                FilterItem::Item(itm) => {
                    let itm = if itm.member_name() == member_name
                        && matches!(itm.filter_operator(), FilterOperator::InDateRange)
                    {
                        let mut values = if let Some(values) = replacement_values {
                            values.clone()
                        } else {
                            itm.values().clone()
                        };
                        values.extend(additional_values.iter().cloned());
                        let use_raw_values = use_raw_values.unwrap_or(itm.use_raw_values());
                        itm.change_operator(operator.clone(), values, use_raw_values)?
                    } else {
                        itm.clone()
                    };
                    result.push(FilterItem::Item(itm));
                }
                FilterItem::Segment(segment) => result.push(FilterItem::Segment(segment.clone())),
            }
        }
        Ok(result)
    }

    fn invalidate_join_groups_cache(&mut self) {
        self.multi_fact_join_groups = OnceCell::new();
    }

    /// Compare only the fields that identify a node in a multi-stage CTE walk:
    /// dimensions/time_dimensions (chain-resolved), the three filter slots,
    /// segments and time_shifts. Other fields (`order_by`, limits, planner
    /// flags, join_hints) propagate unchanged from the root query within a
    /// single multi-stage tree, so including them in the comparison would
    /// either be redundant work or, worse, prevent valid CTE deduplication if
    /// they ever drift. Use this for multi-stage state matching; use the full
    /// `PartialEq` for query-level equality.
    pub fn eq_as_state(&self, other: &Self) -> bool {
        Self::members_equivalent(&self.dimensions, &other.dimensions)
            && Self::members_equivalent(&self.time_dimensions, &other.time_dimensions)
            && self.dimensions_filters == other.dimensions_filters
            && self.time_dimensions_filters == other.time_dimensions_filters
            && self.measures_filters == other.measures_filters
            && self.segments == other.segments
            && self.time_shifts == other.time_shifts
    }
}

impl PartialEq for QueryProperties {
    fn eq(&self, other: &Self) -> bool {
        // Destructure to fail compilation if a new field is added without an
        // explicit decision about whether it participates in semantic equality.
        let Self {
            measures,
            dimensions,
            time_dimensions,
            dimensions_filters,
            time_dimensions_filters,
            measures_filters,
            segments,
            time_shifts,
            order_by,
            row_limit,
            offset,
            ungrouped,
            ignore_cumulative,
            pre_aggregation_query,
            total_query,
            allow_multi_stage,
            disable_external_pre_aggregations,
            pre_aggregation_id,
            query_join_hints,
            // Not part of semantic equality:
            query_tools: _,
            multi_fact_join_groups: _,
        } = self;

        Self::members_equivalent(measures, &other.measures)
            && Self::members_equivalent(dimensions, &other.dimensions)
            && Self::members_equivalent(time_dimensions, &other.time_dimensions)
            && *dimensions_filters == other.dimensions_filters
            && *time_dimensions_filters == other.time_dimensions_filters
            && *measures_filters == other.measures_filters
            && *segments == other.segments
            && *time_shifts == other.time_shifts
            && *order_by == other.order_by
            && *row_limit == other.row_limit
            && *offset == other.offset
            && *ungrouped == other.ungrouped
            && *ignore_cumulative == other.ignore_cumulative
            && *pre_aggregation_query == other.pre_aggregation_query
            && *total_query == other.total_query
            && *allow_multi_stage == other.allow_multi_stage
            && *disable_external_pre_aggregations == other.disable_external_pre_aggregations
            && *pre_aggregation_id == other.pre_aggregation_id
            && *query_join_hints == other.query_join_hints
    }
}
