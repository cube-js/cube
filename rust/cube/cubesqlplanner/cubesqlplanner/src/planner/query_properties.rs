//! `QueryProperties` describes what one (sub)query asks for: members, filters,
//! ordering and the flags that govern its compilation. Options fixed for the
//! whole planner run live in [`RequestOptions`](super::RequestOptions) on `State`.
//!
//! Construction goes through `QueryProperties::builder()`. Most fields default
//! to empty/false; callers spell out only what differs from a vanilla query.
//! For inputs that originate from `BaseQueryOptions`, see
//! [`QueryPropertiesCompiler`](super::query_properties_compiler).

use super::state::State;
use super::MemberSymbol;
use super::RowLimit;
use crate::cube_bridge::base_query_options::FilterValue;
use crate::logical_plan::LogicalSubqueryJoinItem;
use crate::planner::collectors::has_multi_stage_members;
use crate::planner::filter::tree_ops;
use crate::planner::filter::{BaseFilter, Filter, FilterGroup, FilterItem, FilterOperator};
use crate::planner::join_hints::JoinHints;
use crate::planner::planners::multi_stage::TimeShiftState;
use crate::planner::symbols::transforms;
use crate::planner::time_dimension::SeriesSpan;
use crate::planner::{DimensionTimeShift, MeasureTimeShifts, MemberId};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashSet;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// One entry of a query's ORDER BY clause. Equality follows the
/// reference-chain-resolved name of the member, matching the semantics used
/// for member equivalence elsewhere in `QueryProperties`.
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

/// Compare two member symbols by their reference-chain-resolved id.
pub fn member_chain_eq(a: &Rc<MemberSymbol>, b: &Rc<MemberSymbol>) -> bool {
    a.clone().resolve_reference_chain().id() == b.clone().resolve_reference_chain().id()
}

/// The full description of a query: selected members, filters, ordering
/// and planner flags. Constructed via the typed builder; `build()` applies
/// static filters and wraps the result in `Rc`.
///
/// Two equality flavours: [`PartialEq`] compares all semantic fields;
/// [`eq_as_state`](Self::eq_as_state) compares only members, filters,
/// segments and time-shifts.
#[derive(Clone, TypedBuilder)]
#[builder(build_method(into = Result<Rc<QueryProperties>, CubeError>))]
pub struct QueryProperties {
    query_tools: Rc<State>,
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
    #[builder(default)]
    order_by: Vec<OrderByItem>,
    #[builder(default)]
    row_limit: Option<RowLimit>,
    #[builder(default)]
    offset: Option<usize>,
    #[builder(default)]
    ignore_cumulative: bool,
    #[builder(default)]
    ungrouped: bool,
    #[builder(default)]
    pre_aggregation_query: bool,
    #[builder(default = Rc::new(JoinHints::new()))]
    query_join_hints: Rc<JoinHints>,
    #[builder(default = true)]
    allow_multi_stage: bool,
    /// Query-level joins against opaque sub-queries, from the SQL API
    /// `subqueryJoins`. Folded into the query's inputs.
    #[builder(default)]
    subquery_joins: Vec<LogicalSubqueryJoinItem>,
}

/// Finalize the builder output. Wired into
/// `QueryProperties::builder().…build()` via `build_method(into = …)` —
/// not intended for direct `.into()` conversions, which would re-apply
/// the finalization steps.
impl From<QueryProperties> for Result<Rc<QueryProperties>, CubeError> {
    fn from(mut qp: QueryProperties) -> Self {
        qp.apply_static_filters()?;
        Ok(Rc::new(qp))
    }
}

impl QueryProperties {
    pub(crate) fn query_tools(&self) -> &Rc<State> {
        &self.query_tools
    }

    pub(crate) fn ignore_cumulative(&self) -> bool {
        self.ignore_cumulative
    }

    pub fn allow_multi_stage(&self) -> bool {
        self.allow_multi_stage
    }

    pub fn subquery_joins(&self) -> &Vec<LogicalSubqueryJoinItem> {
        &self.subquery_joins
    }

    // Push every entry of `dimensions_filters` into matching `case`
    // expressions, and mark every FILTER_PARAMS binding by whether the query
    // filters the members it renders from. Both cover each member, filter and
    // order item. Run once at construction; mutators do not re-apply it.
    fn apply_static_filters(&mut self) -> Result<(), CubeError> {
        let dimensions_filters = self.dimensions_filters.clone();
        // A FILTER_PARAMS binding may name any filtered member, not only a
        // dimension, so its activity is read from the whole set.
        //
        // A multi-stage stage may then filter more than the query around it. It
        // builds its own `QueryProperties` from its state, so this runs again for
        // it and settles activity against the set that stage renders with.
        let all_filters = transforms::filter_params_activity_filters(&self.all_filter_items());
        for dim in self.dimensions.iter_mut() {
            *dim = transforms::apply_filter_params_activity_to_symbol(dim, &all_filters)?;
        }
        for dim in self.time_dimensions.iter_mut() {
            *dim = transforms::apply_filter_params_activity_to_symbol(dim, &all_filters)?;
        }
        for meas in self.measures.iter_mut() {
            *meas = transforms::apply_filter_params_activity_to_symbol(meas, &all_filters)?;
        }
        // A column renders wherever its symbol does, which includes the symbols
        // a query reaches only through a filter, a segment or an order item.
        for filter_item in self.dimensions_filters.iter_mut() {
            *filter_item =
                transforms::apply_filter_params_activity_to_filter_item(filter_item, &all_filters)?;
        }
        for filter_item in self.measures_filters.iter_mut() {
            *filter_item =
                transforms::apply_filter_params_activity_to_filter_item(filter_item, &all_filters)?;
        }
        for filter_item in self.time_dimensions_filters.iter_mut() {
            *filter_item =
                transforms::apply_filter_params_activity_to_filter_item(filter_item, &all_filters)?;
        }
        for filter_item in self.segments.iter_mut() {
            *filter_item =
                transforms::apply_filter_params_activity_to_filter_item(filter_item, &all_filters)?;
        }
        for order_item in self.order_by.iter_mut() {
            order_item.member_evaluator = transforms::apply_filter_params_activity_to_symbol(
                &order_item.member_evaluator,
                &all_filters,
            )?;
        }
        for dim in self.dimensions.iter_mut() {
            *dim = transforms::apply_static_filter_to_symbol(dim, &dimensions_filters)?;
        }
        for dim in self.time_dimensions.iter_mut() {
            *dim = transforms::apply_static_filter_to_symbol(dim, &dimensions_filters)?;
        }
        for meas in self.measures.iter_mut() {
            *meas = transforms::apply_static_filter_to_symbol(meas, &dimensions_filters)?;
        }
        for filter_item in self.dimensions_filters.iter_mut() {
            *filter_item =
                transforms::apply_static_filter_to_filter_item(filter_item, &dimensions_filters)?;
        }
        for filter_item in self.measures_filters.iter_mut() {
            *filter_item =
                transforms::apply_static_filter_to_filter_item(filter_item, &dimensions_filters)?;
        }
        for filter_item in self.time_dimensions_filters.iter_mut() {
            *filter_item =
                transforms::apply_static_filter_to_filter_item(filter_item, &dimensions_filters)?;
        }
        for filter_item in self.segments.iter_mut() {
            *filter_item =
                transforms::apply_static_filter_to_filter_item(filter_item, &dimensions_filters)?;
        }
        for order_item in self.order_by.iter_mut() {
            order_item.member_evaluator = transforms::apply_static_filter_to_symbol(
                &order_item.member_evaluator,
                &dimensions_filters,
            )?;
        }
        Ok(())
    }

    pub(crate) fn extract_dimensions_from_order(&self) -> Vec<Rc<MemberSymbol>> {
        self.order_by
            .iter()
            .filter_map(|order| {
                if order.member_evaluator.peel_refs().as_dimension().is_ok() {
                    Some(order.member_evaluator.clone())
                } else {
                    None
                }
            })
            .collect()
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

    pub fn row_limit(&self) -> Option<RowLimit> {
        self.row_limit
    }

    pub fn query_join_hints(&self) -> &Rc<JoinHints> {
        &self.query_join_hints
    }

    pub fn offset(&self) -> Option<usize> {
        self.offset
    }

    pub fn order_by(&self) -> &[OrderByItem] {
        &self.order_by
    }

    pub fn ungrouped(&self) -> bool {
        self.ungrouped
    }

    pub fn is_pre_aggregation_query(&self) -> bool {
        self.pre_aggregation_query
    }

    /// Concatenation of `time_dimensions_filters`, `dimensions_filters`, and
    /// `segments` into a single `Filter`. `measures_filters` are not included.
    /// `time_dimensions_filters`, `dimensions_filters` and `segments` as a flat
    /// list. `measures_filters` are HAVING-style and stay out.
    pub fn all_filter_items(&self) -> Vec<FilterItem> {
        self.time_dimensions_filters
            .iter()
            .chain(self.dimensions_filters.iter())
            .chain(self.segments.iter())
            .cloned()
            .collect_vec()
    }

    /// The same set as `all_filter_items`, as a single `Filter`.
    pub fn all_filters(&self) -> Option<Filter> {
        let items = self.all_filter_items();
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

    /// Every symbol the query touches: selected members, members referenced
    /// inside filters, and measures pulled in by measure-filter or order-by
    /// references. Deduplicated by full name.
    pub fn all_used_symbols(&self) -> Result<Vec<Rc<MemberSymbol>>, CubeError> {
        let mut members = vec![];
        members.extend(self.time_dimensions.iter().cloned());
        members.extend(self.dimensions.iter().cloned());
        self.fill_all_filter_symbols(&mut members);
        members.extend(self.all_used_measures()?);

        let res = members
            .into_iter()
            .unique_by(|m| m.id().clone())
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
            .unique_by(|m| m.id().clone())
            .collect_vec()
    }

    fn fill_all_filter_symbols(&self, members: &mut Vec<Rc<MemberSymbol>>) {
        if let Some(all_filters) = self.all_filters() {
            for filter_item in all_filters.items.iter() {
                filter_item.find_all_member_evaluators(members);
            }
        }
    }

    /// First time-dimension with a granularity (ASC) if any; otherwise the
    /// first measure (DESC) when both measures and dimensions are present;
    /// otherwise the first dimension (ASC). Empty when none apply.
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

    pub(crate) fn all_used_measures(&self) -> Result<Vec<Rc<MemberSymbol>>, CubeError> {
        let mut measures = self.measures.clone();
        for item in self.measures_filters.iter() {
            self.fill_missed_measures_from_filter(item, &mut measures)?;
        }
        for item in self.order_by.iter() {
            if item.member_evaluator.peel_refs().as_measure().is_ok() {
                let id = item.member_evaluator.id();
                if !measures.iter().any(|m| m.id() == id) {
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
                let item_member_id = item.member_id();
                if !measures.iter().any(|m| m.id() == &item_member_id) {
                    measures.push(item.member_evaluator().clone());
                }
            }
            FilterItem::Segment(_) => {}
        }
        Ok(())
    }

    // Compare two member slices element-wise by reference-chain-resolved
    // full name.
    fn members_equivalent(a: &[Rc<MemberSymbol>], b: &[Rc<MemberSymbol>]) -> bool {
        a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| member_chain_eq(x, y))
    }

    pub fn set_dimensions(&mut self, dimensions: Vec<Rc<MemberSymbol>>) {
        self.dimensions = dimensions;
    }

    pub fn set_time_dimensions(&mut self, time_dimensions: Vec<Rc<MemberSymbol>>) {
        self.time_dimensions = time_dimensions;
    }

    /// Append `dimensions` to the existing list, deduplicating by
    /// reference-chain-resolved full name. A dimension the grain already
    /// carries as a time dimension is dropped rather than appended: a time
    /// dimension's full name pins its granularity, so an entry that matches
    /// one would render the very same column under the very same alias.
    pub fn add_dimensions(&mut self, dimensions: Vec<Rc<MemberSymbol>>) {
        let time_dimension_names = self
            .time_dimensions
            .iter()
            .map(|d| d.clone().resolve_reference_chain().full_name())
            .collect::<HashSet<_>>();
        let added = dimensions.into_iter().filter(|d| {
            !time_dimension_names.contains(&d.clone().resolve_reference_chain().full_name())
        });
        self.dimensions = self
            .dimensions
            .iter()
            .cloned()
            .chain(added)
            .unique_by(|d| d.clone().resolve_reference_chain().full_name())
            .collect_vec();
    }

    pub fn add_dimension_filter(&mut self, filter: FilterItem) {
        self.dimensions_filters.push(filter);
    }

    /// Keep only dimensions and time-dimensions whose chain-resolved name is
    /// in `resolved_dimensions` or that have no multi-stage members.
    pub fn remove_multistage_dimensions(
        &mut self,
        resolved_dimensions: &HashSet<MemberId>,
    ) -> Result<(), CubeError> {
        let mut filtered = Vec::new();
        for d in &self.dimensions {
            if resolved_dimensions.contains(d.clone().resolve_reference_chain().id())
                || !has_multi_stage_members(d, true)?
            {
                filtered.push(d.clone());
            }
        }
        self.dimensions = filtered;
        let mut filtered = Vec::new();
        for d in &self.time_dimensions {
            if resolved_dimensions.contains(d.clone().resolve_reference_chain().id())
                || !has_multi_stage_members(d, true)?
            {
                filtered.push(d.clone());
            }
        }
        self.time_dimensions = filtered;
        Ok(())
    }

    /// Merge `time_shifts` into [`Self::time_shifts`]. Interval shifts on
    /// the same dimension compose; mixing a named shift with an interval
    /// (or two named shifts) on the same dimension returns an error.
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
                .get_mut(ts.dimension.id())
            {
                if let Some(interval) = exists.interval.clone() {
                    if let Some(new_interval) = ts.interval {
                        exists.interval = Some(interval + new_interval);
                    } else {
                        return Err(CubeError::user(format!(
                            "Cannot use both named ({}) and interval ({}) shifts for the same dimension: {}.",
                            ts.name.clone().unwrap_or("-".to_string()),
                            interval.to_sql(),
                            ts.dimension.full_name(),
                        )));
                    }
                } else if let Some(named_shift) = exists.name.clone() {
                    return if let Some(new_interval) = ts.interval {
                        Err(CubeError::user(format!(
                            "Cannot use both named ({}) and interval ({}) shifts for the same dimension: {}.",
                            named_shift,
                            new_interval.to_sql(),
                            ts.dimension.full_name(),
                        )))
                    } else {
                        Err(CubeError::user(format!(
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
                    .insert(ts.dimension.id().clone(), ts);
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
            .unique_by(|s| s.id().clone())
            .collect_vec()
    }

    pub fn remove_filter_for_member(&mut self, member_name: &MemberId) {
        self.time_dimensions_filters =
            Self::extract_filters_exclude_member(member_name, &self.time_dimensions_filters);
        self.dimensions_filters =
            Self::extract_filters_exclude_member(member_name, &self.dimensions_filters);
        self.measures_filters =
            Self::extract_filters_exclude_member(member_name, &self.measures_filters);
    }

    pub fn remove_filters_for_members(&mut self, member_names: &[MemberId]) {
        self.time_dimensions_filters =
            tree_ops::exclude_members(member_names, &self.time_dimensions_filters);
        self.dimensions_filters = tree_ops::exclude_members(member_names, &self.dimensions_filters);
        self.measures_filters = tree_ops::exclude_members(member_names, &self.measures_filters);
        self.segments = tree_ops::exclude_members(member_names, &self.segments);
    }

    pub fn keep_only_filters_for_members(&mut self, member_names: &[MemberId]) {
        self.time_dimensions_filters =
            tree_ops::keep_only_members(member_names, &self.time_dimensions_filters);
        self.dimensions_filters =
            tree_ops::keep_only_members(member_names, &self.dimensions_filters);
        self.measures_filters = tree_ops::keep_only_members(member_names, &self.measures_filters);
        self.segments = tree_ops::keep_only_members(member_names, &self.segments);
    }

    pub fn add_dimension_filters(&mut self, items: Vec<FilterItem>) {
        self.dimensions_filters.extend(items);
    }

    pub fn add_time_dimension_filters(&mut self, items: Vec<FilterItem>) {
        self.time_dimensions_filters.extend(items);
    }

    pub fn add_measure_filters(&mut self, items: Vec<FilterItem>) {
        self.measures_filters.extend(items);
    }

    fn extract_filters_exclude_member(
        member_name: &MemberId,
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
                    if &itm.member_id() != member_name {
                        result.push(FilterItem::Item(itm.clone()));
                    }
                }
                FilterItem::Segment(_) => {}
            }
        }
        result
    }

    pub fn has_filters_for_member(&self, member_name: &MemberId) -> bool {
        Self::has_filters_for_member_impl(member_name, &self.time_dimensions_filters)
            || Self::has_filters_for_member_impl(member_name, &self.dimensions_filters)
            || Self::has_filters_for_member_impl(member_name, &self.measures_filters)
    }

    fn has_filters_for_member_impl(member_name: &MemberId, filters: &[FilterItem]) -> bool {
        for item in filters.iter() {
            match item {
                FilterItem::Group(group) => {
                    if Self::has_filters_for_member_impl(member_name, &group.items) {
                        return true;
                    }
                }
                FilterItem::Item(itm) => {
                    if &itm.member_id() == member_name {
                        return true;
                    }
                }
                FilterItem::Segment(_) => {}
            }
        }
        false
    }

    /// Rewrite an `InDateRange(from, to)` filter on `member_name` into a single
    /// rolling-window-over-date-range filter anchored by `offset`. The window is
    /// rendered by `RollingWindowOffsetOp`; here we only carry the inputs
    /// (`from`, `to`, `trailing`, `leading`, `offset`). No rolling interval on
    /// either side (e.g. running total, to_date) keeps the filter as-is; both
    /// sides `unbounded` drops it entirely.
    pub fn replace_date_range_for_rolling_window_without_granularity(
        &mut self,
        member_name: &MemberId,
        trailing: &Option<String>,
        leading: &Option<String>,
        offset: &str,
    ) -> Result<(), CubeError> {
        if trailing.is_none() && leading.is_none() {
            return Ok(());
        }

        // Both sides unbounded: the window spans everything, drop the date filter.
        if trailing.as_deref() == Some("unbounded") && leading.as_deref() == Some("unbounded") {
            self.time_dimensions_filters.retain(|item| match item {
                FilterItem::Item(itm) => {
                    !(&itm.member_id() == member_name
                        && matches!(itm.filter_operator(), FilterOperator::InDateRange))
                }
                _ => true,
            });
            return Ok(());
        }

        // Keep the original [from, to] values and append the window inputs, so
        // the filter carries [from, to, trailing, leading, offset].
        let additional_values = vec![
            FilterValue::from(trailing.clone()),
            FilterValue::from(leading.clone()),
            FilterValue::Str(offset.to_string()),
        ];
        self.time_dimensions_filters = self.change_date_range_filter_impl(
            member_name,
            &self.time_dimensions_filters,
            &FilterOperator::RollingWindowOffsetDateRange,
            None,
            &additional_values,
            &None,
        )?;
        Ok(())
    }

    /// Rewrite the `InDateRange` filter on `member_name` into a regular
    /// rolling-window filter. The filter carries
    /// `[from, to, trailing, leading]`, followed by the span the base scan
    /// reads when that is known at plan time.
    pub fn replace_regular_date_range_filter(
        &mut self,
        member_name: &MemberId,
        left_interval: Option<String>,
        right_interval: Option<String>,
        scan_range: Option<SeriesSpan>,
    ) -> Result<(), CubeError> {
        let operator = FilterOperator::RegularRollingWindowDateRange;
        let mut values = vec![
            FilterValue::from(left_interval),
            FilterValue::from(right_interval),
        ];
        values.extend(series_span_values(scan_range));
        self.time_dimensions_filters = self.change_date_range_filter_impl(
            member_name,
            &self.time_dimensions_filters,
            &operator,
            None,
            &values,
            &None,
        )?;
        Ok(())
    }

    /// Rewrite the `InDateRange` filter on `member_name` into a `to_date`
    /// rolling-window filter. The filter carries `[from, to, granularity]`,
    /// followed by the span the window reads when that is known at plan time.
    pub fn replace_to_date_date_range_filter(
        &mut self,
        member_name: &MemberId,
        granularity: &String,
        window_range: Option<SeriesSpan>,
    ) -> Result<(), CubeError> {
        let operator = FilterOperator::ToDateRollingWindowDateRange;
        let mut values = vec![FilterValue::Str(granularity.clone())];
        values.extend(series_span_values(window_range));
        self.time_dimensions_filters = self.change_date_range_filter_impl(
            member_name,
            &self.time_dimensions_filters,
            &operator,
            None,
            &values,
            &None,
        )?;
        Ok(())
    }

    pub fn replace_range_in_date_filter(
        &mut self,
        member_name: &MemberId,
        new_from: String,
        new_to: String,
    ) -> Result<(), CubeError> {
        let operator = FilterOperator::InDateRange;
        let replacement_values = vec![FilterValue::Str(new_from), FilterValue::Str(new_to)];
        self.time_dimensions_filters = self.change_date_range_filter_impl(
            member_name,
            &self.time_dimensions_filters,
            &operator,
            None,
            &vec![],
            &Some(replacement_values),
        )?;
        Ok(())
    }

    /// Rewrites the bounds of one top-level `InDateRange` filter, leaving any
    /// other date range on the same member as it is.
    pub fn replace_bounds_of_date_filter(
        &mut self,
        filter: &Rc<BaseFilter>,
        new_from: String,
        new_to: String,
    ) -> Result<(), CubeError> {
        for item in self.time_dimensions_filters.iter_mut() {
            if let FilterItem::Item(itm) = item {
                if Rc::ptr_eq(itm, filter) {
                    *itm = itm.change_operator(
                        FilterOperator::InDateRange,
                        vec![FilterValue::Str(new_from), FilterValue::Str(new_to)],
                        itm.use_raw_values(),
                        self.query_tools.query_tools().clone(),
                        None,
                    )?;
                    break;
                }
            }
        }
        Ok(())
    }

    /// Same as [`replace_range_in_date_filter`](Self::replace_range_in_date_filter)
    /// but forces the rewritten filter to use raw (unparametrized) values.
    pub fn replace_range_to_subquery_in_date_filter(
        &mut self,
        member_name: &MemberId,
        new_from: String,
        new_to: String,
    ) -> Result<(), CubeError> {
        let operator = FilterOperator::InDateRange;
        let replacement_values = vec![FilterValue::Str(new_from), FilterValue::Str(new_to)];
        self.time_dimensions_filters = self.change_date_range_filter_impl(
            member_name,
            &self.time_dimensions_filters,
            &operator,
            Some(true),
            &vec![],
            &Some(replacement_values),
        )?;
        Ok(())
    }

    fn change_date_range_filter_impl(
        &self,
        member_name: &MemberId,
        filters: &[FilterItem],
        operator: &FilterOperator,
        use_raw_values: Option<bool>,
        additional_values: &Vec<FilterValue>,
        replacement_values: &Option<Vec<FilterValue>>,
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
                    let itm = if &itm.member_id() == member_name
                        && matches!(itm.filter_operator(), FilterOperator::InDateRange)
                    {
                        let mut values = if let Some(values) = replacement_values {
                            values.clone()
                        } else {
                            itm.values().clone()
                        };
                        values.extend(additional_values.iter().cloned());
                        let use_raw_values = use_raw_values.unwrap_or(itm.use_raw_values());
                        itm.change_operator(
                            operator.clone(),
                            values,
                            use_raw_values,
                            self.query_tools.query_tools().clone(),
                            // FIXME: late compilation — only needed to recompile a
                            // to_date rolling-window granularity here.
                            Some(&mut self.query_tools.compiler().borrow_mut()),
                        )?
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

    /// Equality over members (chain-resolved), the three filter slots,
    /// segments and time-shifts. Excludes ordering, limits, planner flags
    /// and join hints; for those fields use the full [`PartialEq`].
    ///
    /// Filters are compared with [`tree_ops::eq_with_member`] rather than with
    /// `FilterItem`'s own equality, which looks at a filter's operator and
    /// values but not at the member it restricts. Two states filtering
    /// different dimensions to the same value are different states, and
    /// conflating them makes a CTE serve a filter it was never built for.
    pub fn eq_as_state(&self, other: &Self) -> bool {
        Self::members_equivalent(&self.dimensions, &other.dimensions)
            && Self::filters_equivalent(&self.dimensions_filters, &other.dimensions_filters)
            && Self::members_equivalent(&self.time_dimensions, &other.time_dimensions)
            && Self::filters_equivalent(
                &self.time_dimensions_filters,
                &other.time_dimensions_filters,
            )
            && Self::filters_equivalent(&self.measures_filters, &other.measures_filters)
            && Self::filters_equivalent(&self.segments, &other.segments)
            && self.time_shifts == other.time_shifts
    }

    fn filters_equivalent(a: &[FilterItem], b: &[FilterItem]) -> bool {
        a.len() == b.len()
            && a.iter()
                .zip(b.iter())
                .all(|(a, b)| tree_ops::eq_with_member(a, b))
    }
}

/// Equality over every semantic field. Members are compared by reference-
/// chain-resolved name; `query_tools` is excluded. See also [`eq_as_state`](QueryProperties::eq_as_state).
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
            allow_multi_stage,
            query_join_hints,
            subquery_joins,
            // Not part of semantic equality:
            query_tools: _,
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
            && *allow_multi_stage == other.allow_multi_stage
            && *query_join_hints == other.query_join_hints
            // Sub-query joins compared semantically: the request triple
            // (`sql`, `alias`, `join_type`) plus the compiled ON condition
            // (`on_sql` via `SqlCall::struct_eq`), so two joins differing
            // only in their ON are not equal.
            && subquery_joins.len() == other.subquery_joins.len()
            && subquery_joins
                .iter()
                .zip(other.subquery_joins.iter())
                .all(|(a, b)| {
                    a.sql == b.sql
                        && a.alias == b.alias
                        && a.join_type == b.join_type
                        && a.on_sql.struct_eq(&b.on_sql)
                })
    }
}

/// Tail a rolling-window filter carries its scan span in: the lower bound, the
/// two upper bounds one per series shape, and whether the granularity is a
/// predefined one. Empty where the span is not derivable while planning.
fn series_span_values(span: Option<SeriesSpan>) -> Vec<FilterValue> {
    match span {
        Some(span) => vec![
            FilterValue::Str(span.from),
            FilterValue::Str(span.to_aligned),
            FilterValue::Str(span.to_stepped),
            FilterValue::Bool(span.predefined_granularity),
        ],
        None => vec![],
    }
}
