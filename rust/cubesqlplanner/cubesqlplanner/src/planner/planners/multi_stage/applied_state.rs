use crate::plan::{FilterGroup, FilterItem};
use crate::planner::filter::FilterOperator;
use crate::planner::planners::multi_stage::time_shift_state::TimeShiftState;
use crate::planner::sql_evaluator::{DimensionTimeShift, MeasureTimeShifts, MemberSymbol};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::cmp::PartialEq;
use std::fmt::Debug;
use std::rc::Rc;

#[derive(Clone)]
pub struct MultiStageAppliedState {
    time_dimensions: Vec<Rc<MemberSymbol>>,
    dimensions: Vec<Rc<MemberSymbol>>,
    time_dimensions_filters: Vec<FilterItem>,
    dimensions_filters: Vec<FilterItem>,
    measures_filters: Vec<FilterItem>,
    segments: Vec<FilterItem>,
    time_shifts: TimeShiftState,
}

impl MultiStageAppliedState {
    pub fn new(
        time_dimensions: Vec<Rc<MemberSymbol>>,
        dimensions: Vec<Rc<MemberSymbol>>,
        time_dimensions_filters: Vec<FilterItem>,
        dimensions_filters: Vec<FilterItem>,
        measures_filters: Vec<FilterItem>,
        segments: Vec<FilterItem>,
    ) -> Rc<Self> {
        Rc::new(Self {
            time_dimensions,
            dimensions,
            time_dimensions_filters,
            dimensions_filters,
            measures_filters,
            segments,
            time_shifts: TimeShiftState::default(),
        })
    }

    pub fn clone_state(&self) -> Self {
        Self {
            time_dimensions: self.time_dimensions.clone(),
            dimensions: self.dimensions.clone(),
            time_dimensions_filters: self.time_dimensions_filters.clone(),
            dimensions_filters: self.dimensions_filters.clone(),
            measures_filters: self.measures_filters.clone(),
            segments: self.segments.clone(),
            time_shifts: self.time_shifts.clone(),
        }
    }

    pub fn add_dimensions(&mut self, dimensions: Vec<Rc<MemberSymbol>>) {
        self.dimensions = self
            .dimensions
            .iter()
            .cloned()
            .chain(dimensions.into_iter())
            .unique_by(|d| d.full_name())
            .collect_vec();
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

    pub fn time_shifts(&self) -> &TimeShiftState {
        &self.time_shifts
    }

    fn all_time_members(&self) -> Vec<Rc<MemberSymbol>> {
        let mut filter_symbols = self.all_dimensions_symbols();
        for filter_item in self
            .time_dimensions_filters
            .iter()
            .chain(self.dimensions_filters.iter())
            .chain(self.segments.iter())
        {
            filter_item.find_all_member_evaluators(&mut filter_symbols);
        }

        let time_symbols = filter_symbols
            .into_iter()
            .filter_map(|m| {
                let symbol = if let Ok(time_dim) = m.as_time_dimension() {
                    time_dim.base_symbol().clone().resolve_reference_chain()
                } else {
                    m.resolve_reference_chain()
                };
                if let Ok(dim) = symbol.as_dimension() {
                    if dim.dimension_type() == "time" {
                        Some(symbol)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .unique_by(|s| s.full_name())
            .collect_vec();
        time_symbols
    }

    pub fn time_dimensions_filters(&self) -> &Vec<FilterItem> {
        &self.time_dimensions_filters
    }

    pub fn time_dimensions_symbols(&self) -> Vec<Rc<MemberSymbol>> {
        self.time_dimensions().clone()
    }

    pub fn dimensions_symbols(&self) -> Vec<Rc<MemberSymbol>> {
        self.dimensions.clone()
    }

    pub fn all_dimensions_symbols(&self) -> Vec<Rc<MemberSymbol>> {
        self.time_dimensions
            .iter()
            .cloned()
            .chain(self.dimensions.iter().cloned())
            .collect()
    }

    pub fn dimensions_filters(&self) -> &Vec<FilterItem> {
        &self.dimensions_filters
    }

    pub fn segments(&self) -> &Vec<FilterItem> {
        &self.segments
    }

    pub fn measures_filters(&self) -> &Vec<FilterItem> {
        &self.measures_filters
    }

    pub fn dimensions(&self) -> &Vec<Rc<MemberSymbol>> {
        &self.dimensions
    }

    pub fn time_dimensions(&self) -> &Vec<Rc<MemberSymbol>> {
        &self.time_dimensions
    }

    pub fn set_time_dimensions(&mut self, time_dimensions: Vec<Rc<MemberSymbol>>) {
        self.time_dimensions = time_dimensions;
    }

    pub fn set_dimensions(&mut self, dimensions: Vec<Rc<MemberSymbol>>) {
        self.dimensions = dimensions;
    }

    pub fn remove_filter_for_member(&mut self, member_name: &String) {
        self.time_dimensions_filters =
            self.extract_filters_exclude_member(member_name, &self.time_dimensions_filters);
        self.dimensions_filters =
            self.extract_filters_exclude_member(member_name, &self.dimensions_filters);
        self.measures_filters =
            self.extract_filters_exclude_member(member_name, &self.measures_filters);
    }

    fn extract_filters_exclude_member(
        &self,
        member_name: &String,
        filters: &Vec<FilterItem>,
    ) -> Vec<FilterItem> {
        let mut result = Vec::new();
        for item in filters.iter() {
            match item {
                FilterItem::Group(group) => {
                    let new_group = FilterItem::Group(Rc::new(FilterGroup::new(
                        group.operator.clone(),
                        self.extract_filters_exclude_member(member_name, &group.items),
                    )));
                    result.push(new_group);
                }
                FilterItem::Item(itm) => {
                    if &itm.member_name() != member_name {
                        result.push(FilterItem::Item(itm.clone()));
                    }
                }
                FilterItem::Segment(_) => {}
            }
        }
        result
    }

    pub fn has_filters_for_member(&self, member_name: &String) -> bool {
        self.has_filters_for_member_impl(member_name, &self.time_dimensions_filters)
            || self.has_filters_for_member_impl(member_name, &self.dimensions_filters)
            || self.has_filters_for_member_impl(member_name, &self.measures_filters)
    }

    fn has_filters_for_member_impl(&self, member_name: &String, filters: &Vec<FilterItem>) -> bool {
        for item in filters.iter() {
            match item {
                FilterItem::Group(group) => {
                    if self.has_filters_for_member_impl(member_name, &group.items) {
                        return true;
                    }
                }
                FilterItem::Item(itm) => {
                    if &itm.member_name() == member_name {
                        return true;
                    }
                }
                FilterItem::Segment(_) => {}
            }
        }
        false
    }

    pub fn replace_regular_date_range_filter(
        &mut self,
        member_name: &String,
        left_interval: Option<String>,
        right_interval: Option<String>,
    ) {
        let operator = FilterOperator::RegularRollingWindowDateRange;
        let values = vec![left_interval.clone(), right_interval.clone()];
        self.time_dimensions_filters = self.change_date_range_filter_impl(
            member_name,
            &self.time_dimensions_filters,
            &operator,
            None,
            &values,
            &None,
        );
    }

    pub fn replace_to_date_date_range_filter(
        &mut self,
        member_name: &String,
        granularity: &String,
    ) {
        let operator = FilterOperator::ToDateRollingWindowDateRange;
        let values = vec![Some(granularity.clone())];
        self.time_dimensions_filters = self.change_date_range_filter_impl(
            member_name,
            &self.time_dimensions_filters,
            &operator,
            None,
            &values,
            &None,
        );
    }

    pub fn replace_range_in_date_filter(
        &mut self,
        member_name: &String,
        new_from: String,
        new_to: String,
    ) {
        let operator = FilterOperator::InDateRange;
        let replacement_values = vec![Some(new_from), Some(new_to)];
        self.time_dimensions_filters = self.change_date_range_filter_impl(
            member_name,
            &self.time_dimensions_filters,
            &operator,
            None,
            &vec![],
            &Some(replacement_values),
        );
    }

    pub fn replace_range_to_subquery_in_date_filter(
        &mut self,
        member_name: &String,
        new_from: String,
        new_to: String,
    ) {
        let operator = FilterOperator::InDateRange;
        let replacement_values = vec![Some(new_from), Some(new_to)];
        self.time_dimensions_filters = self.change_date_range_filter_impl(
            member_name,
            &self.time_dimensions_filters,
            &operator,
            Some(true),
            &vec![],
            &Some(replacement_values),
        );
    }

    fn change_date_range_filter_impl(
        &self,
        member_name: &String,
        filters: &Vec<FilterItem>,
        operator: &FilterOperator,
        use_raw_values: Option<bool>,
        additional_values: &Vec<Option<String>>,
        replacement_values: &Option<Vec<Option<String>>>,
    ) -> Vec<FilterItem> {
        let mut result = Vec::new();
        for item in filters.iter() {
            match item {
                FilterItem::Group(group) => {
                    let new_group = FilterItem::Group(Rc::new(FilterGroup::new(
                        group.operator.clone(),
                        self.change_date_range_filter_impl(
                            member_name,
                            filters,
                            operator,
                            use_raw_values,
                            additional_values,
                            replacement_values,
                        ),
                    )));
                    result.push(new_group);
                }
                FilterItem::Item(itm) => {
                    let itm = if &itm.member_name() == member_name
                        && matches!(itm.filter_operator(), FilterOperator::InDateRange)
                    {
                        let mut values = if let Some(values) = replacement_values {
                            values.clone()
                        } else {
                            itm.values().clone()
                        };
                        values.extend(additional_values.iter().cloned());
                        let use_raw_values = use_raw_values.unwrap_or(itm.use_raw_values());
                        itm.change_operator(operator.clone(), values, use_raw_values)
                    } else {
                        itm.clone()
                    };
                    result.push(FilterItem::Item(itm));
                }
                FilterItem::Segment(segment) => result.push(FilterItem::Segment(segment.clone())),
            }
        }
        result
    }
}

impl PartialEq for MultiStageAppliedState {
    fn eq(&self, other: &Self) -> bool {
        let dims_eq = self.dimensions.len() == other.dimensions.len()
            && self
                .dimensions
                .iter()
                .zip(other.dimensions.iter())
                .all(|(a, b)| a.full_name() == b.full_name());
        dims_eq
            && self.time_dimensions_filters == other.time_dimensions_filters
            && self.dimensions_filters == other.dimensions_filters
            && self.measures_filters == other.measures_filters
            && self.time_shifts.dimensions_shifts == other.time_shifts.dimensions_shifts
    }
}

impl Debug for MultiStageAppliedState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiStageAppliedState")
            .field(
                "dimensions",
                &self.dimensions.iter().map(|d| d.full_name()).join(", "),
            )
            .field("time_shifts", &self.time_shifts)
            .finish()
    }
}
