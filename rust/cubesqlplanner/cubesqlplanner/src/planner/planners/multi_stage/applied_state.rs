use crate::plan::{FilterGroup, FilterItem};
use crate::planner::filter::FilterOperator;
use crate::planner::sql_evaluator::{MeasureTimeShift, MemberSymbol};
use crate::planner::{BaseDimension, BaseMember, BaseTimeDimension};
use itertools::Itertools;
use std::cmp::PartialEq;
use std::collections::HashMap;
use std::fmt::Debug;
use std::rc::Rc;

#[derive(Clone)]
pub struct MultiStageAppliedState {
    time_dimensions: Vec<Rc<BaseTimeDimension>>,
    dimensions: Vec<Rc<BaseDimension>>,
    time_dimensions_filters: Vec<FilterItem>,
    dimensions_filters: Vec<FilterItem>,
    measures_filters: Vec<FilterItem>,
    segments: Vec<FilterItem>,
    time_shifts: HashMap<String, MeasureTimeShift>,
}

impl MultiStageAppliedState {
    pub fn new(
        time_dimensions: Vec<Rc<BaseTimeDimension>>,
        dimensions: Vec<Rc<BaseDimension>>,
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
            time_shifts: HashMap::new(),
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

    pub fn add_dimensions(&mut self, dimensions: Vec<Rc<BaseDimension>>) {
        self.dimensions = self
            .dimensions
            .iter()
            .cloned()
            .chain(dimensions.into_iter())
            .unique_by(|d| d.member_evaluator().full_name())
            .collect_vec();
    }

    pub fn add_time_shifts(&mut self, time_shifts: Vec<MeasureTimeShift>) {
        for ts in time_shifts.into_iter() {
            if let Some(exists) = self.time_shifts.get_mut(&ts.dimension.full_name()) {
                exists.interval += ts.interval;
            } else {
                self.time_shifts.insert(ts.dimension.full_name(), ts);
            }
        }
    }

    pub fn time_shifts(&self) -> &HashMap<String, MeasureTimeShift> {
        &self.time_shifts
    }

    pub fn time_dimensions_filters(&self) -> &Vec<FilterItem> {
        &self.time_dimensions_filters
    }

    pub fn time_dimensions_symbols(&self) -> Vec<Rc<MemberSymbol>> {
        self.time_dimensions
            .iter()
            .map(|d| d.member_evaluator().clone())
            .collect()
    }

    pub fn dimensions_symbols(&self) -> Vec<Rc<MemberSymbol>> {
        self.dimensions
            .iter()
            .map(|d| d.member_evaluator().clone())
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

    pub fn dimensions(&self) -> &Vec<Rc<BaseDimension>> {
        &self.dimensions
    }

    pub fn time_dimensions(&self) -> &Vec<Rc<BaseTimeDimension>> {
        &self.time_dimensions
    }

    pub fn set_time_dimensions(&mut self, time_dimensions: Vec<Rc<BaseTimeDimension>>) {
        self.time_dimensions = time_dimensions;
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
                .all(|(a, b)| a.member_evaluator().full_name() == b.member_evaluator().full_name());
        dims_eq
            && self.time_dimensions_filters == other.time_dimensions_filters
            && self.dimensions_filters == other.dimensions_filters
            && self.measures_filters == other.measures_filters
            && self.time_shifts == other.time_shifts
    }
}

impl Debug for MultiStageAppliedState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiStageAppliedState")
            .field(
                "dimensions",
                &self
                    .dimensions
                    .iter()
                    .map(|d| d.member_evaluator().full_name())
                    .join(", "),
            )
            .field("time_shifts", &self.time_shifts)
            .finish()
    }
}
