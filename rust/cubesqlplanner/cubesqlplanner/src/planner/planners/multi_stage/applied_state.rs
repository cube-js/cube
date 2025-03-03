use crate::plan::{FilterGroup, FilterItem};
use crate::planner::filter::FilterOperator;
use crate::planner::planners::multi_stage::MultiStageTimeShift;
use crate::planner::{BaseDimension, BaseTimeDimension};
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
    time_shifts: HashMap<String, String>,
}

impl MultiStageAppliedState {
    pub fn new(
        time_dimensions: Vec<Rc<BaseTimeDimension>>,
        dimensions: Vec<Rc<BaseDimension>>,
        time_dimensions_filters: Vec<FilterItem>,
        dimensions_filters: Vec<FilterItem>,
        measures_filters: Vec<FilterItem>,
    ) -> Rc<Self> {
        Rc::new(Self {
            time_dimensions,
            dimensions,
            time_dimensions_filters,
            dimensions_filters,
            measures_filters,
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

    pub fn add_time_shifts(&mut self, time_shifts: Vec<MultiStageTimeShift>) {
        for ts in time_shifts.into_iter() {
            self.time_shifts
                .insert(ts.time_dimension.clone(), ts.interval.clone());
        }
    }

    pub fn time_shifts(&self) -> &HashMap<String, String> {
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

    pub fn dimensions(&self) -> &Vec<Rc<BaseDimension>> {
        &self.dimensions
    }

    pub fn time_dimensions(&self) -> &Vec<Rc<BaseTimeDimension>> {
        &self.time_dimensions
    }

    pub fn change_time_dimension_granularity(
        &mut self,
        dimension_name: &str,
        new_granularity: Option<String>,
    ) {
        if let Some(time_dimension) = self
            .time_dimensions
            .iter_mut()
            .find(|dim| dim.member_evaluator().full_name() == dimension_name)
        {
            *time_dimension = time_dimension.change_granularity(new_granularity);
        }
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
            &vec![],
            &Some(replacement_values),
        );
    }

    fn change_date_range_filter_impl(
        &self,
        member_name: &String,
        filters: &Vec<FilterItem>,
        operator: &FilterOperator,
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
                        itm.change_operator(operator.clone(), values)
                    } else {
                        itm.clone()
                    };
                    result.push(FilterItem::Item(itm));
                }
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
