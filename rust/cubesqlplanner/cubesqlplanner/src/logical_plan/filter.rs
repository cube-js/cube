use super::pretty_print::*;
use crate::plan::{Filter, FilterItem};
use itertools::Itertools;

pub struct LogicalFilter {
    pub dimensions_filters: Vec<FilterItem>,
    pub time_dimensions_filters: Vec<FilterItem>,
    pub measures_filter: Vec<FilterItem>,
    pub segments: Vec<FilterItem>,
}

impl LogicalFilter {
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
    pub fn measures_filter(&self) -> Option<Filter> {
        if self.measures_filter.is_empty() {
            None
        } else {
            Some(Filter {
                items: self.measures_filter.clone(),
            })
        }
    }
}

impl PrettyPrint for LogicalFilter {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        let details_state = state.new_level();
        result.println("dimensions_filters:", &state);
        for filter in self.dimensions_filters.iter() {
            pretty_print_filter_item(result, &details_state, filter);
        }
        result.println("time_dimensions_filters:", &state);
        for filter in self.time_dimensions_filters.iter() {
            pretty_print_filter_item(result, &details_state, filter);
        }
        result.println("measures_filter:", &state);
        for filter in self.measures_filter.iter() {
            pretty_print_filter_item(result, &details_state, filter);
        }
        result.println("segments:", &state);
        for filter in self.segments.iter() {
            pretty_print_filter_item(result, &details_state, filter);
        }
    }
}
