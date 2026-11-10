use super::pretty_print::*;
use crate::planner::filter::{map_filter_items, Filter, FilterItem};
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

/// All filters of a query split by what they target. WHERE-style
/// filters (`time_dimensions_filters`, `dimensions_filters`,
/// `segments`) are surfaced together via `all_filters`; HAVING-style
/// `measures_filter` stays separate.
#[derive(Default)]
pub struct LogicalFilter {
    pub dimensions_filters: Vec<FilterItem>,
    pub time_dimensions_filters: Vec<FilterItem>,
    pub measures_filter: Vec<FilterItem>,
    pub segments: Vec<FilterItem>,
}

impl LogicalFilter {
    pub fn map_symbols(
        &self,
        f: &impl Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>,
    ) -> Result<Self, CubeError> {
        Ok(Self {
            dimensions_filters: map_filter_items(&self.dimensions_filters, f)?,
            time_dimensions_filters: map_filter_items(&self.time_dimensions_filters, f)?,
            measures_filter: map_filter_items(&self.measures_filter, f)?,
            segments: map_filter_items(&self.segments, f)?,
        })
    }

    pub fn visit_symbols(
        &self,
        f: &mut impl FnMut(&Rc<MemberSymbol>) -> Result<(), CubeError>,
    ) -> Result<(), CubeError> {
        self.dimensions_filters
            .iter()
            .chain(self.time_dimensions_filters.iter())
            .chain(self.measures_filter.iter())
            .chain(self.segments.iter())
            .try_for_each(|item| item.visit_symbols(f))
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
