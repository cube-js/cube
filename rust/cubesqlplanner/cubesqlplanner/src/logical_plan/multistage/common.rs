use crate::logical_plan::pretty_print::*;
use crate::planner::planners::multi_stage::MultiStageAppliedState;

impl PrettyPrint for MultiStageAppliedState {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        let details_state = state.new_level();
        result.println(
            &format!(
                "-time_dimensions: {}",
                print_symbols(&self.time_dimensions())
            ),
            state,
        );

        result.println(
            &format!("-dimensions: {}", print_symbols(&self.dimensions())),
            state,
        );

        result.println("dimensions_filters:", &state);
        for filter in self.dimensions_filters().iter() {
            pretty_print_filter_item(result, &details_state, filter);
        }
        result.println("time_dimensions_filters:", &state);
        for filter in self.time_dimensions_filters().iter() {
            pretty_print_filter_item(result, &details_state, filter);
        }
        result.println("measures_filter:", &state);
        for filter in self.measures_filters().iter() {
            pretty_print_filter_item(result, &details_state, filter);
        }
        result.println("segments:", &state);
        for filter in self.segments().iter() {
            pretty_print_filter_item(result, &details_state, filter);
        }

        result.println("time_shifts:", &state);
        for (_, time_shift) in self.time_shifts().dimensions_shifts.iter() {
            result.println(
                &format!(
                    "- {}: {}",
                    time_shift.dimension.full_name(),
                    if let Some(interval) = &time_shift.interval {
                        interval.to_sql()
                    } else if let Some(name) = &time_shift.name {
                        format!("{} (named)", name.to_string())
                    } else {
                        "None".to_string()
                    }
                ),
                &details_state,
            );
        }
    }
}
