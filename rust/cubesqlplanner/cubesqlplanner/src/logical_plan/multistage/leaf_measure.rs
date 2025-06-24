use crate::logical_plan::*;
use crate::planner::planners::multi_stage::TimeShiftState;
use crate::planner::sql_evaluator::MemberSymbol;
use std::rc::Rc;

pub struct MultiStageLeafMeasure {
    pub measure: Rc<MemberSymbol>,
    pub render_measure_as_state: bool, //Render measure as state, for example hll state for count_approx
    pub render_measure_for_ungrouped: bool,
    pub time_shifts: TimeShiftState,
    pub query: Rc<Query>,
}

impl PrettyPrint for MultiStageLeafMeasure {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("Leaf Measure Query", state);
        let state = state.new_level();
        result.println(&format!("measure: {}", self.measure.full_name()), &state);
        if self.render_measure_as_state {
            result.println("render_measure_as_state: true", &state);
        }
        if self.render_measure_for_ungrouped {
            result.println("render_measure_for_ungrouped: true", &state);
        }
        if !self.time_shifts.dimensions_shifts.is_empty() {
            result.println("time_shifts:", &state);
            let details_state = state.new_level();
            if let Some(common) = &self.time_shifts.common_time_shift {
                result.println(&format!("- common: {}", common.to_sql()), &details_state);
            }
            for (_, time_shift) in self.time_shifts.dimensions_shifts.iter() {
                result.println(
                    &format!(
                        "- {}: {}",
                        time_shift.dimension.full_name(),
                        time_shift.interval.to_sql()
                    ),
                    &details_state,
                );
            }
        }
        result.println(&format!("query:"), &state);
        let details_state = state.new_level();
        self.query.pretty_print(result, &details_state);
    }
}
