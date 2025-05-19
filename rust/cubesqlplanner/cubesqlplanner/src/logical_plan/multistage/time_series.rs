use crate::logical_plan::*;
use crate::planner::sql_evaluator::MemberSymbol;
use std::rc::Rc;
pub struct MultiStageTimeSeries {
    pub time_dimension: Rc<MemberSymbol>,
    pub date_range: Option<Vec<String>>,
    pub get_date_range_multistage_ref: Option<String>,
}

impl PrettyPrint for MultiStageTimeSeries {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("Time Series", state);
        let state = state.new_level();
        result.println(
            &format!("time_dimension: {}", self.time_dimension.full_name()),
            &state,
        );
        if let Some(date_range) = &self.date_range {
            result.println(
                &format!("date_range: [{}, {}]", date_range[0], date_range[1]),
                &state,
            );
        }
        if let Some(get_date_range_multistage_ref) = &self.get_date_range_multistage_ref {
            result.println(
                &format!(
                    "get_date_range_multistage_ref: {}",
                    get_date_range_multistage_ref
                ),
                &state,
            );
        }
    }
}
