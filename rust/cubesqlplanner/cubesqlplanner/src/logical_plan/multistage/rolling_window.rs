use crate::logical_plan::*;
use crate::planner::query_properties::OrderByItem;
use crate::planner::sql_evaluator::MemberSymbol;
use std::rc::Rc;

pub struct MultiStageRegularRollingWindow {
    pub trailing: Option<String>,
    pub leading: Option<String>,
    pub offset: String,
}

impl PrettyPrint for MultiStageRegularRollingWindow {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("Regular Rolling Window", state);
        let state = state.new_level();
        if let Some(trailing) = &self.trailing {
            result.println(&format!("trailing: {}", trailing), &state);
        }
        if let Some(leading) = &self.leading {
            result.println(&format!("leading: {}", leading), &state);
        }
        result.println(&format!("offset: {}", self.offset), &state);
    }
}

pub struct MultiStageToDateRollingWindow {
    pub granularity: String,
}

impl PrettyPrint for MultiStageToDateRollingWindow {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("ToDate Rolling Window", state);
        let state = state.new_level();
        result.println(&format!("granularity: {}", self.granularity), &state);
    }
}

pub enum MultiStageRollingWindowType {
    Regular(MultiStageRegularRollingWindow),
    ToDate(MultiStageToDateRollingWindow),
    RunningTotal,
}

impl PrettyPrint for MultiStageRollingWindowType {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        match self {
            MultiStageRollingWindowType::Regular(window) => window.pretty_print(result, state),
            MultiStageRollingWindowType::ToDate(window) => window.pretty_print(result, state),
            MultiStageRollingWindowType::RunningTotal => {
                result.println("Running Total Rolling Window", state)
            }
        }
    }
}

pub struct MultiStageRollingWindow {
    pub schema: Rc<LogicalSchema>,
    pub is_ungrouped: bool,
    pub rolling_time_dimension: Rc<MemberSymbol>,
    pub rolling_window: MultiStageRollingWindowType,
    pub order_by: Vec<OrderByItem>,
    pub time_series_input: String,
    pub measure_input: String,
    pub time_dimension_in_measure_input: Rc<MemberSymbol>, //time dimension in measure input can have different granularity
}

impl PrettyPrint for MultiStageRollingWindow {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        self.rolling_window.pretty_print(result, &state);
        let details_state = state.new_level();
        if self.is_ungrouped {
            result.println("is_ungrouped: true", &state);
        }
        result.println("schema:", &state);
        self.schema.pretty_print(result, &details_state);
        result.println(
            &format!(
                "rolling_time_dimension: {}",
                self.rolling_time_dimension.full_name()
            ),
            state,
        );
        if !self.order_by.is_empty() {
            result.println("order_by:", &state);
            for order_by in self.order_by.iter() {
                result.println(
                    &format!(
                        "{} {}",
                        order_by.name(),
                        if order_by.desc() { "desc" } else { "asc" }
                    ),
                    &details_state,
                );
            }
        }
        result.println(
            &format!("time_series_input: {}", self.time_series_input),
            &state,
        );
        result.println(&format!("measure_input: {}", self.measure_input), &state);
        result.println(
            &format!(
                "time_dimension_in_measure_input: {}",
                self.time_dimension_in_measure_input.full_name()
            ),
            &state,
        );
    }
}
