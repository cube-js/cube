use crate::logical_plan::*;

pub enum MultiStageMemberLogicalType {
    LeafMeasure(MultiStageLeafMeasure),
    MeasureCalculation(MultiStageMeasureCalculation),
    GetDateRange(MultiStageGetDateRange),
    TimeSeries(MultiStageTimeSeries),
    RollingWindow(MultiStageRollingWindow),
}

impl PrettyPrint for MultiStageMemberLogicalType {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        match self {
            Self::LeafMeasure(measure) => measure.pretty_print(result, state),
            Self::MeasureCalculation(calculation) => calculation.pretty_print(result, state),
            Self::GetDateRange(get_date_range) => get_date_range.pretty_print(result, state),
            Self::TimeSeries(time_series) => time_series.pretty_print(result, state),
            Self::RollingWindow(rolling_window) => rolling_window.pretty_print(result, state),
        }
    }
}

pub struct LogicalMultiStageMember {
    pub name: String,
    pub member_type: MultiStageMemberLogicalType,
}

impl PrettyPrint for LogicalMultiStageMember {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(&format!("MultiStageMember `{}`: ", self.name), state);
        let details_state = state.new_level();
        self.member_type.pretty_print(result, &details_state);
    }
}
