use crate::logical_plan::*;
use crate::planner::sql_evaluator::MemberSymbol;
use std::rc::Rc;
pub struct MultiStageGetDateRange {
    pub time_dimension: Rc<MemberSymbol>,
    pub dimension_subqueries: Vec<Rc<DimensionSubQuery>>,
    pub source: Rc<LogicalJoin>,
}

impl PrettyPrint for MultiStageGetDateRange {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("Get Date Range", state);
        let state = state.new_level();
        let details_state = state.new_level();
        result.println(
            &format!("time_dimension: {}", self.time_dimension.full_name()),
            &details_state,
        );
        if !self.dimension_subqueries.is_empty() {
            result.println("dimension_subqueries:", &state);
            for subquery in self.dimension_subqueries.iter() {
                subquery.pretty_print(result, &details_state);
            }
        }
        result.println("source:", &state);
        self.source.pretty_print(result, &details_state);
    }
}
