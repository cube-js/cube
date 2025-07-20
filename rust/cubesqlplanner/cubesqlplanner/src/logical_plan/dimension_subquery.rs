use super::pretty_print::*;
use super::*;
use crate::planner::sql_evaluator::MemberSymbol;
use std::rc::Rc;

pub struct DimensionSubQuery {
    pub query: Rc<Query>,
    pub primary_keys_dimensions: Vec<Rc<MemberSymbol>>,
    pub subquery_dimension: Rc<MemberSymbol>,
    pub measure_for_subquery_dimension: Rc<MemberSymbol>,
}

impl PrettyPrint for DimensionSubQuery {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("DimensionSubQuery: ", state);
        let state = state.new_level();
        let details_state = state.new_level();
        result.println(&format!("query: "), &state);
        self.query.pretty_print(result, &details_state);
        result.println(
            &format!(
                "-primary_keys_dimensions: {}",
                print_symbols(&self.primary_keys_dimensions)
            ),
            &state,
        );
        result.println(
            &format!(
                "-subquery_dimension: {}",
                self.subquery_dimension.full_name()
            ),
            &state,
        );
        result.println(
            &format!(
                "-measure_for_subquery_dimension: {}",
                self.measure_for_subquery_dimension.full_name()
            ),
            &state,
        );
    }
}
