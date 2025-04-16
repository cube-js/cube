use super::*;
use crate::planner::sql_evaluator::MemberSymbol;
use std::rc::Rc;

pub struct MeasureSubquery {
    pub primary_keys_dimensions: Vec<Rc<MemberSymbol>>,
    pub measures: Vec<Rc<MemberSymbol>>,
    pub dimension_subqueries: Vec<Rc<DimensionSubQuery>>,
    pub source: Rc<LogicalJoin>,
}

impl PrettyPrint for MeasureSubquery {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        let details_state = state.new_level();
        result.println(
            &format!(
                "primary_key_dimensions: {}",
                print_symbols(&self.primary_keys_dimensions)
            ),
            state,
        );
        result.println(
            &format!("measures: {}", print_symbols(&self.measures)),
            state,
        );
        result.println("dimension_subqueries:", state);
        if !self.dimension_subqueries.is_empty() {
            for subquery in self.dimension_subqueries.iter() {
                subquery.pretty_print(result, &details_state);
            }
        }
        result.println("source:", state);
        self.source.pretty_print(result, &details_state);
    }
}
