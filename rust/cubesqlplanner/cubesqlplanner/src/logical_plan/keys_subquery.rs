use super::*;
use crate::planner::sql_evaluator::MemberSymbol;
use std::rc::Rc;

#[derive(Clone)]
pub struct KeysSubQuery {
    pub key_cube_name: String,
    pub schema: Rc<LogicalSchema>,
    pub primary_keys_dimensions: Vec<Rc<MemberSymbol>>,
    pub filter: Rc<LogicalFilter>,
    pub source: Rc<LogicalJoin>,
}

impl PrettyPrint for KeysSubQuery {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("KeysSubQuery: ", state);
        let state = state.new_level();
        let details_state = state.new_level();
        result.println(&format!("-key_cube_name: {}", self.key_cube_name), &state);

        result.println("schema:", &state);
        self.schema.pretty_print(result, &details_state);
        result.println(
            &format!(
                "-primary_keys_dimensions: {}",
                print_symbols(&self.primary_keys_dimensions)
            ),
            &state,
        );
        result.println("filters:", &state);
        self.filter.pretty_print(result, &details_state);
        result.println("source:", &state);
        self.source.pretty_print(result, &details_state);
    }
}
