use super::*;
use std::rc::Rc;

pub struct MeasureSubquery {
    pub schema: Rc<LogicalSchema>,
    pub source: Rc<LogicalJoin>,
}

impl PrettyPrint for MeasureSubquery {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        let details_state = state.new_level();
        result.println("schema:", &state);
        self.schema.pretty_print(result, &details_state);
        result.println("source:", state);
        self.source.pretty_print(result, &details_state);
    }
}
