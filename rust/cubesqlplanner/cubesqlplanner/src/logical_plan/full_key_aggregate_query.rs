use super::*;
use std::rc::Rc;

pub struct FullKeyAggregateQuery {
    pub multistage_members: Vec<Rc<LogicalMultiStageMember>>,
    pub schema: Rc<LogicalSchema>,
    pub filter: Rc<LogicalFilter>,
    pub modifers: Rc<LogicalQueryModifiers>,
    pub source: Rc<FullKeyAggregate>,
}

impl PrettyPrint for FullKeyAggregateQuery {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("FullKeyAggregateQuery: ", state);
        let state = state.new_level();
        let details_state = state.new_level();
        if !self.multistage_members.is_empty() {
            result.println("multistage_members:", &state);
            for member in self.multistage_members.iter() {
                member.pretty_print(result, &details_state);
            }
        }

        result.println("schema:", &state);
        self.schema.pretty_print(result, &details_state);
        result.println("filter:", &state);
        self.filter.pretty_print(result, &details_state);
        self.modifers.pretty_print(result, &state);
        result.println("source:", &state);
        self.source.pretty_print(result, &details_state);
    }
}
