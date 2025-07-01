use super::*;
use std::rc::Rc;

pub struct ResolveMultipliedMeasures {
    pub schema: Rc<LogicalSchema>,
    pub filter: Rc<LogicalFilter>,
    pub regular_measure_subqueries: Vec<Rc<SimpleQuery>>,
    pub aggregate_multiplied_subqueries: Vec<Rc<AggregateMultipliedSubquery>>,
}

impl PrettyPrint for ResolveMultipliedMeasures {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("ResolveMultipliedMeasures: ", state);
        let state = state.new_level();
        let details_state = state.new_level();
        result.println("schema:", &state);
        self.schema.pretty_print(result, &details_state);
        result.println("filter:", &state);
        self.filter.pretty_print(result, &details_state);
        result.println("regular_measure_subqueries:", &state);
        for subquery in self.regular_measure_subqueries.iter() {
            subquery.pretty_print(result, &details_state);
        }
        result.println("aggregate_multiplied_subqueries:", &state);
        for subquery in self.aggregate_multiplied_subqueries.iter() {
            subquery.pretty_print(result, &details_state);
        }
    }
}
