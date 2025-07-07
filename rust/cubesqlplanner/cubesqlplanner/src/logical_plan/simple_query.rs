use super::*;
use std::rc::Rc;

#[derive(Clone)]
pub enum SimpleQuerySource {
    LogicalJoin(Rc<LogicalJoin>),
    PreAggregation(Rc<PreAggregation>),
}
impl PrettyPrint for SimpleQuerySource {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        match self {
            SimpleQuerySource::LogicalJoin(join) => join.pretty_print(result, state),
            SimpleQuerySource::PreAggregation(pre_aggregation) => {
                pre_aggregation.pretty_print(result, state)
            }
        }
    }
}
#[derive(Clone)]
pub struct SimpleQuery {
    pub schema: Rc<LogicalSchema>,
    pub dimension_subqueries: Vec<Rc<DimensionSubQuery>>,
    pub filter: Rc<LogicalFilter>,
    pub modifers: Rc<LogicalQueryModifiers>,
    pub source: SimpleQuerySource,
}

impl PrettyPrint for SimpleQuery {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("RegularMeasuresQuery: ", state);
        let state = state.new_level();
        let details_state = state.new_level();
        result.println("schema:", &state);
        self.schema.pretty_print(result, &details_state);
        if !self.dimension_subqueries.is_empty() {
            result.println("dimension_subqueries:", &state);
            for subquery in self.dimension_subqueries.iter() {
                subquery.pretty_print(result, &details_state);
            }
        }
        result.println("filters:", &state);
        self.filter.pretty_print(result, &details_state);
        self.modifers.pretty_print(result, &state);

        result.println("source:", &state);
        self.source.pretty_print(result, &details_state);
    }
}
