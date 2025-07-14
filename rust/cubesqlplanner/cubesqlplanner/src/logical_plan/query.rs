use super::*;
use std::rc::Rc;

#[derive(Clone)]
pub enum QuerySource {
    LogicalJoin(Rc<LogicalJoin>),
    FullKeyAggregate(Rc<FullKeyAggregate>),
    PreAggregation(Rc<PreAggregation>),
}
impl PrettyPrint for QuerySource {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        match self {
            QuerySource::LogicalJoin(join) => join.pretty_print(result, state),
            QuerySource::FullKeyAggregate(full_key) => full_key.pretty_print(result, state),
            QuerySource::PreAggregation(pre_aggregation) => {
                pre_aggregation.pretty_print(result, state)
            }
        }
    }
}
#[derive(Clone)]
pub struct Query {
    pub multistage_members: Vec<Rc<LogicalMultiStageMember>>,
    pub schema: Rc<LogicalSchema>,
    pub dimension_subqueries: Vec<Rc<DimensionSubQuery>>,
    pub filter: Rc<LogicalFilter>,
    pub modifers: Rc<LogicalQueryModifiers>,
    pub source: QuerySource,
}

impl PrettyPrint for Query {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("Query: ", state);
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
