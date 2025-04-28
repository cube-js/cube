use super::*;

pub enum Query {
    SimpleQuery(SimpleQuery),
    FullKeyAggregateQuery(FullKeyAggregateQuery),
}

impl PrettyPrint for Query {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        match self {
            Query::SimpleQuery(query) => query.pretty_print(result, state),
            Query::FullKeyAggregateQuery(query) => query.pretty_print(result, state),
        }
    }
}
