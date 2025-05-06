use super::*;

pub enum Query {
    SimpleQuery(SimpleQuery),
    FullKeyAggregateQuery(FullKeyAggregateQuery),
}

impl PrettyPrint for Query {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        match self {
            Self::SimpleQuery(query) => query.pretty_print(result, state),
            Self::FullKeyAggregateQuery(query) => query.pretty_print(result, state),
        }
    }
}
