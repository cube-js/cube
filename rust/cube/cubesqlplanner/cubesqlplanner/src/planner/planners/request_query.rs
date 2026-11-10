use crate::logical_plan::*;
use crate::planner::QueryAnalysis;
use std::rc::Rc;

/// The query the request asks for: its members, filters and modifiers over a
/// source that computes them.
pub struct RequestQuery<'a> {
    analysis: &'a QueryAnalysis,
}

impl<'a> RequestQuery<'a> {
    pub fn new(analysis: &'a QueryAnalysis) -> Self {
        Self { analysis }
    }

    pub fn schema(&self) -> Rc<LogicalSchema> {
        let query = self.analysis.query();
        LogicalSchema::default()
            .set_dimensions(query.dimensions().clone())
            .set_measures(self.analysis.select_measures().to_vec())
            .set_time_dimensions(query.time_dimensions().clone())
            .into_rc()
    }

    pub fn build(&self, schema: Rc<LogicalSchema>, source: Rc<QuerySource>) -> Rc<Query> {
        let query = self.analysis.query();
        let filter = Rc::new(LogicalFilter {
            dimensions_filters: query.dimensions_filters().clone(),
            time_dimensions_filters: query.time_dimensions_filters().clone(),
            measures_filter: query.measures_filters().clone(),
            segments: query.segments().clone(),
        });
        let modifiers = Rc::new(LogicalQueryModifiers {
            offset: query.offset(),
            limit: query.row_limit(),
            ungrouped: query.ungrouped(),
            distinct: false,
            order_by: query.order_by().to_vec(),
        });
        Rc::new(
            Query::builder()
                .schema(schema)
                .filter(filter)
                .modifers(modifiers)
                .source(source)
                .build(),
        )
    }
}
