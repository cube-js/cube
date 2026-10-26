use crate::logical_plan::*;
use crate::planner::planners::multi_stage::PlanningScope;
use crate::planner::QueryProperties;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Final assembly step for non-simple queries: takes the collected
/// multi-stage / multiplied subquery refs and members and wraps
/// them in a `Query` whose source is a `Merge`.
pub struct MergeQueryPlanner {
    query_properties: Rc<QueryProperties>,
}

impl MergeQueryPlanner {
    pub fn new(query_properties: Rc<QueryProperties>) -> Self {
        Self { query_properties }
    }

    /// Builds the `Merge` source from the collected
    /// multi-stage subquery refs. Always renders as FULL OUTER JOIN
    /// + COALESCE.
    pub fn plan_logical_source(
        &self,
        multi_stage_subqueries: Vec<Rc<SubqueryRef>>,
    ) -> Result<Rc<Merge>, CubeError> {
        let schema = LogicalSchema::default()
            .set_dimensions(self.query_properties.dimensions().clone())
            .set_time_dimensions(self.query_properties.time_dimensions().clone())
            .into_rc();
        Ok(Rc::new(
            Merge::builder()
                .subqueries(multi_stage_subqueries)
                .schema(schema)
                .build(),
        ))
    }

    /// Wraps `plan_logical_source` in a `Query` with the request's
    /// filters and modifiers.
    pub fn plan_logical_plan(
        &self,
        multi_stage_subqueries: Vec<Rc<SubqueryRef>>,
        scope: &PlanningScope,
    ) -> Result<Rc<Query>, CubeError> {
        let source = self.plan_logical_source(multi_stage_subqueries)?;

        let schema = LogicalSchema::default()
            .set_dimensions(self.query_properties.dimensions().clone())
            .set_time_dimensions(self.query_properties.time_dimensions().clone())
            .set_measures(self.query_properties.select_measures()?)
            .into_rc();
        // Over subqueries the multi-stage dimensions are read from them.
        let input = if source.is_empty() {
            scope.multi_stage_dimension_input(&schema)?
        } else {
            None
        };
        let source = match input {
            Some(input) => QuerySource::new(source).with_multi_stage_dimension(input),
            None => QuerySource::new(source),
        };

        let logical_filter = Rc::new(LogicalFilter {
            dimensions_filters: self.query_properties.dimensions_filters().clone(),
            time_dimensions_filters: self.query_properties.time_dimensions_filters().clone(),
            measures_filter: self.query_properties.measures_filters().clone(),
            segments: self.query_properties.segments().clone(),
        });
        let result = Query::builder()
            .schema(schema)
            .filter(logical_filter)
            .modifers(Rc::new(LogicalQueryModifiers {
                offset: self.query_properties.offset(),
                limit: self.query_properties.row_limit(),
                ungrouped: self.query_properties.ungrouped(),
                distinct: false,
                order_by: self.query_properties.order_by().to_vec(),
            }))
            .source(source)
            .build();
        Ok(Rc::new(result))
    }
}
