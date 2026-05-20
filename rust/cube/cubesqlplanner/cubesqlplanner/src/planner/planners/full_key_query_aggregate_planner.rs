use crate::logical_plan::*;
use crate::planner::collectors::collect_sub_query_dimensions;
use crate::planner::planners::multi_stage::CteState;
use crate::planner::planners::DimensionSubqueryPlanner;
use crate::planner::query_tools::QueryTools;
use crate::planner::{MemberSymbol, QueryProperties};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

/// Final assembly step for non-simple queries: takes the collected
/// multi-stage / multiplied subquery refs and members and wraps
/// them in a `Query` whose source is a `FullKeyAggregate`.
pub struct FullKeyAggregateQueryPlanner {
    query_tools: Rc<QueryTools>,
    query_properties: Rc<QueryProperties>,
}

impl FullKeyAggregateQueryPlanner {
    pub fn new(query_tools: Rc<QueryTools>, query_properties: Rc<QueryProperties>) -> Self {
        Self {
            query_tools,
            query_properties,
        }
    }

    /// Builds the `FullKeyAggregate` source from the collected
    /// multi-stage subquery refs. Always renders as FULL OUTER JOIN
    /// + COALESCE.
    pub fn plan_logical_source(
        &self,
        multi_stage_subqueries: Vec<Rc<MultiStageSubqueryRef>>,
    ) -> Result<Rc<FullKeyAggregate>, CubeError> {
        let schema = LogicalSchema::default()
            .set_dimensions(self.query_properties.dimensions().clone())
            .set_time_dimensions(self.query_properties.time_dimensions().clone())
            .into_rc();
        Ok(Rc::new(
            FullKeyAggregate::builder()
                .data_inputs(multi_stage_subqueries)
                .schema(schema)
                .build(),
        ))
    }

    /// Wraps `plan_logical_source` in a `Query` with the request's
    /// filters, modifiers and multi-stage members. Also plans any
    /// multi-stage dim / sub-query dim refs the outer Query consumes
    /// — for pure-dim queries this is the only place DSQ planner runs.
    pub fn plan_logical_plan(
        &self,
        multi_stage_subqueries: Vec<Rc<MultiStageSubqueryRef>>,
        cte_state: &mut CteState,
    ) -> Result<Rc<Query>, CubeError> {
        let source = self.plan_logical_source(multi_stage_subqueries)?;
        let source = source.into();

        let multiplied_measures = self
            .query_properties
            .full_key_aggregate_measures()?
            .rendered_as_multiplied_measures
            .clone();
        let schema = LogicalSchema::default()
            .set_dimensions(self.query_properties.dimensions().clone())
            .set_time_dimensions(self.query_properties.time_dimensions().clone())
            .set_measures(self.query_properties.measures().clone())
            .set_multiplied_measures(multiplied_measures)
            .into_rc();

        let multi_stage_dimensions = self.collect_and_plan_multi_stage_dimensions(cte_state)?;

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
                order_by: self.query_properties.order_by().to_vec(),
                time_shifts: self.query_properties.time_shifts().clone(),
                ..Default::default()
            }))
            .source(source)
            .multi_stage_dimensions(multi_stage_dimensions)
            .kind(QueryKind::TopLevelOverCtes)
            .build();
        Ok(Rc::new(result))
    }

    fn collect_and_plan_multi_stage_dimensions(
        &self,
        cte_state: &mut CteState,
    ) -> Result<Vec<Rc<MultiStageDimensionRef>>, CubeError> {
        let mut sub_query_dimensions: Vec<Rc<MemberSymbol>> = vec![];
        for dim in self
            .query_properties
            .dimensions()
            .iter()
            .chain(self.query_properties.time_dimensions().iter())
        {
            sub_query_dimensions.extend(collect_sub_query_dimensions(dim)?);
        }
        let sub_query_dimensions = sub_query_dimensions
            .into_iter()
            .unique_by(|m| m.full_name())
            .collect_vec();

        let dimension_subquery_planner = DimensionSubqueryPlanner::try_new(
            &sub_query_dimensions,
            self.query_tools.clone(),
            self.query_properties.clone(),
        )?;
        dimension_subquery_planner.plan_queries(&sub_query_dimensions, cte_state)
    }
}
