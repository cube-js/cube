use super::{DimensionSubqueryPlanner, JoinPlanner};
use crate::logical_plan::*;
use crate::planner::collectors::collect_sub_query_dimensions_from_symbols;
use crate::planner::planners::multi_stage::CteState;
use crate::planner::query_tools::QueryTools;
use crate::planner::QueryProperties;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Plans a `Query` for the simple case: a single `LogicalJoin`
/// source, no multi-stage or multiplied CTEs. Sub-query dimensions
/// are still woven into the join.
pub struct SimpleQueryPlanner {
    query_tools: Rc<QueryTools>,
    query_properties: Rc<QueryProperties>,
    join_planner: JoinPlanner,
}
impl SimpleQueryPlanner {
    pub fn new(query_tools: Rc<QueryTools>, query_properties: Rc<QueryProperties>) -> Self {
        Self {
            join_planner: JoinPlanner::new(query_tools.clone()),
            query_properties,
            query_tools,
        }
    }

    /// Builds the `Query` for a simple-case request. Sub-query DSQ CTE
    /// bodies are pushed into the outer `cte_state`; this Query just
    /// records the resulting refs on `multi_stage_dimensions`.
    pub fn plan(&self, cte_state: &mut CteState) -> Result<Rc<Query>, CubeError> {
        let (source, multi_stage_dimensions) = self.source_and_subquery_dimensions(cte_state)?;

        let multiplied_measures = self
            .query_properties
            .full_key_aggregate_measures()?
            .rendered_as_multiplied_measures
            .clone();
        let schema = LogicalSchema::default()
            .set_dimensions(self.query_properties.dimensions().clone())
            .set_measures(self.query_properties.measures().clone())
            .set_time_dimensions(self.query_properties.time_dimensions().clone())
            .set_multiplied_measures(multiplied_measures)
            .into_rc();
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
            .source(source.into())
            .multi_stage_dimensions(multi_stage_dimensions)
            .build();
        Ok(Rc::new(result))
    }

    /// Resolves the query's join and the sub-query dimensions that
    /// plug into it, returning the assembled `LogicalJoin` source plus
    /// the `MultiStageDimensionRef`s the join consumes. DSQ CTE bodies
    /// are published into `cte_state` as a side effect.
    pub fn source_and_subquery_dimensions(
        &self,
        cte_state: &mut CteState,
    ) -> Result<(Rc<LogicalJoin>, Vec<Rc<MultiStageDimensionRef>>), CubeError> {
        let join = self.query_properties.simple_query_join()?;
        let subquery_dimensions = if let Some(join) = &join {
            collect_sub_query_dimensions_from_symbols(
                &self
                    .query_properties
                    .get_member_symbols(true, true, true, true, &vec![]),
                &self.join_planner,
                &join,
            )?
        } else {
            vec![]
        };
        let dimension_subquery_planner = DimensionSubqueryPlanner::try_new(
            &subquery_dimensions,
            self.query_tools.clone(),
            self.query_properties.clone(),
        )?;
        let multi_stage_dimensions =
            dimension_subquery_planner.plan_queries(&subquery_dimensions, cte_state)?;
        let source = if let Some(join) = &join {
            self.join_planner.make_join_logical_plan(join.clone())?
        } else {
            self.join_planner.make_empty_join_logical_plan()
        };
        Ok((source, multi_stage_dimensions))
    }
}
