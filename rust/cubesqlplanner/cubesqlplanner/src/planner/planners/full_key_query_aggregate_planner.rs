use crate::logical_plan::*;
use crate::planner::QueryProperties;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

pub struct FullKeyAggregateQueryPlanner {
    query_properties: Rc<QueryProperties>,
}

impl FullKeyAggregateQueryPlanner {
    pub fn new(query_properties: Rc<QueryProperties>) -> Self {
        Self { query_properties }
    }

    pub fn plan_logical_source(
        &self,
        resolve_multiplied_measures: Option<Rc<ResolveMultipliedMeasures>>,
        multi_stage_subqueries: Vec<Rc<MultiStageSubqueryRef>>,
    ) -> Result<Rc<FullKeyAggregate>, CubeError> {
        let mut multi_stage_subquery_refs = Vec::new();
        for subquery in multi_stage_subqueries {
            multi_stage_subquery_refs.push(subquery);
        }
        let resolved_multiplied_source =
            resolve_multiplied_measures.map(|resolve_multiplied_measures| {
                ResolvedMultipliedMeasures::ResolveMultipliedMeasures(
                    resolve_multiplied_measures.clone(),
                )
            });
        let join_dimensions = self
            .query_properties
            .dimension_symbols()
            .iter()
            .chain(self.query_properties.time_dimension_symbols().iter())
            .cloned()
            .collect_vec();
        Ok(Rc::new(FullKeyAggregate {
            multiplied_measures_resolver: resolved_multiplied_source,
            multi_stage_subquery_refs,
            use_full_join_and_coalesce: true,
            join_dimensions,
        }))
    }

    pub fn plan_logical_plan(
        &self,
        resolve_multiplied_measures: Option<Rc<ResolveMultipliedMeasures>>,
        multi_stage_subqueries: Vec<Rc<MultiStageSubqueryRef>>,
        all_multistage_members: Vec<Rc<LogicalMultiStageMember>>,
    ) -> Result<Rc<Query>, CubeError> {
        let source =
            self.plan_logical_source(resolve_multiplied_measures, multi_stage_subqueries)?;
        let multiplied_measures = self
            .query_properties
            .full_key_aggregate_measures()?
            .rendered_as_multiplied_measures
            .clone();
        let schema = Rc::new(LogicalSchema {
            dimensions: self.query_properties.dimension_symbols(),
            measures: self.query_properties.measure_symbols(),
            time_dimensions: self.query_properties.time_dimension_symbols(),
            multiplied_measures,
        });
        let logical_filter = Rc::new(LogicalFilter {
            dimensions_filters: self.query_properties.dimensions_filters().clone(),
            time_dimensions_filters: self.query_properties.time_dimensions_filters().clone(),
            measures_filter: self.query_properties.measures_filters().clone(),
            segments: self.query_properties.segments().clone(),
        });
        let result = FullKeyAggregateQuery {
            schema,
            multistage_members: all_multistage_members,
            filter: logical_filter,
            offset: self.query_properties.offset(),
            limit: self.query_properties.row_limit(),
            ungrouped: self.query_properties.ungrouped(),
            order_by: self.query_properties.order_by().clone(),
            source,
        };
        Ok(Rc::new(Query::FullKeyAggregateQuery(result)))
    }
}
