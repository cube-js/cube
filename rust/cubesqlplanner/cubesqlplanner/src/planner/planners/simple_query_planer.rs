use super::{DimensionSubqueryPlanner, JoinPlanner};
use crate::logical_plan::*;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::collectors::collect_sub_query_dimensions_from_symbols;
use crate::planner::QueryProperties;
use cubenativeutils::CubeError;
use std::rc::Rc;

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

    pub fn plan(&self) -> Result<Rc<Query>, CubeError> {
        let (source, subquery_dimension_queries) = self.source_and_subquery_dimensions()?;

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
        let result = SimpleQuery {
            schema,
            filter: logical_filter,
            offset: self.query_properties.offset(),
            limit: self.query_properties.row_limit(),
            ungrouped: self.query_properties.ungrouped(),
            order_by: self.query_properties.order_by().clone(),
            dimension_subqueries: subquery_dimension_queries,
            source: SimpleQuerySource::LogicalJoin(source),
        };
        Ok(Rc::new(Query::SimpleQuery(result)))
    }

    pub fn source_and_subquery_dimensions(
        &self,
    ) -> Result<(Rc<LogicalJoin>, Vec<Rc<DimensionSubQuery>>), CubeError> {
        let join = self.query_properties.simple_query_join()?;
        let subquery_dimensions = collect_sub_query_dimensions_from_symbols(
            &self.query_properties.all_member_symbols(false),
            &self.join_planner,
            &join,
            self.query_tools.clone(),
        )?;
        let dimension_subquery_planner = DimensionSubqueryPlanner::try_new(
            &subquery_dimensions,
            self.query_tools.clone(),
            self.query_properties.clone(),
        )?;
        let subquery_dimension_queries =
            dimension_subquery_planner.plan_queries(&subquery_dimensions)?;
        let source = self.join_planner.make_join_logical_plan(join.clone())?;
        Ok((source, subquery_dimension_queries))
    }
}
