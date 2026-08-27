use super::{DimensionSubqueryPlanner, JoinPlanner};
use crate::logical_plan::*;
use crate::planner::collectors::{collect_join_hints, collect_sub_query_dimensions_from_symbols};
use crate::planner::join_hints::JoinHints;
use crate::planner::planners::multi_stage::PlanningScope;
use crate::planner::state::State;
use crate::planner::QueryProperties;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Plans a `Query` for the simple case: a single `LogicalJoin`
/// source, no multi-stage or multiplied CTEs. Sub-query dimensions
/// are still woven into the join.
pub struct SimpleQueryPlanner {
    query_tools: Rc<State>,
    query_properties: Rc<QueryProperties>,
    join_planner: JoinPlanner,
}
impl SimpleQueryPlanner {
    pub fn new(query_tools: Rc<State>, query_properties: Rc<QueryProperties>) -> Self {
        Self {
            join_planner: JoinPlanner::new(query_tools.clone()),
            query_properties,
            query_tools,
        }
    }

    /// Builds the `Query` for a simple-case request.
    pub fn plan(&self, scope: &mut PlanningScope) -> Result<Rc<Query>, CubeError> {
        let source = self.source_and_subquery_dimensions(scope)?;

        let schema = LogicalSchema::default()
            .set_dimensions(self.query_properties.dimensions().clone())
            .set_measures(self.query_properties.select_measures()?)
            .set_time_dimensions(self.query_properties.time_dimensions().clone())
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
            }))
            .source(source.into())
            .build();
        Ok(Rc::new(result))
    }

    /// Resolves the query's join and the sub-query dimensions that
    /// plug into it, returning the assembled `LogicalJoin` source.
    pub fn source_and_subquery_dimensions(
        &self,
        scope: &mut PlanningScope,
    ) -> Result<Rc<LogicalJoin>, CubeError> {
        let join = self.query_properties.simple_query_join()?;
        let subquery_dimensions = if let Some(join) = &join {
            collect_sub_query_dimensions_from_symbols(
                &self
                    .query_properties
                    .get_member_symbols(true, true, true, true, &vec![]),
                join,
            )?
        } else {
            vec![]
        };
        let dimension_subquery_planner = DimensionSubqueryPlanner::try_new(
            &subquery_dimensions,
            self.query_tools.clone(),
            self.query_properties.clone(),
        )?;
        let subquery_dimension_queries =
            dimension_subquery_planner.plan_queries(&subquery_dimensions, scope)?;
        // Query-level SQL-API sub-query joins (from `subqueryJoins`) extend the
        // FROM clause with opaque joined sub-queries.
        let subquery_joins = self.query_properties.subquery_joins();
        let source = if let Some(join) = &join {
            self.join_planner
                .make_join_logical_plan(join, subquery_dimension_queries)
        } else if !subquery_joins.is_empty() {
            // No selected member resolves the base cube, but the sub-query joins'
            // ON conditions reference it (e.g. `SELECT t.col FROM Cube JOIN (...) t
            // ON Cube.x = t.x`). Derive the join root from those ON references so
            // the base cube — and the sub-query joins below — are emitted.
            let mut hints = JoinHints::new();

            for subquery_join in subquery_joins {
                for dep in subquery_join.on_sql.get_dependencies() {
                    hints.extend(&collect_join_hints(&dep)?);
                }
            }

            if hints.is_empty() {
                return Err(CubeError::user(
                    "Sub-query join requires its ON condition to reference at least one \
                     cube member so the base table can be resolved \
                     (e.g. `{cube.field} = alias.field`)"
                        .to_string(),
                ));
            }

            self.join_planner
                .make_join_logical_plan_with_join_hints(hints, subquery_dimension_queries)?
        } else {
            self.join_planner.make_empty_join_logical_plan()
        };
        let source = if subquery_joins.is_empty() {
            source
        } else {
            source.with_subquery_joins(subquery_joins.clone())
        };
        Ok(source)
    }
}
