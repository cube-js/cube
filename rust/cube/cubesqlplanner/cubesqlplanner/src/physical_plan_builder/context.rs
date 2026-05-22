use cubenativeutils::CubeError;

use crate::logical_plan::MultiStageDimensionRef;
use crate::physical_plan::sql_nodes::SqlNodesFactory;
use crate::physical_plan::Schema;
use crate::planner::planners::multi_stage::TimeShiftState;
use crate::planner::MemberSymbol;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Clone, Debug, Default)]
pub(super) struct PushDownBuilderContext {
    pub alias_prefix: Option<String>,
    pub render_measure_as_state: bool, //Render measure as state, for example hll state for count_approx
    pub render_measure_for_ungrouped: bool,
    pub time_shifts: TimeShiftState,
    pub original_sql_pre_aggregations: HashMap<String, String>,
    pub required_measures: Option<Vec<Rc<MemberSymbol>>>,
    pub dimensions_query: bool,
    pub measure_subquery: bool,
    /// Schemas of all CTEs published on the top-level Query: multi-stage
    /// member CTEs, dimension-subquery CTEs and measure-subquery CTEs share
    /// this storage. Lookup is by CTE alias / name; all three kinds are
    /// interchangeable as table references at the SQL level.
    pub cte_schemas: HashMap<String, Rc<Schema>>,
    /// MS-dim refs the current Query consumes. The source-render code
    /// reads these out to wire `OnPrimaryKeys` LEFT JOINs inside the
    /// cube-join chain (`LogicalJoin`) and `OnOuterDimensions` LEFT
    /// JOINs at the chain tail. QueryProcessor sets the list before
    /// invoking `process_node(source)`.
    pub multi_stage_dimension_refs: Vec<Rc<MultiStageDimensionRef>>,
}

impl PushDownBuilderContext {
    pub fn make_sql_nodes_factory(&self) -> Result<SqlNodesFactory, CubeError> {
        let mut factory = SqlNodesFactory::new();

        let (time_shifts, calendar_time_shifts) = self.time_shifts.extract_time_shifts()?;
        let common_time_shifts = TimeShiftState {
            dimensions_shifts: time_shifts,
        };

        factory.set_time_shifts(common_time_shifts);
        factory.set_calendar_time_shifts(calendar_time_shifts);
        factory.set_count_approx_as_state(self.render_measure_as_state);
        factory.set_ungrouped_measure(self.render_measure_for_ungrouped);
        factory.set_original_sql_pre_aggregations(self.original_sql_pre_aggregations.clone());
        Ok(factory)
    }

    pub fn add_cte_schema(&mut self, name: String, schema: Rc<Schema>) {
        self.cte_schemas.insert(name, schema);
    }

    pub fn get_cte_schema(&self, name: &str) -> Result<Rc<Schema>, CubeError> {
        if let Some(schema) = self.cte_schemas.get(name) {
            Ok(schema.clone())
        } else {
            Err(CubeError::internal(format!(
                "CTE schema for `{}` not found — caller must publish it on \
                 the top-level Query before any reference site is processed",
                name
            )))
        }
    }
}
