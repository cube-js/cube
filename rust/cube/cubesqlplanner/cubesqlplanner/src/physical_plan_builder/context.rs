use cubenativeutils::CubeError;

use crate::plan::Schema;
use crate::planner::planners::multi_stage::TimeShiftState;
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::sql_evaluator::MemberSymbol;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Clone, Debug, Default)]
pub struct MultiStageDimensionContext {
    pub name: String,
    pub schema: Rc<Schema>,
    pub join_dimensions: Vec<Rc<MemberSymbol>>,
}

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
    pub multi_stage_schemas: HashMap<String, Rc<Schema>>,
    pub multi_stage_dimension_schemas: HashMap<Vec<String>, Rc<MultiStageDimensionContext>>,
    pub multi_stage_dimensions: Vec<String>,
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

    pub fn add_multi_stage_schema(&mut self, name: String, schema: Rc<Schema>) {
        self.multi_stage_schemas.insert(name, schema);
    }

    pub fn remove_multi_stage_dimensions(&mut self) {
        self.multi_stage_dimensions = Vec::new();
    }

    pub fn add_multi_stage_dimension(&mut self, name: String) {
        self.multi_stage_dimensions.push(name);
    }

    pub fn get_multi_stage_dimensions(
        &self,
    ) -> Result<Option<Rc<MultiStageDimensionContext>>, CubeError> {
        if self.multi_stage_dimensions.is_empty() {
            return Ok(None);
        }
        let mut dimensions_to_resolve = self.multi_stage_dimensions.clone();
        dimensions_to_resolve.sort();
        if let Some(schema) = self
            .multi_stage_dimension_schemas
            .get(&dimensions_to_resolve)
        {
            Ok(Some(schema.clone()))
        } else {
            Err(CubeError::internal(format!(
                "Cannot find source for resolve multi stage dimensions {}",
                dimensions_to_resolve.join(", ")
            )))
        }
    }

    pub fn add_multi_stage_dimension_schema(
        &mut self,
        resolved_dimensions: Vec<String>,
        cte_name: String,
        join_dimensions: Vec<Rc<MemberSymbol>>,
        schema: Rc<Schema>,
    ) {
        self.multi_stage_dimension_schemas.insert(
            resolved_dimensions,
            Rc::new(MultiStageDimensionContext {
                name: cte_name,
                join_dimensions,
                schema,
            }),
        );
    }

    pub fn get_multi_stage_schema(&self, name: &str) -> Result<Rc<Schema>, CubeError> {
        if let Some(schema) = self.multi_stage_schemas.get(name) {
            Ok(schema.clone())
        } else {
            Err(CubeError::internal(format!(
                "Cannot find schema for multi stage cte {}",
                name
            )))
        }
    }
}
