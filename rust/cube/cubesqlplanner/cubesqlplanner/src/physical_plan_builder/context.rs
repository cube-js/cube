use cubenativeutils::CubeError;

use crate::physical_plan::sql_nodes::SqlNodesFactory;
use crate::physical_plan::Schema;
use crate::planner::planners::multi_stage::{EvaluationContext, TimeShiftState};
use crate::planner::CubeId;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Clone, Debug, Default)]
pub(super) struct PushDownBuilderContext {
    pub time_shifts: TimeShiftState,
    pub original_sql_pre_aggregations: HashMap<CubeId, String>,
    pub measure_subquery: bool,
    pub multi_stage_schemas: HashMap<String, Rc<Schema>>,
}

impl PushDownBuilderContext {
    /// The render-time part of an `EvaluationContext`. Its row-grain flag
    /// is not applied here: raw values are set on the symbols at planning.
    pub fn apply_evaluation_context(&mut self, evaluation_context: &EvaluationContext) {
        self.time_shifts = evaluation_context.time_shifts.clone();
    }

    pub fn make_sql_nodes_factory(&self) -> Result<SqlNodesFactory, CubeError> {
        let mut factory = SqlNodesFactory::new();

        let extracted = self.time_shifts.extract_time_shifts()?;
        factory.set_time_shifts(TimeShiftState {
            dimensions_shifts: extracted.interval_shifts,
        });
        factory.set_calendar_time_shifts(extracted.calendar_shifts);
        factory.set_filter_params_time_shifts(extracted.filter_params_shifts);
        factory.set_original_sql_pre_aggregations(self.original_sql_pre_aggregations.clone());
        Ok(factory)
    }

    pub fn add_multi_stage_schema(&mut self, name: String, schema: Rc<Schema>) {
        self.multi_stage_schemas.insert(name, schema);
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
