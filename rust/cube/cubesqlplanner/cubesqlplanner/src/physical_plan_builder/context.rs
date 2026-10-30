use cubenativeutils::CubeError;

use crate::logical_plan::{KeyMatch, Subquery, SubqueryOptions};
use crate::physical_plan::sql_nodes::SqlNodesFactory;
use crate::physical_plan::Schema;
use crate::planner::planners::multi_stage::{EvaluationContext, TimeShiftState};
use crate::planner::{CubeId, InputId};
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Clone, Default)]
pub(super) struct PushDownBuilderContext {
    pub time_shifts: TimeShiftState,
    pub original_sql_pre_aggregations: HashMap<CubeId, String>,
    pub multi_stage_schemas: HashMap<String, Rc<Schema>>,
    // Rendered by their readers, under the readers' context.
    inline_subqueries: HashMap<InputId, Rc<Subquery>>,
    subquery_options: HashMap<InputId, SubqueryOptions>,
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

    pub fn add_subquery_options(&mut self, subquery: &Subquery) {
        self.subquery_options.insert(subquery.id, subquery.options);
    }

    pub fn key_match(&self, id: InputId) -> Result<KeyMatch, CubeError> {
        self.subquery_options
            .get(&id)
            .map(|options| options.key_match)
            .ok_or_else(|| CubeError::internal(format!("Subquery {} is not defined", id)))
    }

    pub fn add_inline_subquery(&mut self, subquery: Rc<Subquery>) {
        self.inline_subqueries.insert(subquery.id, subquery);
    }

    pub fn is_inline_subquery(&self, id: InputId) -> bool {
        self.inline_subqueries.contains_key(&id)
    }

    pub fn get_inline_subquery(&self, id: InputId) -> Result<Rc<Subquery>, CubeError> {
        self.inline_subqueries.get(&id).cloned().ok_or_else(|| {
            CubeError::internal(format!("Subquery {} is not an inline subquery", id))
        })
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
