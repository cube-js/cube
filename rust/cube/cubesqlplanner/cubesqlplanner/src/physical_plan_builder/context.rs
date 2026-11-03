use cubenativeutils::CubeError;

use crate::logical_plan::{Subquery, SubqueryRef};
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
    subqueries: HashMap<InputId, RegisteredSubquery>,
}

#[derive(Clone)]
enum RegisteredSubquery {
    Cte(Rc<Schema>),
    // Rendered by its readers, under the readers' context.
    Inline(Rc<Subquery>),
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
        factory.set_calendar_time_shifts(
            TimeShiftState {
                dimensions_shifts: extracted.calendar_interval_shifts,
            },
            extracted.calendar_shifts,
        );
        factory.set_filter_params_time_shifts(extracted.filter_params_shifts);
        factory.set_original_sql_pre_aggregations(self.original_sql_pre_aggregations.clone());
        Ok(factory)
    }

    pub fn add_cte_schema(&mut self, id: InputId, schema: Rc<Schema>) {
        self.subqueries.insert(id, RegisteredSubquery::Cte(schema));
    }

    pub fn add_inline_subquery(&mut self, subquery: Rc<Subquery>) {
        self.subqueries
            .insert(subquery.id, RegisteredSubquery::Inline(subquery));
    }

    pub fn get_inline_subquery(&self, subquery: &SubqueryRef) -> Result<Rc<Subquery>, CubeError> {
        match self.subqueries.get(&subquery.id()) {
            Some(RegisteredSubquery::Inline(body)) => Ok(body.clone()),
            _ => Err(CubeError::internal(format!(
                "Inline subquery {} {} is not defined",
                subquery.name(),
                subquery.id()
            ))),
        }
    }

    pub fn get_cte_schema(&self, subquery: &SubqueryRef) -> Result<Rc<Schema>, CubeError> {
        match self.subqueries.get(&subquery.id()) {
            Some(RegisteredSubquery::Cte(schema)) => Ok(schema.clone()),
            _ => Err(CubeError::internal(format!(
                "Cannot find schema for cte {} {}",
                subquery.name(),
                subquery.id()
            ))),
        }
    }
}
