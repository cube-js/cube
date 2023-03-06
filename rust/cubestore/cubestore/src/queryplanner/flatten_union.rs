use datafusion::error::DataFusionError;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_plan::{DFSchema, LogicalPlan};
use datafusion::optimizer::optimizer::OptimizerRule;
use datafusion::optimizer::utils;
use std::sync::Arc;

pub struct FlattenUnion;
impl OptimizerRule for FlattenUnion {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        execution_props: &ExecutionProps,
    ) -> Result<LogicalPlan, DataFusionError> {
        match plan {
            LogicalPlan::Union { inputs, schema, .. } => {
                let new_inputs = inputs
                    .iter()
                    .map(|p| self.optimize(p, execution_props))
                    .collect::<Result<Vec<_>, _>>()?;

                let result_inputs = try_remove_sub_union(&new_inputs, schema.clone());

                let expr = plan.expressions().clone();

                utils::from_plan(plan, &expr, &result_inputs)
            }
            // Rest: recurse into plan, apply optimization where possible
            LogicalPlan::Filter { .. }
            | LogicalPlan::Projection { .. }
            | LogicalPlan::Window { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::Repartition { .. }
            | LogicalPlan::CreateExternalTable { .. }
            | LogicalPlan::Extension { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::Explain { .. }
            | LogicalPlan::Limit { .. }
            | LogicalPlan::Skip { .. }
            | LogicalPlan::Join { .. }
            | LogicalPlan::CrossJoin { .. } => {
                // apply the optimization to all inputs of the plan
                let inputs = plan.inputs();
                let new_inputs = inputs
                    .iter()
                    .map(|p| self.optimize(p, execution_props))
                    .collect::<Result<Vec<_>, _>>()?;

                let expr = plan.expressions().clone();

                utils::from_plan(plan, &expr, &new_inputs)
            }
            LogicalPlan::TableScan { .. } | LogicalPlan::EmptyRelation { .. } => Ok(plan.clone()),
        }
    }

    fn name(&self) -> &str {
        "flatten_union"
    }
}

fn try_remove_sub_union(
    parent_inputs: &Vec<LogicalPlan>,
    parent_schema: Arc<DFSchema>,
) -> Vec<LogicalPlan> {
    let mut result = Vec::new();
    for inp in parent_inputs.iter() {
        match inp {
            LogicalPlan::Union { inputs, schema, .. } => {
                if schema.to_schema_ref() == parent_schema.to_schema_ref() {
                    result.extend(inputs.iter().cloned());
                } else {
                    return parent_inputs.clone();
                }
            }
            _ => {
                result.push(inp.clone());
            }
        }
    }
    return result;
}
