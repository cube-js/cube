use datafusion::common::tree_node::Transformed;
use datafusion::common::DFSchema;
use datafusion::error::DataFusionError;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{LogicalPlan, Union};
use datafusion::optimizer::optimizer::OptimizerRule;
use datafusion::optimizer::{utils, OptimizerConfig};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[derive(Debug)]
pub struct FlattenUnion;

impl OptimizerRule for FlattenUnion {
    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        match plan {
            LogicalPlan::Union(Union { ref inputs, ref schema, .. }) => {
                let new_inputs = inputs
                    .iter()
                    .map(|p| self.rewrite(p.as_ref().clone(), config))
                    .collect::<Result<Vec<_>, _>>()?;

                let result_inputs = try_remove_sub_union(&new_inputs.into_iter().map(|n| n.data).collect(), schema.clone());

                let expr = plan.expressions().clone();

                Ok(Transformed::yes(plan.with_new_exprs(expr, result_inputs)?))
            }
            // Rest: recurse into plan, apply optimization where possible
            LogicalPlan::Filter { .. }
            | LogicalPlan::Projection { .. }
            | LogicalPlan::Window { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::Repartition { .. }
            | LogicalPlan::Extension { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::Explain { .. }
            | LogicalPlan::Limit { .. }
            | LogicalPlan::Join { .. }
            | LogicalPlan::Subquery(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Statement(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Distinct(_)
            | LogicalPlan::Prepare(_)
            // | LogicalPlan::Execute(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::DescribeTable(_)
            | LogicalPlan::Unnest(_)
            | LogicalPlan::RecursiveQuery(_)
            | LogicalPlan::CrossJoin(_)
            => {
                // apply the optimization to all inputs of the plan
                let inputs = plan.inputs();
                let new_inputs = inputs
                    .iter()
                    .map(|p| self.rewrite((*p).clone(), config))
                    .collect::<Result<Vec<_>, _>>()?;

                let expr = plan.expressions().clone();

                Ok(Transformed::yes(plan.with_new_exprs(expr, new_inputs.into_iter().map(|n| n.data).collect())?))
            }
            LogicalPlan::TableScan { .. } | LogicalPlan::EmptyRelation { .. } => Ok(Transformed::no(plan.clone())),
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
            LogicalPlan::Union(Union { inputs, schema, .. }) => {
                if schema.as_arrow() == parent_schema.as_arrow() {
                    result.extend(inputs.iter().map(|i| i.as_ref().clone()));
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
