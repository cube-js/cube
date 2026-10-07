use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::Transformed;
use datafusion::logical_expr::{LogicalPlan, Union};
use datafusion::optimizer::AnalyzerRule;
use std::sync::Arc;

/// Collapses nested `UNION ALL`s into one `Union` top-down, before type coercion. DataFusion's
/// `EliminateNestedUnion` does it bottom-up after coercion and re-coerces every already flattened
/// input at each level, which is quadratic in the number of inputs.
#[derive(Debug)]
pub struct FlattenUnionRule {}

impl AnalyzerRule for FlattenUnionRule {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<LogicalPlan> {
        plan.transform_down_with_subqueries(|plan| {
            let LogicalPlan::Union(Union { inputs, schema }) = plan else {
                return Ok(Transformed::no(plan));
            };
            if !inputs
                .iter()
                .any(|i| matches!(i.as_ref(), LogicalPlan::Union(_)))
            {
                return Ok(Transformed::no(LogicalPlan::Union(Union {
                    inputs,
                    schema,
                })));
            }
            let mut flattened = Vec::with_capacity(inputs.len());
            collect_union_inputs(inputs, &mut flattened);
            Ok(Transformed::yes(LogicalPlan::Union(Union {
                inputs: flattened,
                schema,
            })))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "flatten_union"
    }
}

fn collect_union_inputs(inputs: Vec<Arc<LogicalPlan>>, onto: &mut Vec<Arc<LogicalPlan>>) {
    for input in inputs {
        if let LogicalPlan::Union(Union { inputs, .. }) = input.as_ref() {
            collect_union_inputs(inputs.clone(), onto);
        } else {
            onto.push(input);
        }
    }
}
