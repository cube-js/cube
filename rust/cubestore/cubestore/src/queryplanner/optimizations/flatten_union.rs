use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::Transformed;
use datafusion::logical_expr::{LogicalPlan, Union};
use datafusion::optimizer::AnalyzerRule;
use std::sync::Arc;

/// Collapses a tree of `UNION ALL`s into one `Union` before type coercion.
///
/// SQL planning builds `a UNION ALL b UNION ALL c ...` as a left-deep chain of binary unions.
/// DataFusion's `EliminateNestedUnion` flattens that chain bottom-up and re-coerces every
/// already flattened input at each level, which is quadratic in the number of inputs.
/// Flattening top-down while the inputs are still uncoerced is linear; type coercion then
/// unifies all inputs of the single `Union` at once.
#[derive(Debug)]
pub struct FlattenUnionRule {}

impl FlattenUnionRule {
    pub fn new() -> Self {
        Self {}
    }
}

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
        if let LogicalPlan::Union(_) = input.as_ref() {
            let LogicalPlan::Union(Union { inputs, .. }) = Arc::unwrap_or_clone(input) else {
                unreachable!()
            };
            collect_union_inputs(inputs, onto);
        } else {
            onto.push(input);
        }
    }
}
