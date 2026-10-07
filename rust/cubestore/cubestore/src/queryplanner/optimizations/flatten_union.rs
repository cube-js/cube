use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::Transformed;
use datafusion::logical_expr::expr::WildcardOptions;
use datafusion::logical_expr::{Expr, LogicalPlan, Projection, SubqueryAlias, Union};
use datafusion::optimizer::AnalyzerRule;
use std::collections::HashSet;
use std::sync::Arc;

/// Collapses nested `UNION ALL`s into one `Union` top-down, before type coercion. DataFusion's
/// `EliminateNestedUnion` does it bottom-up after coercion and re-coerces every already flattened
/// input at each level, which is quadratic in the number of inputs. `SELECT * FROM table` inputs are
/// replaced with the table scan, so wildcard expansion and coercion have nothing to do for them.
#[derive(Debug)]
pub struct FlattenUnionRule {}

impl AnalyzerRule for FlattenUnionRule {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<LogicalPlan> {
        plan.transform_down_with_subqueries(|plan| {
            if let LogicalPlan::SubqueryAlias(alias) = &plan {
                if let LogicalPlan::Union(union) = alias.input.as_ref() {
                    return Ok(match requalify_union_inputs(alias, union)? {
                        Some(plan) => Transformed::yes(plan),
                        None => Transformed::no(plan),
                    });
                }
            }
            let LogicalPlan::Union(union) = plan else {
                return Ok(Transformed::no(plan));
            };
            if !is_aligned_union(&union) {
                return Ok(Transformed::no(LogicalPlan::Union(union)));
            }
            let mut flattened = Vec::with_capacity(union.inputs.len());
            let changed = collect_union_inputs(union.inputs, &mut flattened);
            let union = LogicalPlan::Union(Union {
                inputs: flattened,
                schema: union.schema,
            });
            Ok(if changed {
                Transformed::yes(union)
            } else {
                Transformed::no(union)
            })
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "flatten_union"
    }
}

/// Whether every input of the union has the union's column names in the union's order. Then reading
/// the union by position and by name agree, so its inputs can be flattened, reordered or narrowed
/// by position. A `UNION BY NAME` keeps its inputs in their own column order.
pub fn is_aligned_union(union: &Union) -> bool {
    union.inputs.iter().all(|input| {
        let fields = input.schema().fields();
        fields.len() == union.schema.fields().len()
            && fields
                .iter()
                .zip(union.schema.fields().iter())
                .all(|(a, b)| a.name() == b.name())
    })
}

// Table scan inputs of a union under an alias get that alias too. The alias hides their own
// qualifiers anyway, and with one qualifier `merge_schema` over the inputs, which every optimizer
// rule computes for every node, deduplicates their fields instead of growing to all fields of all
// inputs. Returns `None` if nothing changes.
fn requalify_union_inputs(
    alias: &SubqueryAlias,
    union: &Union,
) -> datafusion::common::Result<Option<LogicalPlan>> {
    if !is_aligned_union(union) {
        return Ok(None);
    }
    let mut flattened = Vec::with_capacity(union.inputs.len());
    let flattened_any = collect_union_inputs(union.inputs.clone(), &mut flattened);
    // Requalified duplicate names would make the schema invalid.
    let names = union
        .schema
        .fields()
        .iter()
        .map(|f| f.name())
        .collect::<HashSet<_>>();
    let requalify = names.len() == union.schema.fields().len()
        && flattened
            .iter()
            .any(|i| matches!(i.as_ref(), LogicalPlan::TableScan(_)));
    if !flattened_any && !requalify {
        return Ok(None);
    }
    let (inputs, schema) = if requalify {
        let inputs = flattened
            .into_iter()
            .map(|input| match input.as_ref() {
                LogicalPlan::TableScan(_) => Ok(Arc::new(LogicalPlan::SubqueryAlias(
                    SubqueryAlias::try_new(input, alias.alias.clone())?,
                ))),
                _ => Ok(input),
            })
            .collect::<datafusion::common::Result<Vec<_>>>()?;
        let schema = union
            .schema
            .as_ref()
            .clone()
            .replace_qualifier(alias.alias.clone());
        (inputs, Arc::new(schema))
    } else {
        (flattened, union.schema.clone())
    };
    let union = LogicalPlan::Union(Union { inputs, schema });
    Ok(Some(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
        Arc::new(union),
        alias.alias.clone(),
    )?)))
}

// Returns whether any input was flattened or had its `SELECT *` removed.
fn collect_union_inputs(inputs: Vec<Arc<LogicalPlan>>, onto: &mut Vec<Arc<LogicalPlan>>) -> bool {
    let mut changed = false;
    for input in inputs {
        let mut input = input;
        while let Some(i) = select_star_input(&input) {
            input = i;
            changed = true;
        }
        match input.as_ref() {
            LogicalPlan::Union(nested) if is_aligned_union(nested) => {
                collect_union_inputs(nested.inputs.clone(), onto);
                changed = true;
            }
            _ => onto.push(input),
        }
    }
    changed
}

// The input of a `SELECT * FROM input` projection that adds nothing to it.
fn select_star_input(plan: &Arc<LogicalPlan>) -> Option<Arc<LogicalPlan>> {
    let LogicalPlan::Projection(Projection {
        expr,
        input,
        schema,
        ..
    }) = plan.as_ref()
    else {
        return None;
    };
    #[allow(deprecated)]
    let [Expr::Wildcard {
        qualifier: None,
        options,
    }] = expr.as_slice()
    else {
        return None;
    };
    if **options != WildcardOptions::default() || schema != input.schema() {
        return None;
    }
    Some(input.clone())
}
