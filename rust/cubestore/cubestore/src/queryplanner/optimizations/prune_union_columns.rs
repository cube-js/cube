use crate::queryplanner::optimizations::flatten_union::is_aligned_union;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, Result};
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::{Expr, LogicalPlan, Projection, SubqueryAlias, TableScan, Union};
use datafusion::optimizer::AnalyzerRule;
use std::collections::HashSet;
use std::sync::Arc;

/// Narrows the inputs of a `UNION ALL` subquery to the columns read by the enclosing projection or
/// aggregate, so type coercion and the optimizer do not process unused columns of every input.
/// Runs after wildcard expansion and before type coercion. DataFusion's `OptimizeProjections`
/// does the same, but only at the end of the optimizer.
#[derive(Debug)]
pub struct PruneUnionColumnsRule {}

impl AnalyzerRule for PruneUnionColumnsRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_down_with_subqueries(|plan| {
            if !matches!(plan, LogicalPlan::Projection(_) | LogicalPlan::Aggregate(_))
                || !reaches_union(&plan)
            {
                return Ok(Transformed::no(plan));
            }
            let mut required = HashSet::new();
            Ok(match prune_chain(&plan, &mut required)? {
                Some(plan) => Transformed::yes(plan),
                None => Transformed::no(plan),
            })
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "prune_union_columns"
    }
}

// Walks down through nodes that pass their input columns through, collecting the column names
// they read, until it reaches a subquery alias over a union. Returns `None` if the chain has any
// other shape or nothing can be pruned.
fn chain_input(node: &LogicalPlan) -> Option<&Arc<LogicalPlan>> {
    match node {
        LogicalPlan::Projection(p) => Some(&p.input),
        LogicalPlan::Aggregate(a) => Some(&a.input),
        LogicalPlan::Filter(f) => Some(&f.input),
        LogicalPlan::Sort(s) => Some(&s.input),
        LogicalPlan::Limit(l) => Some(&l.input),
        _ => None,
    }
}

fn reaches_union(node: &LogicalPlan) -> bool {
    let mut node = node;
    while let Some(input) = chain_input(node) {
        match input.as_ref() {
            LogicalPlan::Filter(_) | LogicalPlan::Sort(_) | LogicalPlan::Limit(_) => node = input,
            LogicalPlan::SubqueryAlias(alias) => {
                return matches!(alias.input.as_ref(), LogicalPlan::Union(_))
            }
            _ => return false,
        }
    }
    false
}

fn prune_chain(node: &LogicalPlan, required: &mut HashSet<String>) -> Result<Option<LogicalPlan>> {
    if !collect_columns(node, required)? {
        return Ok(None);
    }
    let Some(input) = chain_input(node) else {
        return Ok(None);
    };
    let new_input = match input.as_ref() {
        LogicalPlan::Filter(_) | LogicalPlan::Sort(_) | LogicalPlan::Limit(_) => {
            prune_chain(input, required)?
        }
        LogicalPlan::SubqueryAlias(alias) => prune_union(alias, required)?,
        _ => None,
    };
    new_input
        .map(|new_input| node.with_new_exprs(node.expressions(), vec![new_input]))
        .transpose()
}

fn collect_columns(node: &LogicalPlan, required: &mut HashSet<String>) -> Result<bool> {
    let mut supported = true;
    node.apply_expressions(|e| {
        e.apply(|e| {
            #[allow(deprecated)]
            match e {
                Expr::AggregateFunction(f) if is_count_star(f) => {
                    return Ok(TreeNodeRecursion::Jump);
                }
                Expr::Wildcard { .. }
                | Expr::OuterReferenceColumn(..)
                | Expr::Exists(_)
                | Expr::InSubquery(_)
                | Expr::ScalarSubquery(_) => {
                    supported = false;
                    return Ok(TreeNodeRecursion::Stop);
                }
                Expr::Column(c) => {
                    required.insert(c.name.clone());
                }
                _ => {}
            }
            Ok(TreeNodeRecursion::Continue)
        })
    })?;
    Ok(supported)
}

#[allow(deprecated)]
fn is_count_star(f: &AggregateFunction) -> bool {
    f.func.name() == "count"
        && f.params.filter.is_none()
        && f.params.order_by.is_none()
        && matches!(
            f.params.args.as_slice(),
            [Expr::Wildcard {
                qualifier: None,
                ..
            }]
        )
}

fn prune_union(alias: &SubqueryAlias, required: &HashSet<String>) -> Result<Option<LogicalPlan>> {
    let LogicalPlan::Union(union) = alias.input.as_ref() else {
        return Ok(None);
    };
    if !is_aligned_union(union) {
        return Ok(None);
    }
    let Union { inputs, schema } = union;
    let mut keep = alias
        .schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| required.contains(f.name()))
        .map(|(i, _)| i)
        .collect::<Vec<_>>();
    if keep.len() == alias.schema.fields().len() {
        return Ok(None);
    }
    // Keep one column so the union still produces the right number of rows, e.g. for `count(*)`.
    if keep.is_empty() {
        keep.push(0);
    }
    let inputs = inputs
        .iter()
        .map(|i| narrow(i, &keep).map(Arc::new))
        .collect::<Result<Vec<_>>>()?;
    let schema = DFSchema::new_with_metadata(
        keep.iter()
            .map(|&i| {
                let (qualifier, field) = schema.qualified_field(i);
                (qualifier.cloned(), Arc::new(field.clone()))
            })
            .collect(),
        schema.metadata().clone(),
    )?;
    let union = LogicalPlan::Union(Union {
        inputs,
        schema: Arc::new(schema),
    });
    Ok(Some(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
        Arc::new(union),
        alias.alias.clone(),
    )?)))
}

fn narrow(input: &Arc<LogicalPlan>, keep: &[usize]) -> Result<LogicalPlan> {
    let (exprs, input) = match input.as_ref() {
        LogicalPlan::SubqueryAlias(a) => {
            return Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                Arc::new(narrow(&a.input, keep)?),
                a.alias.clone(),
            )?));
        }
        LogicalPlan::TableScan(scan) if scan.filters.is_empty() => {
            let projection = keep
                .iter()
                .map(|&i| scan.projection.as_ref().map_or(i, |p| p[i]))
                .collect();
            return Ok(LogicalPlan::TableScan(TableScan::try_new(
                scan.table_name.clone(),
                scan.source.clone(),
                Some(projection),
                Vec::new(),
                scan.fetch,
            )?));
        }
        LogicalPlan::Projection(p) if p.expr.len() == p.schema.fields().len() => (
            keep.iter().map(|&i| p.expr[i].clone()).collect(),
            p.input.clone(),
        ),
        _ => (
            keep.iter()
                .map(|&i| Expr::Column(Column::from(input.schema().qualified_field(i))))
                .collect(),
            input.clone(),
        ),
    };
    Ok(LogicalPlan::Projection(Projection::try_new(exprs, input)?))
}
