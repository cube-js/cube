use std::{collections::HashSet, sync::Arc};

use datafusion::{
    error::Result,
    logical_plan::{
        plan::{Limit, Projection, Sort, Union},
        Column, Expr, LogicalPlan,
    },
    optimizer::{
        optimizer::{OptimizerConfig, OptimizerRule},
        utils::from_plan,
    },
};

use super::utils::is_const_expr;

/// Union Sort-Limit Push Down optimizer rule duplicates the Sort and/or Limit clauses
/// sitting above a Union into each of the Union's inputs, leaving the original plan as is.
///
/// This is beneficial because the duplicated clauses inside the Union can then be pushed
/// all the way down to CubeScans by the regular Sort and Limit Push Down optimizers, while
/// the clauses kept above the Union take care of merging the partial results back together.
///
/// `SortPushDown` and `LimitPushDown` are expected to have already run, so this rule doesn't
/// do any Sort-Sort or Limit-Limit optimizations: it simply reissues every node it meets,
/// preserving the plan, and only duplicates the values into the Union's inputs. Those two
/// rules should be run *again* afterwards so the duplicated clauses get pushed down inside
/// the Union.
#[derive(Default)]
pub struct UnionSortLimitPushDown {}

impl UnionSortLimitPushDown {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for UnionSortLimitPushDown {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &OptimizerConfig,
    ) -> Result<LogicalPlan> {
        union_sort_limit_push_down(self, plan, None, None, None, optimizer_config)
    }

    fn name(&self) -> &str {
        "__cube__union_sort_limit_push_down"
    }
}

/// Recursively optimizes plan, keeping track of the closest Sort expression and Limit clause
/// above the current node so they can be duplicated into a Union below them.
fn union_sort_limit_push_down(
    optimizer: &UnionSortLimitPushDown,
    plan: &LogicalPlan,
    sort_expr: Option<Vec<Expr>>,
    skip: Option<usize>,
    fetch: Option<usize>,
    optimizer_config: &OptimizerConfig,
) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Sort(Sort { expr, input }) => {
            // Save this Sort and issue it right away, then keep recursing. The saved Limit
            // is kept as is, as Sort runs before Limit.
            Ok(LogicalPlan::Sort(Sort {
                expr: expr.clone(),
                input: Arc::new(union_sort_limit_push_down(
                    optimizer,
                    input,
                    Some(expr.clone()),
                    skip,
                    fetch,
                    optimizer_config,
                )?),
            }))
        }
        LogicalPlan::Limit(Limit { skip, fetch, input }) => {
            // Drop any saved Sort (it would run after this Limit), save this Limit and issue
            // it right away, then keep recursing.
            Ok(LogicalPlan::Limit(Limit {
                skip: *skip,
                fetch: *fetch,
                input: Arc::new(union_sort_limit_push_down(
                    optimizer,
                    input,
                    None,
                    *skip,
                    *fetch,
                    optimizer_config,
                )?),
            }))
        }
        LogicalPlan::Union(Union {
            inputs,
            schema,
            alias,
        }) => {
            // Duplicate the saved Limit and Sort, if any, into each of the Union's inputs.
            // The original Limit and Sort above the Union were already issued on the way
            // down, so they're left untouched here.
            Ok(LogicalPlan::Union(Union {
                inputs: inputs
                    .iter()
                    .map(|input| {
                        let input = union_sort_limit_push_down(
                            optimizer,
                            input,
                            None,
                            None,
                            None,
                            optimizer_config,
                        )?;
                        // Drop sort keys that are constant within this input. Ordering by a
                        // constant has no effect inside the input, while keeping it would
                        // block the duplicated Sort from being pushed down to the CubeScan
                        // (a constant can't be a native order member). The Sort kept above
                        // the Union still orders by all keys.
                        let input_sort_expr = drop_const_sort_keys(&input, &sort_expr);
                        issue_limit(skip, fetch, issue_sort(input_sort_expr, input)?)
                    })
                    .collect::<Result<_>>()?,
                schema: schema.clone(),
                alias: alias.clone(),
            }))
        }
        other => {
            // Any other node: the saved Limit and Sort were already issued above, so drop
            // them and keep recursing the node's inputs.
            let inputs = other
                .inputs()
                .into_iter()
                .map(|input| {
                    union_sort_limit_push_down(optimizer, input, None, None, None, optimizer_config)
                })
                .collect::<Result<Vec<_>>>()?;
            from_plan(other, &other.expressions(), &inputs)
        }
    }
}

/// Issues a Sort containing the provided input if the provided `sort_expr` is `Some`;
/// otherwise, issues the provided input instead.
fn issue_sort(sort_expr: Option<Vec<Expr>>, input: LogicalPlan) -> Result<LogicalPlan> {
    if let Some(sort_expr) = sort_expr {
        return Ok(LogicalPlan::Sort(Sort {
            expr: sort_expr,
            input: Arc::new(input),
        }));
    }
    Ok(input)
}

/// Issues a Limit containing the provided `skip` and `fetch` if any of those are `Some`;
/// otherwise, issues the provided input instead.
fn issue_limit(
    skip: Option<usize>,
    fetch: Option<usize>,
    input: LogicalPlan,
) -> Result<LogicalPlan> {
    if skip.is_some() || fetch.is_some() {
        return Ok(LogicalPlan::Limit(Limit {
            skip,
            fetch,
            input: Arc::new(input),
        }));
    }
    Ok(input)
}

/// Drops sort keys from the provided `sort_expr` that refer to columns which are constant in
/// the provided plan (i.e. its topmost Projection projects a constant expression for them).
/// Returns `None` when no sort keys remain (or there were none to begin with).
fn drop_const_sort_keys(plan: &LogicalPlan, sort_expr: &Option<Vec<Expr>>) -> Option<Vec<Expr>> {
    let sort_expr = sort_expr.as_ref()?;
    let const_columns = const_output_columns(plan);
    let sort_expr = sort_expr
        .iter()
        .filter(|expr| match expr {
            Expr::Sort { expr, .. } => match expr.as_ref() {
                Expr::Column(column) => !const_columns.contains(column),
                _ => true,
            },
            _ => true,
        })
        .cloned()
        .collect::<Vec<_>>();
    if sort_expr.is_empty() {
        return None;
    }
    Some(sort_expr)
}

/// Collects the output columns (both qualified and unqualified) of the plan's topmost
/// Projection whose projected expression is constant. Returns an empty set when the plan
/// isn't a Projection, in which case no sort keys will be dropped.
fn const_output_columns(plan: &LogicalPlan) -> HashSet<Column> {
    match plan {
        LogicalPlan::Projection(Projection { expr, schema, .. }) => schema
            .fields()
            .iter()
            .zip(expr)
            .filter_map(|(field, expr)| {
                // Aliases are never part of an ORDER BY clause so they must be removed.
                let expr = match expr {
                    Expr::Alias(expr, _) => expr,
                    expr => expr,
                };
                if is_const_expr(expr) {
                    Some([field.qualified_column(), field.unqualified_column()])
                } else {
                    None
                }
            })
            .flatten()
            .collect(),
        _ => HashSet::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::{super::utils::make_sample_table, *};
    use datafusion::logical_plan::{col, lit, Expr, LogicalPlanBuilder};

    fn optimize(plan: &LogicalPlan) -> Result<LogicalPlan> {
        let rule = UnionSortLimitPushDown::new();
        rule.optimize(plan, &OptimizerConfig::new())
    }

    fn assert_optimized_plan_eq(plan: LogicalPlan, expected: &str) {
        let optimized_plan = optimize(&plan).expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
    }

    fn sort(expr: Expr, asc: bool, nulls_first: bool) -> Expr {
        Expr::Sort {
            expr: Box::new(expr),
            asc,
            nulls_first,
        }
    }

    fn union_of_two() -> Result<LogicalPlan> {
        let left = LogicalPlanBuilder::from(make_sample_table("t1", vec!["c1", "c2"], vec![])?)
            .project(vec![col("c1"), col("c2")])?
            .build()?;
        let right = LogicalPlanBuilder::from(make_sample_table("t2", vec!["c1", "c2"], vec![])?)
            .project(vec![col("c1"), col("c2")])?
            .build()?;
        LogicalPlanBuilder::from(left).union(right)?.build()
    }

    #[test]
    fn test_sort_above_union() -> Result<()> {
        let plan = LogicalPlanBuilder::from(union_of_two()?)
            .sort(vec![sort(col("c1"), true, false)])?
            .build()?;

        let expected = "\
              Sort: #c1 ASC NULLS LAST\
            \n  Union\
            \n    Sort: #c1 ASC NULLS LAST\
            \n      Projection: #t1.c1, #t1.c2\
            \n        TableScan: t1 projection=None\
            \n    Sort: #c1 ASC NULLS LAST\
            \n      Projection: #t2.c1, #t2.c2\
            \n        TableScan: t2 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);
        Ok(())
    }

    #[test]
    fn test_limit_above_union() -> Result<()> {
        let plan = LogicalPlanBuilder::from(union_of_two()?)
            .limit(None, Some(10))?
            .build()?;

        let expected = "\
              Limit: skip=None, fetch=10\
            \n  Union\
            \n    Limit: skip=None, fetch=10\
            \n      Projection: #t1.c1, #t1.c2\
            \n        TableScan: t1 projection=None\
            \n    Limit: skip=None, fetch=10\
            \n      Projection: #t2.c1, #t2.c2\
            \n        TableScan: t2 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);
        Ok(())
    }

    #[test]
    fn test_sort_limit_above_union() -> Result<()> {
        let plan = LogicalPlanBuilder::from(union_of_two()?)
            .sort(vec![sort(col("c1"), true, false)])?
            .limit(None, Some(10))?
            .build()?;

        let expected = "\
              Limit: skip=None, fetch=10\
            \n  Sort: #c1 ASC NULLS LAST\
            \n    Union\
            \n      Limit: skip=None, fetch=10\
            \n        Sort: #c1 ASC NULLS LAST\
            \n          Projection: #t1.c1, #t1.c2\
            \n            TableScan: t1 projection=None\
            \n      Limit: skip=None, fetch=10\
            \n        Sort: #c1 ASC NULLS LAST\
            \n          Projection: #t2.c1, #t2.c2\
            \n            TableScan: t2 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);
        Ok(())
    }

    #[test]
    fn test_sort_limit_above_union_drops_const_keys() -> Result<()> {
        // The Union's first input projects a constant for `flag`, the second projects a
        // constant for both `flag` and `c1`. Constant sort keys must be dropped per input,
        // while the Sort above the Union keeps all keys.
        let left = LogicalPlanBuilder::from(make_sample_table("t1", vec!["c1"], vec![])?)
            .project(vec![col("c1"), lit(false).alias("flag")])?
            .build()?;
        let right = LogicalPlanBuilder::from(make_sample_table("t2", vec!["c1"], vec![])?)
            .project(vec![lit(0).alias("c1"), lit(true).alias("flag")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .union(right)?
            .sort(vec![
                sort(col("flag"), false, false),
                sort(col("c1"), true, false),
            ])?
            .limit(None, Some(10))?
            .build()?;

        let expected = "\
              Limit: skip=None, fetch=10\
            \n  Sort: #flag DESC NULLS LAST, #c1 ASC NULLS LAST\
            \n    Union\
            \n      Limit: skip=None, fetch=10\
            \n        Sort: #c1 ASC NULLS LAST\
            \n          Projection: #t1.c1, Boolean(false) AS flag\
            \n            TableScan: t1 projection=None\
            \n      Limit: skip=None, fetch=10\
            \n        Projection: Int32(0) AS c1, Boolean(true) AS flag\
            \n          TableScan: t2 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);
        Ok(())
    }

    #[test]
    fn test_no_union() -> Result<()> {
        let plan = LogicalPlanBuilder::from(make_sample_table("t1", vec!["c1", "c2"], vec![])?)
            .project(vec![col("c1"), col("c2")])?
            .sort(vec![sort(col("c1"), true, false)])?
            .limit(None, Some(10))?
            .build()?;

        let expected = "\
              Limit: skip=None, fetch=10\
            \n  Sort: #t1.c1 ASC NULLS LAST\
            \n    Projection: #t1.c1, #t1.c2\
            \n      TableScan: t1 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);
        Ok(())
    }
}
