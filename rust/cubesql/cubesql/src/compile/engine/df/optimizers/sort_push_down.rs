use std::{collections::HashMap, sync::Arc};

use datafusion::{
    error::{DataFusionError, Result},
    logical_plan::{
        plan::{Aggregate, Distinct, Limit, Projection, Sort, Subquery, Union, Window},
        Column, DFSchema, Expr, Filter, LogicalPlan,
    },
    optimizer::optimizer::{OptimizerConfig, OptimizerRule},
};

use super::utils::{is_column_expr, plan_has_projections, rewrite};

/// Sort Push Down optimizer rule pushes ORDER BY clauses consisting of specific,
/// mostly simple, expressions down the plan, all the way to the Projection
/// closest to TableScan. This is beneficial for CubeScans when some of the Projections
/// on the way contain post-processing operations and cannot be pushed down.
#[derive(Default)]
pub struct SortPushDown {}

impl SortPushDown {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for SortPushDown {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &OptimizerConfig,
    ) -> Result<LogicalPlan> {
        sort_push_down(self, plan, None, optimizer_config)
    }

    fn name(&self) -> &str {
        "__cube__sort_push_down"
    }
}

/// Recursively optimizes plan, collecting sort expressions that can possibly be pushed down.
/// Only the topmost sort expression is kept when one pushes through another.
fn sort_push_down(
    optimizer: &SortPushDown,
    plan: &LogicalPlan,
    sort_expr: Option<Vec<Expr>>,
    optimizer_config: &OptimizerConfig,
) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Projection(Projection {
            expr,
            input,
            schema,
            alias,
        }) => {
            // Sort can be pushed down to projection, however we only map specific expressions.
            // Complex expressions can't be pushed down, so if there are any, Sort is issued
            // before the projection.
            if plan_has_projections(input) {
                if let Some(sort_expr) = &sort_expr {
                    let rewrite_map = rewrite_map_for_projection(expr, schema);
                    if let Some(new_sort_expr) = sort_expr
                        .iter()
                        .map(|expr| match expr {
                            Expr::Sort {
                                expr,
                                asc,
                                nulls_first,
                            } => Ok(if is_column_expr(expr) {
                                rewrite(expr, &rewrite_map)?.map(|expr| Expr::Sort {
                                    expr: Box::new(expr),
                                    asc: *asc,
                                    nulls_first: *nulls_first,
                                })
                            } else {
                                None
                            }),
                            _ => Err(DataFusionError::Internal(
                                "Unable to optimize plan: sort contains non-sort expressions"
                                    .to_string(),
                            )),
                        })
                        .collect::<Result<Option<_>>>()?
                    {
                        return Ok(LogicalPlan::Projection(Projection {
                            expr: expr.clone(),
                            input: Arc::new(sort_push_down(
                                optimizer,
                                input,
                                Some(new_sort_expr),
                                optimizer_config,
                            )?),
                            schema: schema.clone(),
                            alias: alias.clone(),
                        }));
                    }
                }
            }

            issue_sort(
                sort_expr,
                LogicalPlan::Projection(Projection {
                    expr: expr.clone(),
                    input: Arc::new(sort_push_down(optimizer, input, None, optimizer_config)?),
                    schema: schema.clone(),
                    alias: alias.clone(),
                }),
            )
        }
        LogicalPlan::Filter(Filter { predicate, input }) => {
            // Sort can be pushed down Filter, and while it may seem weird to do that
            // after doing the exact opposite in `FilterPushDown`, this may allow the sort
            // to push through some complex filters, ultimately reaching CubeScan.
            Ok(LogicalPlan::Filter(Filter {
                predicate: predicate.clone(),
                input: Arc::new(sort_push_down(
                    optimizer,
                    input,
                    sort_expr,
                    optimizer_config,
                )?),
            }))
        }
        LogicalPlan::Window(Window {
            input,
            window_expr,
            schema,
        }) => {
            // Sort can't be pushed down Window, but we can optimize its input.
            issue_sort(
                sort_expr,
                LogicalPlan::Window(Window {
                    input: Arc::new(sort_push_down(optimizer, input, None, optimizer_config)?),
                    window_expr: window_expr.clone(),
                    schema: schema.clone(),
                }),
            )
        }
        LogicalPlan::Aggregate(Aggregate {
            input,
            group_expr,
            aggr_expr,
            schema,
        }) => {
            // It may be unsafe to push Sort down Aggregate; optimize just the input.
            issue_sort(
                sort_expr,
                LogicalPlan::Aggregate(Aggregate {
                    input: Arc::new(sort_push_down(optimizer, input, None, optimizer_config)?),
                    group_expr: group_expr.clone(),
                    aggr_expr: aggr_expr.clone(),
                    schema: schema.clone(),
                }),
            )
        }
        LogicalPlan::Sort(Sort { expr, input }) => {
            // When encountering Sort, drop it from the plan, keeping the expr.
            // If we already have an expr, however, then there was a sort above which
            // would override this sort expression; drop the new one in such case.
            sort_push_down(
                optimizer,
                input,
                Some(sort_expr.unwrap_or(expr.clone())),
                optimizer_config,
            )
        }
        LogicalPlan::Union(Union {
            inputs,
            schema,
            alias,
        }) => {
            // Union randomizes sorting, so Sort can't be pushed down.
            issue_sort(
                sort_expr,
                LogicalPlan::Union(Union {
                    inputs: inputs
                        .iter()
                        .map(|input| sort_push_down(optimizer, input, None, optimizer_config))
                        .collect::<Result<_>>()?,
                    schema: schema.clone(),
                    alias: alias.clone(),
                }),
            )
        }
        plan @ LogicalPlan::TableScan(_) | plan @ LogicalPlan::EmptyRelation(_) => {
            // TableScan or EmptyRelation's as far as we can push our sort expression.
            issue_sort(sort_expr, plan.clone())
        }
        LogicalPlan::Limit(Limit { skip, fetch, input }) => {
            // Pushing down Sort to Limit will affect the results; issue the sort expression.
            issue_sort(
                sort_expr,
                LogicalPlan::Limit(Limit {
                    skip: *skip,
                    fetch: *fetch,
                    input: Arc::new(sort_push_down(optimizer, input, None, optimizer_config)?),
                }),
            )
        }
        LogicalPlan::Subquery(Subquery {
            subqueries,
            input,
            schema,
            types,
        }) => {
            // TODO: Pushing Sort down Subquery?
            issue_sort(
                sort_expr,
                LogicalPlan::Subquery(Subquery {
                    subqueries: subqueries
                        .iter()
                        .map(|subquery| sort_push_down(optimizer, subquery, None, optimizer_config))
                        .collect::<Result<_>>()?,
                    input: Arc::new(sort_push_down(optimizer, input, None, optimizer_config)?),
                    schema: schema.clone(),
                    types: types.clone(),
                }),
            )
        }
        LogicalPlan::Distinct(Distinct { input }) => {
            // Distinct randomizes the sorting; issue the sort expression.
            issue_sort(
                sort_expr,
                LogicalPlan::Distinct(Distinct {
                    input: Arc::new(sort_push_down(optimizer, input, None, optimizer_config)?),
                }),
            )
        }
        other => {
            // The rest of the plans have no inputs to optimize, can't have sort expressions
            // be pushed down them, or it makes no sense to optimize them.
            issue_sort(sort_expr, other.clone())
        }
    }
}

/// Generates a rewrite map for projection, taking qualified and unqualified fields into account.
/// Only simple realiasing expressions are mapped, with specific exceptions; more complex
/// projection expressions might produce complex sort expressions which cannot be pushed down to CubeScan,
/// and will block other nodes: those are mapped as `None` to explicitly mark them as non-mappable.
/// Extend this on case-by-case basis.
fn rewrite_map_for_projection(
    exprs: &Vec<Expr>,
    schema: &Arc<DFSchema>,
) -> HashMap<Column, Option<Expr>> {
    schema
        .fields()
        .iter()
        .zip(exprs)
        .flat_map(|(field, expr)| {
            // Aliases are never part of ORDER BY clause so they must be removed
            let expr = match expr {
                Expr::Alias(expr, _) => expr,
                expr @ _ => expr,
            };

            let expr = match expr {
                // We always expand simple realiasing expressions
                expr @ Expr::Column(_) => Some(expr.clone()),
                _ => None,
            };

            // Duplicate fields for projections without an alias
            // will be dropped while collecting as HashMap
            vec![
                (field.qualified_column(), expr.clone()),
                (field.unqualified_column(), expr),
            ]
        })
        .collect()
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

#[cfg(test)]
mod tests {
    use super::{
        super::utils::{make_sample_table, sample_table},
        *,
    };
    use datafusion::logical_plan::{col, JoinType, LogicalPlanBuilder};

    fn optimize(plan: &LogicalPlan) -> LogicalPlan {
        let rule = SortPushDown::new();
        rule.optimize(plan, &OptimizerConfig::new())
            .expect("failed to optimize plan")
    }

    fn sort(expr: Expr, asc: bool, nulls_first: bool) -> Expr {
        Expr::Sort {
            expr: Box::new(expr),
            asc,
            nulls_first,
        }
    }

    #[test]
    fn test_sort_down_projection() -> Result<()> {
        let plan = LogicalPlanBuilder::from(sample_table()?)
            .project(vec![col("c1"), col("c2"), col("c3")])?
            .project_with_alias(
                vec![col("c1").alias("n1"), col("c2"), col("c3").alias("n2")],
                Some("t2".to_string()),
            )?
            .sort(vec![
                sort(col("t2.c2"), true, false),
                sort(col("t2.n2"), false, true),
            ])?
            .build()?;

        insta::assert_debug_snapshot!(optimize(&plan));
        Ok(())
    }

    #[test]
    fn test_sort_down_multiple_projections() -> Result<()> {
        let plan = LogicalPlanBuilder::from(sample_table()?)
            .project(vec![col("c1"), col("c2"), col("c3")])?
            .project_with_alias(
                vec![col("c1").alias("n1"), col("c2"), col("c3").alias("n2")],
                Some("t2".to_string()),
            )?
            .project_with_alias(
                vec![col("n1").alias("n3"), col("c2").alias("n4"), col("n2")],
                Some("t3".to_string()),
            )?
            .project_with_alias(
                vec![col("n3"), col("n4"), col("n2")],
                Some("t4".to_string()),
            )?
            .sort(vec![
                sort(col("t4.n4"), true, false),
                sort(col("t4.n2"), false, true),
            ])?
            .build()?;

        insta::assert_debug_snapshot!(optimize(&plan));
        Ok(())
    }

    #[test]
    fn test_sort_down_sort() -> Result<()> {
        let plan = LogicalPlanBuilder::from(sample_table()?)
            .project(vec![col("c1"), col("c2"), col("c3")])?
            .project_with_alias(
                vec![col("c1").alias("n1"), col("c2"), col("c3").alias("n2")],
                Some("t2".to_string()),
            )?
            .sort(vec![sort(col("t2.n1"), false, false)])?
            .project_with_alias(
                vec![col("n1").alias("n3"), col("c2").alias("n4"), col("n2")],
                Some("t3".to_string()),
            )?
            .sort(vec![sort(col("t3.n2"), true, true)])?
            .project_with_alias(
                vec![col("n3"), col("n4"), col("n2")],
                Some("t4".to_string()),
            )?
            .sort(vec![
                sort(col("t4.n4"), true, false),
                sort(col("t4.n2"), false, true),
            ])?
            .build()?;

        insta::assert_debug_snapshot!(optimize(&plan));
        Ok(())
    }

    #[test]
    fn test_sort_down_join_sort_left() -> Result<()> {
        let plan = LogicalPlanBuilder::from(
            LogicalPlanBuilder::from(make_sample_table("j1", vec!["key", "c1"], vec![])?)
                .project(vec![col("key"), col("c1")])?
                .build()?,
        )
        .join(
            &LogicalPlanBuilder::from(make_sample_table("j2", vec!["key", "c2"], vec![])?)
                .project(vec![col("key"), col("c2")])?
                .build()?,
            JoinType::Inner,
            (
                vec![Column::from_name("key")],
                vec![Column::from_name("key")],
            ),
        )?
        .project(vec![col("j1.c1"), col("j2.c2")])?
        .sort(vec![sort(col("j1.c1"), true, false)])?
        .build()?;

        insta::assert_debug_snapshot!(optimize(&plan));
        Ok(())
    }

    #[test]
    fn test_sort_down_join_sort_right() -> Result<()> {
        let plan = LogicalPlanBuilder::from(
            LogicalPlanBuilder::from(make_sample_table("j1", vec!["key", "c1"], vec![])?)
                .project(vec![col("key"), col("c1")])?
                .build()?,
        )
        .join(
            &LogicalPlanBuilder::from(make_sample_table("j2", vec!["key", "c2"], vec![])?)
                .project(vec![col("key"), col("c2")])?
                .build()?,
            JoinType::Inner,
            (
                vec![Column::from_name("key")],
                vec![Column::from_name("key")],
            ),
        )?
        .project(vec![col("j1.c1"), col("j2.c2")])?
        .sort(vec![sort(col("j2.c2"), true, false)])?
        .build()?;

        insta::assert_debug_snapshot!(optimize(&plan));
        Ok(())
    }

    #[test]
    fn test_sort_down_cross_join_sort_left() -> Result<()> {
        let plan = LogicalPlanBuilder::from(
            LogicalPlanBuilder::from(make_sample_table("j1", vec!["key", "c1"], vec![])?)
                .project(vec![col("key"), col("c1")])?
                .build()?,
        )
        .cross_join(
            &LogicalPlanBuilder::from(make_sample_table("j2", vec!["key", "c2"], vec![])?)
                .project(vec![col("key"), col("c2")])?
                .build()?,
        )?
        .project(vec![col("j1.c1"), col("j2.c2")])?
        .sort(vec![sort(col("j1.c1"), true, false)])?
        .build()?;

        insta::assert_debug_snapshot!(optimize(&plan));
        Ok(())
    }

    #[test]
    fn test_sort_down_cross_join_sort_right() -> Result<()> {
        let plan = LogicalPlanBuilder::from(
            LogicalPlanBuilder::from(make_sample_table("j1", vec!["key", "c1"], vec![])?)
                .project(vec![col("key"), col("c1")])?
                .build()?,
        )
        .cross_join(
            &LogicalPlanBuilder::from(make_sample_table("j2", vec!["key", "c2"], vec![])?)
                .project(vec![col("key"), col("c2")])?
                .build()?,
        )?
        .project(vec![col("j1.c1"), col("j2.c2")])?
        .sort(vec![sort(col("j2.c2"), true, false)])?
        .build()?;

        insta::assert_debug_snapshot!(optimize(&plan));

        Ok(())
    }
}
