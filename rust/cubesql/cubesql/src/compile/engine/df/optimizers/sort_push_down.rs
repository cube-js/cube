use std::{collections::HashMap, sync::Arc};

use datafusion::{
    error::{DataFusionError, Result},
    logical_plan::{
        plan::{
            Aggregate, CrossJoin, Distinct, Join, Limit, Projection, Sort, Subquery, Union, Window,
        },
        Column, DFSchema, Expr, Filter, LogicalPlan,
    },
    optimizer::optimizer::{OptimizerConfig, OptimizerRule},
};

use super::utils::{get_schema_columns, is_column_expr, plan_has_projections, rewrite};

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
        LogicalPlan::Join(Join {
            left,
            right,
            on,
            join_type,
            join_constraint,
            schema,
            null_equals_null,
        }) => {
            // DataFusion preserves the sorting of the joined plans, prioritizing left side.
            // Taking this into account, we can push Sort down the left plan if Sort references
            // columns just from the left side.
            // TODO: check if this is still the case with multiple target partitions
            if let Some(some_sort_expr) = &sort_expr {
                let left_columns = get_schema_columns(left.schema());
                if some_sort_expr.iter().all(|expr| {
                    if let Expr::Sort { expr, .. } = expr {
                        if let Expr::Column(column) = expr.as_ref() {
                            return left_columns.contains(column);
                        }
                    }
                    false
                }) {
                    return Ok(LogicalPlan::Join(Join {
                        left: Arc::new(sort_push_down(
                            optimizer,
                            left,
                            sort_expr,
                            optimizer_config,
                        )?),
                        right: Arc::new(sort_push_down(optimizer, right, None, optimizer_config)?),
                        on: on.clone(),
                        join_type: *join_type,
                        join_constraint: *join_constraint,
                        schema: schema.clone(),
                        null_equals_null: *null_equals_null,
                    }));
                }
            }

            issue_sort(
                sort_expr,
                LogicalPlan::Join(Join {
                    left: Arc::new(sort_push_down(optimizer, left, None, optimizer_config)?),
                    right: Arc::new(sort_push_down(optimizer, right, None, optimizer_config)?),
                    on: on.clone(),
                    join_type: *join_type,
                    join_constraint: *join_constraint,
                    schema: schema.clone(),
                    null_equals_null: *null_equals_null,
                }),
            )
        }
        LogicalPlan::CrossJoin(CrossJoin {
            left,
            right,
            schema,
        }) => {
            // See `LogicalPlan::Join` notes above.
            if let Some(some_sort_expr) = &sort_expr {
                let left_columns = get_schema_columns(left.schema());
                if some_sort_expr.iter().all(|expr| {
                    if let Expr::Sort { expr, .. } = expr {
                        if let Expr::Column(column) = expr.as_ref() {
                            return left_columns.contains(column);
                        }
                    }
                    false
                }) {
                    return Ok(LogicalPlan::CrossJoin(CrossJoin {
                        left: Arc::new(sort_push_down(
                            optimizer,
                            left,
                            sort_expr,
                            optimizer_config,
                        )?),
                        right: Arc::new(sort_push_down(optimizer, right, None, optimizer_config)?),
                        schema: schema.clone(),
                    }));
                }
            }

            issue_sort(
                sort_expr,
                LogicalPlan::CrossJoin(CrossJoin {
                    left: Arc::new(sort_push_down(optimizer, left, None, optimizer_config)?),
                    right: Arc::new(sort_push_down(optimizer, right, None, optimizer_config)?),
                    schema: schema.clone(),
                }),
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

    fn optimize(plan: &LogicalPlan) -> Result<LogicalPlan> {
        let rule = SortPushDown::new();
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

        let expected = "\
              Projection: #t1.c1 AS n1, #t1.c2, #t1.c3 AS n2, alias=t2\
            \n  Sort: #t1.c2 ASC NULLS LAST, #t1.c3 DESC NULLS FIRST\
            \n    Projection: #t1.c1, #t1.c2, #t1.c3\
            \n      TableScan: t1 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);
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

        let expected = "\
              Projection: #t3.n3, #t3.n4, #t3.n2, alias=t4\
            \n  Projection: #t2.n1 AS n3, #t2.c2 AS n4, #t2.n2, alias=t3\
            \n    Projection: #t1.c1 AS n1, #t1.c2, #t1.c3 AS n2, alias=t2\
            \n      Sort: #t1.c2 ASC NULLS LAST, #t1.c3 DESC NULLS FIRST\
            \n        Projection: #t1.c1, #t1.c2, #t1.c3\
            \n          TableScan: t1 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);
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

        let expected = "\
              Projection: #t3.n3, #t3.n4, #t3.n2, alias=t4\
            \n  Projection: #t2.n1 AS n3, #t2.c2 AS n4, #t2.n2, alias=t3\
            \n    Projection: #t1.c1 AS n1, #t1.c2, #t1.c3 AS n2, alias=t2\
            \n      Sort: #t1.c2 ASC NULLS LAST, #t1.c3 DESC NULLS FIRST\
            \n        Projection: #t1.c1, #t1.c2, #t1.c3\
            \n          TableScan: t1 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);
        Ok(())
    }

    #[test]
    fn test_sort_down_join() -> Result<()> {
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

        let expected = "\
              Projection: #j1.c1, #j2.c2\
            \n  Inner Join: #j1.key = #j2.key\
            \n    Sort: #j1.c1 ASC NULLS LAST\
            \n      Projection: #j1.key, #j1.c1\
            \n        TableScan: j1 projection=None\
            \n    Projection: #j2.key, #j2.c2\
            \n      TableScan: j2 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);

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

        let expected = "\
              Projection: #j1.c1, #j2.c2\
            \n  Sort: #j2.c2 ASC NULLS LAST\
            \n    Inner Join: #j1.key = #j2.key\
            \n      Projection: #j1.key, #j1.c1\
            \n        TableScan: j1 projection=None\
            \n      Projection: #j2.key, #j2.c2\
            \n        TableScan: j2 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);

        Ok(())
    }

    #[test]
    fn test_sort_down_cross_join() -> Result<()> {
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

        let expected = "\
              Projection: #j1.c1, #j2.c2\
            \n  CrossJoin:\
            \n    Sort: #j1.c1 ASC NULLS LAST\
            \n      Projection: #j1.key, #j1.c1\
            \n        TableScan: j1 projection=None\
            \n    Projection: #j2.key, #j2.c2\
            \n      TableScan: j2 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);

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

        let expected = "\
              Projection: #j1.c1, #j2.c2\
            \n  Sort: #j2.c2 ASC NULLS LAST\
            \n    CrossJoin:\
            \n      Projection: #j1.key, #j1.c1\
            \n        TableScan: j1 projection=None\
            \n      Projection: #j2.key, #j2.c2\
            \n        TableScan: j2 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);

        Ok(())
    }
}
