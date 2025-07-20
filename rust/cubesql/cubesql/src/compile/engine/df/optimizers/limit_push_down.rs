use std::{cmp::min, sync::Arc};

use datafusion::{
    error::Result,
    logical_plan::{
        plan::{
            Aggregate, CrossJoin, Distinct, Join, Limit, Projection, Sort, Subquery, Union, Window,
        },
        Filter, LogicalPlan,
    },
    optimizer::optimizer::{OptimizerConfig, OptimizerRule},
};

use super::utils::{is_plan_yielding_one_row, plan_has_projections};

/// Limit Push Down optimizer rule pushes LIMIT/OFFSET clauses  down the plan,
/// all the way to the Projection closest to TableScan.
/// This is beneficial for CubeScans when some of the Projections on the way
/// contain post-processing operations and cannot be pushed down.
#[derive(Default)]
pub struct LimitPushDown {}

impl LimitPushDown {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for LimitPushDown {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &OptimizerConfig,
    ) -> Result<LogicalPlan> {
        limit_push_down(self, plan, None, None, optimizer_config)
    }

    fn name(&self) -> &str {
        "__cube__limit_push_down"
    }
}

/// Recursively optimizes plan, collecting limit clauses that can possibly be pushed down.
/// Some limit clauses may be combined; those are handled accordingly.
fn limit_push_down(
    optimizer: &LimitPushDown,
    plan: &LogicalPlan,
    skip: Option<usize>,
    fetch: Option<usize>,
    optimizer_config: &OptimizerConfig,
) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Projection(Projection {
            expr,
            input,
            schema,
            alias,
        }) => {
            // Limit can be pushed down Projections with no restrictions, unless
            // it's the Projection closest to TableScan.
            if plan_has_projections(input) {
                return Ok(LogicalPlan::Projection(Projection {
                    expr: expr.clone(),
                    input: Arc::new(limit_push_down(
                        optimizer,
                        input,
                        skip,
                        fetch,
                        optimizer_config,
                    )?),
                    schema: schema.clone(),
                    alias: alias.clone(),
                }));
            }

            issue_limit(
                skip,
                fetch,
                LogicalPlan::Projection(Projection {
                    expr: expr.clone(),
                    input: Arc::new(limit_push_down(
                        optimizer,
                        input,
                        None,
                        None,
                        optimizer_config,
                    )?),
                    schema: schema.clone(),
                    alias: alias.clone(),
                }),
            )
        }
        LogicalPlan::Filter(Filter { predicate, input }) => {
            // Pushing Limit down Filter will affect results; issue limit and continue
            // down the plan.
            issue_limit(
                skip,
                fetch,
                LogicalPlan::Filter(Filter {
                    predicate: predicate.clone(),
                    input: Arc::new(limit_push_down(
                        optimizer,
                        input,
                        None,
                        None,
                        optimizer_config,
                    )?),
                }),
            )
        }
        LogicalPlan::Window(Window {
            input,
            window_expr,
            schema,
        }) => {
            // Pushing Limit down Window will affect results; issue limit and continue
            // down the plan.
            issue_limit(
                skip,
                fetch,
                LogicalPlan::Window(Window {
                    input: Arc::new(limit_push_down(
                        optimizer,
                        input,
                        None,
                        None,
                        optimizer_config,
                    )?),
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
            // Pushing Limit down Aggregate will affect results.
            issue_limit(
                skip,
                fetch,
                LogicalPlan::Aggregate(Aggregate {
                    input: Arc::new(limit_push_down(
                        optimizer,
                        input,
                        None,
                        None,
                        optimizer_config,
                    )?),
                    group_expr: group_expr.clone(),
                    aggr_expr: aggr_expr.clone(),
                    schema: schema.clone(),
                }),
            )
        }
        LogicalPlan::Sort(Sort { expr, input }) => {
            // Sort must run before Limit.
            issue_limit(
                skip,
                fetch,
                LogicalPlan::Sort(Sort {
                    expr: expr.clone(),
                    input: Arc::new(limit_push_down(
                        optimizer,
                        input,
                        None,
                        None,
                        optimizer_config,
                    )?),
                }),
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
            // TODO: It is unsafe to push LIMIT down most JOIN clauses.
            // Optimize only the plans for the time being.
            issue_limit(
                skip,
                fetch,
                LogicalPlan::Join(Join {
                    left: Arc::new(limit_push_down(
                        optimizer,
                        left,
                        None,
                        None,
                        optimizer_config,
                    )?),
                    right: Arc::new(limit_push_down(
                        optimizer,
                        right,
                        None,
                        None,
                        optimizer_config,
                    )?),
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
            // There is one case where it's allowed to push LIMIT down a CROSS JOIN side;
            // If one of the sides is guaranteed to always produce one row (aggregates
            // with no `group_expr`), the other side will yield the exact number of rows
            // CROSS JOIN would. Taking into consideration that Cube's joins are LEFT JOINs,
            // this is only safe to do with left side of a CROSS JOIN.
            if is_plan_yielding_one_row(right) {
                return Ok(LogicalPlan::CrossJoin(CrossJoin {
                    left: Arc::new(limit_push_down(
                        optimizer,
                        left,
                        skip,
                        fetch,
                        optimizer_config,
                    )?),
                    right: Arc::new(limit_push_down(
                        optimizer,
                        right,
                        None,
                        None,
                        optimizer_config,
                    )?),
                    schema: schema.clone(),
                }));
            }

            issue_limit(
                skip,
                fetch,
                LogicalPlan::CrossJoin(CrossJoin {
                    left: Arc::new(limit_push_down(
                        optimizer,
                        left,
                        None,
                        None,
                        optimizer_config,
                    )?),
                    right: Arc::new(limit_push_down(
                        optimizer,
                        right,
                        None,
                        None,
                        optimizer_config,
                    )?),
                    schema: schema.clone(),
                }),
            )
        }
        LogicalPlan::Union(Union {
            inputs,
            schema,
            alias,
        }) => {
            // TODO: push Limit down Union?
            issue_limit(
                skip,
                fetch,
                LogicalPlan::Union(Union {
                    inputs: inputs
                        .iter()
                        .map(|input| {
                            limit_push_down(optimizer, input, None, None, optimizer_config)
                        })
                        .collect::<Result<_>>()?,
                    schema: schema.clone(),
                    alias: alias.clone(),
                }),
            )
        }
        plan @ LogicalPlan::TableScan(_) | plan @ LogicalPlan::EmptyRelation(_) => {
            // TableScan or EmptyRelation's as far as we can push our limit.
            issue_limit(skip, fetch, plan.clone())
        }
        LogicalPlan::Limit(limit) => {
            // Consume the Limit from the plan.
            // Depending on the situation, limit clauses may be combined.
            match (skip, fetch) {
                (None, None) => limit_push_down(
                    optimizer,
                    &limit.input,
                    limit.skip,
                    limit.fetch,
                    optimizer_config,
                ),
                (None, Some(fetch)) => {
                    // `limit`s can be combined by taking min value
                    limit_push_down(
                        optimizer,
                        &limit.input,
                        limit.skip,
                        Some(min(fetch, limit.fetch.unwrap_or(fetch))),
                        optimizer_config,
                    )
                }
                (Some(skip), _) => {
                    if limit.fetch.is_some() {
                        // `skip` can't be added to LIMIT with `fetch`, as this will remove rows
                        issue_limit(
                            Some(skip),
                            fetch,
                            limit_push_down(
                                optimizer,
                                &limit.input,
                                limit.skip,
                                limit.fetch,
                                optimizer_config,
                            )?,
                        )
                    } else {
                        // `skip`s can be added together when no `fetch` is present
                        limit_push_down(
                            optimizer,
                            &limit.input,
                            Some(skip + limit.skip.unwrap_or(0)),
                            fetch,
                            optimizer_config,
                        )
                    }
                }
            }
        }
        LogicalPlan::Subquery(Subquery {
            subqueries,
            input,
            schema,
            types,
        }) => {
            // TODO: Pushing Limit down Subquery?
            issue_limit(
                skip,
                fetch,
                LogicalPlan::Subquery(Subquery {
                    subqueries: subqueries
                        .iter()
                        .map(|subquery| {
                            limit_push_down(optimizer, subquery, None, None, optimizer_config)
                        })
                        .collect::<Result<_>>()?,
                    input: Arc::new(limit_push_down(
                        optimizer,
                        input,
                        None,
                        None,
                        optimizer_config,
                    )?),
                    schema: schema.clone(),
                    types: types.clone(),
                }),
            )
        }
        LogicalPlan::Distinct(Distinct { input }) => {
            // Distinct itself removes rows, so pushing Limit down the plan isn't possible.
            issue_limit(
                skip,
                fetch,
                LogicalPlan::Distinct(Distinct {
                    input: Arc::new(limit_push_down(
                        optimizer,
                        input,
                        None,
                        None,
                        optimizer_config,
                    )?),
                }),
            )
        }
        other => {
            // The rest of the plans have no inputs to optimize, can't have limit expressions
            // be pushed down them, or it makes no sense to optimize them.
            issue_limit(skip, fetch, other.clone())
        }
    }
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

#[cfg(test)]
mod tests {
    use super::{
        super::utils::{make_sample_table, sample_table},
        *,
    };
    use datafusion::logical_plan::{col, count, lit, Expr, LogicalPlanBuilder};

    fn optimize(plan: &LogicalPlan) -> Result<LogicalPlan> {
        let rule = LimitPushDown::new();
        rule.optimize(plan, &OptimizerConfig::new())
    }

    fn assert_optimized_plan_eq(plan: LogicalPlan, expected: &str) {
        let optimized_plan = optimize(&plan).expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
    }

    #[test]
    fn test_limit_down_projection() -> Result<()> {
        let plan = LogicalPlanBuilder::from(sample_table()?)
            .project(vec![col("c1"), col("c2"), col("c3")])?
            .project_with_alias(
                vec![col("c1").alias("n1"), col("c2"), col("c3").alias("n2")],
                Some("t2".to_string()),
            )?
            .limit(Some(5), Some(10))?
            .build()?;

        let expected = "\
              Projection: #t1.c1 AS n1, #t1.c2, #t1.c3 AS n2, alias=t2\
            \n  Limit: skip=5, fetch=10\
            \n    Projection: #t1.c1, #t1.c2, #t1.c3\
            \n      TableScan: t1 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);
        Ok(())
    }

    #[test]
    fn test_limit_down_cross_join_right_one_row() -> Result<()> {
        let plan = LogicalPlanBuilder::from(
            LogicalPlanBuilder::from(make_sample_table("j1", vec!["c1"], vec![])?)
                .project(vec![col("c1")])?
                .build()?,
        )
        .cross_join(
            &LogicalPlanBuilder::from(make_sample_table("j2", vec!["c2"], vec![])?)
                .project(vec![col("c2")])?
                .aggregate(vec![] as Vec<Expr>, vec![count(lit(1u8))])?
                .project_with_alias(
                    vec![col("COUNT(UInt8(1))").alias("c2")],
                    Some("j2".to_string()),
                )?
                .build()?,
        )?
        .limit(None, Some(10))?
        .build()?;

        let expected = "\
              CrossJoin:\
            \n  Limit: skip=None, fetch=10\
            \n    Projection: #j1.c1\
            \n      TableScan: j1 projection=None\
            \n  Projection: #COUNT(UInt8(1)) AS c2, alias=j2\
            \n    Aggregate: groupBy=[[]], aggr=[[COUNT(UInt8(1))]]\
            \n      Projection: #j2.c2\
            \n        TableScan: j2 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);
        Ok(())
    }

    #[test]
    fn test_limit_down_limit() -> Result<()> {
        // OFFSET then OFFSET
        let plan = LogicalPlanBuilder::from(sample_table()?)
            .project(vec![col("c1")])?
            .limit(Some(5), None)?
            .project(vec![col("c1")])?
            .limit(Some(5), None)?
            .build()?;

        let expected = "\
              Projection: #t1.c1\
            \n  Limit: skip=10, fetch=None\
            \n    Projection: #t1.c1\
            \n      TableScan: t1 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);

        // LIMIT then OFFSET
        let plan = LogicalPlanBuilder::from(sample_table()?)
            .project(vec![col("c1")])?
            .limit(None, Some(5))?
            .project(vec![col("c1")])?
            .limit(Some(5), None)?
            .build()?;

        let expected = "\
              Projection: #t1.c1\
            \n  Limit: skip=5, fetch=None\
            \n    Limit: skip=None, fetch=5\
            \n      Projection: #t1.c1\
            \n        TableScan: t1 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);

        // LIMIT OFFSET then LIMIT
        let plan = LogicalPlanBuilder::from(sample_table()?)
            .project(vec![col("c1")])?
            .limit(Some(10), Some(15))?
            .project(vec![col("c1")])?
            .limit(None, Some(5))?
            .build()?;

        let expected = "\
              Projection: #t1.c1\
            \n  Limit: skip=10, fetch=5\
            \n    Projection: #t1.c1\
            \n      TableScan: t1 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);

        Ok(())
    }
}
