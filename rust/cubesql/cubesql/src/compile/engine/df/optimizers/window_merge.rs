use std::sync::Arc;

use datafusion::{
    error::Result,
    logical_plan::{
        plan::{
            Aggregate, CrossJoin, Distinct, Join, Limit, Projection, Repartition, Sort, Subquery,
            Union, Window,
        },
        DFSchemaRef, Expr, Filter, LogicalPlan,
    },
    optimizer::optimizer::{OptimizerConfig, OptimizerRule},
};

/// Window Merge optimizer rule merges WindowAggr plans that are on top of each other
#[derive(Default)]
pub struct WindowMerge {}

impl WindowMerge {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for WindowMerge {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &OptimizerConfig,
    ) -> Result<LogicalPlan> {
        window_merge(self, plan, None, optimizer_config)
    }

    fn name(&self) -> &str {
        "__cube__window_merge"
    }
}

/// Recursively optimizes plan, collecting window expressions that can possibly be pushed down.
fn window_merge(
    optimizer: &WindowMerge,
    plan: &LogicalPlan,
    window_expr_with_schema: Option<(Vec<Expr>, DFSchemaRef)>,
    optimizer_config: &OptimizerConfig,
) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Projection(Projection {
            expr,
            input,
            schema,
            alias,
        }) => issue_window(
            window_expr_with_schema,
            LogicalPlan::Projection(Projection {
                expr: expr.clone(),
                input: Arc::new(window_merge(optimizer, input, None, optimizer_config)?),
                schema: Arc::clone(schema),
                alias: alias.clone(),
            }),
        ),
        LogicalPlan::Filter(Filter { predicate, input }) => issue_window(
            window_expr_with_schema,
            LogicalPlan::Filter(Filter {
                predicate: predicate.clone(),
                input: Arc::new(window_merge(optimizer, input, None, optimizer_config)?),
            }),
        ),
        LogicalPlan::Window(Window {
            input,
            window_expr,
            schema,
        }) => {
            // Collect the expressions. We always use the topmost schema, and expressions down the plan
            // go first
            let new_window_expr_with_schema = window_expr_with_schema.map_or_else(
                || (window_expr.clone(), Arc::clone(schema)),
                |(top_window_expr, top_schema)| {
                    (
                        window_expr
                            .iter()
                            .cloned()
                            .chain(top_window_expr.into_iter())
                            .collect(),
                        top_schema,
                    )
                },
            );
            window_merge(
                optimizer,
                input,
                Some(new_window_expr_with_schema),
                optimizer_config,
            )
        }
        LogicalPlan::Aggregate(Aggregate {
            input,
            group_expr,
            aggr_expr,
            schema,
        }) => issue_window(
            window_expr_with_schema,
            LogicalPlan::Aggregate(Aggregate {
                input: Arc::new(window_merge(optimizer, input, None, optimizer_config)?),
                group_expr: group_expr.clone(),
                aggr_expr: aggr_expr.clone(),
                schema: Arc::clone(schema),
            }),
        ),
        LogicalPlan::Sort(Sort { expr, input }) => issue_window(
            window_expr_with_schema,
            LogicalPlan::Sort(Sort {
                expr: expr.clone(),
                input: Arc::new(window_merge(optimizer, input, None, optimizer_config)?),
            }),
        ),
        LogicalPlan::Join(Join {
            left,
            right,
            on,
            join_type,
            join_constraint,
            schema,
            null_equals_null,
        }) => issue_window(
            window_expr_with_schema,
            LogicalPlan::Join(Join {
                left: Arc::new(window_merge(optimizer, left, None, optimizer_config)?),
                right: Arc::new(window_merge(optimizer, right, None, optimizer_config)?),
                on: on.clone(),
                join_type: *join_type,
                join_constraint: *join_constraint,
                schema: Arc::clone(schema),
                null_equals_null: *null_equals_null,
            }),
        ),
        LogicalPlan::CrossJoin(CrossJoin {
            left,
            right,
            schema,
        }) => issue_window(
            window_expr_with_schema,
            LogicalPlan::CrossJoin(CrossJoin {
                left: Arc::new(window_merge(optimizer, left, None, optimizer_config)?),
                right: Arc::new(window_merge(optimizer, right, None, optimizer_config)?),
                schema: Arc::clone(schema),
            }),
        ),
        LogicalPlan::Repartition(Repartition {
            input,
            partitioning_scheme,
        }) => issue_window(
            window_expr_with_schema,
            LogicalPlan::Repartition(Repartition {
                input: Arc::new(window_merge(optimizer, input, None, optimizer_config)?),
                partitioning_scheme: partitioning_scheme.clone(),
            }),
        ),
        LogicalPlan::Union(Union {
            inputs,
            schema,
            alias,
        }) => issue_window(
            window_expr_with_schema,
            LogicalPlan::Union(Union {
                inputs: inputs
                    .iter()
                    .map(|input| window_merge(optimizer, input, None, optimizer_config))
                    .collect::<Result<_>>()?,
                schema: Arc::clone(schema),
                alias: alias.clone(),
            }),
        ),
        plan @ LogicalPlan::TableScan(_) | plan @ LogicalPlan::EmptyRelation(_) => {
            // TableScan or EmptyRelation's as far as we can push our window expression.
            issue_window(window_expr_with_schema, plan.clone())
        }
        LogicalPlan::Limit(Limit { skip, fetch, input }) => issue_window(
            window_expr_with_schema,
            LogicalPlan::Limit(Limit {
                skip: *skip,
                fetch: *fetch,
                input: Arc::new(window_merge(optimizer, input, None, optimizer_config)?),
            }),
        ),
        LogicalPlan::Subquery(Subquery {
            subqueries,
            input,
            schema,
            types,
        }) => issue_window(
            window_expr_with_schema,
            LogicalPlan::Subquery(Subquery {
                subqueries: subqueries
                    .iter()
                    .map(|subquery| window_merge(optimizer, subquery, None, optimizer_config))
                    .collect::<Result<_>>()?,
                input: Arc::new(window_merge(optimizer, input, None, optimizer_config)?),
                schema: Arc::clone(schema),
                types: types.clone(),
            }),
        ),
        LogicalPlan::Distinct(Distinct { input }) => issue_window(
            window_expr_with_schema,
            LogicalPlan::Distinct(Distinct {
                input: Arc::new(window_merge(optimizer, input, None, optimizer_config)?),
            }),
        ),
        other => issue_window(window_expr_with_schema, other.clone()),
    }
}

/// Issues a Window containing the provided input if the provided `window_expr_and_schema` is `Some`;
/// otherwise, issues the provided input instead.
fn issue_window(
    window_expr_with_schema: Option<(Vec<Expr>, DFSchemaRef)>,
    input: LogicalPlan,
) -> Result<LogicalPlan> {
    if let Some((window_expr, schema)) = window_expr_with_schema {
        return Ok(LogicalPlan::Window(Window {
            input: Arc::new(input),
            window_expr,
            schema,
        }));
    }
    Ok(input)
}

#[cfg(test)]
mod tests {
    use super::{super::utils::sample_table, *};
    use datafusion::{
        logical_expr::AggregateFunction,
        logical_plan::{col, window_frames::WindowFrame, LogicalPlanBuilder},
        physical_plan::windows::WindowFunction,
    };

    fn optimize(plan: &LogicalPlan) -> Result<LogicalPlan> {
        let rule = WindowMerge::new();
        rule.optimize(plan, &OptimizerConfig::new())
    }

    fn assert_optimized_plan_eq(plan: LogicalPlan, expected: &str) {
        let optimized_plan = optimize(&plan).expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
    }

    fn window(
        fun: WindowFunction,
        args: Vec<Expr>,
        partition_by: Vec<Expr>,
        order_by: Vec<Expr>,
        window_frame: Option<WindowFrame>,
    ) -> Expr {
        Expr::WindowFunction {
            fun,
            args,
            partition_by,
            order_by,
            window_frame,
        }
    }

    fn aggregate_function(fun: AggregateFunction, args: Vec<Expr>, distinct: bool) -> Expr {
        Expr::AggregateFunction {
            fun,
            args,
            distinct,
        }
    }

    fn sort(expr: Expr, asc: bool, nulls_first: bool) -> Expr {
        Expr::Sort {
            expr: Box::new(expr),
            asc,
            nulls_first,
        }
    }

    #[test]
    fn test_double_window_aggr() -> Result<()> {
        let plan = LogicalPlanBuilder::from(sample_table()?)
            .window(vec![window(
                WindowFunction::AggregateFunction(AggregateFunction::Sum),
                vec![aggregate_function(
                    AggregateFunction::Sum,
                    vec![col("c1")],
                    false,
                )],
                vec![col("c2")],
                vec![sort(col("c3"), true, false)],
                None,
            )
            .alias("c4")])?
            .window(vec![window(
                WindowFunction::AggregateFunction(AggregateFunction::Avg),
                vec![aggregate_function(
                    AggregateFunction::Sum,
                    vec![col("c1")],
                    false,
                )],
                vec![col("c2")],
                vec![],
                None,
            )
            .alias("c5")])?
            .project(vec![col("c4"), col("c5")])?
            .build()?;

        let expected = "\
              Projection: #c4, #c5\
            \n  WindowAggr: windowExpr=[[\
                  SUM(SUM(#t1.c1)) PARTITION BY [#t1.c2] ORDER BY [#t1.c3 ASC NULLS LAST] AS c4, \
                  AVG(SUM(#t1.c1)) PARTITION BY [#t1.c2] AS c5\
                ]]\
            \n    TableScan: t1 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);
        Ok(())
    }
}
