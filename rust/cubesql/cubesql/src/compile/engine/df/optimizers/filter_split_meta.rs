use std::sync::Arc;

use datafusion::{
    error::{DataFusionError, Result},
    logical_plan::{
        plan::{
            Aggregate, CrossJoin, Distinct, Join, Limit, Projection, Repartition, Sort, Subquery,
            Union, Window,
        },
        Column, Expr, Filter, LogicalPlan, Operator,
    },
    optimizer::optimizer::{OptimizerConfig, OptimizerRule},
    physical_plan::functions::BuiltinScalarFunction,
};

/// Filter Split Meta optimizer rule splits a `WHERE` clause into two distinct filters,
/// pushing meta filters (currently only `__user`) down the plan, separate from other filters.
/// This helps with SQL push down, as otherwise `CubeScan` would not contain `ChangeUserMember`
/// since filters would contain replacers.
#[derive(Default)]
pub struct FilterSplitMeta {}

impl FilterSplitMeta {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for FilterSplitMeta {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &OptimizerConfig,
    ) -> Result<LogicalPlan> {
        filter_split_meta(self, plan, optimizer_config)
    }

    fn name(&self) -> &str {
        "__cube__filter_split_meta"
    }
}

/// Recursively optimizes plan, searching for filters that can be split.
/// Continues optimizing down the plan after splitting.
fn filter_split_meta(
    optimizer: &FilterSplitMeta,
    plan: &LogicalPlan,
    optimizer_config: &OptimizerConfig,
) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Projection(Projection {
            expr,
            input,
            schema,
            alias,
        }) => Ok(LogicalPlan::Projection(Projection {
            expr: expr.clone(),
            input: Arc::new(filter_split_meta(optimizer, input, optimizer_config)?),
            schema: schema.clone(),
            alias: alias.clone(),
        })),
        LogicalPlan::Filter(Filter { predicate, input }) => {
            // Filter expressions can be moved around or split when they're chained with `AND` safely.
            // However, the input of `Filter` might be realiased, so we can't be sure if `__user` is really
            // for the original meta column; it makes sense to apply this only if input is `TableScan`.
            // However, we also have joins complicating things.
            // Additionally, there's no harm in splitting `__user` filter from other filters anyway;
            // hence we'll split all `Filter` nodes.
            let (normal_predicates, meta_predicates) = split_predicates(predicate, vec![], vec![]);
            let mut plan = filter_split_meta(optimizer, input, optimizer_config)?;
            if !meta_predicates.is_empty() {
                plan = LogicalPlan::Filter(Filter {
                    predicate: collect_predicates(meta_predicates)?,
                    input: Arc::new(plan),
                });
            }
            if !normal_predicates.is_empty() {
                plan = LogicalPlan::Filter(Filter {
                    predicate: collect_predicates(normal_predicates)?,
                    input: Arc::new(plan),
                });
            }
            Ok(plan)
        }
        LogicalPlan::Window(Window {
            input,
            window_expr,
            schema,
        }) => Ok(LogicalPlan::Window(Window {
            input: Arc::new(filter_split_meta(optimizer, input, optimizer_config)?),
            window_expr: window_expr.clone(),
            schema: schema.clone(),
        })),
        LogicalPlan::Aggregate(Aggregate {
            input,
            group_expr,
            aggr_expr,
            schema,
        }) => Ok(LogicalPlan::Aggregate(Aggregate {
            input: Arc::new(filter_split_meta(optimizer, input, optimizer_config)?),
            group_expr: group_expr.clone(),
            aggr_expr: aggr_expr.clone(),
            schema: schema.clone(),
        })),
        LogicalPlan::Sort(Sort { expr, input }) => Ok(LogicalPlan::Sort(Sort {
            expr: expr.clone(),
            input: Arc::new(filter_split_meta(optimizer, input, optimizer_config)?),
        })),
        LogicalPlan::Join(Join {
            left,
            right,
            on,
            join_type,
            join_constraint,
            schema,
            null_equals_null,
        }) => Ok(LogicalPlan::Join(Join {
            left: Arc::new(filter_split_meta(optimizer, left, optimizer_config)?),
            right: Arc::new(filter_split_meta(optimizer, right, optimizer_config)?),
            on: on.clone(),
            join_type: *join_type,
            join_constraint: *join_constraint,
            schema: schema.clone(),
            null_equals_null: *null_equals_null,
        })),
        LogicalPlan::CrossJoin(CrossJoin {
            left,
            right,
            schema,
        }) => Ok(LogicalPlan::CrossJoin(CrossJoin {
            left: Arc::new(filter_split_meta(optimizer, left, optimizer_config)?),
            right: Arc::new(filter_split_meta(optimizer, right, optimizer_config)?),
            schema: schema.clone(),
        })),
        LogicalPlan::Repartition(Repartition {
            input,
            partitioning_scheme,
        }) => Ok(LogicalPlan::Repartition(Repartition {
            input: Arc::new(filter_split_meta(optimizer, input, optimizer_config)?),
            partitioning_scheme: partitioning_scheme.clone(),
        })),
        LogicalPlan::Union(Union {
            inputs,
            schema,
            alias,
        }) => Ok(LogicalPlan::Union(Union {
            inputs: inputs
                .iter()
                .map(|plan| filter_split_meta(optimizer, plan, optimizer_config))
                .collect::<Result<_>>()?,
            schema: schema.clone(),
            alias: alias.clone(),
        })),
        plan @ LogicalPlan::TableScan(_) | plan @ LogicalPlan::EmptyRelation(_) => {
            // `TableScan` and `EmptyRelation` are as far as we can optimize.
            Ok(plan.clone())
        }
        LogicalPlan::Limit(Limit { skip, fetch, input }) => Ok(LogicalPlan::Limit(Limit {
            skip: *skip,
            fetch: *fetch,
            input: Arc::new(filter_split_meta(optimizer, input, optimizer_config)?),
        })),
        LogicalPlan::Subquery(Subquery {
            subqueries,
            input,
            schema,
            types,
        }) => Ok(LogicalPlan::Subquery(Subquery {
            subqueries: subqueries
                .iter()
                .map(|subquery| filter_split_meta(optimizer, subquery, optimizer_config))
                .collect::<Result<_>>()?,
            input: Arc::new(filter_split_meta(optimizer, input, optimizer_config)?),
            schema: schema.clone(),
            types: types.clone(),
        })),
        LogicalPlan::Distinct(Distinct { input }) => Ok(LogicalPlan::Distinct(Distinct {
            input: Arc::new(filter_split_meta(optimizer, input, optimizer_config)?),
        })),
        other => {
            // The rest of the plans have no inputs to optimize, or it makes no sense
            // to optimize them.
            Ok(other.clone())
        }
    }
}

/// Splits the provided predicate into two vectors: one for normal predicates and one for meta predicates.
/// These will later be concatenated into a single `Filter` node each.
fn split_predicates(
    predicate: &Expr,
    mut normal_predicates: Vec<Expr>,
    mut meta_predicates: Vec<Expr>,
) -> (Vec<Expr>, Vec<Expr>) {
    if let Expr::BinaryExpr { left, op, right } = predicate {
        if *op == Operator::And {
            let (normal_predicates, meta_predicates) =
                split_predicates(left, normal_predicates, meta_predicates);
            let (normal_predicates, meta_predicates) =
                split_predicates(right, normal_predicates, meta_predicates);
            return (normal_predicates, meta_predicates);
        }
    }

    if is_meta_predicate(predicate) {
        meta_predicates.push(predicate.clone());
    } else {
        normal_predicates.push(predicate.clone());
    }
    (normal_predicates, meta_predicates)
}

/// Determines if the provided expression is a meta predicate.
/// Supported variants:
/// - `BinaryExpr` with `Eq`, `Like`, or `ILike` operators and one of the sides being a meta column;
/// - `Like` or `ILike` with expr or pattern being a meta column;
/// - `IsNotNull` over a meta column (or `Not` over `IsNull` over a meta column);
/// - `InList` with one value in list and expr or list value being a meta column.
fn is_meta_predicate(predicate: &Expr) -> bool {
    match predicate {
        Expr::BinaryExpr { left, op, right } => {
            if matches!(op, Operator::Eq | Operator::Like | Operator::ILike) {
                return is_meta_column(left) || is_meta_column(right);
            }
            false
        }
        Expr::Like(like) | Expr::ILike(like) => {
            is_meta_column(&like.expr) || is_meta_column(&like.pattern)
        }
        Expr::IsNotNull(expr) => is_meta_column(expr),
        Expr::Not(expr) => match expr.as_ref() {
            Expr::IsNull(expr) => is_meta_column(expr),
            _ => false,
        },
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            if list.len() != 1 {
                return false;
            }
            is_meta_column(expr) || is_meta_column(&list[0])
        }
        _ => false,
    }
}

/// Determines if the provided expression is meta column reference.
/// Currently, only `__user` is considered a meta column.
/// Additionally, `Lower` function over a meta column is also considered a meta column.
fn is_meta_column(expr: &Expr) -> bool {
    match expr {
        Expr::Column(Column { name, .. }) => name.eq_ignore_ascii_case("__user"),
        Expr::ScalarFunction { fun, args } => {
            if matches!(fun, BuiltinScalarFunction::Lower) && args.len() == 1 {
                return is_meta_column(&args[0]);
            }
            false
        }
        _ => false,
    }
}

/// Concatenates the provided predicates into a single expression using `AND` operator.
fn collect_predicates(predicates: Vec<Expr>) -> Result<Expr> {
    predicates
        .into_iter()
        .reduce(|last, next| Expr::BinaryExpr {
            left: Box::new(last),
            op: Operator::And,
            right: Box::new(next),
        })
        .ok_or_else(|| {
            DataFusionError::Internal(
                "Unable to optimize plan: can't concatenate predicates, vec is unexpectedly empty"
                    .to_string(),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::{super::utils::make_sample_table, *};
    use datafusion::logical_plan::{col, lit, LogicalPlanBuilder};

    fn optimize(plan: &LogicalPlan) -> Result<LogicalPlan> {
        let rule = FilterSplitMeta::new();
        rule.optimize(plan, &OptimizerConfig::new())
    }

    fn assert_optimized_plan_eq(plan: LogicalPlan, expected: &str) {
        let optimized_plan = optimize(&plan).expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
    }

    #[test]
    fn test_filter_split_meta() -> Result<()> {
        let plan = LogicalPlanBuilder::from(make_sample_table(
            "t1",
            vec!["c1", "c2", "c3"],
            vec!["__user"],
        )?)
        .filter(
            col("c1")
                .gt(lit(10i32))
                .and(col("__user").eq(lit("postgres".to_string())))
                .and(col("c2").lt(lit(5i32)))
                .and(col("__user").is_not_null()),
        )?
        .project(vec![col("c1"), col("c2"), col("c3")])?
        .build()?;

        let expected = "\
              Projection: #t1.c1, #t1.c2, #t1.c3\
            \n  Filter: #t1.c1 > Int32(10) AND #t1.c2 < Int32(5)\
            \n    Filter: #t1.__user = Utf8(\"postgres\") AND #t1.__user IS NOT NULL\
            \n      TableScan: t1 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);
        Ok(())
    }
}
