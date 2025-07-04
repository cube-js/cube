use std::{iter::FromIterator, mem::take, sync::Arc};

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
use indexmap::IndexSet;

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
        let mut meta_predicates = IndexSet::new();
        let result = filter_split_meta(self, plan, &mut meta_predicates, optimizer_config)?;
        if !meta_predicates.is_empty() {
            return Err(DataFusionError::Internal(
                "Unexpected non-issued meta predicates while running FilterSplitMeta optimizer"
                    .to_string(),
            ));
        }
        Ok(result)
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
    meta_predicates: &mut IndexSet<Expr>,
    optimizer_config: &OptimizerConfig,
) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Projection(Projection {
            expr,
            input,
            schema,
            alias,
        }) => {
            // Push meta predicates down `Projection` if possible.
            let plan = filter_split_meta(optimizer, input, meta_predicates, optimizer_config)?;
            let plan = issue_meta_predicates(plan, meta_predicates)?;
            Ok(LogicalPlan::Projection(Projection {
                expr: expr.clone(),
                input: Arc::new(plan),
                schema: schema.clone(),
                alias: alias.clone(),
            }))
        }
        LogicalPlan::Filter(Filter { predicate, input }) => {
            // Filter expressions can be moved around or split when they're chained with `AND` safely.
            // However, the input of `Filter` might be realiased, so we can't be sure if `__user` is really
            // for the original meta column; it makes sense to apply this only if input is `TableScan`.
            // However, we also have joins complicating things.
            // Additionally, there's no harm in splitting `__user` filter from other filters anyway;
            // hence we'll split all `Filter` nodes.
            let mut normal_predicates = vec![];
            split_predicates(predicate, &mut normal_predicates, meta_predicates);
            let plan = filter_split_meta(optimizer, input, meta_predicates, optimizer_config)?;
            let mut plan = issue_meta_predicates(plan, meta_predicates)?;
            if let Some(collected_predicates) = collect_predicates(normal_predicates, false) {
                plan = LogicalPlan::Filter(Filter {
                    predicate: collected_predicates,
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
            // Don't push meta predicates down `Window`.
            input: Arc::new(filter_split_meta(
                optimizer,
                input,
                &mut IndexSet::new(),
                optimizer_config,
            )?),
            window_expr: window_expr.clone(),
            schema: schema.clone(),
        })),
        LogicalPlan::Aggregate(Aggregate {
            input,
            group_expr,
            aggr_expr,
            schema,
        }) => Ok(LogicalPlan::Aggregate(Aggregate {
            // Don't push meta predicates down `Aggregate`.
            input: Arc::new(filter_split_meta(
                optimizer,
                input,
                &mut IndexSet::new(),
                optimizer_config,
            )?),
            group_expr: group_expr.clone(),
            aggr_expr: aggr_expr.clone(),
            schema: schema.clone(),
        })),
        LogicalPlan::Sort(Sort { expr, input }) => {
            // Push meta predicates down `Sort`.
            let plan = filter_split_meta(optimizer, input, meta_predicates, optimizer_config)?;
            let plan = issue_meta_predicates(plan, meta_predicates)?;
            Ok(LogicalPlan::Sort(Sort {
                expr: expr.clone(),
                input: Arc::new(plan),
            }))
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
            // For `Join`, we can push down both sides and collect non-issued meta predicates.
            let mut left_meta_predicates = take(meta_predicates);
            let mut right_meta_predicates = left_meta_predicates.clone();
            let left_plan =
                filter_split_meta(optimizer, left, &mut left_meta_predicates, optimizer_config)?;
            let left_plan = issue_meta_predicates(left_plan, &mut left_meta_predicates)?;
            let right_plan = filter_split_meta(
                optimizer,
                right,
                &mut right_meta_predicates,
                optimizer_config,
            )?;
            let right_plan = issue_meta_predicates(right_plan, &mut right_meta_predicates)?;
            *meta_predicates = IndexSet::from_iter(
                left_meta_predicates
                    .intersection(&right_meta_predicates)
                    .cloned(),
            );
            Ok(LogicalPlan::Join(Join {
                left: Arc::new(left_plan),
                right: Arc::new(right_plan),
                on: on.clone(),
                join_type: *join_type,
                join_constraint: *join_constraint,
                schema: schema.clone(),
                null_equals_null: *null_equals_null,
            }))
        }
        LogicalPlan::CrossJoin(CrossJoin {
            left,
            right,
            schema,
        }) => {
            // For `CrossJoin`, we can push down both sides and collect non-issued meta predicates.
            let mut left_meta_predicates = take(meta_predicates);
            let mut right_meta_predicates = left_meta_predicates.clone();
            let left_plan =
                filter_split_meta(optimizer, left, &mut left_meta_predicates, optimizer_config)?;
            let left_plan = issue_meta_predicates(left_plan, &mut left_meta_predicates)?;
            let right_plan = filter_split_meta(
                optimizer,
                right,
                &mut right_meta_predicates,
                optimizer_config,
            )?;
            let right_plan = issue_meta_predicates(right_plan, &mut right_meta_predicates)?;
            *meta_predicates = IndexSet::from_iter(
                left_meta_predicates
                    .intersection(&right_meta_predicates)
                    .cloned(),
            );
            Ok(LogicalPlan::CrossJoin(CrossJoin {
                left: Arc::new(left_plan),
                right: Arc::new(right_plan),
                schema: schema.clone(),
            }))
        }
        LogicalPlan::Repartition(Repartition {
            input,
            partitioning_scheme,
        }) => Ok(LogicalPlan::Repartition(Repartition {
            // Don't push meta predicates down `Repartition`.
            input: Arc::new(filter_split_meta(
                optimizer,
                input,
                &mut IndexSet::new(),
                optimizer_config,
            )?),
            partitioning_scheme: partitioning_scheme.clone(),
        })),
        LogicalPlan::Union(Union {
            inputs,
            schema,
            alias,
        }) => Ok(LogicalPlan::Union(Union {
            // Don't push meta predicates down `Union`.
            inputs: inputs
                .iter()
                .map(|plan| {
                    filter_split_meta(optimizer, plan, &mut IndexSet::new(), optimizer_config)
                })
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
            // Don't push meta predicates down `Limit`.
            input: Arc::new(filter_split_meta(
                optimizer,
                input,
                &mut IndexSet::new(),
                optimizer_config,
            )?),
        })),
        LogicalPlan::Subquery(Subquery {
            subqueries,
            input,
            schema,
            types,
        }) => {
            // Push meta predicates down `Subquery` input.
            let plan = filter_split_meta(optimizer, input, meta_predicates, optimizer_config)?;
            let plan = issue_meta_predicates(plan, meta_predicates)?;
            Ok(LogicalPlan::Subquery(Subquery {
                // Don't push meta predicates down subqueries.
                subqueries: subqueries
                    .iter()
                    .map(|subquery| {
                        filter_split_meta(
                            optimizer,
                            subquery,
                            &mut IndexSet::new(),
                            optimizer_config,
                        )
                    })
                    .collect::<Result<_>>()?,
                input: Arc::new(plan),
                schema: schema.clone(),
                types: types.clone(),
            }))
        }
        LogicalPlan::Distinct(Distinct { input }) => {
            // Push meta predicates down `Distinct`.
            let plan = filter_split_meta(optimizer, input, meta_predicates, optimizer_config)?;
            let plan = issue_meta_predicates(plan, meta_predicates)?;
            Ok(LogicalPlan::Distinct(Distinct {
                input: Arc::new(plan),
            }))
        }
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
    normal_predicates: &mut Vec<Expr>,
    meta_predicates: &mut IndexSet<Expr>,
) {
    if let Expr::BinaryExpr { left, op, right } = predicate {
        if *op == Operator::And {
            split_predicates(left, normal_predicates, meta_predicates);
            split_predicates(right, normal_predicates, meta_predicates);
            return;
        }
    }

    if meta_column_from_predicate(predicate).is_some() {
        meta_predicates.insert(predicate.clone());
    } else {
        normal_predicates.push(predicate.clone());
    }
}

/// Gets a reference to the meta column in the provided expression, if any.
/// Supported variants:
/// - `BinaryExpr` with `Eq`, `Like`, or `ILike` operators and one of the sides being a meta column;
/// - `Like` or `ILike` with expr or pattern being a meta column;
/// - `IsNotNull` over a meta column (or `Not` over `IsNull` over a meta column);
/// - `InList` with one value in list and expr or list value being a meta column.
fn meta_column_from_predicate(predicate: &Expr) -> Option<&Column> {
    match predicate {
        Expr::BinaryExpr {
            left,
            op: Operator::Eq | Operator::Like | Operator::ILike,
            right,
        } => meta_column_from_column(left).or_else(|| meta_column_from_column(right)),
        Expr::Like(like) | Expr::ILike(like) => {
            meta_column_from_column(&like.expr).or_else(|| meta_column_from_column(&like.pattern))
        }
        Expr::IsNotNull(expr) => meta_column_from_column(expr),
        Expr::Not(expr) => match expr.as_ref() {
            Expr::IsNull(expr) => meta_column_from_column(expr),
            _ => None,
        },
        Expr::InList {
            expr,
            list,
            negated: false,
        } if list.len() == 1 => {
            meta_column_from_column(expr).or_else(|| meta_column_from_column(&list[0]))
        }
        _ => None,
    }
}

/// Gets reference to the meta column in the provided column expression, if any.
/// Currently, only `__user` is considered a meta column.
/// Additionally, `Lower` function over a meta column or casting meta column
/// is also considered a meta column.
fn meta_column_from_column(expr: &Expr) -> Option<&Column> {
    match expr {
        Expr::Column(column) if column.name.eq_ignore_ascii_case("__user") => Some(column),
        Expr::ScalarFunction { fun, args }
            if matches!(fun, BuiltinScalarFunction::Lower) && args.len() == 1 =>
        {
            meta_column_from_column(&args[0])
        }
        Expr::Cast { expr, .. } => meta_column_from_column(expr),
        _ => None,
    }
}

/// Concatenates the provided predicates into a single expression using `AND` operator.
fn collect_predicates(predicates: Vec<Expr>, reverse: bool) -> Option<Expr> {
    let predicates_iter = predicates.into_iter();
    if reverse {
        predicates_iter.rev().reduce(|last, next| Expr::BinaryExpr {
            left: Box::new(last),
            op: Operator::And,
            right: Box::new(next),
        })
    } else {
        predicates_iter.reduce(|last, next| Expr::BinaryExpr {
            left: Box::new(last),
            op: Operator::And,
            right: Box::new(next),
        })
    }
}

/// Issues meta predicates, if any and if applicable, returning either the original plan
/// or a filtered plan with meta predicates applied.
/// Predicates that have been issued are removed from the `meta_predicates` set.
fn issue_meta_predicates(
    plan: LogicalPlan,
    meta_predicates: &mut IndexSet<Expr>,
) -> Result<LogicalPlan> {
    if meta_predicates.is_empty() {
        return Ok(plan);
    }

    // Collect meta predicates that can be applied to the plan.
    let schema = plan.schema();
    let mut can_be_applied_indices = vec![];
    for (index, predicate) in meta_predicates.iter().enumerate() {
        let Some(meta_column) = meta_column_from_predicate(predicate) else {
            continue;
        };
        if schema.field_from_column(meta_column).is_ok() {
            can_be_applied_indices.push(index);
        }
    }
    if can_be_applied_indices.is_empty() {
        return Ok(plan);
    }

    // Apply the predicates.
    let can_be_applied = can_be_applied_indices
        .iter()
        .rev()
        .filter_map(|index| meta_predicates.shift_remove_index(*index))
        .collect::<Vec<_>>();
    let Some(issued_predicates) = collect_predicates(can_be_applied, true) else {
        return Ok(plan);
    };
    Ok(LogicalPlan::Filter(Filter {
        predicate: issued_predicates,
        input: Arc::new(plan),
    }))
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
