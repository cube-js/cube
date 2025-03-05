use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use datafusion::{
    error::{DataFusionError, Result},
    logical_plan::{
        plan::{
            Aggregate, CrossJoin, Distinct, Join, Limit, Projection, Repartition, Sort, Subquery,
            Union, Window,
        },
        Column, DFSchema, Expr, Filter, LogicalPlan, Operator,
    },
    optimizer::optimizer::{OptimizerConfig, OptimizerRule},
};

use super::utils::{
    get_expr_columns, get_schema_columns, is_column_expr, is_const_expr, is_plan_yielding_one_row,
    plan_has_projections, rewrite,
};

/// Filter Push Down optimizer rule pushes WHERE clauses consisting of specific,
/// mostly simple, expressions down the plan, all the way to the Projection
/// closest to TableScan. This is beneficial for CubeScans when some of the Projections
/// on the way contain post-processing operations and cannot be pushed down.
#[derive(Default)]
pub struct FilterPushDown {}

impl FilterPushDown {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for FilterPushDown {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &OptimizerConfig,
    ) -> Result<LogicalPlan> {
        filter_push_down(self, plan, vec![], optimizer_config)
    }

    fn name(&self) -> &str {
        "__cube__filter_push_down"
    }
}

/// Recursively optimizes plan, collecting the filters that can possibly be pushed down.
/// Several filters will be concatenated to one Filter node joined with AND operator.
fn filter_push_down(
    optimizer: &FilterPushDown,
    plan: &LogicalPlan,
    predicates: Vec<Expr>,
    optimizer_config: &OptimizerConfig,
) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Projection(Projection {
            expr,
            input,
            schema,
            alias,
        }) => {
            // Filter can be pushed down to projection, however we only map specific expressions.
            // Complex predicates will not be pushed down, and get issued before the projection.
            // As for the simple predicates, push down optimization is continued with those.
            if predicates.is_empty() || !plan_has_projections(input) {
                return issue_filter(
                    predicates,
                    LogicalPlan::Projection(Projection {
                        expr: expr.clone(),
                        input: Arc::new(filter_push_down(
                            optimizer,
                            input,
                            vec![],
                            optimizer_config,
                        )?),
                        schema: schema.clone(),
                        alias: alias.clone(),
                    }),
                );
            }

            let rewrite_map = rewrite_map_for_projection(expr, schema);
            let mut rewritten_predicates = vec![];
            let mut non_rewrittable_predicates = vec![];
            for predicate in predicates {
                let new_predicate = rewrite(&predicate, &rewrite_map)?;
                if let Some(predicate) = new_predicate {
                    rewritten_predicates.push(predicate);
                    continue;
                }
                non_rewrittable_predicates.push(predicate);
            }

            issue_filter(
                non_rewrittable_predicates,
                LogicalPlan::Projection(Projection {
                    expr: expr.clone(),
                    input: Arc::new(filter_push_down(
                        optimizer,
                        input,
                        rewritten_predicates,
                        optimizer_config,
                    )?),
                    schema: schema.clone(),
                    alias: alias.clone(),
                }),
            )
        }
        LogicalPlan::Filter(Filter { predicate, input }) => {
            // When encountering a filter, collect it to our list of predicates,
            // remove the filter from the plan and continue down the plan.

            // TODO: splitting predicates by AND doesn't break anything per se,
            // but does alter how rewrites work with date ranges, generating filters
            // instead of a date range. Add a single predicate for the time being.
            // let predicates = split_predicates(predicate)
            let predicates = vec![predicate.clone()]
                .into_iter()
                .chain(predicates)
                .collect::<Vec<_>>();
            let mut pushable_predicates = vec![];
            let mut non_pushable_predicates = vec![];
            for predicate in predicates {
                if is_predicate_pushable(&predicate) {
                    pushable_predicates.push(predicate);
                    continue;
                }
                non_pushable_predicates.push(predicate);
            }

            issue_filter(
                non_pushable_predicates,
                filter_push_down(optimizer, input, pushable_predicates, optimizer_config)?,
            )
        }
        LogicalPlan::Window(Window {
            input,
            window_expr,
            schema,
        }) => {
            // Filter can't be pushed down Window.
            issue_filter(
                predicates,
                LogicalPlan::Window(Window {
                    input: Arc::new(filter_push_down(
                        optimizer,
                        input,
                        vec![],
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
            // Filters can be pushed down Aggregate if the predicate only references
            // columns in `group_expr`. Issue the rest of the filters and continue
            // down the plan.
            if predicates.is_empty() {
                return Ok(LogicalPlan::Aggregate(Aggregate {
                    input: Arc::new(filter_push_down(
                        optimizer,
                        input,
                        vec![],
                        optimizer_config,
                    )?),
                    group_expr: group_expr.clone(),
                    aggr_expr: aggr_expr.clone(),
                    schema: schema.clone(),
                }));
            }

            let group_expr_exprs = group_expr.iter().collect::<HashSet<_>>();
            let mut pushable_predicates = vec![];
            let mut non_pushable_predicates = vec![];
            for predicate in predicates {
                let predicate_column_exprs = get_expr_columns(&predicate)
                    .into_iter()
                    .map(|column| Expr::Column(column))
                    .collect::<Vec<_>>();
                let all_columns_in_group_expr = predicate_column_exprs
                    .iter()
                    .all(|column| group_expr_exprs.contains(column));
                if all_columns_in_group_expr {
                    pushable_predicates.push(predicate);
                    continue;
                }
                non_pushable_predicates.push(predicate);
            }

            issue_filter(
                non_pushable_predicates,
                LogicalPlan::Aggregate(Aggregate {
                    input: Arc::new(filter_push_down(
                        optimizer,
                        input,
                        pushable_predicates,
                        optimizer_config,
                    )?),
                    group_expr: group_expr.clone(),
                    aggr_expr: aggr_expr.clone(),
                    schema: schema.clone(),
                }),
            )
        }
        LogicalPlan::Sort(Sort { expr, input }) => {
            // Filtering won't affect the sorting; on the contrary, filtering before sorting
            // may be beneficial for sorting performance.
            Ok(LogicalPlan::Sort(Sort {
                expr: expr.clone(),
                input: Arc::new(filter_push_down(
                    optimizer,
                    input,
                    predicates,
                    optimizer_config,
                )?),
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
            // Joins are tricky since whether pushing filters to each side is allowed depends
            // on the join type, since some joins would issue NULLs with no matches, hence
            // affect the result. Additionally, Cube joins are always LEFT JOINs.
            // It is unknown whether the join will be pushed down CubeScan or post-processed.
            // Taking the above into account, the safest option is to push down filter only
            // the left side of a JOIN, and only when both conditions are true:
            // - `join_type` must be an INNER JOIN or LEFT JOIN
            // - All columns in predicate must reference columns in left side of JOIN

            // TODO: Nested joins with filters between will behave incorrectly with rewrites,
            // so this code is disabled for the time being. Only join inputs are optimized.
            /*
            let pushable_join_type = match join_type {
                JoinType::Inner | JoinType::Left => true,
                _ => false,
            };
            if predicates.is_empty() || !pushable_join_type {
                return issue_filter(
                    predicates,
                    LogicalPlan::Join(Join {
                        left: Arc::new(filter_push_down(
                            optimizer,
                            left,
                            vec![],
                            optimizer_config,
                        )?),
                        right: Arc::new(filter_push_down(
                            optimizer,
                            right,
                            vec![],
                            optimizer_config,
                        )?),
                        on: on.clone(),
                        join_type: join_type.clone(),
                        join_constraint: join_constraint.clone(),
                        schema: schema.clone(),
                        null_equals_null: null_equals_null.clone(),
                    }),
                );
            }

            let left_columns = get_schema_columns(left.schema());
            let mut pushable_predicates = vec![];
            let mut non_pushable_predicates = vec![];
            for predicate in predicates {
                let predicate_column_exprs = get_expr_columns(&predicate);
                let all_columns_in_schema = predicate_column_exprs
                    .iter()
                    .all(|column| left_columns.contains(column));
                if all_columns_in_schema {
                    pushable_predicates.push(predicate);
                    continue;
                }
                non_pushable_predicates.push(predicate);
            }

            issue_filter(
                non_pushable_predicates,
                LogicalPlan::Join(Join {
                    left: Arc::new(filter_push_down(
                        optimizer,
                        left,
                        pushable_predicates,
                        optimizer_config,
                    )?),
                    right: Arc::new(filter_push_down(
                        optimizer,
                        right,
                        vec![],
                        optimizer_config,
                    )?),
                    on: on.clone(),
                    join_type: join_type.clone(),
                    join_constraint: join_constraint.clone(),
                    schema: schema.clone(),
                    null_equals_null: null_equals_null.clone(),
                }),
            )
            */

            issue_filter(
                predicates,
                LogicalPlan::Join(Join {
                    left: Arc::new(filter_push_down(optimizer, left, vec![], optimizer_config)?),
                    right: Arc::new(filter_push_down(
                        optimizer,
                        right,
                        vec![],
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
            // CROSS JOINs have the same limitations as JOINs (read above), yet they never
            // produce NULLs, so it's always safe to push to the left side if the predicate
            // only reference left side columns.

            // TODO: Nested joins with filters between will behave incorrectly with rewrites,
            // so this code should be disabled for the time being. There is, however,
            // at least one specific case that should be supported regardless:
            // if the right side always produces exactly one row (we know this if the first
            // meaningful input of a plan is an Aggregate with no `group_expr` and one or more
            // `aggr_expr`s), push filters down the left input.
            if predicates.is_empty() || !is_plan_yielding_one_row(right) {
                return issue_filter(
                    predicates,
                    LogicalPlan::CrossJoin(CrossJoin {
                        left: Arc::new(filter_push_down(
                            optimizer,
                            left,
                            vec![],
                            optimizer_config,
                        )?),
                        right: Arc::new(filter_push_down(
                            optimizer,
                            right,
                            vec![],
                            optimizer_config,
                        )?),
                        schema: schema.clone(),
                    }),
                );
            }

            let left_columns = get_schema_columns(left.schema());
            let mut pushable_predicates = vec![];
            let mut non_pushable_predicates = vec![];
            for predicate in predicates {
                let predicate_column_exprs = get_expr_columns(&predicate);
                let all_columns_in_schema = predicate_column_exprs
                    .iter()
                    .all(|column| left_columns.contains(column));
                if all_columns_in_schema {
                    pushable_predicates.push(predicate);
                    continue;
                }
                non_pushable_predicates.push(predicate);
            }

            issue_filter(
                non_pushable_predicates,
                LogicalPlan::CrossJoin(CrossJoin {
                    left: Arc::new(filter_push_down(
                        optimizer,
                        left,
                        pushable_predicates,
                        optimizer_config,
                    )?),
                    right: Arc::new(filter_push_down(
                        optimizer,
                        right,
                        vec![],
                        optimizer_config,
                    )?),
                    schema: schema.clone(),
                }),
            )
        }
        LogicalPlan::Repartition(Repartition {
            input,
            partitioning_scheme,
        }) => {
            // TODO: figure out if Filter and Repartition can be swapped around
            issue_filter(
                predicates,
                LogicalPlan::Repartition(Repartition {
                    input: Arc::new(filter_push_down(
                        optimizer,
                        input,
                        vec![],
                        optimizer_config,
                    )?),
                    partitioning_scheme: partitioning_scheme.clone(),
                }),
            )
        }
        LogicalPlan::Union(Union {
            inputs,
            schema,
            alias,
        }) => {
            // It's safe to push filters down UNION inputs with a simple rewrite.

            // TODO: However, Projection schema duplicates UNION schema, so rewriting is impossible.
            /*
            if predicates.is_empty() {
                return Ok(LogicalPlan::Union(Union {
                    inputs: inputs
                        .iter()
                        .map(|plan| filter_push_down(optimizer, plan, vec![], optimizer_config))
                        .collect::<Result<_>>()?,
                    schema: schema.clone(),
                    alias: alias.clone(),
                }));
            }

            Ok(LogicalPlan::Union(Union {
                inputs: inputs
                    .iter()
                    .map(|plan| {
                        let rewrite_map = rewrite_map_for_union_input(schema, plan.schema());
                        let new_predicates = predicates
                            .iter()
                            .map(|predicate| rewrite(predicate, &rewrite_map))
                            .collect::<Result<Option<_>>>()?
                            .ok_or(DataFusionError::Internal(
                                "Unable to optimize plan: union schema doesn't match input schema"
                                    .to_string(),
                            ))?;
                        filter_push_down(optimizer, plan, new_predicates, optimizer_config)
                    })
                    .collect::<Result<_>>()?,
                schema: schema.clone(),
                alias: alias.clone(),
            }))
            */

            issue_filter(
                predicates,
                LogicalPlan::Union(Union {
                    inputs: inputs
                        .iter()
                        .map(|plan| filter_push_down(optimizer, plan, vec![], optimizer_config))
                        .collect::<Result<_>>()?,
                    schema: schema.clone(),
                    alias: alias.clone(),
                }),
            )
        }
        plan @ LogicalPlan::TableScan(_) | plan @ LogicalPlan::EmptyRelation(_) => {
            // TableScan or EmptyRelation's as far as we can push our filters.
            issue_filter(predicates, plan.clone())
        }
        LogicalPlan::Limit(Limit { skip, fetch, input }) => {
            // Swapping Limit and Filter affects the final result.
            issue_filter(
                predicates,
                LogicalPlan::Limit(Limit {
                    skip: *skip,
                    fetch: *fetch,
                    input: Arc::new(filter_push_down(
                        optimizer,
                        input,
                        vec![],
                        optimizer_config,
                    )?),
                }),
            )
        }
        LogicalPlan::Subquery(Subquery {
            subqueries,
            input,
            schema,
            types,
        }) => {
            // TODO: Push Filter down Subquery
            issue_filter(
                predicates,
                LogicalPlan::Subquery(Subquery {
                    subqueries: subqueries
                        .iter()
                        .map(|subquery| {
                            filter_push_down(optimizer, subquery, vec![], optimizer_config)
                        })
                        .collect::<Result<_>>()?,
                    input: Arc::new(filter_push_down(
                        optimizer,
                        input,
                        vec![],
                        optimizer_config,
                    )?),
                    schema: schema.clone(),
                    types: types.clone(),
                }),
            )
        }
        LogicalPlan::Distinct(Distinct { input }) => {
            // Distinct removes duplicate rows, so it is safe to keep pushing the filters down.
            Ok(LogicalPlan::Distinct(Distinct {
                input: Arc::new(filter_push_down(
                    optimizer,
                    input,
                    predicates,
                    optimizer_config,
                )?),
            }))
        }
        other => {
            // The rest of the plans have no inputs to optimize, can't have the filters
            // be pushed down them, or it makes no sense to optimize them.
            issue_filter(predicates, other.clone())
        }
    }
}

/// Generates a rewrite map for projection, taking qualified and unqualified fields into account.
/// Only simple realiasing expressions are mapped; more complex projection expressions might
/// produce complex filters which cannot be pushed down to CubeScan, and will block other nodes:
/// those are mapped as `None` to explicitly mark them as non-mappable.
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
            // Aliases are never part of WHERE clause so they must be removed
            let expr = match expr {
                Expr::Alias(expr, _) => expr,
                expr @ _ => expr,
            };

            let expr = match expr {
                // We only expand simple realiasing expressions
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

// /// Generates a rewrite map for union inputs, taking UNION's schema and one of the input's schema.
// /// These expressions can't be more complex than simple column references by definition.
// ///
// /// TODO: see `LogicalPlan::Union` above
// fn rewrite_map_for_union_input(
//     union_schema: &DFSchema,
//     input_schema: &DFSchema,
// ) -> HashMap<Column, Option<Expr>> {
//     union_schema
//         .fields()
//         .iter()
//         .zip(input_schema.fields().iter())
//         .flat_map(|(union_field, input_field)| {
//             vec![
//                 (
//                     union_field.qualified_column(),
//                     Some(Expr::Column(input_field.unqualified_column())),
//                 ),
//                 (
//                     union_field.unqualified_column(),
//                     Some(Expr::Column(input_field.unqualified_column())),
//                 ),
//             ]
//         })
//         .collect()
// }

/// Recursively concatenates several filter predicates into one binary AND expression.
/// `predicates` must contain at least one expression.
fn concatenate_predicates(predicates: Vec<Expr>) -> Result<Expr> {
    predicates
        .into_iter()
        .reduce(|acc, el| Expr::BinaryExpr {
            left: Box::new(acc),
            op: Operator::And,
            right: Box::new(el),
        })
        .ok_or(DataFusionError::Internal(
            "Unable to optimize plan: can't concatenate predicates, vec is unexpectedly empty"
                .to_string(),
        ))
}

// /// Recursively splits filter predicate from binary AND expressions into a flat vec of predicates.
// ///
// /// TODO: see `LogicalPlan::Filter` above
// fn split_predicates(predicate: &Expr) -> Vec<Expr> {
//     match predicate {
//         Expr::BinaryExpr {
//             left,
//             op: Operator::And,
//             right,
//         } => split_predicates(left)
//             .into_iter()
//             .chain(split_predicates(right).into_iter())
//             .collect(),
//         expr => vec![expr.clone()],
//     }
// }

/// Recursively checks if the passed expr is a filter predicate that can be pushed down.
/// The predicate should be pushed down the plan if it can ultimately be pushed down to CubeScan.
/// Extend this on case-by-case basis.
fn is_predicate_pushable(predicate: &Expr) -> bool {
    match predicate {
        Expr::Column(_) => true,
        Expr::BinaryExpr { left, op, right } => match op {
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => {
                (is_column_expr(left) && is_const_expr(right))
                    || (is_const_expr(left) && is_column_expr(right))
            }
            Operator::And | Operator::Or => {
                is_predicate_pushable(left) && is_predicate_pushable(right)
            }
            Operator::Like | Operator::NotLike | Operator::ILike | Operator::NotILike => {
                is_column_expr(left) && is_const_expr(right)
            }
            _ => false,
        },
        Expr::Like(like) | Expr::ILike(like) | Expr::SimilarTo(like) => {
            like.escape_char.is_none() && is_column_expr(&like.expr) && is_const_expr(&like.pattern)
        }
        Expr::Not(expr) => is_predicate_pushable(expr),
        Expr::IsNotNull(expr) | Expr::IsNull(expr) => is_column_expr(expr),
        Expr::InList { expr, list, .. } => {
            is_column_expr(expr) && list.iter().all(|item| is_const_expr(item))
        }
        _ => false,
    }
}

/// Issues a Filter containing the provided input if the provided vec contains predicates;
/// otherwise, issues the provided input instead.
fn issue_filter(predicates: Vec<Expr>, input: LogicalPlan) -> Result<LogicalPlan> {
    if predicates.is_empty() {
        return Ok(input);
    }
    Ok(LogicalPlan::Filter(Filter {
        predicate: concatenate_predicates(predicates)?,
        input: Arc::new(input),
    }))
}

#[cfg(test)]
mod tests {
    use super::{
        super::utils::{make_sample_table, sample_table},
        *,
    };
    use datafusion::logical_plan::{binary_expr, col, count, lit, sum, LogicalPlanBuilder};

    fn optimize(plan: &LogicalPlan) -> Result<LogicalPlan> {
        let rule = FilterPushDown::new();
        rule.optimize(plan, &OptimizerConfig::new())
    }

    fn assert_optimized_plan_eq(plan: LogicalPlan, expected: &str) {
        let optimized_plan = optimize(&plan).expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
    }

    #[test]
    fn test_filter_down_projection() -> Result<()> {
        let plan = LogicalPlanBuilder::from(sample_table()?)
            .project(vec![col("c1"), col("c3")])?
            .project_with_alias(
                vec![col("c1").alias("n1"), col("c3").alias("n2")],
                Some("t2".to_string()),
            )?
            .filter(col("t2.n2").gt(lit(5i32)))?
            .build()?;

        let expected = "\
              Projection: #t1.c1 AS n1, #t1.c3 AS n2, alias=t2\
            \n  Filter: #t1.c3 > Int32(5)\
            \n    Projection: #t1.c1, #t1.c3\
            \n      TableScan: t1 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);
        Ok(())
    }

    #[test]
    fn test_multiple_filters_down_projections() -> Result<()> {
        let plan = LogicalPlanBuilder::from(sample_table()?)
            .project(vec![col("c1"), col("c2"), col("c3")])?
            .project_with_alias(
                vec![
                    col("c1").alias("c4"),
                    col("c2").alias("c5"),
                    col("c3").alias("c6"),
                ],
                Some("t2".to_string()),
            )?
            .filter(col("t2.c5").gt(lit(5i32)))?
            .project_with_alias(
                vec![
                    col("t2.c4").alias("c7"),
                    col("t2.c5"),
                    col("t2.c6").alias("c8"),
                ],
                Some("t3".to_string()),
            )?
            .filter(col("t3.c5").lt_eq(lit(10i32)).and(col("c8").eq(lit(0i32))))?
            .project(vec![col("t3.c7"), col("c5"), col("t3.c8").alias("c9")])?
            .filter(col("c7").lt(lit(0i32)).not())?
            .project(vec![col("c7"), col("c5"), col("c9")])?
            .build()?;

        let expected = "\
              Projection: #t3.c7, #t3.c5, #c9\
            \n  Projection: #t3.c7, #t3.c5, #t3.c8 AS c9\
            \n    Projection: #t2.c4 AS c7, #t2.c5, #t2.c6 AS c8, alias=t3\
            \n      Projection: #t1.c1 AS c4, #t1.c2 AS c5, #t1.c3 AS c6, alias=t2\
            \n        Filter: #t1.c2 > Int32(5) AND #t1.c2 <= Int32(10) AND #t1.c3 = Int32(0) AND NOT #t1.c1 < Int32(0)\
            \n          Projection: #t1.c1, #t1.c2, #t1.c3\
            \n            TableScan: t1 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);
        Ok(())
    }

    #[test]
    fn test_multiple_filters_down_projections_with_post_processing() -> Result<()> {
        let plan = LogicalPlanBuilder::from(sample_table()?)
            .project(vec![col("c1"), col("c2"), col("c3")])?
            .project(vec![
                col("c1"),
                binary_expr(col("c2"), Operator::Plus, lit(5i32)).alias("c2"),
                col("c3"),
            ])?
            .project(vec![col("c1"), col("c2"), col("c3")])?
            .filter(col("c1").gt(col("c3")))?
            .filter(col("c2").eq(lit(5i32)))?
            .filter(col("c3").lt(lit(5i32)))?
            .project(vec![col("c1"), col("c2"), col("c3")])?
            .build()?;

        let expected = "\
              Projection: #t1.c1, #c2, #t1.c3\
            \n  Filter: #t1.c1 > #t1.c3\
            \n    Projection: #t1.c1, #c2, #t1.c3\
            \n      Filter: #c2 = Int32(5)\
            \n        Projection: #t1.c1, #t1.c2 + Int32(5) AS c2, #t1.c3\
            \n          Filter: #t1.c3 < Int32(5)\
            \n            Projection: #t1.c1, #t1.c2, #t1.c3\
            \n              TableScan: t1 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);
        Ok(())
    }

    // TODO: see `LogicalPlan::Filter` above
    /*
    #[test]
    fn test_complex_filters_down_projections_with_post_processing() -> Result<()> {
        let plan = LogicalPlanBuilder::from(sample_table()?)
            .project(vec![col("c1"), col("c2"), col("c3")])?
            .project(vec![
                col("c1"),
                binary_expr(col("c2"), Operator::Plus, lit(5i32)).alias("c2"),
                col("c3"),
            ])?
            .project(vec![col("c1"), col("c2"), col("c3")])?
            .filter(
                col("c1")
                    .gt(col("c3"))
                    .and(col("c2").eq(lit(5i32)).and(col("c3").lt(lit(5i32)))),
            )?
            .project(vec![col("c1"), col("c2"), col("c3")])?
            .build()?;

        let expected = "\
              Projection: #t1.c1, #c2, #t1.c3\
            \n  Filter: #t1.c1 > #t1.c3\
            \n    Projection: #t1.c1, #c2, #t1.c3\
            \n      Filter: #c2 = Int32(5)\
            \n        Projection: #t1.c1, #t1.c2 + Int32(5) AS c2, #t1.c3\
            \n          Filter: #t1.c3 < Int32(5)\
            \n            Projection: #t1.c1, #t1.c2, #t1.c3\
            \n              TableScan: t1 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);
        Ok(())
    }
    */

    #[test]
    fn test_filters_down_aggregate() -> Result<()> {
        let plan = LogicalPlanBuilder::from(sample_table()?)
            .project(vec![col("c1"), col("c2"), col("c3")])?
            .aggregate(vec![col("c1"), col("c3")], vec![sum(col("c2"))])?
            .project(vec![
                col("c1"),
                col("SUM(t1.c2)").alias("c2_sum"),
                col("c3"),
            ])?
            .filter(col("c2_sum").gt(lit(10i32)))?
            .filter(col("c3").eq(lit(0i32)))?
            .build()?;

        let expected = "\
              Projection: #t1.c1, #SUM(t1.c2) AS c2_sum, #t1.c3\
            \n  Filter: #SUM(t1.c2) > Int32(10)\
            \n    Aggregate: groupBy=[[#t1.c1, #t1.c3]], aggr=[[SUM(#t1.c2)]]\
            \n      Filter: #t1.c3 = Int32(0)\
            \n        Projection: #t1.c1, #t1.c2, #t1.c3\
            \n          TableScan: t1 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);
        Ok(())
    }

    // TODO: see `LogicalPlan::Filter` above
    /*
    #[test]
    fn test_complex_filters_down_aggregate() -> Result<()> {
        let plan = LogicalPlanBuilder::from(sample_table()?)
            .project(vec![col("c1"), col("c2"), col("c3")])?
            .aggregate(vec![col("c1"), col("c3")], vec![sum(col("c2"))])?
            .project(vec![
                col("c1"),
                col("SUM(t1.c2)").alias("c2_sum"),
                col("c3"),
            ])?
            .filter(col("c2_sum").gt(lit(10i32)).and(col("c3").eq(lit(0i32))))?
            .build()?;

        let expected = "\
              Projection: #t1.c1, #SUM(t1.c2) AS c2_sum, #t1.c3\
            \n  Filter: #SUM(t1.c2) > Int32(10)\
            \n    Aggregate: groupBy=[[#t1.c1, #t1.c3]], aggr=[[SUM(#t1.c2)]]\
            \n      Filter: #t1.c3 = Int32(0)\
            \n        Projection: #t1.c1, #t1.c2, #t1.c3\
            \n          TableScan: t1 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);
        Ok(())
    }
    */

    #[test]
    fn test_filter_down_sort() -> Result<()> {
        let plan = LogicalPlanBuilder::from(sample_table()?)
            .project(vec![col("c1"), col("c2"), col("c3")])?
            .sort(vec![col("c2")])?
            .filter(col("c3").eq(lit(5i32)))?
            .build()?;

        let expected = "\
              Sort: #t1.c2\
            \n  Filter: #t1.c3 = Int32(5)\
            \n    Projection: #t1.c1, #t1.c2, #t1.c3\
            \n      TableScan: t1 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);
        Ok(())
    }

    // TODO: see `LogicalPlan::Join` above
    /*
    #[test]
    fn test_filter_down_join() -> Result<()> {
        let plan = LogicalPlanBuilder::from(
                LogicalPlanBuilder::from(make_sample_table("j1", vec!["key", "c1"])?)
                    .project(vec![col("key"), col("c1")])?
                    .build()?
            )
            .join(
                &LogicalPlanBuilder::from(make_sample_table("j2", vec!["key", "c2"])?)
                    .project(vec![col("key"), col("c2")])?
                    .build()?,
                JoinType::Inner,
                (
                    vec![Column::from_name("key")],
                    vec![Column::from_name("key")],
                ),
            )?
            .filter(col("c1").eq(lit(5i32)).and(col("c2").eq(lit(10i32))))?
            .build()?;

        let expected = "\
              Filter: #j2.c2 = Int32(10)\
            \n  Inner Join: #j1.key = #j2.key\
            \n    Filter: #j1.c1 = Int32(5)\
            \n      Projection: #j1.key, #j1.c1\
            \n        TableScan: j1 projection=None\
            \n    Projection: #j2.key, #j2.c2\
            \n      TableScan: j2 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);
        Ok(())
    }
    */

    // TODO: see `LogicalPlan::CrossJoin` above
    /*
    #[test]
    fn test_filter_down_cross_join() -> Result<()> {
        let plan = LogicalPlanBuilder::from(
            LogicalPlanBuilder::from(make_sample_table("j1", vec!["c1"])?)
                .project(vec![col("c1")])?
                .build()?,
        )
        .cross_join(
            &LogicalPlanBuilder::from(make_sample_table("j2", vec!["c2"])?)
                .project(vec![col("c2")])?
                .build()?,
        )?
        .filter(col("c1").eq(lit(5i32)).and(col("c2").eq(lit(10i32))))?
        .build()?;

        let expected = "\
              Filter: #j2.c2 = Int32(10)\
            \n  CrossJoin:\
            \n    Filter: #j1.c1 = Int32(5)\
            \n      Projection: #j1.c1\
            \n        TableScan: j1 projection=None\
            \n    Projection: #j2.c2\
            \n      TableScan: j2 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);
        Ok(())
    }
    */

    #[test]
    fn test_filter_down_cross_join_right_one_row() -> Result<()> {
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
        .filter(col("c1").eq(lit(5i32)))?
        .filter(col("c2").eq(lit(10i32)))?
        .build()?;

        let expected = "\
              Filter: #j2.c2 = Int32(10)\
            \n  CrossJoin:\
            \n    Filter: #j1.c1 = Int32(5)\
            \n      Projection: #j1.c1\
            \n        TableScan: j1 projection=None\
            \n    Projection: #COUNT(UInt8(1)) AS c2, alias=j2\
            \n      Aggregate: groupBy=[[]], aggr=[[COUNT(UInt8(1))]]\
            \n        Projection: #j2.c2\
            \n          TableScan: j2 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);
        Ok(())
    }

    // TODO: see `LogicalPlan::Union` above
    /*
    #[test]
    fn test_filter_down_union_distinct() -> Result<()> {
        let plan = LogicalPlanBuilder::from(
                LogicalPlanBuilder::from(make_sample_table("u1", vec!["c1"])?)
                    .project(vec![col("c1")])?
                    .build()?
            )
            .union_distinct(
                LogicalPlanBuilder::from(make_sample_table("u2", vec!["c2"])?)
                    .project(vec![col("c2")])?
                    .build()?
            )?
            .filter(col("c1").gt(lit(10i32)))?
            .build()?;

        let expected = "\
              Distinct:\
            \n  Union\
            \n    Filter: #c1 > Int32(10)\
            \n      Projection: #u1.c1\
            \n        TableScan: u1 projection=None\
            \n    Filter: #c2 > Int32(10)\
            \n      Projection: #u2.c2\
            \n        TableScan: u2 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);
        Ok(())
    }
    */
}
