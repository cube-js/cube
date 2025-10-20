use std::{collections::HashMap, sync::Arc};

use datafusion::{
    arrow::datatypes::DataType,
    error::{DataFusionError, Result},
    logical_expr::{BuiltinScalarFunction, Expr, GroupingSet, Like},
    logical_plan::{
        build_join_schema, build_table_udf_schema,
        plan::{
            Aggregate, Analyze, CreateMemoryTable, CrossJoin, Distinct, Explain, Filter, Join,
            Limit, Partitioning, Projection, Repartition, Sort, Subquery, TableScan, TableUDFs,
            Union, Values, Window,
        },
        union_with_alias, Column, DFSchema, ExprRewritable, ExprSchemable, LogicalPlan,
        LogicalPlanBuilder, Operator,
    },
    optimizer::{
        optimizer::{OptimizerConfig, OptimizerRule},
        simplify_expressions::ConstEvaluator,
    },
    scalar::ScalarValue,
    sql::planner::ContextProvider,
};

use crate::compile::{engine::CubeContext, rewrite::rules::utils::DatePartToken};

/// PlanNormalize optimizer rule walks through the query and applies transformations
/// to normalize the logical plan structure and expressions.
///
/// Currently this includes replacing:
/// - literal granularities in `DatePart` and `DateTrunc` functions
///   with their normalized equivalents
/// - `DATE - DATE` expressions with `DATEDIFF` equivalent
/// - binary operations between a literal string and an expression
///   of a different type to a string casted to that type
/// - binary operations between a timestamp and a date to a timestamp and timestamp operation
/// - IN list expressions where expression being tested is `TIMESTAMP`
///   and values might be `DATE` to values casted to `TIMESTAMP`
pub struct PlanNormalize<'a> {
    cube_ctx: &'a CubeContext,
}

impl<'a> PlanNormalize<'a> {
    pub fn new(cube_ctx: &'a CubeContext) -> Self {
        Self { cube_ctx }
    }
}

impl OptimizerRule for PlanNormalize<'_> {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &OptimizerConfig,
    ) -> Result<LogicalPlan> {
        plan_normalize(self, plan, &mut HashMap::new(), optimizer_config)
    }

    fn name(&self) -> &str {
        "__cube__plan_normalize"
    }
}

/// Recursively optimizes the logical plan, searching for logical plan nodes
/// and expressions that can be normalized.
///
/// `remapped_columns` passed to the function is assumed to be empty unless stated otherwise.
fn plan_normalize(
    optimizer: &PlanNormalize,
    plan: &LogicalPlan,
    remapped_columns: &mut HashMap<Column, Column>,
    optimizer_config: &OptimizerConfig,
) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Projection(Projection {
            expr,
            input,
            schema: _,
            alias,
        }) => {
            let input = plan_normalize(optimizer, input, remapped_columns, optimizer_config)?;
            let schema = input.schema();
            let new_expr = expr
                .iter()
                .map(|expr| {
                    expr_normalize_stacked(
                        optimizer,
                        expr,
                        schema,
                        remapped_columns,
                        optimizer_config,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let alias = alias.clone();

            *remapped_columns = HashMap::new();
            for (expr, new_expr) in expr.iter().zip(new_expr.iter()) {
                let old_name = expr.name(&DFSchema::empty())?;
                let new_name = new_expr.name(&DFSchema::empty())?;
                if old_name != new_name {
                    let old_column = Column {
                        relation: alias.clone(),
                        name: old_name,
                    };
                    let new_column = Column {
                        relation: alias.clone(),
                        name: new_name,
                    };
                    remapped_columns.insert(old_column, new_column);
                }
            }

            LogicalPlanBuilder::from(input)
                .project_with_alias(new_expr, alias)?
                .build()
        }

        LogicalPlan::Filter(Filter { predicate, input }) => {
            let input = plan_normalize(optimizer, input, remapped_columns, optimizer_config)?;
            let schema = input.schema();
            let predicate = expr_normalize_stacked(
                optimizer,
                predicate,
                schema,
                remapped_columns,
                optimizer_config,
            )?;

            LogicalPlanBuilder::from(input).filter(predicate)?.build()
        }

        LogicalPlan::Window(Window {
            input,
            window_expr,
            schema: _,
        }) => {
            let input = plan_normalize(optimizer, input, remapped_columns, optimizer_config)?;
            let schema = input.schema();
            let new_window_expr = window_expr
                .iter()
                .map(|expr| {
                    expr_normalize_stacked(
                        optimizer,
                        expr,
                        schema,
                        remapped_columns,
                        optimizer_config,
                    )
                })
                .collect::<Result<Vec<_>>>()?;

            for (window_expr, new_window_expr) in window_expr.iter().zip(new_window_expr.iter()) {
                let old_name = window_expr.name(&DFSchema::empty())?;
                let new_name = new_window_expr.name(&DFSchema::empty())?;
                if old_name != new_name {
                    let old_column = Column::from_name(old_name);
                    let new_column = Column::from_name(new_name);
                    remapped_columns.insert(old_column, new_column);
                }
            }

            LogicalPlanBuilder::from(input)
                .window(new_window_expr)?
                .build()
        }

        LogicalPlan::Aggregate(Aggregate {
            input,
            group_expr,
            aggr_expr,
            schema: _,
        }) => {
            let input = plan_normalize(optimizer, input, remapped_columns, optimizer_config)?;
            let schema = input.schema();
            let new_group_expr = group_expr
                .iter()
                .map(|expr| {
                    expr_normalize_stacked(
                        optimizer,
                        expr,
                        schema,
                        remapped_columns,
                        optimizer_config,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let new_aggr_expr = aggr_expr
                .iter()
                .map(|expr| {
                    expr_normalize_stacked(
                        optimizer,
                        expr,
                        schema,
                        remapped_columns,
                        optimizer_config,
                    )
                })
                .collect::<Result<Vec<_>>>()?;

            *remapped_columns = HashMap::new();
            for (group_expr, new_group_expr) in group_expr.iter().zip(new_group_expr.iter()) {
                let old_name = group_expr.name(&DFSchema::empty())?;
                let new_name = new_group_expr.name(&DFSchema::empty())?;
                if old_name != new_name {
                    let old_column = Column::from_name(old_name);
                    let new_column = Column::from_name(new_name);
                    remapped_columns.insert(old_column, new_column);
                }
            }
            for (aggr_expr, new_aggr_expr) in aggr_expr.iter().zip(new_aggr_expr.iter()) {
                let old_name = aggr_expr.name(&DFSchema::empty())?;
                let new_name = new_aggr_expr.name(&DFSchema::empty())?;
                if old_name != new_name {
                    let old_column = Column::from_name(old_name);
                    let new_column = Column::from_name(new_name);
                    remapped_columns.insert(old_column, new_column);
                }
            }

            LogicalPlanBuilder::from(input)
                .aggregate(new_group_expr, new_aggr_expr)?
                .build()
        }

        LogicalPlan::Sort(Sort { expr, input }) => {
            let input = plan_normalize(optimizer, input, remapped_columns, optimizer_config)?;
            let schema = input.schema();
            let expr = expr
                .iter()
                .map(|expr| {
                    expr_normalize_stacked(
                        optimizer,
                        expr,
                        schema,
                        remapped_columns,
                        optimizer_config,
                    )
                })
                .collect::<Result<Vec<_>>>()?;

            LogicalPlanBuilder::from(input).sort(expr)?.build()
        }

        LogicalPlan::Join(Join {
            left,
            right,
            on,
            join_type,
            join_constraint,
            schema: _,
            null_equals_null,
        }) => {
            let mut right_remapped_columns = HashMap::new();
            let left = Arc::new(plan_normalize(
                optimizer,
                left,
                remapped_columns,
                optimizer_config,
            )?);
            let right = Arc::new(plan_normalize(
                optimizer,
                right,
                &mut right_remapped_columns,
                optimizer_config,
            )?);
            let on = on
                .iter()
                .map(|(left_column, right_column)| {
                    let left_column = column_normalize(left_column, remapped_columns)?;
                    let right_column = column_normalize(right_column, &right_remapped_columns)?;
                    Ok((left_column, right_column))
                })
                .collect::<Result<Vec<_>>>()?;
            let join_type = *join_type;
            let join_constraint = *join_constraint;
            let schema = Arc::new(build_join_schema(
                &left.schema(),
                &right.schema(),
                &join_type,
            )?);
            let null_equals_null = *null_equals_null;

            remapped_columns.extend(right_remapped_columns);

            Ok(LogicalPlan::Join(Join {
                left,
                right,
                on,
                join_type,
                join_constraint,
                schema,
                null_equals_null,
            }))
        }

        LogicalPlan::CrossJoin(CrossJoin {
            left,
            right,
            schema: _,
        }) => {
            let mut right_remapped_columns = HashMap::new();
            let left = plan_normalize(optimizer, left, remapped_columns, optimizer_config)?;
            let right = plan_normalize(
                optimizer,
                right,
                &mut right_remapped_columns,
                optimizer_config,
            )?;

            remapped_columns.extend(right_remapped_columns);

            LogicalPlanBuilder::from(left).cross_join(&right)?.build()
        }

        LogicalPlan::Repartition(Repartition {
            input,
            partitioning_scheme,
        }) => {
            let input = plan_normalize(optimizer, input, remapped_columns, optimizer_config)?;
            let schema = input.schema();
            let partitioning_scheme = match partitioning_scheme {
                Partitioning::RoundRobinBatch(n) => Partitioning::RoundRobinBatch(*n),
                Partitioning::Hash(exprs, n) => {
                    let exprs = exprs
                        .iter()
                        .map(|expr| {
                            expr_normalize_stacked(
                                optimizer,
                                expr,
                                schema,
                                remapped_columns,
                                optimizer_config,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;

                    Partitioning::Hash(exprs, *n)
                }
            };

            LogicalPlanBuilder::from(input)
                .repartition(partitioning_scheme)?
                .build()
        }

        LogicalPlan::Union(Union {
            inputs,
            schema: _,
            alias,
        }) => {
            let mut plan = None;
            for input in inputs {
                let mut new_remapped_columns = HashMap::new();
                let input = plan_normalize(
                    optimizer,
                    input,
                    &mut new_remapped_columns,
                    optimizer_config,
                )?;
                if let Some(last_plan) = plan.take() {
                    plan = Some(union_with_alias(last_plan, input, alias.clone())?);
                } else {
                    plan = Some(input);
                    *remapped_columns = new_remapped_columns;
                }
            }

            plan.ok_or_else(|| {
                DataFusionError::Internal("Found UNION plan with no inputs".to_string())
            })
        }

        LogicalPlan::TableScan(TableScan {
            table_name,
            source,
            projection,
            projected_schema,
            filters,
            fetch,
        }) => {
            let table_name = table_name.clone();
            let source = Arc::clone(source);
            let projection = projection.clone();
            let projected_schema = Arc::clone(projected_schema);
            let filters = filters
                .iter()
                .map(|expr| {
                    expr_normalize_stacked(
                        optimizer,
                        expr,
                        &projected_schema,
                        remapped_columns,
                        optimizer_config,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let fetch = *fetch;

            Ok(LogicalPlan::TableScan(TableScan {
                table_name,
                source,
                projection,
                projected_schema,
                filters,
                fetch,
            }))
        }

        p @ LogicalPlan::EmptyRelation(_) => Ok(p.clone()),

        LogicalPlan::Limit(Limit { skip, fetch, input }) => {
            let skip = *skip;
            let fetch = *fetch;
            let input = plan_normalize(optimizer, input, remapped_columns, optimizer_config)?;

            LogicalPlanBuilder::from(input).limit(skip, fetch)?.build()
        }

        LogicalPlan::Subquery(Subquery {
            input,
            subqueries,
            types,
            schema: _,
        }) => {
            let input = plan_normalize(optimizer, input, remapped_columns, optimizer_config)?;
            let mut new_subqueries = Vec::with_capacity(subqueries.len());
            for subquery in subqueries {
                let mut subquery_remapped_columns = HashMap::new();
                let new_subquery = plan_normalize(
                    optimizer,
                    subquery,
                    &mut subquery_remapped_columns,
                    optimizer_config,
                )?;
                new_subqueries.push(new_subquery);
                remapped_columns.extend(subquery_remapped_columns);
            }
            let types = types.clone();

            LogicalPlanBuilder::from(input)
                .subquery(new_subqueries, types)?
                .build()
        }

        p @ LogicalPlan::CreateExternalTable(_) => Ok(p.clone()),

        LogicalPlan::CreateMemoryTable(CreateMemoryTable { name, input }) => {
            let name = name.clone();
            let input = Arc::new(plan_normalize(
                optimizer,
                input,
                remapped_columns,
                optimizer_config,
            )?);
            Ok(LogicalPlan::CreateMemoryTable(CreateMemoryTable {
                name,
                input,
            }))
        }

        p @ LogicalPlan::CreateCatalogSchema(_) => Ok(p.clone()),

        p @ LogicalPlan::DropTable(_) => Ok(p.clone()),

        LogicalPlan::Values(Values { schema, values }) => {
            let values = values
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|expr| {
                            expr_normalize_stacked(
                                optimizer,
                                expr,
                                schema,
                                remapped_columns,
                                optimizer_config,
                            )
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;

            LogicalPlanBuilder::values(values)?.build()
        }

        LogicalPlan::Explain(Explain {
            verbose,
            plan,
            stringified_plans: _,
            schema: _,
        }) => {
            let verbose = *verbose;
            let plan = plan_normalize(optimizer, plan, remapped_columns, optimizer_config)?;

            *remapped_columns = HashMap::new();

            LogicalPlanBuilder::from(plan)
                .explain(verbose, false)?
                .build()
        }

        LogicalPlan::Analyze(Analyze {
            verbose,
            input,
            schema: _,
        }) => {
            let verbose = *verbose;
            let input = plan_normalize(optimizer, input, remapped_columns, optimizer_config)?;

            *remapped_columns = HashMap::new();

            LogicalPlanBuilder::from(input)
                .explain(verbose, true)?
                .build()
        }

        LogicalPlan::TableUDFs(TableUDFs {
            expr,
            input,
            schema: _,
        }) => {
            let input = Arc::new(plan_normalize(
                optimizer,
                input,
                remapped_columns,
                optimizer_config,
            )?);
            let schema = input.schema();
            let new_expr = expr
                .iter()
                .map(|expr| {
                    expr_normalize_stacked(
                        optimizer,
                        expr,
                        schema,
                        remapped_columns,
                        optimizer_config,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let new_schema = build_table_udf_schema(&input, &new_expr)?;

            for (expr, new_expr) in expr.iter().zip(new_expr.iter()) {
                let old_name = expr.name(&DFSchema::empty())?;
                let new_name = new_expr.name(&DFSchema::empty())?;
                if old_name != new_name {
                    let old_column = Column::from_name(old_name);
                    let new_column = Column::from_name(new_name);
                    remapped_columns.insert(old_column, new_column);
                }
            }

            Ok(LogicalPlan::TableUDFs(TableUDFs {
                expr: new_expr,
                input,
                schema: new_schema,
            }))
        }

        p @ LogicalPlan::Extension(_) => {
            // TODO: we don't know how to optimize generic `Extension` node,
            // but we might need this if we implement our own `Extension` nodes
            // that might appear in the **initial** plan.
            // Let's clean remapped columns to be sure though.
            *remapped_columns = HashMap::new();
            Ok(p.clone())
        }

        LogicalPlan::Distinct(Distinct { input }) => {
            let input = plan_normalize(optimizer, input, remapped_columns, optimizer_config)?;

            LogicalPlanBuilder::from(input).distinct()?.build()
        }
    }
}

fn expr_normalize_stacked(
    optimizer: &PlanNormalize,
    expr: &Expr,
    schema: &DFSchema,
    remapped_columns: &HashMap<Column, Column>,
    optimizer_config: &OptimizerConfig,
) -> Result<Expr> {
    expr_normalize(optimizer, expr, schema, remapped_columns, optimizer_config).map(|e| *e)
}

/// Recursively normalizes expressions.
#[inline(never)]
fn expr_normalize(
    optimizer: &PlanNormalize,
    expr: &Expr,
    schema: &DFSchema,
    remapped_columns: &HashMap<Column, Column>,
    optimizer_config: &OptimizerConfig,
) -> Result<Box<Expr>> {
    match expr {
        e @ Expr::ScalarVariable(..) => Ok(Box::new(e.clone())),
        e @ Expr::Literal(..) => Ok(Box::new(e.clone())),
        Expr::Alias(expr, alias) => {
            let expr = expr_normalize(optimizer, expr, schema, remapped_columns, optimizer_config)?;
            let alias = alias.clone();
            Ok(Box::new(Expr::Alias(expr, alias)))
        }
        Expr::OuterColumn(data_type, column) => {
            let data_type = data_type.clone();
            let column = column_normalize(column, remapped_columns)?;
            Ok(Box::new(Expr::OuterColumn(data_type, column)))
        }
        Expr::Column(column) => {
            let column = column_normalize(column, remapped_columns)?;
            Ok(Box::new(Expr::Column(column)))
        }
        Expr::Cast { expr, data_type } => {
            let expr = expr_normalize(optimizer, expr, schema, remapped_columns, optimizer_config)?;
            let data_type = data_type.clone();
            Ok(Box::new(Expr::Cast { expr, data_type }))
        }
        Expr::TryCast { expr, data_type } => {
            let expr = expr_normalize(optimizer, expr, schema, remapped_columns, optimizer_config)?;
            let data_type = data_type.clone();
            Ok(Box::new(Expr::TryCast { expr, data_type }))
        }

        // Deep nested node, use as a hot path
        Expr::BinaryExpr { left, op, right } => binary_expr_normalize(
            optimizer,
            left,
            op,
            right,
            schema,
            remapped_columns,
            optimizer_config,
        ),
        // Deep nested node, use as a hot path
        Expr::InList {
            expr,
            list,
            negated,
        } => in_list_expr_normalize(
            optimizer,
            expr,
            list,
            *negated,
            schema,
            remapped_columns,
            optimizer_config,
        ),

        // See expr_normalize_cold_path, for explanation.
        other => {
            expr_normalize_cold_path(optimizer, other, schema, remapped_columns, optimizer_config)
        }
    }
}

/// Cold path for expression normalization, handling less common expression variants.
///
/// This function is separated from `expr_normalize` to reduce stack usage in the hot path.
/// When matching on the large `Expr` enum, LLVM pre-allocates stack space for all variants'
/// temporaries in a single function. This results in ~13KB of stack allocations (215 alloca
/// instructions) per call in release mode. By splitting the enum match into hot and cold paths
/// with `#[inline(never)]`, we ensure that common queries only pay the cost of the hot path
/// (~1.5KB with 29 allocations), while rare expression types are handled here.
///
/// This optimization is critical for deeply nested expressions, as it reduces stack usage
/// by ~87% for typical queries, preventing stack overflow on recursive expression trees.
#[inline(never)]
fn expr_normalize_cold_path(
    optimizer: &PlanNormalize,
    expr: &Expr,
    schema: &DFSchema,
    remapped_columns: &HashMap<Column, Column>,
    optimizer_config: &OptimizerConfig,
) -> Result<Box<Expr>> {
    match expr {
        // These nodes are used in the hot path
        Expr::Alias(..) => unreachable!("Alias in a cold path"),
        Expr::OuterColumn(..) => unreachable!("OuterColumn in a cold path"),
        Expr::Column(..) => unreachable!("Column in a cold path"),
        Expr::ScalarVariable(..) => unreachable!("ScalarVariable in a cold path"),
        Expr::Literal(..) => unreachable!("Literal in a cold path"),
        Expr::BinaryExpr { .. } => unreachable!("BinaryExpr in a cold path"),
        Expr::InList { .. } => unreachable!("InList in a cold path"),
        Expr::Cast { .. } => unreachable!("Cast in a cold path"),
        Expr::TryCast { .. } => unreachable!("TryCast in a cold path"),

        Expr::AnyExpr {
            left,
            op,
            right,
            all,
        } => {
            let left = expr_normalize(optimizer, left, schema, remapped_columns, optimizer_config)?;
            let op = *op;
            let right =
                expr_normalize(optimizer, right, schema, remapped_columns, optimizer_config)?;
            let all = *all;
            Ok(Box::new(Expr::AnyExpr {
                left,
                op,
                right,
                all,
            }))
        }

        Expr::Like(Like {
            negated,
            expr,
            pattern,
            escape_char,
        }) => {
            let negated = *negated;
            let expr = expr_normalize(optimizer, expr, schema, remapped_columns, optimizer_config)?;
            let pattern = expr_normalize(
                optimizer,
                pattern,
                schema,
                remapped_columns,
                optimizer_config,
            )?;
            let escape_char = *escape_char;
            Ok(Box::new(Expr::Like(Like {
                negated,
                expr,
                pattern,
                escape_char,
            })))
        }

        Expr::ILike(Like {
            negated,
            expr,
            pattern,
            escape_char,
        }) => {
            let negated = *negated;
            let expr = expr_normalize(optimizer, expr, schema, remapped_columns, optimizer_config)?;
            let pattern = expr_normalize(
                optimizer,
                pattern,
                schema,
                remapped_columns,
                optimizer_config,
            )?;
            let escape_char = *escape_char;
            Ok(Box::new(Expr::ILike(Like {
                negated,
                expr,
                pattern,
                escape_char,
            })))
        }

        Expr::SimilarTo(Like {
            negated,
            expr,
            pattern,
            escape_char,
        }) => {
            let negated = *negated;
            let expr = expr_normalize(optimizer, expr, schema, remapped_columns, optimizer_config)?;
            let pattern = expr_normalize(
                optimizer,
                pattern,
                schema,
                remapped_columns,
                optimizer_config,
            )?;
            let escape_char = *escape_char;
            Ok(Box::new(Expr::SimilarTo(Like {
                negated,
                expr,
                pattern,
                escape_char,
            })))
        }

        Expr::Not(expr) => {
            let expr = expr_normalize(optimizer, expr, schema, remapped_columns, optimizer_config)?;
            Ok(Box::new(Expr::Not(expr)))
        }

        Expr::IsNotNull(expr) => {
            let expr = expr_normalize(optimizer, expr, schema, remapped_columns, optimizer_config)?;
            Ok(Box::new(Expr::IsNotNull(expr)))
        }

        Expr::IsNull(expr) => {
            let expr = expr_normalize(optimizer, expr, schema, remapped_columns, optimizer_config)?;
            Ok(Box::new(Expr::IsNull(expr)))
        }

        Expr::Negative(expr) => {
            let expr = expr_normalize(optimizer, expr, schema, remapped_columns, optimizer_config)?;
            Ok(Box::new(Expr::Negative(expr)))
        }

        Expr::GetIndexedField { expr, key } => {
            let expr = expr_normalize(optimizer, expr, schema, remapped_columns, optimizer_config)?;
            let key = expr_normalize(optimizer, key, schema, remapped_columns, optimizer_config)?;
            Ok(Box::new(Expr::GetIndexedField { expr, key }))
        }

        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let expr = expr_normalize(optimizer, expr, schema, remapped_columns, optimizer_config)?;
            let negated = *negated;
            let low = expr_normalize(optimizer, low, schema, remapped_columns, optimizer_config)?;
            let high = expr_normalize(optimizer, high, schema, remapped_columns, optimizer_config)?;
            Ok(Box::new(Expr::Between {
                expr,
                negated,
                low,
                high,
            }))
        }

        Expr::Case {
            expr,
            when_then_expr,
            else_expr,
        } => {
            let expr = expr
                .as_ref()
                .map(|e| expr_normalize(optimizer, e, schema, remapped_columns, optimizer_config))
                .transpose()?;
            let when_then_expr = when_then_expr
                .iter()
                .map(|(when, then)| {
                    Ok((
                        expr_normalize(
                            optimizer,
                            when,
                            schema,
                            remapped_columns,
                            optimizer_config,
                        )?,
                        expr_normalize(
                            optimizer,
                            then,
                            schema,
                            remapped_columns,
                            optimizer_config,
                        )?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            let else_expr = else_expr
                .as_ref()
                .map(|e| expr_normalize(optimizer, e, schema, remapped_columns, optimizer_config))
                .transpose()?;
            Ok(Box::new(Expr::Case {
                expr,
                when_then_expr,
                else_expr,
            }))
        }

        Expr::Sort {
            expr,
            asc,
            nulls_first,
        } => {
            let expr = expr_normalize(optimizer, expr, schema, remapped_columns, optimizer_config)?;
            let asc = *asc;
            let nulls_first = *nulls_first;
            Ok(Box::new(Expr::Sort {
                expr,
                asc,
                nulls_first,
            }))
        }

        Expr::ScalarFunction { fun, args } => scalar_function_normalize(
            optimizer,
            fun,
            args,
            schema,
            remapped_columns,
            optimizer_config,
        ),

        Expr::ScalarUDF { fun, args } => {
            let fun = Arc::clone(fun);
            let args = args
                .iter()
                .map(|arg| {
                    expr_normalize_stacked(
                        optimizer,
                        arg,
                        schema,
                        remapped_columns,
                        optimizer_config,
                    )
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(Box::new(Expr::ScalarUDF { fun, args }))
        }

        Expr::TableUDF { fun, args } => {
            let fun = Arc::clone(fun);
            let args = args
                .iter()
                .map(|arg| {
                    expr_normalize_stacked(
                        optimizer,
                        arg,
                        schema,
                        remapped_columns,
                        optimizer_config,
                    )
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(Box::new(Expr::TableUDF { fun, args }))
        }

        Expr::AggregateFunction {
            fun,
            args,
            distinct,
            within_group,
        } => {
            let fun = fun.clone();
            let args = args
                .iter()
                .map(|arg| {
                    expr_normalize_stacked(
                        optimizer,
                        arg,
                        schema,
                        remapped_columns,
                        optimizer_config,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let distinct = *distinct;
            let within_group = within_group
                .as_ref()
                .map(|expr| {
                    expr.iter()
                        .map(|e| {
                            expr_normalize_stacked(
                                optimizer,
                                e,
                                schema,
                                remapped_columns,
                                optimizer_config,
                            )
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?;
            Ok(Box::new(Expr::AggregateFunction {
                fun,
                args,
                distinct,
                within_group,
            }))
        }

        Expr::WindowFunction {
            fun,
            args,
            partition_by,
            order_by,
            window_frame,
        } => {
            let fun = fun.clone();
            let args = args
                .iter()
                .map(|arg| {
                    expr_normalize_stacked(
                        optimizer,
                        arg,
                        schema,
                        remapped_columns,
                        optimizer_config,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let partition_by = partition_by
                .iter()
                .map(|expr| {
                    expr_normalize_stacked(
                        optimizer,
                        expr,
                        schema,
                        remapped_columns,
                        optimizer_config,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let order_by = order_by
                .iter()
                .map(|expr| {
                    expr_normalize_stacked(
                        optimizer,
                        expr,
                        schema,
                        remapped_columns,
                        optimizer_config,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let window_frame = *window_frame;
            Ok(Box::new(Expr::WindowFunction {
                fun,
                args,
                partition_by,
                order_by,
                window_frame,
            }))
        }

        Expr::AggregateUDF { fun, args } => {
            let fun = Arc::clone(fun);
            let args = args
                .iter()
                .map(|arg| {
                    expr_normalize_stacked(
                        optimizer,
                        arg,
                        schema,
                        remapped_columns,
                        optimizer_config,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Box::new(Expr::AggregateUDF { fun, args }))
        }

        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let expr = expr_normalize(optimizer, expr, schema, remapped_columns, optimizer_config)?;
            let subquery = expr_normalize(
                optimizer,
                subquery,
                schema,
                remapped_columns,
                optimizer_config,
            )?;
            let negated = *negated;
            Ok(Box::new(Expr::InSubquery {
                expr,
                subquery,
                negated,
            }))
        }

        e @ Expr::Wildcard => Ok(Box::new(e.clone())),

        e @ Expr::QualifiedWildcard { .. } => Ok(Box::new(e.clone())),

        Expr::GroupingSet(grouping_set) => grouping_set_normalize(
            optimizer,
            grouping_set,
            schema,
            remapped_columns,
            optimizer_config,
        ),
    }
}

/// Normalizes columns, taking remapped columns into account.
#[inline(always)]
fn column_normalize(column: &Column, remapped_columns: &HashMap<Column, Column>) -> Result<Column> {
    if let Some(new_column) = remapped_columns.get(column) {
        return Ok(new_column.clone());
    }
    Ok(column.clone())
}

/// Recursively normalizes scalar functions.
/// Currently this includes replacing literal granularities in `DatePart` and `DateTrunc`
/// functions with their normalized (parsed or lowercase) equivalents.
fn scalar_function_normalize(
    optimizer: &PlanNormalize,
    fun: &BuiltinScalarFunction,
    args: &[Expr],
    schema: &DFSchema,
    remapped_columns: &HashMap<Column, Column>,
    optimizer_config: &OptimizerConfig,
) -> Result<Box<Expr>> {
    let fun = fun.clone();
    let mut args = args
        .iter()
        .map(|arg| {
            expr_normalize_stacked(optimizer, arg, schema, remapped_columns, optimizer_config)
        })
        .collect::<Result<Vec<_>>>()?;

    // If the function is `DatePart` or `DateTrunc` and the first argument is a literal string,
    // normalize the granularity by parsing it and replacing with standartized granularity.
    // If it cannot be parsed, simply convert it to lowercase.
    if matches!(
        fun,
        BuiltinScalarFunction::DatePart | BuiltinScalarFunction::DateTrunc
    ) && args.len() > 0
    {
        if let Expr::Literal(ScalarValue::Utf8(Some(granularity))) = &mut args[0] {
            if let Ok(parsed_granularity) = granularity.parse::<DatePartToken>() {
                *granularity = parsed_granularity.as_str().to_string();
            } else {
                *granularity = granularity.to_ascii_lowercase();
            }
        }
    }

    Ok(Box::new(Expr::ScalarFunction { fun, args }))
}

/// Recursively normalizes grouping sets.
fn grouping_set_normalize(
    optimizer: &PlanNormalize,
    grouping_set: &GroupingSet,
    schema: &DFSchema,
    remapped_columns: &HashMap<Column, Column>,
    optimizer_config: &OptimizerConfig,
) -> Result<Box<Expr>> {
    match grouping_set {
        GroupingSet::Rollup(exprs) => {
            let exprs = exprs
                .iter()
                .map(|expr| {
                    expr_normalize_stacked(
                        optimizer,
                        expr,
                        schema,
                        remapped_columns,
                        optimizer_config,
                    )
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(Box::new(Expr::GroupingSet(GroupingSet::Rollup(exprs))))
        }

        GroupingSet::Cube(exprs) => {
            let exprs = exprs
                .iter()
                .map(|expr| {
                    expr_normalize_stacked(
                        optimizer,
                        expr,
                        schema,
                        remapped_columns,
                        optimizer_config,
                    )
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(Box::new(Expr::GroupingSet(GroupingSet::Cube(exprs))))
        }

        GroupingSet::GroupingSets(exprs) => {
            let exprs = exprs
                .iter()
                .map(|exprs| {
                    exprs
                        .iter()
                        .map(|expr| {
                            expr_normalize_stacked(
                                optimizer,
                                expr,
                                schema,
                                remapped_columns,
                                optimizer_config,
                            )
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(Box::new(Expr::GroupingSet(GroupingSet::GroupingSets(
                exprs,
            ))))
        }
    }
}

/// Recursively normalizes binary expressions.
/// Currently this includes replacing:
/// - `DATE - DATE` expressions with respective `DATEDIFF` function calls
/// - binary operations between a literal string and an expression
///   of a different type to a string casted to that type
/// - binary operations between a timestamp and a date to a timestamp and timestamp operation
#[inline(never)]
fn binary_expr_normalize(
    optimizer: &PlanNormalize,
    left: &Expr,
    op: &Operator,
    right: &Expr,
    schema: &DFSchema,
    remapped_columns: &HashMap<Column, Column>,
    optimizer_config: &OptimizerConfig,
) -> Result<Box<Expr>> {
    let left = expr_normalize(optimizer, left, schema, remapped_columns, optimizer_config)?;
    let op = *op;
    let right = expr_normalize(optimizer, right, schema, remapped_columns, optimizer_config)?;

    // Check if the expression is `DATE - DATE` and replace it with `DATEDIFF` with same semantics.
    // Rationale to do this in optimizer than rewrites is that while the expression
    // can be rewritten to something else, a binary variation still exists and would be picked
    // for SQL push down generation either way. This creates an issue in dialects
    // other than Postgres that would return INTERVAL on `DATE - DATE` expression.
    let left_type = left.get_type(schema)?;
    let right_type = right.get_type(schema)?;
    if left_type == DataType::Date32 && op == Operator::Minus && right_type == DataType::Date32 {
        let fun = optimizer
            .cube_ctx
            .get_function_meta("datediff")
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "Unable to find 'datediff' function in cube context".to_string(),
                )
            })?;
        let args = vec![
            Expr::Literal(ScalarValue::Utf8(Some("day".to_string()))),
            *right,
            *left,
        ];
        return Ok(Box::new(Expr::ScalarUDF { fun, args }));
    }

    // Check if the expression is `TIMESTAMP <op> DATE` or `DATE <op> TIMESTAMP`
    // and cast the `DATE` to `TIMESTAMP` to match the types.
    match (&left_type, &right_type) {
        (DataType::Timestamp(_, _), DataType::Date32) => {
            return Ok(Box::new(Expr::BinaryExpr {
                left,
                op,
                right: evaluate_expr(optimizer, right.cast_to(&left_type, schema)?)?,
            }));
        }
        (DataType::Date32, DataType::Timestamp(_, _)) => {
            return Ok(Box::new(Expr::BinaryExpr {
                left: evaluate_expr(optimizer, left.cast_to(&right_type, schema)?)?,
                op,
                right,
            }));
        }
        _ => (),
    };

    // Check if one side of the binary expression is a literal string. If that's the case,
    // attempt to cast the string to other type based on the operator and type on the other side.
    // If none of the sides is a literal string, the normalization is complete.
    let (other_type, literal_on_the_left) = match (left.as_ref(), right.as_ref()) {
        (_, Expr::Literal(ScalarValue::Utf8(Some(_)))) => (left_type, false),
        (Expr::Literal(ScalarValue::Utf8(Some(_))), _) => (right_type, true),
        _ => return Ok(Box::new(Expr::BinaryExpr { left, op, right })),
    };

    let Some(cast_type) = binary_expr_cast_literal(&op, &other_type) else {
        return Ok(Box::new(Expr::BinaryExpr { left, op, right }));
    };

    if literal_on_the_left {
        Ok(Box::new(Expr::BinaryExpr {
            left: evaluate_expr(optimizer, left.cast_to(&cast_type, schema)?)?,
            op,
            right,
        }))
    } else {
        Ok(Box::new(Expr::BinaryExpr {
            left,
            op,
            right: evaluate_expr(optimizer, right.cast_to(&cast_type, schema)?)?,
        }))
    }
}

/// Returns the type a literal string should be casted to based on the operator
/// and the type on the other side of the binary expression.
/// If no casting is needed, returns `None`.
fn binary_expr_cast_literal(op: &Operator, other_type: &DataType) -> Option<DataType> {
    if other_type == &DataType::Utf8 {
        // If the other side is a string, casting is never required
        return None;
    }

    match op {
        // Comparison operators should cast strings to the other side type
        Operator::Eq
        | Operator::NotEq
        | Operator::Lt
        | Operator::LtEq
        | Operator::Gt
        | Operator::GtEq
        | Operator::IsDistinctFrom
        | Operator::IsNotDistinctFrom => Some(other_type.clone()),
        // Arithmetic operators should cast strings to the other side type
        Operator::Plus
        | Operator::Minus
        | Operator::Multiply
        | Operator::Divide
        | Operator::Modulo
        | Operator::Exponentiate => Some(other_type.clone()),
        // Logical operators operate only on booleans
        Operator::And | Operator::Or => Some(DataType::Boolean),
        // LIKE and regexes operate only on strings, no casting needed
        Operator::Like
        | Operator::NotLike
        | Operator::ILike
        | Operator::NotILike
        | Operator::RegexMatch
        | Operator::RegexIMatch
        | Operator::RegexNotMatch
        | Operator::RegexNotIMatch => None,
        // Bitwise oprators should cast strings to the other side type
        Operator::BitwiseAnd
        | Operator::BitwiseOr
        | Operator::BitwiseShiftRight
        | Operator::BitwiseShiftLeft => Some(other_type.clone()),
        // String concat allows string on either side, no casting needed
        Operator::StringConcat => None,
    }
}

/// Recursively normalizes IN list expressions.
/// Currently this includes replacing:
/// - IN list expressions where expression being tested is `TIMESTAMP`
///   and values are `DATE` to values casted to `TIMESTAMP`
#[inline(never)]
fn in_list_expr_normalize(
    optimizer: &PlanNormalize,
    expr: &Expr,
    list: &[Expr],
    negated: bool,
    schema: &DFSchema,
    remapped_columns: &HashMap<Column, Column>,
    optimizer_config: &OptimizerConfig,
) -> Result<Box<Expr>> {
    let expr = expr_normalize(optimizer, expr, schema, remapped_columns, optimizer_config)?;
    let expr_type = expr.get_type(schema)?;
    let expr_is_timestamp = matches!(expr_type, DataType::Timestamp(_, _));
    let list = list
        .iter()
        .map(|list_expr| {
            let list_expr_normalized = expr_normalize_stacked(
                optimizer,
                list_expr,
                schema,
                remapped_columns,
                optimizer_config,
            )?;
            if !expr_is_timestamp {
                return Ok(list_expr_normalized);
            }

            let list_expr_type = list_expr_normalized.get_type(schema)?;
            if !matches!(list_expr_type, DataType::Date32) {
                return Ok(list_expr_normalized);
            }

            evaluate_expr_stacked(optimizer, list_expr_normalized.cast_to(&expr_type, schema)?)
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Box::new(Expr::InList {
        expr,
        list,
        negated,
    }))
}

fn evaluate_expr_stacked(optimizer: &PlanNormalize, expr: Expr) -> Result<Expr> {
    let execution_props = &optimizer.cube_ctx.state.execution_props;
    let mut const_evaluator = ConstEvaluator::new(execution_props);
    expr.rewrite(&mut const_evaluator)
}

/// Evaluates an expression to a constant if possible.
#[inline(never)]
fn evaluate_expr(optimizer: &PlanNormalize, expr: Expr) -> Result<Box<Expr>> {
    Ok(Box::new(evaluate_expr_stacked(optimizer, expr)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compile::test::{
        get_test_tenant_ctx, rewrite_engine::create_test_postgresql_cube_context, run_async_test,
    };
    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        logical_plan::{col, lit, LogicalPlanBuilder},
    };

    /// Helper function to create a deeply nested OR expression.
    /// This creates a chain like: col = 1 OR col = 2 OR col = 3 OR ... OR col = depth
    fn create_deeply_nested_or_expr(column_name: &str, depth: usize) -> Expr {
        if depth == 0 {
            return col(column_name).eq(lit(0i32));
        }

        let mut expr = col(column_name).eq(lit(0i32));

        for i in 1..depth {
            expr = expr.or(col(column_name).eq(lit(i as i32)));
        }

        expr
    }

    // plan_normalize is recursive, at the same time ExprRewriter from DF is too
    // let's guard it with test, that our code in dev profile is optimized to rewrite N nodes
    #[test]
    fn test_stack_overflow_deeply_nested_or() -> Result<()> {
        run_async_test(async move {
            let meta = get_test_tenant_ctx();
            let cube_ctx = create_test_postgresql_cube_context(meta)
                .await
                .expect("Failed to create cube context");

            // Create a simple table
            let schema = Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Int32, true),
            ]);

            let table_scan = LogicalPlanBuilder::scan_empty(Some("test_table"), &schema, None)
                .expect("Failed to create table scan")
                .build()
                .expect("Failed to build plan");

            // Create a deeply nested OR expression
            let deeply_nested_filter = create_deeply_nested_or_expr("value", 500);

            let plan = LogicalPlanBuilder::from(table_scan)
                .filter(deeply_nested_filter)
                .expect("Failed to add filter")
                .build()
                .expect("Failed to build plan");

            let optimizer = PlanNormalize::new(&cube_ctx);
            optimizer.optimize(&plan, &OptimizerConfig::new()).unwrap();
        });

        Ok(())
    }
}
