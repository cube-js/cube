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
        union_with_alias, Column, DFSchema, ExprSchemable, LogicalPlan, LogicalPlanBuilder,
        Operator,
    },
    optimizer::optimizer::{OptimizerConfig, OptimizerRule},
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
/// - replacing `DATE - DATE` expressions with `DATEDIFF` equivalent
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
                    expr_normalize(optimizer, expr, schema, remapped_columns, optimizer_config)
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
            let predicate = expr_normalize(
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
                    expr_normalize(optimizer, expr, schema, remapped_columns, optimizer_config)
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
                    expr_normalize(optimizer, expr, schema, remapped_columns, optimizer_config)
                })
                .collect::<Result<Vec<_>>>()?;
            let new_aggr_expr = aggr_expr
                .iter()
                .map(|expr| {
                    expr_normalize(optimizer, expr, schema, remapped_columns, optimizer_config)
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
                    expr_normalize(optimizer, expr, schema, remapped_columns, optimizer_config)
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
                    let left_column = column_normalize(
                        optimizer,
                        left_column,
                        remapped_columns,
                        optimizer_config,
                    )?;
                    let right_column = column_normalize(
                        optimizer,
                        right_column,
                        &right_remapped_columns,
                        optimizer_config,
                    )?;
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
                            expr_normalize(
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
                    expr_normalize(
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
                            expr_normalize(
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
                    expr_normalize(optimizer, expr, schema, remapped_columns, optimizer_config)
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

/// Recursively normalizes expressions.
fn expr_normalize(
    optimizer: &PlanNormalize,
    expr: &Expr,
    schema: &DFSchema,
    remapped_columns: &HashMap<Column, Column>,
    optimizer_config: &OptimizerConfig,
) -> Result<Expr> {
    match expr {
        Expr::Alias(expr, alias) => {
            let expr = Box::new(expr_normalize(
                optimizer,
                expr,
                schema,
                remapped_columns,
                optimizer_config,
            )?);
            let alias = alias.clone();
            Ok(Expr::Alias(expr, alias))
        }

        Expr::OuterColumn(data_type, column) => {
            let data_type = data_type.clone();
            let column = column_normalize(optimizer, column, remapped_columns, optimizer_config)?;
            Ok(Expr::OuterColumn(data_type, column))
        }

        Expr::Column(column) => {
            let column = column_normalize(optimizer, column, remapped_columns, optimizer_config)?;
            Ok(Expr::Column(column))
        }

        e @ Expr::ScalarVariable(..) => Ok(e.clone()),

        e @ Expr::Literal(..) => Ok(e.clone()),

        Expr::BinaryExpr { left, op, right } => binary_expr_normalize(
            optimizer,
            left,
            op,
            right,
            schema,
            remapped_columns,
            optimizer_config,
        ),

        Expr::AnyExpr {
            left,
            op,
            right,
            all,
        } => {
            let left = Box::new(expr_normalize(
                optimizer,
                left,
                schema,
                remapped_columns,
                optimizer_config,
            )?);
            let op = *op;
            let right = Box::new(expr_normalize(
                optimizer,
                right,
                schema,
                remapped_columns,
                optimizer_config,
            )?);
            let all = *all;
            Ok(Expr::AnyExpr {
                left,
                op,
                right,
                all,
            })
        }

        Expr::Like(Like {
            negated,
            expr,
            pattern,
            escape_char,
        }) => {
            let negated = *negated;
            let expr = Box::new(expr_normalize(
                optimizer,
                expr,
                schema,
                remapped_columns,
                optimizer_config,
            )?);
            let pattern = Box::new(expr_normalize(
                optimizer,
                pattern,
                schema,
                remapped_columns,
                optimizer_config,
            )?);
            let escape_char = *escape_char;
            Ok(Expr::Like(Like {
                negated,
                expr,
                pattern,
                escape_char,
            }))
        }

        Expr::ILike(Like {
            negated,
            expr,
            pattern,
            escape_char,
        }) => {
            let negated = *negated;
            let expr = Box::new(expr_normalize(
                optimizer,
                expr,
                schema,
                remapped_columns,
                optimizer_config,
            )?);
            let pattern = Box::new(expr_normalize(
                optimizer,
                pattern,
                schema,
                remapped_columns,
                optimizer_config,
            )?);
            let escape_char = *escape_char;
            Ok(Expr::ILike(Like {
                negated,
                expr,
                pattern,
                escape_char,
            }))
        }

        Expr::SimilarTo(Like {
            negated,
            expr,
            pattern,
            escape_char,
        }) => {
            let negated = *negated;
            let expr = Box::new(expr_normalize(
                optimizer,
                expr,
                schema,
                remapped_columns,
                optimizer_config,
            )?);
            let pattern = Box::new(expr_normalize(
                optimizer,
                pattern,
                schema,
                remapped_columns,
                optimizer_config,
            )?);
            let escape_char = *escape_char;
            Ok(Expr::SimilarTo(Like {
                negated,
                expr,
                pattern,
                escape_char,
            }))
        }

        Expr::Not(expr) => {
            let expr = Box::new(expr_normalize(
                optimizer,
                expr,
                schema,
                remapped_columns,
                optimizer_config,
            )?);
            Ok(Expr::Not(expr))
        }

        Expr::IsNotNull(expr) => {
            let expr = Box::new(expr_normalize(
                optimizer,
                expr,
                schema,
                remapped_columns,
                optimizer_config,
            )?);
            Ok(Expr::IsNotNull(expr))
        }

        Expr::IsNull(expr) => {
            let expr = Box::new(expr_normalize(
                optimizer,
                expr,
                schema,
                remapped_columns,
                optimizer_config,
            )?);
            Ok(Expr::IsNull(expr))
        }

        Expr::Negative(expr) => {
            let expr = Box::new(expr_normalize(
                optimizer,
                expr,
                schema,
                remapped_columns,
                optimizer_config,
            )?);
            Ok(Expr::Negative(expr))
        }

        Expr::GetIndexedField { expr, key } => {
            let expr = Box::new(expr_normalize(
                optimizer,
                expr,
                schema,
                remapped_columns,
                optimizer_config,
            )?);
            let key = Box::new(expr_normalize(
                optimizer,
                key,
                schema,
                remapped_columns,
                optimizer_config,
            )?);
            Ok(Expr::GetIndexedField { expr, key })
        }

        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let expr = Box::new(expr_normalize(
                optimizer,
                expr,
                schema,
                remapped_columns,
                optimizer_config,
            )?);
            let negated = *negated;
            let low = Box::new(expr_normalize(
                optimizer,
                low,
                schema,
                remapped_columns,
                optimizer_config,
            )?);
            let high = Box::new(expr_normalize(
                optimizer,
                high,
                schema,
                remapped_columns,
                optimizer_config,
            )?);
            Ok(Expr::Between {
                expr,
                negated,
                low,
                high,
            })
        }

        Expr::Case {
            expr,
            when_then_expr,
            else_expr,
        } => {
            let expr = expr
                .as_ref()
                .map(|e| {
                    Ok::<_, DataFusionError>(Box::new(expr_normalize(
                        optimizer,
                        e,
                        schema,
                        remapped_columns,
                        optimizer_config,
                    )?))
                })
                .transpose()?;
            let when_then_expr = when_then_expr
                .iter()
                .map(|(when, then)| {
                    Ok((
                        Box::new(expr_normalize(
                            optimizer,
                            when,
                            schema,
                            remapped_columns,
                            optimizer_config,
                        )?),
                        Box::new(expr_normalize(
                            optimizer,
                            then,
                            schema,
                            remapped_columns,
                            optimizer_config,
                        )?),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            let else_expr = else_expr
                .as_ref()
                .map(|e| {
                    Ok::<_, DataFusionError>(Box::new(expr_normalize(
                        optimizer,
                        e,
                        schema,
                        remapped_columns,
                        optimizer_config,
                    )?))
                })
                .transpose()?;
            Ok(Expr::Case {
                expr,
                when_then_expr,
                else_expr,
            })
        }

        Expr::Cast { expr, data_type } => {
            let expr = Box::new(expr_normalize(
                optimizer,
                expr,
                schema,
                remapped_columns,
                optimizer_config,
            )?);
            let data_type = data_type.clone();
            Ok(Expr::Cast { expr, data_type })
        }

        Expr::TryCast { expr, data_type } => {
            let expr = Box::new(expr_normalize(
                optimizer,
                expr,
                schema,
                remapped_columns,
                optimizer_config,
            )?);
            let data_type = data_type.clone();
            Ok(Expr::TryCast { expr, data_type })
        }

        Expr::Sort {
            expr,
            asc,
            nulls_first,
        } => {
            let expr = Box::new(expr_normalize(
                optimizer,
                expr,
                schema,
                remapped_columns,
                optimizer_config,
            )?);
            let asc = *asc;
            let nulls_first = *nulls_first;
            Ok(Expr::Sort {
                expr,
                asc,
                nulls_first,
            })
        }

        Expr::ScalarFunction { fun, args } => {
            let (fun, args) = scalar_function_normalize(
                optimizer,
                fun,
                args,
                schema,
                remapped_columns,
                optimizer_config,
            )?;
            Ok(Expr::ScalarFunction { fun, args })
        }

        Expr::ScalarUDF { fun, args } => {
            let fun = Arc::clone(fun);
            let args = args
                .iter()
                .map(|arg| {
                    expr_normalize(optimizer, arg, schema, remapped_columns, optimizer_config)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Expr::ScalarUDF { fun, args })
        }

        Expr::TableUDF { fun, args } => {
            let fun = Arc::clone(fun);
            let args = args
                .iter()
                .map(|arg| {
                    expr_normalize(optimizer, arg, schema, remapped_columns, optimizer_config)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Expr::TableUDF { fun, args })
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
                    expr_normalize(optimizer, arg, schema, remapped_columns, optimizer_config)
                })
                .collect::<Result<Vec<_>>>()?;
            let distinct = *distinct;
            let within_group = within_group
                .as_ref()
                .map(|expr| {
                    expr.iter()
                        .map(|e| {
                            expr_normalize(optimizer, e, schema, remapped_columns, optimizer_config)
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?;
            Ok(Expr::AggregateFunction {
                fun,
                args,
                distinct,
                within_group,
            })
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
                    expr_normalize(optimizer, arg, schema, remapped_columns, optimizer_config)
                })
                .collect::<Result<Vec<_>>>()?;
            let partition_by = partition_by
                .iter()
                .map(|expr| {
                    expr_normalize(optimizer, expr, schema, remapped_columns, optimizer_config)
                })
                .collect::<Result<Vec<_>>>()?;
            let order_by = order_by
                .iter()
                .map(|expr| {
                    expr_normalize(optimizer, expr, schema, remapped_columns, optimizer_config)
                })
                .collect::<Result<Vec<_>>>()?;
            let window_frame = *window_frame;
            Ok(Expr::WindowFunction {
                fun,
                args,
                partition_by,
                order_by,
                window_frame,
            })
        }

        Expr::AggregateUDF { fun, args } => {
            let fun = Arc::clone(fun);
            let args = args
                .iter()
                .map(|arg| {
                    expr_normalize(optimizer, arg, schema, remapped_columns, optimizer_config)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Expr::AggregateUDF { fun, args })
        }

        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let expr = Box::new(expr_normalize(
                optimizer,
                expr,
                schema,
                remapped_columns,
                optimizer_config,
            )?);
            let list = list
                .iter()
                .map(|e| expr_normalize(optimizer, e, schema, remapped_columns, optimizer_config))
                .collect::<Result<Vec<_>>>()?;
            let negated = *negated;
            Ok(Expr::InList {
                expr,
                list,
                negated,
            })
        }

        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let expr = Box::new(expr_normalize(
                optimizer,
                expr,
                schema,
                remapped_columns,
                optimizer_config,
            )?);
            let subquery = Box::new(expr_normalize(
                optimizer,
                subquery,
                schema,
                remapped_columns,
                optimizer_config,
            )?);
            let negated = *negated;
            Ok(Expr::InSubquery {
                expr,
                subquery,
                negated,
            })
        }

        e @ Expr::Wildcard => Ok(e.clone()),

        e @ Expr::QualifiedWildcard { .. } => Ok(e.clone()),

        Expr::GroupingSet(grouping_set) => {
            let grouping_set = grouping_set_normalize(
                optimizer,
                grouping_set,
                schema,
                remapped_columns,
                optimizer_config,
            )?;
            Ok(Expr::GroupingSet(grouping_set))
        }
    }
}

/// Normalizes columns, taking remapped columns into account.
fn column_normalize(
    _optimizer: &PlanNormalize,
    column: &Column,
    remapped_columns: &HashMap<Column, Column>,
    _optimizer_config: &OptimizerConfig,
) -> Result<Column> {
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
) -> Result<(BuiltinScalarFunction, Vec<Expr>)> {
    let fun = fun.clone();
    let mut args = args
        .iter()
        .map(|arg| expr_normalize(optimizer, arg, schema, remapped_columns, optimizer_config))
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

    Ok((fun, args))
}

/// Recursively normalizes grouping sets.
fn grouping_set_normalize(
    optimizer: &PlanNormalize,
    grouping_set: &GroupingSet,
    schema: &DFSchema,
    remapped_columns: &HashMap<Column, Column>,
    optimizer_config: &OptimizerConfig,
) -> Result<GroupingSet> {
    match grouping_set {
        GroupingSet::Rollup(exprs) => {
            let exprs = exprs
                .iter()
                .map(|expr| {
                    expr_normalize(optimizer, expr, schema, remapped_columns, optimizer_config)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(GroupingSet::Rollup(exprs))
        }

        GroupingSet::Cube(exprs) => {
            let exprs = exprs
                .iter()
                .map(|expr| {
                    expr_normalize(optimizer, expr, schema, remapped_columns, optimizer_config)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(GroupingSet::Cube(exprs))
        }

        GroupingSet::GroupingSets(exprs) => {
            let exprs = exprs
                .iter()
                .map(|exprs| {
                    Ok(exprs
                        .iter()
                        .map(|expr| {
                            expr_normalize(
                                optimizer,
                                expr,
                                schema,
                                remapped_columns,
                                optimizer_config,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(GroupingSet::GroupingSets(exprs))
        }
    }
}

/// Recursively normalizes binary expressions.
/// Currently this includes replacing `DATE - DATE` expressions
/// with respective `DATEDIFF` function calls.
fn binary_expr_normalize(
    optimizer: &PlanNormalize,
    left: &Expr,
    op: &Operator,
    right: &Expr,
    schema: &DFSchema,
    remapped_columns: &HashMap<Column, Column>,
    optimizer_config: &OptimizerConfig,
) -> Result<Expr> {
    let left = Box::new(expr_normalize(
        optimizer,
        left,
        schema,
        remapped_columns,
        optimizer_config,
    )?);
    let op = *op;
    let right = Box::new(expr_normalize(
        optimizer,
        right,
        schema,
        remapped_columns,
        optimizer_config,
    )?);

    // Check if the expression is `DATE - DATE` and replace it with `DATEDIFF` with same semantics.
    // Rationale to do this in optimizer than rewrites is that while the expression
    // can be rewritten to something else, a binary variation still exists and would be picked
    // for SQL push down generation either way. This creates an issue in dialects
    // other than Postgres that would return INTERVAL on `DATE - DATE` expression.
    if left.get_type(schema)? == DataType::Date32
        && op == Operator::Minus
        && right.get_type(schema)? == DataType::Date32
    {
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
        return Ok(Expr::ScalarUDF { fun, args });
    }

    Ok(Expr::BinaryExpr { left, op, right })
}
