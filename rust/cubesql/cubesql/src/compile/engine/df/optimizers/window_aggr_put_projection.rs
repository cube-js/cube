use std::{collections::HashMap, sync::Arc};

use datafusion::{
    error::Result,
    logical_plan::{
        plan::{Aggregate, Analyze, Explain, Join, Projection, Sort, TableUDFs, Window},
        Column, CreateMemoryTable, CrossJoin, DFField, DFSchema, Distinct, Expr, ExprSchemable,
        Filter, Limit, LogicalPlan, Repartition, Subquery, Union,
    },
    optimizer::optimizer::{OptimizerConfig, OptimizerRule},
    physical_plan::window_functions::{BuiltInWindowFunction, WindowFunction},
};

use super::utils::{get_expr_columns, rewrite_columns};

/// Window Aggr Put Projection optimizer rule searches for WindowAggr plans
/// inside Projection plan, collects the expressions, replacing the WindowAggr expressions
/// with their inner expression equivalent, and produces an extra Projection under WindowAggr plans.
/// This projection is then expected to be pushed down to CubeScan; the data returned
/// by CubeScan will be post-processed with DF window functions.
///
/// TODO: current implementation does not support references to window functions
/// with post-processing containing extra columns. Add support for that at some point.
#[derive(Default)]
pub struct WindowAggrPutProjection {}

impl WindowAggrPutProjection {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for WindowAggrPutProjection {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &OptimizerConfig,
    ) -> Result<LogicalPlan> {
        find_projection_with_window_aggr(self, plan, optimizer_config)
    }

    fn name(&self) -> &str {
        "__cube__window_aggr_put_projection"
    }
}

fn projection_alias() -> &'static str {
    "__cube-internal"
}

/// Recursively finds a projection with window plan inside, then passes
/// the execution to `replace_window_aggrs`. Then rewrites the top projection
/// to account in the changes to the input plans.
fn find_projection_with_window_aggr(
    optimizer: &WindowAggrPutProjection,
    plan: &LogicalPlan,
    optimizer_config: &OptimizerConfig,
) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Projection(Projection {
            expr,
            input,
            schema,
            alias,
        }) => {
            if let LogicalPlan::Window(_) = input.as_ref() {
                let new_projection = Projection {
                    expr: expr.clone(),
                    input: Arc::clone(input),
                    schema: schema.clone(),
                    alias: Some(projection_alias().to_string()),
                };
                let new_plan_and_replacers =
                    replace_window_aggrs(optimizer, Arc::new(new_projection), optimizer_config)?;
                if let Some((new_plan, replacers)) = new_plan_and_replacers {
                    let new_exprs: Result<Vec<_>> = expr
                        .iter()
                        .map(|expr| {
                            let original_name = expr.name(&DFSchema::empty())?;
                            let rewritten_expr = rewrite_columns(expr, &replacers)
                                .map(|expr| match expr {
                                    Expr::Alias(..) => expr,
                                    expr => Expr::Alias(Box::new(expr), original_name.clone()),
                                })
                                .unwrap_or_else(|| {
                                    let alias_name = match &expr {
                                        Expr::Column(column) => column.name.clone(),
                                        _ => original_name.clone(),
                                    };
                                    Expr::Alias(
                                        Box::new(Expr::Column(Column {
                                            relation: Some(projection_alias().to_string()),
                                            name: original_name,
                                        })),
                                        alias_name,
                                    )
                                });
                            Ok(rewritten_expr)
                        })
                        .collect();
                    return Ok(LogicalPlan::Projection(Projection {
                        expr: new_exprs?,
                        input: Arc::new(new_plan),
                        schema: schema.clone(),
                        alias: alias.clone(),
                    }));
                }
            }
            Ok(LogicalPlan::Projection(Projection {
                expr: expr.clone(),
                input: Arc::new(find_projection_with_window_aggr(
                    optimizer,
                    input,
                    optimizer_config,
                )?),
                schema: schema.clone(),
                alias: alias.clone(),
            }))
        }
        LogicalPlan::Filter(Filter { predicate, input }) => Ok(LogicalPlan::Filter(Filter {
            predicate: predicate.clone(),
            input: Arc::new(find_projection_with_window_aggr(
                optimizer,
                input,
                optimizer_config,
            )?),
        })),
        LogicalPlan::Window(Window {
            input,
            window_expr,
            schema,
        }) => Ok(LogicalPlan::Window(Window {
            input: Arc::new(find_projection_with_window_aggr(
                optimizer,
                input,
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
            input: Arc::new(find_projection_with_window_aggr(
                optimizer,
                input,
                optimizer_config,
            )?),
            group_expr: group_expr.clone(),
            aggr_expr: aggr_expr.clone(),
            schema: schema.clone(),
        })),
        LogicalPlan::Sort(Sort { expr, input }) => Ok(LogicalPlan::Sort(Sort {
            expr: expr.clone(),
            input: Arc::new(find_projection_with_window_aggr(
                optimizer,
                input,
                optimizer_config,
            )?),
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
            left: Arc::new(find_projection_with_window_aggr(
                optimizer,
                left,
                optimizer_config,
            )?),
            right: Arc::new(find_projection_with_window_aggr(
                optimizer,
                right,
                optimizer_config,
            )?),
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
            left: Arc::new(find_projection_with_window_aggr(
                optimizer,
                left,
                optimizer_config,
            )?),
            right: Arc::new(find_projection_with_window_aggr(
                optimizer,
                right,
                optimizer_config,
            )?),
            schema: schema.clone(),
        })),
        LogicalPlan::Repartition(Repartition {
            input,
            partitioning_scheme,
        }) => Ok(LogicalPlan::Repartition(Repartition {
            input: Arc::new(find_projection_with_window_aggr(
                optimizer,
                input,
                optimizer_config,
            )?),
            partitioning_scheme: partitioning_scheme.clone(),
        })),
        LogicalPlan::Union(Union {
            inputs,
            schema,
            alias,
        }) => Ok(LogicalPlan::Union(Union {
            inputs: inputs
                .iter()
                .map(|input| find_projection_with_window_aggr(optimizer, input, optimizer_config))
                .collect::<Result<Vec<_>>>()?,
            schema: schema.clone(),
            alias: alias.clone(),
        })),
        plan @ LogicalPlan::TableScan(_) => Ok(plan.clone()),
        plan @ LogicalPlan::EmptyRelation(_) => Ok(plan.clone()),
        LogicalPlan::Limit(Limit { skip, fetch, input }) => Ok(LogicalPlan::Limit(Limit {
            skip: skip.clone(),
            fetch: fetch.clone(),
            input: Arc::new(find_projection_with_window_aggr(
                optimizer,
                input,
                optimizer_config,
            )?),
        })),
        LogicalPlan::Subquery(Subquery {
            subqueries,
            input,
            schema,
        }) => Ok(LogicalPlan::Subquery(Subquery {
            subqueries: subqueries
                .iter()
                .map(|subquery| {
                    find_projection_with_window_aggr(optimizer, subquery, optimizer_config)
                })
                .collect::<Result<Vec<_>>>()?,
            input: Arc::new(find_projection_with_window_aggr(
                optimizer,
                input,
                optimizer_config,
            )?),
            schema: schema.clone(),
        })),
        plan @ LogicalPlan::CreateExternalTable(_) => Ok(plan.clone()),
        LogicalPlan::CreateMemoryTable(CreateMemoryTable { name, input }) => {
            Ok(LogicalPlan::CreateMemoryTable(CreateMemoryTable {
                name: name.clone(),
                input: Arc::new(find_projection_with_window_aggr(
                    optimizer,
                    input,
                    optimizer_config,
                )?),
            }))
        }
        plan @ LogicalPlan::CreateCatalogSchema(_) => Ok(plan.clone()),
        plan @ LogicalPlan::DropTable(_) => Ok(plan.clone()),
        plan @ LogicalPlan::Values(_) => Ok(plan.clone()),
        LogicalPlan::Explain(Explain {
            verbose,
            plan,
            stringified_plans,
            schema,
        }) => Ok(LogicalPlan::Explain(Explain {
            verbose: *verbose,
            plan: Arc::new(find_projection_with_window_aggr(
                optimizer,
                plan,
                optimizer_config,
            )?),
            stringified_plans: stringified_plans.clone(),
            schema: schema.clone(),
        })),
        LogicalPlan::Analyze(Analyze {
            verbose,
            input,
            schema,
        }) => Ok(LogicalPlan::Analyze(Analyze {
            verbose: *verbose,
            input: Arc::new(find_projection_with_window_aggr(
                optimizer,
                input,
                optimizer_config,
            )?),
            schema: schema.clone(),
        })),
        LogicalPlan::TableUDFs(TableUDFs {
            expr,
            input,
            schema,
        }) => Ok(LogicalPlan::TableUDFs(TableUDFs {
            expr: expr.clone(),
            input: Arc::new(find_projection_with_window_aggr(
                optimizer,
                input,
                optimizer_config,
            )?),
            schema: schema.clone(),
        })),
        plan @ LogicalPlan::Extension(_) => Ok(plan.clone()),
        LogicalPlan::Distinct(Distinct { input }) => Ok(LogicalPlan::Distinct(Distinct {
            input: Arc::new(find_projection_with_window_aggr(
                optimizer,
                input,
                optimizer_config,
            )?),
        })),
    }
}

/// Recursively replaces window_aggr expressions in order to account for an extra projection
/// after the window aggrs. Once the bottom of window aggr plans is reached, issues a projection.
fn replace_window_aggrs(
    optimizer: &WindowAggrPutProjection,
    projection: Arc<Projection>,
    optimizer_config: &OptimizerConfig,
) -> Result<Option<(LogicalPlan, HashMap<String, String>)>> {
    match projection.input.as_ref() {
        LogicalPlan::Window(Window {
            input,
            window_expr,
            schema,
        }) => {
            let mut new_projection_exprs = projection.expr.clone();
            let mut new_projection_schema_fields = projection.schema.as_ref().fields().clone();
            let mut replacers = HashMap::new();
            let mut new_window_exprs = vec![];
            let mut new_window_schema_fields = vec![];
            for (i, window_expr) in window_expr.iter().enumerate() {
                let (fun, order_by) = match window_expr {
                    Expr::WindowFunction {
                        fun,
                        args,
                        partition_by,
                        order_by,
                        window_frame,
                    } => {
                        if !args.is_empty() || !partition_by.is_empty() || window_frame.is_some() {
                            // Since we only optimize ORDER BY expressions for now, abort this projection.
                            return Ok(None);
                        }

                        match fun {
                            WindowFunction::BuiltInWindowFunction(
                                BuiltInWindowFunction::Rank
                                | BuiltInWindowFunction::DenseRank
                                | BuiltInWindowFunction::PercentRank,
                            ) => (fun, order_by),
                            _ => {
                                // The function listed above have been tested; others may require
                                // additional support and testing. Abort this projection.
                                return Ok(None);
                            }
                        }
                    }
                    _ => {
                        // We cannot optimize any expression besides WindowFunction. Abort this projection.
                        return Ok(None);
                    }
                };

                // Remove any projection exprs containing references to the window function
                let window_expr_name = window_expr.name(schema)?;
                let window_fn_column = Column::from_name(&window_expr_name);
                for i in (0..new_projection_exprs.len()).rev() {
                    let expr = &new_projection_exprs[i];
                    let expr_columns = get_expr_columns(expr);
                    if expr_columns.contains(&window_fn_column) {
                        new_projection_exprs.remove(i);
                        new_projection_schema_fields.remove(i);
                    }
                }

                // Append the ORDER BY exprs to the list of projection exprs and the schema
                let mut new_order_by = vec![];
                for expr in order_by.iter() {
                    match expr {
                        Expr::Sort {
                            expr,
                            asc,
                            nulls_first,
                        } => {
                            let schema_field_names = new_projection_schema_fields
                                .iter()
                                .map(|field| field.qualified_name())
                                .collect::<Vec<_>>();
                            let expr_name = expr.name(schema)?;
                            if !schema_field_names.contains(&expr_name) {
                                // If an expression is not a part of schema, add it
                                let new_expr = Expr::Alias(expr.clone(), expr_name.clone());
                                new_projection_exprs.push(new_expr);
                                let data_type = expr.get_type(schema)?;
                                let new_field = DFField::new(
                                    Some(projection_alias()),
                                    &expr_name,
                                    data_type,
                                    true,
                                );
                                new_projection_schema_fields.push(new_field);
                            }

                            let column = Expr::Column(Column {
                                relation: Some(projection_alias().to_string()),
                                name: expr_name,
                            });
                            let sort = Expr::Sort {
                                expr: Box::new(column),
                                asc: *asc,
                                nulls_first: *nulls_first,
                            };
                            new_order_by.push(sort);
                        }
                        _ => {
                            // There should be no exprs other than Sort. Aborting this projection.
                            return Ok(None);
                        }
                    }
                }

                // Construct the replacement expr and add the replacer
                let new_window_expr = Expr::WindowFunction {
                    fun: fun.clone(),
                    args: vec![],
                    partition_by: vec![],
                    order_by: new_order_by,
                    window_frame: None,
                };
                let new_window_expr_name = new_window_expr.name(&DFSchema::empty())?;

                let old_field = schema.field(i);
                let new_field = DFField::new(
                    None,
                    &new_window_expr_name,
                    old_field.data_type().clone(),
                    old_field.is_nullable(),
                );

                new_window_schema_fields.push(new_field);
                replacers.insert(window_expr_name, new_window_expr_name);
                new_window_exprs.push(new_window_expr);
            }

            let new_projection = Projection {
                expr: new_projection_exprs,
                input: Arc::clone(input),
                schema: Arc::new(DFSchema::new_with_metadata(
                    new_projection_schema_fields,
                    projection.schema.metadata().clone(),
                )?),
                alias: projection.alias.clone(),
            };
            let (new_plan, new_replacers) = match replace_window_aggrs(
                optimizer,
                Arc::new(new_projection),
                optimizer_config,
            )? {
                Some(s) => s,
                None => return Ok(None),
            };

            // Merge the schema and replacers
            let mut new_schema =
                DFSchema::new_with_metadata(new_window_schema_fields, schema.metadata().clone())?;
            let new_plan_schema = match &new_plan {
                LogicalPlan::Projection(Projection { schema, .. })
                | LogicalPlan::Window(Window { schema, .. }) => schema,
                _ => {
                    // This is impossible. Abort the optimization
                    return Ok(None);
                }
            };
            new_schema.merge(&new_plan_schema);
            replacers.extend(new_replacers);

            let new_window = LogicalPlan::Window(Window {
                input: Arc::new(new_plan),
                window_expr: new_window_exprs,
                schema: Arc::new(new_schema),
            });
            Ok(Some((new_window, replacers)))
        }
        plan => {
            // Encountering any plan other than Window should yield a Projection.
            // Walk through all the fields, re-alias them if required, and modify the schema
            let mut new_exprs = vec![];
            let mut new_fields = vec![];
            let fields = projection.schema.fields();
            for i in 0..fields.len() {
                let expr = projection.expr[i].clone();
                let field = &fields[i];

                if let Some(qualifier) = field.qualifier() {
                    if qualifier.as_str() == projection_alias() {
                        // No need to do anything if we appended the field ourselves
                        new_exprs.push(expr);
                        new_fields.push(field.clone());
                        continue;
                    }
                }

                let expr_name = expr.name(&DFSchema::empty())?;
                let new_field = DFField::new(
                    Some(projection_alias()),
                    &expr_name,
                    field.data_type().clone(),
                    field.is_nullable(),
                );
                new_fields.push(new_field);

                if let Expr::Alias(..) = expr {
                    // Since the expr is aliased, there is no need to wrap it in an alias
                    new_exprs.push(expr);
                    continue;
                }

                let new_expr = Expr::Alias(Box::new(expr), expr_name);
                new_exprs.push(new_expr);
            }

            let new_plan = LogicalPlan::Projection(Projection {
                expr: new_exprs,
                input: Arc::new(plan.clone()),
                schema: Arc::new(DFSchema::new_with_metadata(
                    new_fields,
                    projection.schema.metadata().clone(),
                )?),
                alias: projection.alias.clone(),
            });
            Ok(Some((new_plan, HashMap::new())))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{super::utils::sample_table, *};
    use datafusion::logical_plan::{col, lit, LogicalPlanBuilder};

    fn optimize(plan: &LogicalPlan) -> Result<LogicalPlan> {
        let rule = WindowAggrPutProjection::new();
        rule.optimize(plan, &OptimizerConfig::new())
    }

    fn assert_optimized_plan_eq(plan: LogicalPlan, expected: &str) {
        let optimized_plan = optimize(&plan).expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
    }

    #[test]
    fn test_window_aggr_put_projection() -> Result<()> {
        let plan = LogicalPlanBuilder::from(sample_table()?)
            .filter(col("t1.c3").gt(lit(5i32)))?
            .window(vec![Expr::WindowFunction {
                fun: WindowFunction::BuiltInWindowFunction(BuiltInWindowFunction::DenseRank),
                args: vec![],
                partition_by: vec![],
                order_by: vec![Expr::Sort {
                    expr: Box::new(Expr::Column(Column {
                        relation: Some("t1".to_string()),
                        name: "c2".to_string(),
                    })),
                    asc: true,
                    nulls_first: false,
                }],
                window_frame: None,
            }])?
            .project(vec![
                col("c1"),
                col("DENSE_RANK() ORDER BY [#t1.c2 ASC NULLS LAST]"),
            ])?
            .build()?;

        let expected = "\
              Projection: #__cube-internal.t1.c1 AS c1, #DENSE_RANK() ORDER BY [#__cube-internal.t1.c2 ASC NULLS LAST] AS DENSE_RANK() ORDER BY [#t1.c2 ASC NULLS LAST]\
            \n  WindowAggr: windowExpr=[[DENSE_RANK() ORDER BY [#__cube-internal.t1.c2 ASC NULLS LAST]]]\
            \n    Projection: #t1.c1 AS t1.c1, #t1.c2 AS t1.c2, alias=__cube-internal\
            \n      Filter: #t1.c3 > Int32(5)\
            \n        TableScan: t1 projection=None\
        ";

        assert_optimized_plan_eq(plan, expected);
        Ok(())
    }
}
