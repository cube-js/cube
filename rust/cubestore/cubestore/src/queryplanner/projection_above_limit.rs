use datafusion::error::Result;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_plan::{
    replace_col, Column, DFField, DFSchema, Expr, ExpressionVisitor, LogicalPlan, Recursion,
};
use datafusion::optimizer::optimizer::OptimizerRule;
use datafusion::optimizer::utils;
use itertools::Itertools;
use std::{collections::HashSet, sync::Arc};

macro_rules! pal_debug {
    ($($a:expr),*) => {}; // ($($a:expr),*) => { println!($($a),*) };
}

/// Optimizer that moves Projection calculations above Limit/Sort. This seems useful in combination
/// with Cubestore optimizations like materialize_topk.
pub struct ProjectionAboveLimit {}

impl OptimizerRule for ProjectionAboveLimit {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        _execution_props: &ExecutionProps,
    ) -> Result<LogicalPlan> {
        let after = projection_above_limit(plan);
        pal_debug!("Before: {:?}\nAfter: {:?}", plan, after);
        after
    }

    fn name(&self) -> &str {
        "projection_above_limit"
    }
}

fn projection_above_limit(plan: &LogicalPlan) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Limit { n, input } => {
            let schema: &Arc<DFSchema> = input.schema();

            let lift_up_result = lift_up_expensive_projections(input, HashSet::new());
            pal_debug!("lift_up_res: {:?}", lift_up_result);
            match lift_up_result {
                Ok((inner_plan, None)) => Ok(LogicalPlan::Limit {
                    n: *n,
                    input: Arc::new(inner_plan),
                }),
                Ok((inner_plan, Some(mut projection_exprs))) => {
                    for (projection_expr, original_schema_field) in
                        projection_exprs.iter_mut().zip_eq(schema.fields().iter())
                    {
                        let projection_expr_field =
                            projection_expr.to_field(inner_plan.schema())?;
                        if projection_expr_field.name() != original_schema_field.name() {
                            // The projection expr had columns renamed, and its generated name is
                            // thus not equal to the original. Stick it inside an alias to get it
                            // back to the original name.

                            // This logic that attaches alias could also be performed in the
                            // LogicalPlan::Projection case in lift_up_expensive_projections.

                            let proj_expr = std::mem::replace(projection_expr, Expr::Wildcard);
                            // If the expr were an alias expr, we know we wouldn't have this problem.
                            assert!(!matches!(proj_expr, Expr::Alias(_, _)));

                            *projection_expr = proj_expr.alias(original_schema_field.name());
                        }
                    }

                    let limit = Arc::new(LogicalPlan::Limit {
                        n: *n,
                        input: Arc::new(inner_plan),
                    });
                    let projection = LogicalPlan::Projection {
                        expr: projection_exprs,
                        schema: schema.clone(),
                        input: limit,
                    };
                    Ok(projection)
                }
                Err(e) => {
                    // This case could happen if we had a bug.  So we just abandon the optimization.
                    log::error!(
                        "pull_up_expensive_projections failed with unexpected error: {}",
                        e
                    );

                    Ok(plan.clone())
                }
            }
        }
        _ => {
            // Recurse and look for other Limits under which to search for lazy projections.
            let expr = plan.expressions();

            // apply the optimization to all inputs of the plan
            let inputs = plan.inputs();
            let new_inputs = inputs
                .iter()
                .map(|plan| projection_above_limit(plan))
                .collect::<Result<Vec<_>>>()?;

            utils::from_plan(plan, &expr, &new_inputs)

            // TODO: If we did find a deeper Limit, we might want to move the projection up past
            // more than one Limit.
        }
    }
}

struct ColumnRecorder {
    columns: HashSet<Column>,
}

impl ExpressionVisitor for ColumnRecorder {
    fn pre_visit(mut self, expr: &Expr) -> Result<Recursion<Self>> {
        match expr {
            Expr::Column(c) => {
                self.columns.insert(c.clone());
            }
            Expr::ScalarVariable(_var_names) => {
                // expr_to_columns, with its ColumnNameVisitor includes ScalarVariable for some
                // reason -- but here we wouldn't want that.
            }
            _ => {
                // Do nothing
            }
        }
        Ok(Recursion::Continue(self))
    }
}

struct ExpressionCost {
    computation_depth: usize,
    looks_expensive: bool,
}

impl ExpressionVisitor for ExpressionCost {
    fn pre_visit(mut self, expr: &Expr) -> Result<Recursion<Self>> {
        match expr {
            Expr::Alias(_, _) => {}
            Expr::Column(_) => {
                // Anything that accesses a column inside of a computation is too expensive.
                if self.computation_depth > 0 {
                    self.looks_expensive = true;
                    return Ok(Recursion::Stop(self));
                }
            }
            // Technically could be part of the catch-all case.
            Expr::ScalarVariable(_) | Expr::Literal(_) => {}
            _ => {
                self.computation_depth += 1;
            }
        }
        Ok(Recursion::Continue(self))
    }

    fn post_visit(mut self, expr: &Expr) -> Result<Self> {
        match expr {
            Expr::Alias(_, _) => {}
            Expr::Column(_) => {}
            Expr::ScalarVariable(_) | Expr::Literal(_) => {}
            _ => {
                self.computation_depth -= 1;
            }
        }
        Ok(self)
    }
}

fn looks_expensive(ex: &Expr) -> Result<bool> {
    // Basically anything that accesses any column, in this particular Limit -> Sort -> Projection
    // combination, is something we'd like to lift up above the limit.
    let mut cost_visitor = ExpressionCost {
        computation_depth: 0,
        looks_expensive: false,
    };
    cost_visitor = ex.accept(cost_visitor)?;
    Ok(cost_visitor.looks_expensive)
}

fn lift_up_expensive_projections(
    plan: &LogicalPlan,
    used_columns: HashSet<Column>,
) -> Result<(LogicalPlan, Option<Vec<Expr>>)> {
    match plan {
        LogicalPlan::Sort { expr, input } => {
            let mut recorder = ColumnRecorder {
                columns: used_columns,
            };
            for ex in expr {
                recorder = ex.accept(recorder)?;
            }

            let used_columns = recorder.columns;

            let (new_input, lifted_projection) =
                lift_up_expensive_projections(&input, used_columns)?;
            pal_debug!(
                "Sort sees result:\n{:?};;;{:?};;;",
                new_input,
                lifted_projection
            );
            return Ok((
                LogicalPlan::Sort {
                    expr: expr.clone(),
                    input: Arc::new(new_input),
                },
                lifted_projection,
            ));
        }
        LogicalPlan::Projection {
            expr,
            input,
            schema,
        } => {
            let mut column_recorder = ColumnRecorder {
                columns: HashSet::new(),
            };

            let mut this_projection_exprs = Vec::<usize>::new();

            let mut expensive_expr_list = Vec::<(usize, Expr)>::new();

            // Columns that we are already retaining.  .0 field indexes into `expr`.  .1 field is
            // the Column pointing into `input`.  .2 is the alias, if any.
            let mut already_retained_cols = Vec::<(Column, Option<String>)>::new();

            pal_debug!("Expr length: {}", expr.len());
            for (i, ex) in expr.iter().enumerate() {
                let field: &DFField = schema.field(i);
                if let Expr::Column(col) = ex {
                    pal_debug!("Expr {} added to already_retained_cols: {:?}", i, col);
                    already_retained_cols.push((col.clone(), None));
                } else if let Expr::Alias(box Expr::Column(col), alias) = ex {
                    pal_debug!(
                        "Expr {} added to already_retained_cols (alias {}): {:?}",
                        i,
                        alias,
                        col
                    );
                    already_retained_cols.push((col.clone(), Some(alias.clone())));
                }

                if used_columns.contains(&field.qualified_column()) {
                    pal_debug!(
                        "Expr {}: used_columns contains field {:?}",
                        i,
                        field.qualified_column()
                    );
                    this_projection_exprs.push(i);
                    continue;
                }

                if looks_expensive(ex)? {
                    pal_debug!("Expr {}: Looks expensive.", i);
                    column_recorder = ex.accept(column_recorder)?;
                    expensive_expr_list.push((i, ex.clone()));
                } else {
                    pal_debug!("Expr {}: Not expensive.", i);
                    this_projection_exprs.push(i);
                    continue;
                }
            }
            if expensive_expr_list.is_empty() {
                pal_debug!("No lifted exprs, returning.");
                return Ok((plan.clone(), None));
            }

            // So, we have some expensive exprs.
            // Now push columns of inexpensive exprs.
            let mut expr_builder = vec![None::<Expr>; expr.len()];
            for &ex_index in &this_projection_exprs {
                let column: Column = schema.field(ex_index).qualified_column();
                expr_builder[ex_index] = Some(Expr::Column(column));
            }
            for (ex_index, ex) in expensive_expr_list.iter() {
                expr_builder[*ex_index] = Some(ex.clone());
            }

            let mut lifted_exprs: Vec<Expr> =
                expr_builder.into_iter().map(|ex| ex.unwrap()).collect();

            // expr, but with columns we need to retain for lifted_exprs, and without old exprs.
            let mut new_expr = Vec::<Expr>::new();
            let mut new_field = Vec::<DFField>::new();
            for i in this_projection_exprs {
                new_expr.push(expr[i].clone());
                new_field.push(schema.field(i).clone());
            }

            let mut used_field_names = new_field
                .iter()
                .map(|f| f.name().clone())
                .collect::<HashSet<String>>();

            let mut expensive_expr_column_replacements = Vec::<(Column, Column)>::new();

            let mut generated_col_number = 0;
            let needed_columns = column_recorder.columns;
            'outer: for col in needed_columns {
                pal_debug!("Processing column {:?} in needed_columns", col);

                for (ar_col, ar_alias) in &already_retained_cols {
                    pal_debug!("ar_col {:?} comparing to col {:?}", ar_col, col);
                    if ar_col.eq(&col) {
                        pal_debug!("already_retained_cols already sees it");
                        if let Some(alias) = ar_alias {
                            expensive_expr_column_replacements
                                .push((col.clone(), Column::from_name(alias.clone())));
                        }
                        continue 'outer;
                    }
                }

                // This column isn't already retained, so we need to add it to the projection.

                let schema_index: usize = input.schema().index_of_column(&col)?;
                pal_debug!("Needed column has schema index {}", schema_index);

                let input_field = input.schema().field(schema_index);
                if !used_field_names.contains(input_field.name()) {
                    new_field.push(input_field.clone());
                    new_expr.push(Expr::Column(col));
                    used_field_names.insert(input_field.name().clone());
                } else {
                    let unique_alias: String;
                    'this_loop: loop {
                        let proposed = format!("p_a_l_generated_{}", generated_col_number);
                        generated_col_number += 1;
                        if !used_field_names.contains(&proposed) {
                            unique_alias = proposed;
                            break 'this_loop;
                        }
                    }

                    expensive_expr_column_replacements
                        .push((col.clone(), Column::from_name(unique_alias.clone())));

                    let field = DFField::new(
                        None,
                        &unique_alias,
                        input_field.data_type().clone(),
                        input_field.is_nullable(),
                    );
                    new_field.push(field);
                    new_expr.push(Expr::Column(col).alias(&unique_alias));
                    used_field_names.insert(unique_alias);
                }
            }

            if !expensive_expr_column_replacements.is_empty() {
                let replace_map: std::collections::HashMap<&Column, &Column> =
                    expensive_expr_column_replacements
                        .iter()
                        .map(|pair| (&pair.0, &pair.1))
                        .collect();
                for (ex_index, _) in expensive_expr_list.iter() {
                    let lifted_expr: &mut Expr = &mut lifted_exprs[*ex_index];
                    let expr = std::mem::replace(lifted_expr, Expr::Wildcard);
                    *lifted_expr = replace_col(expr, &replace_map)?;
                }
            }

            pal_debug!("Invoking DFSchema::new");
            let new_schema = DFSchema::new(new_field)?;
            pal_debug!("Created new schema {:?}", new_schema);

            let projection = LogicalPlan::Projection {
                expr: new_expr,
                input: input.clone(),
                schema: Arc::new(new_schema),
            };

            return Ok((projection, Some(lifted_exprs)));
        }
        _ => {
            // Just abandon
            return Ok((plan.clone(), None));
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        logical_plan::{col, lit, when, LogicalPlanBuilder},
    };

    #[test]
    fn basic_plan() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project([col("a"), col("b"), col("c")])?
            .build()?;

        let expected = "Projection: #test.a, #test.b, #test.c\
        \n  TableScan: test projection=None";

        let formatted = format!("{:?}", plan);
        assert_eq!(expected, formatted);

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn sorted_plan() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project([col("a"), col("b"), col("c")])?
            .sort([col("a").sort(true, true)])?
            .build()?;

        let expected = "Sort: #test.a ASC NULLS FIRST\
        \n  Projection: #test.a, #test.b, #test.c\
        \n    TableScan: test projection=None";

        let formatted = format!("{:?}", plan);
        assert_eq!(expected, formatted);

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn limit_sorted_plan() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project([col("a"), col("b"), col("c")])?
            .sort([col("a").sort(true, true)])?
            .limit(50)?
            .build()?;

        let expected = "Limit: 50\
        \n  Sort: #test.a ASC NULLS FIRST\
        \n    Projection: #test.a, #test.b, #test.c\
        \n      TableScan: test projection=None";

        let formatted = format!("{:?}", plan);
        assert_eq!(expected, formatted);

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn limit_sorted_plan_with_aliases() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project([
                col("a").alias("a1"),
                col("b").alias("b1"),
                col("c").alias("c1"),
            ])?
            .sort([col("a1").sort(true, true)])?
            .limit(50)?
            .build()?;

        let expected = "Limit: 50\
        \n  Sort: #a1 ASC NULLS FIRST\
        \n    Projection: #test.a AS a1, #test.b AS b1, #test.c AS c1\
        \n      TableScan: test projection=None";

        let formatted = format!("{:?}", plan);
        assert_eq!(expected, formatted);

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn limit_sorted_plan_with_expensive_expr_optimized() -> Result<()> {
        let table_scan = test_table_scan()?;

        let case_expr = when(col("c").eq(lit(3)), col("b") + lit(2)).otherwise(lit(5))?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project([
                col("a").alias("a1"),
                col("b").alias("b1"),
                case_expr.alias("c1"),
            ])?
            .sort([col("a1").sort(true, true)])?
            .limit(50)?
            .build()?;

        let expected = "Limit: 50\
        \n  Sort: #a1 ASC NULLS FIRST\
        \n    Projection: #test.a AS a1, #test.b AS b1, CASE WHEN #test.c Eq Int32(3) THEN #test.b Plus Int32(2) ELSE Int32(5) END AS c1\
        \n      TableScan: test projection=None";

        let formatted = format!("{:?}", plan);
        assert_eq!(formatted, expected);

        let optimized_expected = "Projection: #a1, #b1, CASE WHEN #test.c Eq Int32(3) THEN #b1 Plus Int32(2) ELSE Int32(5) END AS c1\
        \n  Limit: 50\
        \n    Sort: #a1 ASC NULLS FIRST\
        \n      Projection: #test.a AS a1, #test.b AS b1, #test.c\
        \n        TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, optimized_expected);

        Ok(())
    }

    /// Tests that we re-alias fields in the lifted up projection.
    #[test]
    fn limit_sorted_plan_with_nonaliased_expensive_expr_optimized() -> Result<()> {
        let table_scan = test_table_scan()?;

        let case_expr = when(col("c").eq(lit(3)), col("b") + lit(2)).otherwise(lit(5))?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project([col("a").alias("a1"), col("b").alias("b1"), case_expr])?
            .sort([col("a1").sort(true, true)])?
            .limit(50)?
            .build()?;

        let expected = "Limit: 50\
        \n  Sort: #a1 ASC NULLS FIRST\
        \n    Projection: #test.a AS a1, #test.b AS b1, CASE WHEN #test.c Eq Int32(3) THEN #test.b Plus Int32(2) ELSE Int32(5) END\
        \n      TableScan: test projection=None";

        let formatted = format!("{:?}", plan);
        assert_eq!(formatted, expected);

        let optimized_expected = "Projection: #a1, #b1, CASE WHEN #test.c Eq Int32(3) THEN #b1 Plus Int32(2) ELSE Int32(5) END AS CASE WHEN #test.c Eq Int32(3) THEN #test.b Plus Int32(2) ELSE Int32(5) END\
        \n  Limit: 50\
        \n    Sort: #a1 ASC NULLS FIRST\
        \n      Projection: #test.a AS a1, #test.b AS b1, #test.c\
        \n        TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, optimized_expected);

        Ok(())
    }

    #[test]
    fn limit_sorted_plan_with_nonexpensive_expr() -> Result<()> {
        let table_scan = test_table_scan()?;

        let cheap_expr = lit(3) + lit(4);

        let plan = LogicalPlanBuilder::from(table_scan)
            .project([col("a").alias("a1"), col("b").alias("b1"), cheap_expr])?
            .sort([col("a1").sort(true, true)])?
            .limit(50)?
            .build()?;

        let expected = "Limit: 50\
        \n  Sort: #a1 ASC NULLS FIRST\
        \n    Projection: #test.a AS a1, #test.b AS b1, Int32(3) Plus Int32(4)\
        \n      TableScan: test projection=None";

        let formatted = format!("{:?}", plan);
        assert_eq!(formatted, expected);

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn limit_sorted_plan_with_nonexpensive_aliased_expr() -> Result<()> {
        let table_scan = test_table_scan()?;

        let cheap_expr = lit(3) + lit(4);

        let plan = LogicalPlanBuilder::from(table_scan)
            .project([
                col("a").alias("a1"),
                col("b").alias("b1"),
                cheap_expr.alias("cheap"),
            ])?
            .sort([col("a1").sort(true, true)])?
            .limit(50)?
            .build()?;

        let expected = "Limit: 50\
        \n  Sort: #a1 ASC NULLS FIRST\
        \n    Projection: #test.a AS a1, #test.b AS b1, Int32(3) Plus Int32(4) AS cheap\
        \n      TableScan: test projection=None";

        let formatted = format!("{:?}", plan);
        assert_eq!(formatted, expected);

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn limit_sorted_plan_with_expr_referencing_column() -> Result<()> {
        let table_scan = test_table_scan()?;

        let expensive_expr: Expr = Expr::Negative(Box::new(col("d1")));

        let plan = LogicalPlanBuilder::from(table_scan)
            .project([
                col("a").alias("a1"),
                col("b").alias("b1"),
                col("c").alias("d1"),
            ])?
            .project([col("a1"), col("b1").alias("d1"), expensive_expr])?
            .sort([col("a1").sort(true, true)])?
            .limit(50)?
            .build()?;

        let expected = "Limit: 50\
        \n  Sort: #a1 ASC NULLS FIRST\
        \n    Projection: #a1, #b1 AS d1, (- #d1)\
        \n      Projection: #test.a AS a1, #test.b AS b1, #test.c AS d1\
        \n        TableScan: test projection=None";

        let formatted = format!("{:?}", plan);
        assert_eq!(formatted, expected);

        let optimized_expected = "Projection: #a1, #d1, (- #p_a_l_generated_0) AS (- d1)\
        \n  Limit: 50\
        \n    Sort: #a1 ASC NULLS FIRST\
        \n      Projection: #a1, #b1 AS d1, #d1 AS p_a_l_generated_0\
        \n        Projection: #test.a AS a1, #test.b AS b1, #test.c AS d1\
        \n          TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, optimized_expected);

        Ok(())
    }

    // Code below is from datafusion.

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let optimized_plan = optimize(plan).expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
    }

    fn optimize(plan: &LogicalPlan) -> Result<LogicalPlan> {
        let rule = ProjectionAboveLimit {};
        rule.optimize(plan, &ExecutionProps::new())
    }

    pub fn test_table_scan_with_name(name: &str) -> Result<LogicalPlan> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::UInt32, false),
            Field::new("b", DataType::UInt32, false),
            Field::new("c", DataType::UInt32, false),
        ]);
        LogicalPlanBuilder::scan_empty(Some(name), &schema, None)?.build()
    }

    pub fn test_table_scan() -> Result<LogicalPlan> {
        test_table_scan_with_name("test")
    }
}
