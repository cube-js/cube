use std::sync::Arc;

use datafusion::{
    error::Result,
    logical_plan::{
        plan::{CrossJoin, Filter, Join, JoinConstraint, TableScan},
        Column, Expr, JoinType, LogicalPlan, Operator,
    },
    optimizer::optimizer::{OptimizerConfig, OptimizerRule},
};

/// CrossJoinToLeftJoin optimizer rule converts Filter(CrossJoin) patterns
/// back to LeftJoin when appropriate.
///
/// DataFusion converts LEFT JOIN with complex ON conditions (containing OR)
/// to Filter(CrossJoin), which loses the LEFT JOIN semantics. This optimizer
/// detects specific patterns (especially for pg_catalog queries) and converts
/// them back to proper LEFT JOINs.
pub struct CrossJoinToLeftJoin;

impl CrossJoinToLeftJoin {
    pub fn new() -> Self {
        Self
    }
}

impl OptimizerRule for CrossJoinToLeftJoin {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &OptimizerConfig,
    ) -> Result<LogicalPlan> {
        cross_join_to_left_join(plan, optimizer_config)
    }

    fn name(&self) -> &str {
        "__cube__cross_join_to_left_join"
    }
}

fn cross_join_to_left_join(
    plan: &LogicalPlan,
    optimizer_config: &OptimizerConfig,
) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Filter(Filter { predicate, input }) => {
            if let LogicalPlan::CrossJoin(CrossJoin {
                left,
                right,
                schema: _,
            }) = input.as_ref()
            {
                // Check if this is a pg_catalog join that should be LEFT JOIN
                if should_convert_to_left_join(left, right, predicate) {
                    // Extract join columns from the predicate
                    if let Some((left_cols, right_cols)) =
                        extract_join_columns(predicate, left, right)
                    {
                        let left = Arc::new(cross_join_to_left_join(left, optimizer_config)?);
                        let right = Arc::new(cross_join_to_left_join(right, optimizer_config)?);

                        // Build the ON clause as pairs of columns
                        let on: Vec<(Column, Column)> =
                            left_cols.into_iter().zip(right_cols).collect();

                        if on.is_empty() {
                            // Can't convert without join columns - keep as cross join with filter
                            let cross_join = LogicalPlan::CrossJoin(CrossJoin {
                                left,
                                right,
                                schema: input.schema().clone(),
                            });
                            return Ok(LogicalPlan::Filter(Filter {
                                predicate: predicate.clone(),
                                input: Arc::new(cross_join),
                            }));
                        }

                        // Create a LEFT JOIN
                        let join_schema = datafusion::logical_plan::build_join_schema(
                            left.schema(),
                            right.schema(),
                            &JoinType::Left,
                        )?;

                        return Ok(LogicalPlan::Join(Join {
                            left,
                            right,
                            on,
                            join_type: JoinType::Left,
                            join_constraint: JoinConstraint::On,
                            schema: Arc::new(join_schema),
                            null_equals_null: false,
                        }));
                    }
                }
            }

            // Recurse into the input
            let input = cross_join_to_left_join(input, optimizer_config)?;
            Ok(LogicalPlan::Filter(Filter {
                predicate: predicate.clone(),
                input: Arc::new(input),
            }))
        }

        // Recurse into other plan nodes
        LogicalPlan::Projection(proj) => {
            let input = cross_join_to_left_join(&proj.input, optimizer_config)?;
            Ok(LogicalPlan::Projection(
                datafusion::logical_plan::plan::Projection {
                    expr: proj.expr.clone(),
                    input: Arc::new(input),
                    schema: proj.schema.clone(),
                    alias: proj.alias.clone(),
                },
            ))
        }

        LogicalPlan::CrossJoin(CrossJoin {
            left,
            right,
            schema,
        }) => {
            let left = Arc::new(cross_join_to_left_join(left, optimizer_config)?);
            let right = Arc::new(cross_join_to_left_join(right, optimizer_config)?);
            Ok(LogicalPlan::CrossJoin(CrossJoin {
                left,
                right,
                schema: schema.clone(),
            }))
        }

        LogicalPlan::Join(join) => {
            let left = Arc::new(cross_join_to_left_join(&join.left, optimizer_config)?);
            let right = Arc::new(cross_join_to_left_join(&join.right, optimizer_config)?);
            Ok(LogicalPlan::Join(Join {
                left,
                right,
                on: join.on.clone(),
                join_type: join.join_type,
                join_constraint: join.join_constraint,
                schema: join.schema.clone(),
                null_equals_null: join.null_equals_null,
            }))
        }

        // For other nodes, just clone them
        other => Ok(other.clone()),
    }
}

/// Check if this CrossJoin should be converted to a LEFT JOIN.
/// We specifically target pg_catalog queries where we know the original
/// intent was a LEFT JOIN.
fn should_convert_to_left_join(left: &LogicalPlan, right: &LogicalPlan, _predicate: &Expr) -> bool {
    // Check if either side is a pg_catalog table scan
    let left_is_pg_catalog = is_pg_catalog_scan(left);
    let right_is_pg_catalog = is_pg_catalog_scan(right);

    // Convert to LEFT JOIN if both sides are pg_catalog tables
    // This is a conservative heuristic - we only convert known-safe cases
    left_is_pg_catalog && right_is_pg_catalog
}

/// Check if the plan involves pg_catalog tables.
/// We identify pg_catalog tables by checking if their schema contains
/// characteristic column names.
fn is_pg_catalog_scan(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::TableScan(TableScan {
            projected_schema, ..
        }) => is_pg_catalog_schema(projected_schema),
        LogicalPlan::Projection(proj) => is_pg_catalog_scan(&proj.input),
        LogicalPlan::Filter(filter) => is_pg_catalog_scan(&filter.input),
        LogicalPlan::Join(join) => {
            // For joins, check if either side involves pg_catalog tables
            is_pg_catalog_scan(&join.left) || is_pg_catalog_scan(&join.right)
        }
        LogicalPlan::CrossJoin(cross) => {
            is_pg_catalog_scan(&cross.left) || is_pg_catalog_scan(&cross.right)
        }
        _ => false,
    }
}

/// Check if a schema has characteristic pg_catalog column names
fn is_pg_catalog_schema(schema: &datafusion::logical_plan::DFSchemaRef) -> bool {
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

    // pg_type has columns like: oid, typname, typnamespace, typowner, etc.
    let is_pg_type = field_names.contains(&"oid")
        && field_names.contains(&"typname")
        && field_names.contains(&"typnamespace");

    // pg_range has columns like: rngtypid, rngsubtype, rngmultitypid
    let is_pg_range = field_names.contains(&"rngtypid") && field_names.contains(&"rngsubtype");

    // pg_namespace has columns like: oid, nspname
    let is_pg_namespace = field_names.contains(&"oid") && field_names.contains(&"nspname");

    // pg_class has columns like: oid, relname, relnamespace
    let is_pg_class = field_names.contains(&"oid")
        && field_names.contains(&"relname")
        && field_names.contains(&"relnamespace");

    is_pg_type || is_pg_range || is_pg_namespace || is_pg_class
}

/// Extract join columns from a predicate.
/// Returns (left_columns, right_columns) if the predicate contains equi-join conditions.
fn extract_join_columns(
    predicate: &Expr,
    left: &LogicalPlan,
    right: &LogicalPlan,
) -> Option<(Vec<Column>, Vec<Column>)> {
    let left_schema = left.schema();
    let right_schema = right.schema();

    let mut left_cols = Vec::new();
    let mut right_cols = Vec::new();

    // Collect all equi-join conditions from the predicate
    collect_equi_conditions(
        predicate,
        left_schema,
        right_schema,
        &mut left_cols,
        &mut right_cols,
    );

    if left_cols.is_empty() {
        None
    } else {
        Some((left_cols, right_cols))
    }
}

/// Recursively collect equi-join conditions from a predicate
fn collect_equi_conditions(
    predicate: &Expr,
    left_schema: &datafusion::logical_plan::DFSchemaRef,
    right_schema: &datafusion::logical_plan::DFSchemaRef,
    left_cols: &mut Vec<Column>,
    right_cols: &mut Vec<Column>,
) {
    match predicate {
        Expr::BinaryExpr { left, op, right } => {
            match op {
                Operator::Eq => {
                    // Check if this is a column = column condition
                    if let (Expr::Column(left_col), Expr::Column(right_col)) =
                        (left.as_ref(), right.as_ref())
                    {
                        // Determine which column belongs to which side
                        let left_in_left = left_schema.field_from_column(left_col).is_ok();
                        let right_in_right = right_schema.field_from_column(right_col).is_ok();

                        if left_in_left && right_in_right {
                            left_cols.push(left_col.clone());
                            right_cols.push(right_col.clone());
                        } else if right_schema.field_from_column(left_col).is_ok()
                            && left_schema.field_from_column(right_col).is_ok()
                        {
                            // Columns are swapped
                            left_cols.push(right_col.clone());
                            right_cols.push(left_col.clone());
                        }
                    }
                }
                Operator::Or | Operator::And => {
                    // Recurse into OR/AND conditions
                    collect_equi_conditions(left, left_schema, right_schema, left_cols, right_cols);
                    collect_equi_conditions(
                        right,
                        left_schema,
                        right_schema,
                        left_cols,
                        right_cols,
                    );
                }
                _ => {}
            }
        }
        _ => {}
    }
}
