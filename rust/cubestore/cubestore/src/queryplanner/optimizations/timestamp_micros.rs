use crate::queryplanner::udfs::CubeToTimestampMicros;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::DFSchema;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::utils::merge_schema;
use datafusion::logical_expr::{Cast, Expr, ExprSchemable, LogicalPlan, ScalarUDF, TryCast, Union};
use datafusion::optimizer::AnalyzerRule;
use std::sync::Arc;

/// Resolves `CAST(.. AS TIMESTAMP)` and `to_timestamp('..')` to microseconds, the unit tables
/// store, instead of nanoseconds, which end at 2262-04-11. Must run before type coercion.
#[derive(Debug)]
pub struct TimestampMicrosRule {}

impl AnalyzerRule for TimestampMicrosRule {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<LogicalPlan> {
        plan.transform_up_with_subqueries(rewrite_node)
            .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "timestamp_micros"
    }
}

fn rewrite_node(plan: LogicalPlan) -> datafusion::common::Result<Transformed<LogicalPlan>> {
    let mut schema = merge_schema(&plan.inputs());
    if let LogicalPlan::TableScan(ts) = &plan {
        schema.merge(&DFSchema::try_from_qualified_schema(
            ts.table_name.clone(),
            &ts.source.schema(),
        )?);
    }
    let name_preserver = NamePreserver::new(&plan);
    plan.map_expressions(|expr| {
        let original_name = name_preserver.save(&expr);
        expr.transform_up(|e| rewrite_expr(e, &schema))
            .map(|t| t.update_data(|e| original_name.restore(e)))
    })?
    .map_data(|plan| plan.recompute_schema())?
    .map_data(|plan| match plan {
        LogicalPlan::Union(union) => retype_union(union),
        plan => Ok(plan),
    })
}

// `recompute_schema` keeps a same-width `Union`'s schema, which would still declare nanoseconds
// for inputs rewritten to microseconds. Types follow the first input, as in the SQL-built `Union`.
fn retype_union(union: Union) -> datafusion::common::Result<LogicalPlan> {
    let first = union.inputs[0].schema();
    let unchanged = first.fields().len() != union.schema.fields().len()
        || first
            .fields()
            .iter()
            .zip(union.schema.fields())
            .all(|(i, u)| i.data_type() == u.data_type());
    if unchanged {
        return Ok(LogicalPlan::Union(union));
    }
    let fields = union
        .schema
        .iter()
        .zip(first.fields())
        .map(|((qualifier, field), input)| {
            (
                qualifier.cloned(),
                Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(input.data_type().clone()),
                ),
            )
        })
        .collect();
    let schema = DFSchema::new_with_metadata(fields, union.schema.metadata().clone())?;
    Ok(LogicalPlan::Union(Union {
        inputs: union.inputs,
        schema: Arc::new(schema),
    }))
}

fn rewrite_expr(expr: Expr, schema: &DFSchema) -> datafusion::common::Result<Transformed<Expr>> {
    Ok(match expr {
        Expr::Cast(Cast {
            expr,
            data_type: DataType::Timestamp(TimeUnit::Nanosecond, tz),
        }) => Transformed::yes(Expr::Cast(Cast {
            expr,
            data_type: DataType::Timestamp(TimeUnit::Microsecond, tz),
        })),
        Expr::TryCast(TryCast {
            expr,
            data_type: DataType::Timestamp(TimeUnit::Nanosecond, tz),
        }) => Transformed::yes(Expr::TryCast(TryCast {
            expr,
            data_type: DataType::Timestamp(TimeUnit::Microsecond, tz),
        })),
        // A numeric argument means seconds to `to_timestamp` but microseconds to
        // `to_timestamp_micros`, so numbers keep their function.
        Expr::ScalarFunction(ScalarFunction { func, args })
            if func.name() == "to_timestamp"
                && args
                    .first()
                    .map_or(false, |arg| same_meaning_in_micros(arg, schema)) =>
        {
            Transformed::yes(Expr::ScalarFunction(ScalarFunction {
                func: Arc::new(ScalarUDF::new_from_impl(CubeToTimestampMicros::new())),
                args,
            }))
        }
        expr => Transformed::no(expr),
    })
}

fn same_meaning_in_micros(expr: &Expr, schema: &DFSchema) -> bool {
    matches!(
        expr.get_type(schema),
        Ok(DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Timestamp(_, _)
            | DataType::Null)
    )
}
