use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::Transformed;
use datafusion::common::DFSchema;
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::utils::merge_schema;
use datafusion::logical_expr::{Cast, ExprSchemable, LogicalPlan};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use itertools::Itertools;
use std::fmt::Debug;

#[derive(Debug)]
pub struct RewriteInListLiterals;

impl AnalyzerRule for RewriteInListLiterals {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &ConfigOptions,
    ) -> Result<LogicalPlan, DataFusionError> {
        plan.transform_with_subqueries(|plan| {
            let schema: DFSchema = if let LogicalPlan::TableScan(ts) = &plan {
                let source_schema = DFSchema::try_from_qualified_schema(
                    ts.table_name.clone(),
                    &ts.source.schema(),
                )?;
                source_schema
            } else {
                merge_schema(&plan.inputs())
            };

            plan.map_expressions(|expr| {
                // TODO upgrade DF: We clone inner and castee -- for performance, avoid that.

                // TODO upgrade DF: The problem is, this assumes that the Cast we see was added by
                // type conversion -- what if the query actually has CAST(1 AS Utf8) IN ('1', '2')?
                // Can we put this rewrite ahead of type conversion?
                match &expr {
                    Expr::InList(InList {
                        expr: inner,
                        list,
                        negated,
                    }) => match inner.as_ref() {
                        Expr::Cast(Cast {
                            expr: castee,
                            data_type,
                        }) => {
                            if data_type == &DataType::Utf8 {
                                if list.iter().all(|item| {
                                    matches!(item, Expr::Literal(ScalarValue::Utf8(Some(_))))
                                }) {
                                    let castee_type: DataType = castee.get_type(&schema)?;
                                    return Ok(Transformed::yes(Expr::InList(InList {
                                        expr: castee.clone(),
                                        list: list
                                            .iter()
                                            .map(|ex| {
                                                Expr::Cast(Cast {
                                                    expr: Box::new(ex.clone()),
                                                    data_type: castee_type.clone(),
                                                })
                                            })
                                            .collect_vec(),
                                        negated: *negated,
                                    })));
                                }
                            }
                        }
                        _ => {}
                    },
                    _ => {}
                };
                return Ok(Transformed::no(expr));
            })
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "rewrite_inlist_literals"
    }
}
