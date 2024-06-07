use crate::{
    compile::rewrite::{
        agg_fun_expr, alias_expr,
        analysis::{ConstantFolding, LogicalPlanAnalysis, OriginalExpr},
        binary_expr, column_expr, fun_expr,
        rewriter::{RewriteRules, Rewriter},
        transform_original_expr_to_alias, transforming_rewrite_with_root, udf_expr, AliasExprAlias,
        LogicalPlanLanguage,
    },
    config::ConfigObj,
    var,
};
use datafusion::{logical_plan::DFSchema, scalar::ScalarValue};
use egg::{EGraph, Id, Rewrite, Subst};
use std::{fmt::Display, sync::Arc};

pub struct CommonRules {
    config_obj: Arc<dyn ConfigObj>,
}

impl RewriteRules for CommonRules {
    fn rewrite_rules(&self) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
        let mut rules = vec![];

        if Rewriter::sql_push_down_enabled() {
            rules.push(transforming_rewrite_with_root(
                "aggregate-expr-division-unwrap",
                agg_fun_expr(
                    "Sum",
                    vec![binary_expr(column_expr("?column"), "/", "?literal")],
                    "?distinct",
                ),
                alias_expr(
                    binary_expr(
                        agg_fun_expr("Sum", vec![column_expr("?column")], "?distinct"),
                        "/",
                        "?literal",
                    ),
                    "?alias",
                ),
                self.transform_aggregate_binary_unwrap("?literal", "?alias"),
            ));
            rules.push(transforming_rewrite_with_root(
                "aggregate-expr-mul-unwrap",
                agg_fun_expr(
                    "Sum",
                    vec![binary_expr(column_expr("?column"), "*", "?literal")],
                    "?distinct",
                ),
                alias_expr(
                    binary_expr(
                        agg_fun_expr("Sum", vec![column_expr("?column")], "?distinct"),
                        "*",
                        "?literal",
                    ),
                    "?alias",
                ),
                self.transform_aggregate_binary_unwrap("?literal", "?alias"),
            ));
        }

        rules.extend(vec![
            // Redshift CHARINDEX to STRPOS
            transforming_rewrite_with_root(
                "redshift-charindex-to-strpos",
                udf_expr("charindex", vec!["?substring", "?string"]),
                alias_expr(
                    self.fun_expr("Strpos", vec!["?string", "?substring"]),
                    "?alias",
                ),
                transform_original_expr_to_alias("?alias"),
            ),
        ]);

        rules
    }
}

impl CommonRules {
    pub fn new(config_obj: Arc<dyn ConfigObj>) -> Self {
        Self { config_obj }
    }

    fn fun_expr(&self, fun_name: impl Display, args: Vec<impl Display>) -> String {
        fun_expr(fun_name, args, self.config_obj.push_down_pull_up_split())
    }

    fn transform_aggregate_binary_unwrap(
        &self,
        literal_var: &'static str,
        alias_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, Id, &mut Subst) -> bool
    {
        let literal_var = var!(literal_var);
        let alias_var = var!(alias_var);

        move |egraph, root, subst| {
            if let Some(ConstantFolding::Scalar(number)) = &egraph[subst[literal_var]].data.constant
            {
                match number {
                    ScalarValue::Float64(Some(_)) => {}
                    ScalarValue::Float32(Some(_)) => {}
                    ScalarValue::Int32(Some(_)) => {}
                    ScalarValue::Int64(Some(_)) => {}
                    _ => return false,
                };

                if let Some(OriginalExpr::Expr(original_expr)) =
                    egraph[root].data.original_expr.as_ref()
                {
                    let alias = original_expr.name(&DFSchema::empty()).unwrap();
                    subst.insert(
                        alias_var,
                        egraph.add(LogicalPlanLanguage::AliasExprAlias(AliasExprAlias(
                            alias.to_string(),
                        ))),
                    );

                    return true;
                } else {
                    return false;
                }
            }

            false
        }
    }
}
