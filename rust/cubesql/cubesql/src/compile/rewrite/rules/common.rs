use crate::{
    compile::rewrite::{
        agg_fun_expr, alias_expr,
        analysis::{ConstantFolding, LogicalPlanAnalysis, OriginalExpr},
        binary_expr, column_expr, AliasExprAlias, LogicalPlanLanguage,
    },
    var,
};
use datafusion::{logical_plan::DFSchema, scalar::ScalarValue};
use egg::{EGraph, Id, Rewrite, Subst};

use crate::compile::rewrite::{
    rewriter::{RewriteRules, Rewriter},
    transforming_rewrite_with_root,
};

pub struct CommonRules {}

impl RewriteRules for CommonRules {
    fn rewrite_rules(&self) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
        let mut rules = vec![];

        if Rewriter::sql_push_down_enabled() {
            rules.push(transforming_rewrite_with_root(
                "aggregate-expr-division-unwrap",
                agg_fun_expr(
                    "?fun",
                    vec![binary_expr(column_expr("?column"), "/", "?literal")],
                    "?distinct",
                ),
                alias_expr(
                    binary_expr(
                        agg_fun_expr("?fun", vec![column_expr("?column")], "?distinct"),
                        "/",
                        "?literal",
                    ),
                    "?alias",
                ),
                self.transform_aggregate_binary_unwrap("?literal", "?alias"),
            ));
            rules.push(transforming_rewrite_with_root(
                "aggregate-expr-mut-unwrap",
                agg_fun_expr(
                    "?fun",
                    vec![binary_expr(column_expr("?column"), "*", "?literal")],
                    "?distinct",
                ),
                alias_expr(
                    binary_expr(
                        agg_fun_expr("?fun", vec![column_expr("?column")], "?distinct"),
                        "*",
                        "?literal",
                    ),
                    "?alias",
                ),
                self.transform_aggregate_binary_unwrap("?literal", "?alias"),
            ));
        }

        rules
    }
}

impl CommonRules {
    pub fn new() -> Self {
        Self {}
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
            if let Some(ConstantFolding::Scalar(interval)) =
                &egraph[subst[literal_var]].data.constant
            {
                match interval {
                    ScalarValue::Float64(_)
                    | ScalarValue::Float32(_)
                    | ScalarValue::Int64(_)
                    | ScalarValue::Int32(_) => {
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
                    _ => (),
                }
            }

            false
        }
    }
}
