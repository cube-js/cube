use crate::{
    compile::rewrite::{
        rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        transforming_rewrite, udaf_expr_var_arg, udaf_fun_expr_args, udaf_fun_expr_args_empty_tail,
        wrapper_pullup_replacer, wrapper_pushdown_replacer, wrapper_replacer_context,
        AggregateUDFExprFun,
    },
    var, var_iter,
};
use egg::Subst;

impl WrapperRules {
    pub fn udaf_function_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            rewrite(
                "wrapper-push-down-udaf",
                wrapper_pushdown_replacer(
                    udaf_expr_var_arg("?fun", "?args", "?distinct"),
                    "?context",
                ),
                udaf_expr_var_arg(
                    "?fun",
                    wrapper_pushdown_replacer("?args", "?context"),
                    "?distinct",
                ),
            ),
            transforming_rewrite(
                "wrapper-pull-up-udaf",
                udaf_expr_var_arg(
                    "?fun",
                    wrapper_pullup_replacer(
                        "?args",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                    "?distinct",
                ),
                wrapper_pullup_replacer(
                    udaf_expr_var_arg("?fun", "?args", "?distinct"),
                    wrapper_replacer_context(
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                        "?ungrouped_scan",
                        "?input_data_source",
                    ),
                ),
                self.transform_udaf_expr("?fun", "?input_data_source"),
            ),
            rewrite(
                "wrapper-push-down-udaf-args",
                wrapper_pushdown_replacer(udaf_fun_expr_args("?left", "?right"), "?context"),
                udaf_fun_expr_args(
                    wrapper_pushdown_replacer("?left", "?context"),
                    wrapper_pushdown_replacer("?right", "?context"),
                ),
            ),
            rewrite(
                "wrapper-pull-up-udaf-args",
                udaf_fun_expr_args(
                    wrapper_pullup_replacer("?left", "?context"),
                    wrapper_pullup_replacer("?right", "?context"),
                ),
                wrapper_pullup_replacer(udaf_fun_expr_args("?left", "?right"), "?context"),
            ),
            rewrite(
                "wrapper-push-down-udaf-empty-tail",
                wrapper_pushdown_replacer(udaf_fun_expr_args_empty_tail(), "?context"),
                wrapper_pullup_replacer(udaf_fun_expr_args_empty_tail(), "?context"),
            ),
        ]);
    }

    fn transform_udaf_expr(
        &self,
        fun_var: &'static str,
        input_data_source_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let fun_var = var!(fun_var);
        let input_data_source_var = var!(input_data_source_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            let Ok(data_source) = Self::get_data_source(egraph, subst, input_data_source_var)
            else {
                return false;
            };

            for fun in var_iter!(egraph[subst[fun_var]], AggregateUDFExprFun).cloned() {
                if Self::can_rewrite_template(
                    &data_source,
                    &meta,
                    &format!("functions/{}", fun.to_uppercase()),
                ) {
                    return true;
                }
            }
            false
        }
    }
}
