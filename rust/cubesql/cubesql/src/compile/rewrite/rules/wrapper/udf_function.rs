use crate::{
    compile::rewrite::{
        rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        transforming_rewrite, udf_expr_var_arg, udf_fun_expr_args, udf_fun_expr_args_empty_tail,
        wrapper_pullup_replacer, wrapper_pushdown_replacer, wrapper_replacer_context,
        ScalarUDFExprFun,
    },
    var, var_iter,
};
use egg::Subst;

impl WrapperRules {
    pub fn udf_function_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            rewrite(
                "wrapper-push-down-udf",
                wrapper_pushdown_replacer(udf_expr_var_arg("?fun", "?args"), "?context"),
                udf_expr_var_arg("?fun", wrapper_pushdown_replacer("?args", "?context")),
            ),
            transforming_rewrite(
                "wrapper-pull-up-udf",
                udf_expr_var_arg(
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
                ),
                wrapper_pullup_replacer(
                    udf_expr_var_arg("?fun", "?args"),
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
                self.transform_udf_expr("?fun", "?input_data_source"),
            ),
            rewrite(
                "wrapper-push-down-udf-args",
                wrapper_pushdown_replacer(udf_fun_expr_args("?left", "?right"), "?context"),
                udf_fun_expr_args(
                    wrapper_pushdown_replacer("?left", "?context"),
                    wrapper_pushdown_replacer("?right", "?context"),
                ),
            ),
            rewrite(
                "wrapper-pull-up-udf-args",
                udf_fun_expr_args(
                    wrapper_pullup_replacer("?left", "?context"),
                    wrapper_pullup_replacer("?right", "?context"),
                ),
                wrapper_pullup_replacer(udf_fun_expr_args("?left", "?right"), "?context"),
            ),
            rewrite(
                "wrapper-push-down-udf-empty-tail",
                wrapper_pushdown_replacer(udf_fun_expr_args_empty_tail(), "?context"),
                wrapper_pullup_replacer(udf_fun_expr_args_empty_tail(), "?context"),
            ),
        ]);
    }

    fn transform_udf_expr(
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

            for fun in var_iter!(egraph[subst[fun_var]], ScalarUDFExprFun).cloned() {
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
