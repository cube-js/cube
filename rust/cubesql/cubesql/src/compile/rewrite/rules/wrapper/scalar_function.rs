use crate::{
    compile::rewrite::{
        fun_expr_var_arg, list_rewrite, list_rewrite_with_vars, rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        scalar_fun_expr_args_empty_tail, scalar_fun_expr_args_legacy, transforming_rewrite,
        wrapper_pullup_replacer, wrapper_pushdown_replacer, wrapper_replacer_context, ListPattern,
        ListType, ScalarFunctionExprFun,
    },
    var, var_iter,
};
use egg::Subst;

impl WrapperRules {
    pub fn scalar_function_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            rewrite(
                "wrapper-push-down-function",
                wrapper_pushdown_replacer(fun_expr_var_arg("?fun", "?args"), "?context"),
                fun_expr_var_arg("?fun", wrapper_pushdown_replacer("?args", "?context")),
            ),
            transforming_rewrite(
                "wrapper-pull-up-function",
                fun_expr_var_arg(
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
                    fun_expr_var_arg("?fun", "?args"),
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
                self.transform_fun_expr("?fun", "?input_data_source"),
            ),
            rewrite(
                "wrapper-push-down-scalar-function-empty-tail",
                wrapper_pushdown_replacer(scalar_fun_expr_args_empty_tail(), "?context"),
                wrapper_pullup_replacer(scalar_fun_expr_args_empty_tail(), "?context"),
            ),
        ]);

        if self.config_obj.push_down_pull_up_split() {
            rules.extend(vec![
                list_rewrite(
                    "wrapper-push-down-scalar-function-args",
                    ListType::ScalarFunctionExprArgs,
                    ListPattern {
                        pattern: wrapper_pushdown_replacer("?args", "?context"),
                        list_var: "?args".to_string(),
                        elem: "?arg".to_string(),
                    },
                    ListPattern {
                        pattern: "?new_args".to_string(),
                        list_var: "?new_args".to_string(),
                        elem: wrapper_pushdown_replacer("?arg", "?context"),
                    },
                ),
                list_rewrite_with_vars(
                    "wrapper-pull-up-scalar-function-args",
                    ListType::ScalarFunctionExprArgs,
                    ListPattern {
                        pattern: "?args".to_string(),
                        list_var: "?args".to_string(),
                        elem: wrapper_pullup_replacer("?arg", "?context"),
                    },
                    ListPattern {
                        pattern: wrapper_pullup_replacer("?new_args", "?context"),
                        list_var: "?new_args".to_string(),
                        elem: "?arg".to_string(),
                    },
                    &["?context"],
                ),
            ]);
        } else {
            rules.extend(vec![
                rewrite(
                    "wrapper-push-down-scalar-function-args",
                    wrapper_pushdown_replacer(
                        scalar_fun_expr_args_legacy("?left", "?right"),
                        "?context",
                    ),
                    scalar_fun_expr_args_legacy(
                        wrapper_pushdown_replacer("?left", "?context"),
                        wrapper_pushdown_replacer("?right", "?context"),
                    ),
                ),
                rewrite(
                    "wrapper-pull-up-scalar-function-args",
                    scalar_fun_expr_args_legacy(
                        wrapper_pullup_replacer("?left", "?context"),
                        wrapper_pullup_replacer("?right", "?context"),
                    ),
                    wrapper_pullup_replacer(
                        scalar_fun_expr_args_legacy("?left", "?right"),
                        "?context",
                    ),
                ),
            ]);
        }
    }

    fn transform_fun_expr(
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

            for fun in var_iter!(egraph[subst[fun_var]], ScalarFunctionExprFun).cloned() {
                if Self::can_rewrite_template(
                    &data_source,
                    &meta,
                    &format!("functions/{}", fun.to_string().to_uppercase()),
                ) {
                    return true;
                }
            }
            false
        }
    }
}
