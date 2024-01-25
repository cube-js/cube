use crate::{
    compile::rewrite::{
        analysis::LogicalPlanAnalysis, fun_expr_var_arg, rewrite, rules::wrapper::WrapperRules,
        scalar_fun_expr_args, scalar_fun_expr_args_empty_tail, transforming_rewrite,
        wrapper_pullup_replacer, wrapper_pushdown_replacer, LogicalPlanLanguage,
        ScalarFunctionExprFun, WrapperPullupReplacerAliasToCube,
    },
    var, var_iter,
};
use egg::{EGraph, Rewrite, Subst};

impl WrapperRules {
    pub fn scalar_function_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        rules.extend(vec![
            rewrite(
                "wrapper-push-down-function",
                wrapper_pushdown_replacer(
                    fun_expr_var_arg("?fun", "?args"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?cube_members",
                ),
                fun_expr_var_arg(
                    "?fun",
                    wrapper_pushdown_replacer(
                        "?args",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                ),
            ),
            transforming_rewrite(
                "wrapper-pull-up-function",
                fun_expr_var_arg(
                    "?fun",
                    wrapper_pullup_replacer(
                        "?args",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                ),
                wrapper_pullup_replacer(
                    fun_expr_var_arg("?fun", "?args"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?cube_members",
                ),
                self.transform_fun_expr("?fun", "?alias_to_cube"),
            ),
            rewrite(
                "wrapper-push-down-scalar-function-args",
                wrapper_pushdown_replacer(
                    scalar_fun_expr_args("?left", "?right"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?cube_members",
                ),
                scalar_fun_expr_args(
                    wrapper_pushdown_replacer(
                        "?left",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                    wrapper_pushdown_replacer(
                        "?right",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                ),
            ),
            rewrite(
                "wrapper-pull-up-scalar-function-args",
                scalar_fun_expr_args(
                    wrapper_pullup_replacer(
                        "?left",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        "?right",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                ),
                wrapper_pullup_replacer(
                    scalar_fun_expr_args("?left", "?right"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?cube_members",
                ),
            ),
            rewrite(
                "wrapper-push-down-scalar-function-empty-tail",
                wrapper_pushdown_replacer(
                    scalar_fun_expr_args_empty_tail(),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?cube_members",
                ),
                wrapper_pullup_replacer(
                    scalar_fun_expr_args_empty_tail(),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?cube_members",
                ),
            ),
        ]);
    }

    fn transform_fun_expr(
        &self,
        fun_var: &'static str,
        alias_to_cube_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let fun_var = var!(fun_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            for alias_to_cube in var_iter!(
                egraph[subst[alias_to_cube_var]],
                WrapperPullupReplacerAliasToCube
            )
            .cloned()
            {
                if let Some(sql_generator) = meta.sql_generator_by_alias_to_cube(&alias_to_cube) {
                    for fun in var_iter!(egraph[subst[fun_var]], ScalarFunctionExprFun).cloned() {
                        if sql_generator
                            .get_sql_templates()
                            .templates
                            .contains_key(&format!("functions/{}", fun.to_string().to_uppercase()))
                        {
                            return true;
                        }
                    }
                }
            }
            false
        }
    }
}
