use crate::{
    compile::rewrite::{
        analysis::LogicalPlanAnalysis, fun_expr, fun_expr_args_empty, list_rewrite,
        list_rewrite_with_vars, rewrite, rules::wrapper::WrapperRules, transforming_rewrite,
        wrapper_pullup_replacer, wrapper_pushdown_replacer, ListPattern, ListType,
        LogicalPlanLanguage, ScalarFunctionExprFun, WrapperPullupReplacerAliasToCube,
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
                    fun_expr("?fun", "?args"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                fun_expr(
                    "?fun",
                    wrapper_pushdown_replacer(
                        "?args",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                ),
            ),
            transforming_rewrite(
                "wrapper-pull-up-function",
                fun_expr(
                    "?fun",
                    wrapper_pullup_replacer(
                        "?args",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                ),
                wrapper_pullup_replacer(
                    fun_expr("?fun", "?args"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                self.transform_fun_expr("?fun", "?alias_to_cube"),
            ),
            list_rewrite(
                "wrapper-push-down-scalar-functions-args",
                ListType::ScalarFunctionExprArgs,
                ListPattern {
                    pattern: wrapper_pushdown_replacer(
                        "?args",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    list_var: "?args".to_string(),
                    elem: "?arg".to_string(),
                },
                ListPattern {
                    pattern: "?new_args".to_string(),
                    list_var: "?new_args".to_string(),
                    elem: wrapper_pushdown_replacer(
                        "?arg",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                },
            ),
            list_rewrite_with_vars(
                "wrapper-pull-up-scalar-function-args",
                ListType::ScalarFunctionExprArgs,
                ListPattern {
                    pattern: "?args".to_string(),
                    list_var: "?args".to_string(),
                    elem: wrapper_pullup_replacer(
                        "?arg",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                },
                ListPattern {
                    pattern: wrapper_pullup_replacer(
                        "?new_args",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    list_var: "?new_args".to_string(),
                    elem: "?arg".to_string(),
                },
                &[
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ],
            ),
            rewrite(
                "wrapper-push-down-scalar-function-empty",
                wrapper_pushdown_replacer(
                    fun_expr_args_empty(),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                wrapper_pullup_replacer(
                    fun_expr_args_empty(),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
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
