use crate::{
    compile::rewrite::{
        analysis::LogicalPlanAnalysis, rewrite, rules::wrapper::WrapperRules, transforming_rewrite,
        window_fun_expr_var_arg, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        LogicalPlanLanguage, WindowFunctionExprFun, WrapperPullupReplacerAliasToCube,
    },
    var, var_iter,
};
use datafusion::physical_plan::window_functions::WindowFunction;
use egg::{EGraph, Rewrite, Subst};

impl WrapperRules {
    pub fn aggregate_function_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        rules.extend(vec![
            rewrite(
                "wrapper-push-down-window-function",
                wrapper_pushdown_replacer(
                    window_fun_expr_var_arg(
                        "?fun",
                        "?expr",
                        "?partition_by",
                        "?order_by",
                        "?window_frame",
                    ),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?cube_members",
                ),
                window_fun_expr_var_arg(
                    "?fun",
                    wrapper_pushdown_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                    wrapper_pushdown_replacer(
                        "?partition_by",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                    wrapper_pushdown_replacer(
                        "?order_by",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                    "?window_frame",
                ),
            ),
            transforming_rewrite(
                "wrapper-pull-up-window-function",
                window_fun_expr_var_arg(
                    "?fun",
                    wrapper_pullup_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        "?partition_by",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        "?order_by",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                    "?window_frame",
                ),
                wrapper_pullup_replacer(
                    window_fun_expr_var_arg(
                        "?fun",
                        "?expr",
                        "?partition_by",
                        "?order_by",
                        "?window_frame",
                    ),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?cube_members",
                ),
                self.transform_window_fun_expr("?fun", "?alias_to_cube"),
            ),
        ]);

        Self::expr_list_pushdown_pullup_rules(
            rules,
            "wrapper-window-fun-args",
            "WindowFunctionExprArgs",
        );

        Self::expr_list_pushdown_pullup_rules(
            rules,
            "wrapper-window-fun-partition-by",
            "WindowFunctionExprPartitionBy",
        );

        Self::expr_list_pushdown_pullup_rules(
            rules,
            "wrapper-window-fun-order-by",
            "WindowFunctionExprOrderBy",
        );
    }

    fn transform_window_fun_expr(
        &self,
        fun_var: &'static str,
        alias_to_cube_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let fun_var = var!(fun_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let meta = self.cube_context.meta.clone();
        move |egraph, subst| {
            for alias_to_cube in var_iter!(
                egraph[subst[alias_to_cube_var]],
                WrapperPullupReplacerAliasToCube
            )
            .cloned()
            {
                if let Some(sql_generator) = meta.sql_generator_by_alias_to_cube(&alias_to_cube) {
                    if sql_generator
                        .get_sql_templates()
                        .templates
                        .contains_key("expressions/window_function")
                    {
                        for fun in var_iter!(egraph[subst[fun_var]], WindowFunctionExprFun).cloned()
                        {
                            let fun = match fun {
                                WindowFunction::AggregateFunction(agg_fun) => agg_fun.to_string(),
                                WindowFunction::BuiltInWindowFunction(window_fun) => {
                                    window_fun.to_string()
                                }
                            };

                            if sql_generator
                                .get_sql_templates()
                                .templates
                                .contains_key(&format!("functions/{}", fun.as_str()))
                            {
                                return true;
                            }
                        }
                    }
                }
            }
            false
        }
    }
}
