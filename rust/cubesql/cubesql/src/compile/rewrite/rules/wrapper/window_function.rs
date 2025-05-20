use crate::{
    compile::rewrite::{
        rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        transforming_rewrite, window_fun_expr_var_arg, wrapper_pullup_replacer,
        wrapper_pushdown_replacer, wrapper_replacer_context, WindowFunctionExprFun,
    },
    var, var_iter,
};
use datafusion::physical_plan::windows::WindowFunction;
use egg::Subst;

impl WrapperRules {
    pub fn aggregate_function_rules(&self, rules: &mut Vec<CubeRewrite>) {
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
                    "?context",
                ),
                window_fun_expr_var_arg(
                    "?fun",
                    wrapper_pushdown_replacer("?expr", "?context"),
                    wrapper_pushdown_replacer("?partition_by", "?context"),
                    wrapper_pushdown_replacer("?order_by", "?context"),
                    "?window_frame",
                ),
            ),
            transforming_rewrite(
                "wrapper-pull-up-window-function",
                window_fun_expr_var_arg(
                    "?fun",
                    wrapper_pullup_replacer(
                        "?expr",
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
                    wrapper_pullup_replacer(
                        "?partition_by",
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
                    wrapper_pullup_replacer(
                        "?order_by",
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
                self.transform_window_fun_expr("?fun", "?input_data_source"),
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

            if !Self::can_rewrite_template(&data_source, &meta, "expressions/window_function") {
                return false;
            }

            for fun in var_iter!(egraph[subst[fun_var]], WindowFunctionExprFun).cloned() {
                let fun = match fun {
                    WindowFunction::AggregateFunction(agg_fun) => agg_fun.to_string(),
                    WindowFunction::BuiltInWindowFunction(window_fun) => window_fun.to_string(),
                };

                if Self::can_rewrite_template(
                    &data_source,
                    &meta,
                    &format!("functions/{}", fun.as_str()),
                ) {
                    return true;
                }
            }
            false
        }
    }
}
