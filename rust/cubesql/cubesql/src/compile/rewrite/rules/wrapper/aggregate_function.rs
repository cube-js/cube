use crate::{
    compile::rewrite::{
        agg_fun_expr, rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        transforming_rewrite, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        wrapper_replacer_context, AggregateFunctionExprDistinct, AggregateFunctionExprFun,
    },
    var, var_iter,
};
use datafusion::physical_plan::aggregates::AggregateFunction;
use egg::Subst;

impl WrapperRules {
    pub fn window_function_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            rewrite(
                "wrapper-push-down-aggregate-function",
                wrapper_pushdown_replacer(
                    agg_fun_expr("?fun", vec!["?expr"], "?distinct"),
                    "?context",
                ),
                agg_fun_expr(
                    "?fun",
                    vec![wrapper_pushdown_replacer("?expr", "?context")],
                    "?distinct",
                ),
            ),
            transforming_rewrite(
                "wrapper-pull-up-aggregate-function",
                agg_fun_expr(
                    "?fun",
                    vec![wrapper_pullup_replacer(
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
                    )],
                    "?distinct",
                ),
                wrapper_pullup_replacer(
                    agg_fun_expr("?fun", vec!["?expr"], "?distinct"),
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
                self.transform_agg_fun_expr("?fun", "?distinct", "?input_data_source"),
            ),
        ]);
    }

    fn transform_agg_fun_expr(
        &self,
        fun_var: &'static str,
        distinct_var: &'static str,
        input_data_source_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let fun_var = var!(fun_var);
        let distinct_var = var!(distinct_var);
        let input_data_source_var = var!(input_data_source_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            let Ok(data_source) = Self::get_data_source(egraph, subst, input_data_source_var)
            else {
                return false;
            };

            for fun in var_iter!(egraph[subst[fun_var]], AggregateFunctionExprFun).cloned() {
                for distinct in
                    var_iter!(egraph[subst[distinct_var]], AggregateFunctionExprDistinct)
                {
                    let fun = if *distinct && fun == AggregateFunction::Count {
                        "COUNT_DISTINCT".to_string()
                    } else {
                        fun.to_string()
                    };

                    if Self::can_rewrite_template(
                        &data_source,
                        &meta,
                        &format!("functions/{}", fun.as_str()),
                    ) {
                        return true;
                    }
                }
            }

            false
        }
    }
}
