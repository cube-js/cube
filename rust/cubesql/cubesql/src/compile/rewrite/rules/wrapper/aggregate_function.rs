use crate::{
    compile::rewrite::{
        agg_fun_expr, agg_fun_expr_within_group, agg_fun_expr_within_group_empty_tail, rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        transforming_rewrite, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        wrapper_replacer_context, AggregateFunctionExprDistinct, AggregateFunctionExprFun,
        LogicalPlanLanguage,
    },
    var, var_iter,
};
use datafusion::physical_plan::aggregates::AggregateFunction;
use egg::Subst;

impl WrapperRules {
    pub fn aggregate_function_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            rewrite(
                "wrapper-push-down-aggregate-function",
                wrapper_pushdown_replacer(
                    agg_fun_expr("?fun", vec!["?expr"], "?distinct", "?within_group"),
                    "?context",
                ),
                agg_fun_expr(
                    "?fun",
                    vec![wrapper_pushdown_replacer("?expr", "?context")],
                    "?distinct",
                    wrapper_pushdown_replacer("?within_group", "?context"),
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
                    wrapper_pullup_replacer(
                        "?within_group",
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
                    agg_fun_expr("?fun", vec!["?expr"], "?distinct", "?within_group"),
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
                self.transform_agg_fun_expr(
                    "?fun",
                    "?distinct",
                    "?within_group",
                    "?input_data_source",
                ),
            ),
            rewrite(
                "wrapper-push-down-aggregate-function-within-group",
                wrapper_pushdown_replacer(agg_fun_expr_within_group("?left", "?right"), "?context"),
                agg_fun_expr_within_group(
                    wrapper_pushdown_replacer("?left", "?context"),
                    wrapper_pushdown_replacer("?right", "?context"),
                ),
            ),
            rewrite(
                "wrapper-push-down-aggregate-function-within-group-empty-tail",
                wrapper_pushdown_replacer(agg_fun_expr_within_group_empty_tail(), "?context"),
                wrapper_pullup_replacer(agg_fun_expr_within_group_empty_tail(), "?context"),
            ),
            rewrite(
                "wrapper-pull-up-aggregate-function-within-group",
                agg_fun_expr_within_group(
                    wrapper_pullup_replacer("?left", "?context"),
                    wrapper_pullup_replacer("?right", "?context"),
                ),
                wrapper_pullup_replacer(agg_fun_expr_within_group("?left", "?right"), "?context"),
            ),
        ]);
    }

    fn transform_agg_fun_expr(
        &self,
        fun_var: &'static str,
        distinct_var: &'static str,
        within_group_var: &'static str,
        input_data_source_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let fun_var = var!(fun_var);
        let distinct_var = var!(distinct_var);
        let within_group_var = var!(within_group_var);
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

                    if !Self::can_rewrite_template(
                        &data_source,
                        &meta,
                        &format!("functions/{}", fun.as_str()),
                    ) {
                        continue;
                    }

                    for within_group_node in &egraph[subst[within_group_var]].nodes {
                        let LogicalPlanLanguage::AggregateFunctionExprWithinGroup(nodes) =
                            within_group_node
                        else {
                            continue;
                        };
                        if nodes.len() > 0 {
                            if !Self::can_rewrite_template(
                                &data_source,
                                &meta,
                                "expressions/within_group",
                            ) {
                                continue;
                            }
                        }
                        return true;
                    }
                }
            }

            false
        }
    }
}
