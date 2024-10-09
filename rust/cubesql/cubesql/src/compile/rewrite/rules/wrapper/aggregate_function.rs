use crate::{
    compile::rewrite::{
        agg_fun_expr, agg_fun_expr_within_group, agg_fun_expr_within_group_empty_tail,
        analysis::LogicalPlanAnalysis, rewrite, rules::wrapper::WrapperRules, transforming_rewrite,
        wrapper_pullup_replacer, wrapper_pushdown_replacer, AggregateFunctionExprDistinct,
        AggregateFunctionExprFun, LogicalPlanLanguage, WrapperPullupReplacerAliasToCube,
    },
    var, var_iter,
};
use datafusion::physical_plan::aggregates::AggregateFunction;
use egg::{EGraph, Rewrite, Subst};

impl WrapperRules {
    pub fn aggregate_function_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        rules.extend(vec![
            rewrite(
                "wrapper-push-down-aggregate-function",
                wrapper_pushdown_replacer(
                    agg_fun_expr("?fun", vec!["?expr"], "?distinct", "?within_group"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                agg_fun_expr(
                    "?fun",
                    vec![wrapper_pushdown_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    )],
                    "?distinct",
                    wrapper_pushdown_replacer(
                        "?within_group",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                ),
            ),
            transforming_rewrite(
                "wrapper-pull-up-aggregate-function",
                agg_fun_expr(
                    "?fun",
                    vec![wrapper_pullup_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    )],
                    "?distinct",
                    wrapper_pullup_replacer(
                        "?within_group",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                ),
                wrapper_pullup_replacer(
                    agg_fun_expr("?fun", vec!["?expr"], "?distinct", "?within_group"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                self.transform_agg_fun_expr("?fun", "?distinct", "?alias_to_cube", "?within_group"),
            ),
            rewrite(
                "wrapper-push-down-aggregate-function-within-group",
                wrapper_pushdown_replacer(
                    agg_fun_expr_within_group("?left", "?right"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                agg_fun_expr_within_group(
                    wrapper_pushdown_replacer(
                        "?left",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    wrapper_pushdown_replacer(
                        "?right",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                ),
            ),
            rewrite(
                "wrapper-push-down-aggregate-function-within-group-empty-tail",
                wrapper_pushdown_replacer(
                    agg_fun_expr_within_group_empty_tail(),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                wrapper_pullup_replacer(
                    agg_fun_expr_within_group_empty_tail(),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
            ),
            rewrite(
                "wrapper-pull-up-aggregate-function-within-group",
                agg_fun_expr_within_group(
                    wrapper_pullup_replacer(
                        "?left",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        "?right",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                ),
                wrapper_pullup_replacer(
                    agg_fun_expr_within_group("?left", "?right"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
            ),
        ]);
    }

    fn transform_agg_fun_expr(
        &self,
        fun_var: &'static str,
        distinct_var: &'static str,
        alias_to_cube_var: &'static str,
        within_group_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let fun_var = var!(fun_var);
        let distinct_var = var!(distinct_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let within_group_var = var!(within_group_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            for alias_to_cube in var_iter!(
                egraph[subst[alias_to_cube_var]],
                WrapperPullupReplacerAliasToCube
            )
            .cloned()
            {
                if let Some(sql_generator) = meta.sql_generator_by_alias_to_cube(&alias_to_cube) {
                    for fun in var_iter!(egraph[subst[fun_var]], AggregateFunctionExprFun).cloned()
                    {
                        for distinct in
                            var_iter!(egraph[subst[distinct_var]], AggregateFunctionExprDistinct)
                        {
                            let fun = if *distinct && fun == AggregateFunction::Count {
                                "COUNT_DISTINCT".to_string()
                            } else {
                                fun.to_string()
                            };

                            if !sql_generator
                                .get_sql_templates()
                                .templates
                                .contains_key(&format!("functions/{}", fun.as_str()))
                            {
                                continue;
                            }

                            for within_group_node in &egraph[subst[within_group_var]].nodes {
                                match within_group_node {
                                    LogicalPlanLanguage::AggregateFunctionExprWithinGroup(
                                        nodes,
                                    ) => {
                                        if nodes.len() > 0 {
                                            if !sql_generator
                                                .get_sql_templates()
                                                .templates
                                                .contains_key("expressions/within_group")
                                            {
                                                continue;
                                            }
                                        }
                                        return true;
                                    }
                                    _ => (),
                                }
                            }
                        }
                    }
                }
            }
            false
        }
    }
}
