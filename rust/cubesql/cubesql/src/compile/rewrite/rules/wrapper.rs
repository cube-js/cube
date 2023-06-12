use crate::{
    compile::{
        engine::provider::CubeContext,
        rewrite::{
            agg_fun_expr, aggregate,
            analysis::LogicalPlanAnalysis,
            column_expr, cube_scan, cube_scan_wrapper, literal_expr, rewrite,
            rewriter::RewriteRules,
            rules::{replacer_pull_up_node, replacer_push_down_node},
            transforming_rewrite, wrapped_select, wrapped_select_filter_expr_empty_tail,
            wrapped_select_having_expr_empty_tail, wrapped_select_joins_empty_tail,
            wrapped_select_order_expr_empty_tail, wrapped_select_projection_expr_empty_tail,
            wrapper_pullup_replacer, wrapper_pushdown_replacer, AggregateFunctionExprDistinct,
            AggregateFunctionExprFun, CubeScanAliasToCube, LogicalPlanLanguage,
            WrapperPullupReplacerAliasToCube,
        },
    },
    var, var_iter,
};
use datafusion::physical_plan::aggregates::AggregateFunction;
use egg::{EGraph, Rewrite, Subst};
use std::sync::Arc;

pub struct WrapperRules {
    cube_context: Arc<CubeContext>,
}

impl RewriteRules for WrapperRules {
    fn rewrite_rules(&self) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
        let mut rules = vec![
            transforming_rewrite(
                "wrapper-cube-scan-wrap",
                cube_scan(
                    "?alias_to_cube",
                    "?members",
                    "?filters",
                    "?order",
                    "?limit",
                    "?offset",
                    "?aliases",
                    "CubeScanSplit:false",
                    "?can_pushdown_join",
                    "CubeScanWrapped:false",
                ),
                cube_scan_wrapper(
                    wrapper_pullup_replacer(
                        cube_scan(
                            "?alias_to_cube",
                            "?members",
                            "?filters",
                            "?order",
                            "?limit",
                            "?offset",
                            "?aliases",
                            "CubeScanSplit:false",
                            "?can_pushdown_join",
                            "CubeScanWrapped:true",
                        ),
                        "?alias_to_cube_out",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                self.transform_wrap_cube_scan("?alias_to_cube", "?alias_to_cube_out"),
            ),
            rewrite(
                "wrapper-finalize-pull-up-replacer",
                cube_scan_wrapper(
                    wrapper_pullup_replacer("?cube_scan_input", "?alias_to_cube"),
                    "CubeScanWrapperFinalized:false",
                ),
                cube_scan_wrapper("?cube_scan_input", "CubeScanWrapperFinalized:true"),
            ),
            // Aggregate
            rewrite(
                "wrapper-push-down-aggregate-to-cube-scan",
                aggregate(
                    cube_scan_wrapper(
                        wrapper_pullup_replacer("?cube_scan_input", "?alias_to_cube"),
                        "CubeScanWrapperFinalized:false",
                    ),
                    "?group_expr",
                    "?aggr_expr",
                    "AggregateSplit:false",
                ),
                cube_scan_wrapper(
                    wrapped_select(
                        "WrappedSelectSelectType:Aggregate",
                        wrapped_select_projection_expr_empty_tail(),
                        wrapper_pushdown_replacer("?group_expr", "?alias_to_cube"),
                        wrapper_pushdown_replacer("?aggr_expr", "?alias_to_cube"),
                        wrapper_pullup_replacer("?cube_scan_input", "?alias_to_cube"),
                        wrapped_select_joins_empty_tail(),
                        wrapped_select_filter_expr_empty_tail(),
                        wrapped_select_having_expr_empty_tail(),
                        "WrappedSelectLimit:None",
                        "WrappedSelectOffset:None",
                        wrapped_select_order_expr_empty_tail(),
                        "WrappedSelectAlias:None",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
            ),
            rewrite(
                "wrapper-pull-up-aggregate-to-cube-scan",
                cube_scan_wrapper(
                    wrapped_select(
                        "WrappedSelectSelectType:Aggregate",
                        wrapped_select_projection_expr_empty_tail(),
                        wrapper_pullup_replacer("?group_expr", "?alias_to_cube"),
                        wrapper_pullup_replacer("?aggr_expr", "?alias_to_cube"),
                        wrapper_pullup_replacer("?cube_scan_input", "?alias_to_cube"),
                        wrapped_select_joins_empty_tail(),
                        wrapped_select_filter_expr_empty_tail(),
                        wrapped_select_having_expr_empty_tail(),
                        "WrappedSelectLimit:None",
                        "WrappedSelectOffset:None",
                        wrapped_select_order_expr_empty_tail(),
                        "WrappedSelectAlias:None",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                cube_scan_wrapper(
                    wrapper_pullup_replacer(
                        wrapped_select(
                            "WrappedSelectSelectType:Aggregate",
                            wrapped_select_projection_expr_empty_tail(),
                            "?group_expr",
                            "?aggr_expr",
                            "?cube_scan_input",
                            wrapped_select_joins_empty_tail(),
                            wrapped_select_filter_expr_empty_tail(),
                            wrapped_select_having_expr_empty_tail(),
                            "WrappedSelectLimit:None",
                            "WrappedSelectOffset:None",
                            wrapped_select_order_expr_empty_tail(),
                            "WrappedSelectAlias:None",
                        ),
                        "?alias_to_cube",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
            ),
            // Aggregate function
            rewrite(
                "wrapper-push-down-aggregate-function",
                wrapper_pushdown_replacer(
                    agg_fun_expr("?fun", vec!["?expr"], "?distinct"),
                    "?alias_to_cube",
                ),
                agg_fun_expr(
                    "?fun",
                    vec![wrapper_pushdown_replacer("?expr", "?alias_to_cube")],
                    "?distinct",
                ),
            ),
            transforming_rewrite(
                "wrapper-pull-up-aggregate-function",
                agg_fun_expr(
                    "?fun",
                    vec![wrapper_pullup_replacer("?expr", "?alias_to_cube")],
                    "?distinct",
                ),
                wrapper_pullup_replacer(
                    agg_fun_expr("?fun", vec!["?expr"], "?distinct"),
                    "?alias_to_cube",
                ),
                self.transform_agg_fun_expr("?fun", "?distinct", "?alias_to_cube"),
            ),
            // Column
            rewrite(
                "wrapper-push-down-column",
                wrapper_pushdown_replacer(column_expr("?name"), "?alias_to_cube"),
                wrapper_pullup_replacer(column_expr("?name"), "?alias_to_cube"),
            ),
            // Literal
            rewrite(
                "wrapper-push-down-literal",
                wrapper_pushdown_replacer(literal_expr("?value"), "?alias_to_cube"),
                wrapper_pullup_replacer(literal_expr("?value"), "?alias_to_cube"),
            ),
        ];

        Self::list_pushdown_pullup_rules(
            &mut rules,
            "wrapper-aggregate-aggr-expr",
            "AggregateAggrExpr",
            "WrappedSelectAggrExpr",
        );

        Self::list_pushdown_pullup_rules(
            &mut rules,
            "wrapper-aggregate-group-expr",
            "AggregateGroupExpr",
            "WrappedSelectGroupExpr",
        );

        rules
    }
}

impl WrapperRules {
    pub fn new(cube_context: Arc<CubeContext>) -> Self {
        Self { cube_context }
    }

    fn transform_wrap_cube_scan(
        &self,
        alias_to_cube_var: &'static str,
        alias_to_cube_var_out: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let alias_to_cube_var_out = var!(alias_to_cube_var_out);
        move |egraph, subst| {
            for alias_to_cube in
                var_iter!(egraph[subst[alias_to_cube_var]], CubeScanAliasToCube).cloned()
            {
                subst.insert(
                    alias_to_cube_var_out,
                    egraph.add(LogicalPlanLanguage::WrapperPullupReplacerAliasToCube(
                        WrapperPullupReplacerAliasToCube(alias_to_cube),
                    )),
                );
                return true;
            }
            false
        }
    }

    fn transform_agg_fun_expr(
        &self,
        fun_var: &'static str,
        distinct_var: &'static str,
        alias_to_cube_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let fun_var = var!(fun_var);
        let distinct_var = var!(distinct_var);
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

                            if sql_generator
                                .get_sql_templates()
                                .functions
                                .contains_key(fun.as_str())
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

    fn list_pushdown_pullup_rules(
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
        rule_name: &str,
        list_node: &str,
        substitute_list_node: &str,
    ) {
        rules.extend(replacer_push_down_node(
            rule_name,
            list_node,
            |node| wrapper_pushdown_replacer(node, "?alias_to_cube"),
            false,
        ));

        rules.extend(replacer_pull_up_node(
            rule_name,
            list_node,
            substitute_list_node,
            |node| wrapper_pullup_replacer(node, "?alias_to_cube"),
        ));

        rules.extend(vec![rewrite(
            rule_name,
            wrapper_pushdown_replacer(list_node, "?alias_to_cube"),
            wrapper_pullup_replacer(substitute_list_node, "?alias_to_cube"),
        )]);
    }
}
