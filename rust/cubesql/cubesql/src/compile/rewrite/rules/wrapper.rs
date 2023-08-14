use crate::{
    compile::{
        engine::provider::CubeContext,
        rewrite::{
            agg_fun_expr, aggregate, alias_expr,
            analysis::LogicalPlanAnalysis,
            binary_expr, case_expr_var_arg, column_expr, cube_scan, cube_scan_wrapper,
            fun_expr_var_arg, limit, literal_expr, projection, rewrite,
            rewriter::RewriteRules,
            rules::{replacer_pull_up_node, replacer_push_down_node},
            scalar_fun_expr_args, scalar_fun_expr_args_empty_tail, transforming_rewrite,
            wrapped_select, wrapped_select_aggr_expr_empty_tail,
            wrapped_select_filter_expr_empty_tail, wrapped_select_group_expr_empty_tail,
            wrapped_select_having_expr_empty_tail, wrapped_select_joins_empty_tail,
            wrapped_select_order_expr_empty_tail, wrapped_select_projection_expr_empty_tail,
            wrapper_pullup_replacer, wrapper_pushdown_replacer, AggregateFunctionExprDistinct,
            AggregateFunctionExprFun, CubeScanAliasToCube, LimitFetch, LimitSkip,
            LogicalPlanLanguage, ProjectionAlias, ScalarFunctionExprFun, WrappedSelectAlias,
            WrappedSelectLimit, WrappedSelectOffset, WrappedSelectSelectType, WrappedSelectType,
            WrapperPullupReplacerAliasToCube,
        },
    },
    var, var_iter, var_list_iter,
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
            // Wrapper pull up
            transforming_rewrite(
                "wrapper-pull-up-to-cube-scan-non-wrapped-select",
                cube_scan_wrapper(
                    wrapped_select(
                        "?select_type",
                        wrapper_pullup_replacer("?projection_expr", "?alias_to_cube"),
                        wrapper_pullup_replacer("?group_expr", "?alias_to_cube"),
                        wrapper_pullup_replacer("?aggr_expr", "?alias_to_cube"),
                        wrapper_pullup_replacer("?cube_scan_input", "?alias_to_cube"),
                        wrapped_select_joins_empty_tail(),
                        wrapped_select_filter_expr_empty_tail(),
                        wrapped_select_having_expr_empty_tail(),
                        "WrappedSelectLimit:None",
                        "WrappedSelectOffset:None",
                        wrapped_select_order_expr_empty_tail(),
                        "?select_alias",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                cube_scan_wrapper(
                    wrapper_pullup_replacer(
                        wrapped_select(
                            "?select_type",
                            "?projection_expr",
                            "?group_expr",
                            "?aggr_expr",
                            "?cube_scan_input",
                            wrapped_select_joins_empty_tail(),
                            wrapped_select_filter_expr_empty_tail(),
                            wrapped_select_having_expr_empty_tail(),
                            "WrappedSelectLimit:None",
                            "WrappedSelectOffset:None",
                            wrapped_select_order_expr_empty_tail(),
                            "?select_alias",
                        ),
                        "?alias_to_cube",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                self.transform_pull_up_wrapper_select("?cube_scan_input"),
            ),
            transforming_rewrite(
                "wrapper-pull-up-to-cube-scan-non-trivial-wrapped-select",
                cube_scan_wrapper(
                    wrapped_select(
                        "?select_type",
                        wrapper_pullup_replacer("?projection_expr", "?alias_to_cube"),
                        wrapper_pullup_replacer("?group_expr", "?alias_to_cube"),
                        wrapper_pullup_replacer("?aggr_expr", "?alias_to_cube"),
                        wrapper_pullup_replacer(
                            wrapped_select(
                                "?inner_select_type",
                                "?inner_projection_expr",
                                "?inner_group_expr",
                                "?inner_aggr_expr",
                                "?inner_cube_scan_input",
                                "?inner_joins",
                                "?inner_filter_expr",
                                "?inner_having_expr",
                                "?inner_limit",
                                "?inner_offset",
                                "?inner_order_expr",
                                "?inner_alias",
                            ),
                            "?alias_to_cube",
                        ),
                        wrapped_select_joins_empty_tail(),
                        wrapped_select_filter_expr_empty_tail(),
                        wrapped_select_having_expr_empty_tail(),
                        "WrappedSelectLimit:None",
                        "WrappedSelectOffset:None",
                        wrapped_select_order_expr_empty_tail(),
                        "?select_alias",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                cube_scan_wrapper(
                    wrapper_pullup_replacer(
                        wrapped_select(
                            "?select_type",
                            "?projection_expr",
                            "?group_expr",
                            "?aggr_expr",
                            wrapped_select(
                                "?inner_select_type",
                                "?inner_projection_expr",
                                "?inner_group_expr",
                                "?inner_aggr_expr",
                                "?inner_cube_scan_input",
                                "?inner_joins",
                                "?inner_filter_expr",
                                "?inner_having_expr",
                                "?inner_limit",
                                "?inner_offset",
                                "?inner_order_expr",
                                "?inner_alias",
                            ),
                            wrapped_select_joins_empty_tail(),
                            wrapped_select_filter_expr_empty_tail(),
                            wrapped_select_having_expr_empty_tail(),
                            "WrappedSelectLimit:None",
                            "WrappedSelectOffset:None",
                            wrapped_select_order_expr_empty_tail(),
                            "?select_alias",
                        ),
                        "?alias_to_cube",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                self.transform_pull_up_non_trivial_wrapper_select(
                    "?select_type",
                    "?projection_expr",
                    "?group_expr",
                    "?aggr_expr",
                    "?inner_select_type",
                    "?inner_projection_expr",
                    "?inner_group_expr",
                    "?inner_aggr_expr",
                ),
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
                        wrapper_pullup_replacer(
                            wrapped_select_projection_expr_empty_tail(),
                            "?alias_to_cube",
                        ),
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
            // Projection
            transforming_rewrite(
                "wrapper-push-down-projection-to-cube-scan",
                projection(
                    "?expr",
                    cube_scan_wrapper(
                        wrapper_pullup_replacer("?cube_scan_input", "?alias_to_cube"),
                        "CubeScanWrapperFinalized:false",
                    ),
                    "?projection_alias",
                    "ProjectionSplit:false",
                ),
                cube_scan_wrapper(
                    wrapped_select(
                        "WrappedSelectSelectType:Projection",
                        wrapper_pushdown_replacer("?expr", "?alias_to_cube"),
                        wrapper_pullup_replacer(
                            wrapped_select_group_expr_empty_tail(),
                            "?alias_to_cube",
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_aggr_expr_empty_tail(),
                            "?alias_to_cube",
                        ),
                        wrapper_pullup_replacer("?cube_scan_input", "?alias_to_cube"),
                        wrapped_select_joins_empty_tail(),
                        wrapped_select_filter_expr_empty_tail(),
                        wrapped_select_having_expr_empty_tail(),
                        "WrappedSelectLimit:None",
                        "WrappedSelectOffset:None",
                        wrapped_select_order_expr_empty_tail(),
                        "?select_alias",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                self.transform_projection("?projection_alias", "?select_alias"),
            ),
            // Limit
            transforming_rewrite(
                "wrapper-push-down-limit-to-cube-scan",
                limit(
                    "?offset",
                    "?limit",
                    cube_scan_wrapper(
                        wrapper_pullup_replacer(
                            wrapped_select(
                                "?select_type",
                                "?projection_expr",
                                "?group_expr",
                                "?aggr_expr",
                                "?cube_scan_input",
                                "?joins",
                                "?filter_expr",
                                "?having_expr",
                                "WrappedSelectLimit:None",
                                "WrappedSelectOffset:None",
                                "?order_expr",
                                "?select_alias",
                            ),
                            "?alias_to_cube",
                        ),
                        "CubeScanWrapperFinalized:false".to_string(),
                    ),
                ),
                cube_scan_wrapper(
                    wrapper_pullup_replacer(
                        wrapped_select(
                            "?select_type",
                            "?projection_expr",
                            "?group_expr",
                            "?aggr_expr",
                            "?cube_scan_input",
                            "?joins",
                            "?filter_expr",
                            "?having_expr",
                            "?wrapped_select_limit",
                            "?wrapped_select_offset",
                            "?order_expr",
                            "?select_alias",
                        ),
                        "?alias_to_cube",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                self.transform_limit(
                    "?limit",
                    "?offset",
                    "?wrapped_select_limit",
                    "?wrapped_select_offset",
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
            // Scalar function
            rewrite(
                "wrapper-push-down-function",
                wrapper_pushdown_replacer(fun_expr_var_arg("?fun", "?args"), "?alias_to_cube"),
                fun_expr_var_arg("?fun", wrapper_pushdown_replacer("?args", "?alias_to_cube")),
            ),
            transforming_rewrite(
                "wrapper-pull-up-function",
                fun_expr_var_arg("?fun", wrapper_pullup_replacer("?args", "?alias_to_cube")),
                wrapper_pullup_replacer(fun_expr_var_arg("?fun", "?args"), "?alias_to_cube"),
                self.transform_fun_expr("?fun", "?alias_to_cube"),
            ),
            rewrite(
                "wrapper-push-down-scalar-function-args",
                wrapper_pushdown_replacer(
                    scalar_fun_expr_args("?left", "?right"),
                    "?alias_to_cube",
                ),
                scalar_fun_expr_args(
                    wrapper_pushdown_replacer("?left", "?alias_to_cube"),
                    wrapper_pushdown_replacer("?right", "?alias_to_cube"),
                ),
            ),
            rewrite(
                "wrapper-pull-up-scalar-function-args",
                scalar_fun_expr_args(
                    wrapper_pullup_replacer("?left", "?alias_to_cube"),
                    wrapper_pullup_replacer("?right", "?alias_to_cube"),
                ),
                wrapper_pullup_replacer(scalar_fun_expr_args("?left", "?right"), "?alias_to_cube"),
            ),
            rewrite(
                "wrapper-push-down-scalar-function-empty-tail",
                wrapper_pushdown_replacer(scalar_fun_expr_args_empty_tail(), "?alias_to_cube"),
                wrapper_pullup_replacer(scalar_fun_expr_args_empty_tail(), "?alias_to_cube"),
            ),
            // Alias
            rewrite(
                "wrapper-push-down-alias",
                wrapper_pushdown_replacer(alias_expr("?expr", "?alias"), "?alias_to_cube"),
                alias_expr(
                    wrapper_pushdown_replacer("?expr", "?alias_to_cube"),
                    "?alias",
                ),
            ),
            rewrite(
                "wrapper-pull-up-alias",
                alias_expr(wrapper_pullup_replacer("?expr", "?alias_to_cube"), "?alias"),
                wrapper_pullup_replacer(alias_expr("?expr", "?alias"), "?alias_to_cube"),
            ),
            // Case
            rewrite(
                "wrapper-push-down-case",
                wrapper_pushdown_replacer(
                    case_expr_var_arg("?when", "?then", "?else"),
                    "?alias_to_cube",
                ),
                case_expr_var_arg(
                    wrapper_pushdown_replacer("?when", "?alias_to_cube"),
                    wrapper_pushdown_replacer("?then", "?alias_to_cube"),
                    wrapper_pushdown_replacer("?else", "?alias_to_cube"),
                ),
            ),
            transforming_rewrite(
                "wrapper-pull-up-case",
                case_expr_var_arg(
                    wrapper_pullup_replacer("?when", "?alias_to_cube"),
                    wrapper_pullup_replacer("?then", "?alias_to_cube"),
                    wrapper_pullup_replacer("?else", "?alias_to_cube"),
                ),
                wrapper_pullup_replacer(
                    case_expr_var_arg("?when", "?then", "?else"),
                    "?alias_to_cube",
                ),
                self.transform_case_expr("?alias_to_cube"),
            ),
            // Binary Expr
            rewrite(
                "wrapper-push-down-binary-expr",
                wrapper_pushdown_replacer(binary_expr("?left", "?op", "?right"), "?alias_to_cube"),
                binary_expr(
                    wrapper_pushdown_replacer("?left", "?alias_to_cube"),
                    "?op",
                    wrapper_pushdown_replacer("?right", "?alias_to_cube"),
                ),
            ),
            transforming_rewrite(
                "wrapper-pull-up-binary-expr",
                binary_expr(
                    wrapper_pullup_replacer("?left", "?alias_to_cube"),
                    "?op",
                    wrapper_pullup_replacer("?right", "?alias_to_cube"),
                ),
                wrapper_pullup_replacer(binary_expr("?left", "?op", "?right"), "?alias_to_cube"),
                self.transform_binary_expr("?op", "?alias_to_cube"),
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

        Self::expr_list_pushdown_pullup_rules(&mut rules, "wrapper-case-expr", "CaseExprExpr");

        Self::expr_list_pushdown_pullup_rules(
            &mut rules,
            "wrapper-case-when-expr",
            "CaseExprWhenThenExpr",
        );

        Self::expr_list_pushdown_pullup_rules(
            &mut rules,
            "wrapper-case-else-expr",
            "CaseExprElseExpr",
        );

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

        Self::list_pushdown_pullup_rules(
            &mut rules,
            "wrapper-projection-expr",
            "ProjectionExpr",
            "WrappedSelectProjectionExpr",
        );

        rules
    }
}

impl WrapperRules {
    pub fn new(cube_context: Arc<CubeContext>) -> Self {
        Self { cube_context }
    }

    fn transform_pull_up_wrapper_select(
        &self,
        cube_scan_input_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_scan_input_var = var!(cube_scan_input_var);
        move |egraph, subst| {
            for _ in var_list_iter!(egraph[subst[cube_scan_input_var]], WrappedSelect).cloned() {
                return false;
            }
            true
        }
    }

    fn transform_pull_up_non_trivial_wrapper_select(
        &self,
        select_type_var: &'static str,
        projection_expr_var: &'static str,
        _group_expr_var: &'static str,
        _aggr_expr_var: &'static str,
        inner_select_type_var: &'static str,
        inner_projection_expr_var: &'static str,
        _inner_group_expr_var: &'static str,
        _inner_aggr_expr_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let select_type_var = var!(select_type_var);
        let projection_expr_var = var!(projection_expr_var);
        let inner_select_type_var = var!(inner_select_type_var);
        let inner_projection_expr_var = var!(inner_projection_expr_var);
        move |egraph, subst| {
            for select_type in
                var_iter!(egraph[subst[select_type_var]], WrappedSelectSelectType).cloned()
            {
                for inner_select_type in var_iter!(
                    egraph[subst[inner_select_type_var]],
                    WrappedSelectSelectType
                )
                .cloned()
                {
                    if select_type != inner_select_type {
                        return true;
                    }

                    return match select_type {
                        WrappedSelectType::Projection => {
                            // TODO changes of alias can be non-trivial
                            subst[projection_expr_var] != subst[inner_projection_expr_var]
                        }
                        WrappedSelectType::Aggregate => {
                            // TODO write rules for non trivial wrapped aggregate
                            true
                        }
                    };
                }
            }
            false
        }
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

    fn transform_projection(
        &self,
        projection_alias_var: &'static str,
        select_alias_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let projection_alias_var = var!(projection_alias_var);
        let select_alias_var = var!(select_alias_var);
        move |egraph, subst| {
            for projection_alias in
                var_iter!(egraph[subst[projection_alias_var]], ProjectionAlias).cloned()
            {
                subst.insert(
                    select_alias_var,
                    egraph.add(LogicalPlanLanguage::WrappedSelectAlias(WrappedSelectAlias(
                        projection_alias,
                    ))),
                );
                return true;
            }
            false
        }
    }

    fn transform_limit(
        &self,
        limit_var: &'static str,
        offset_var: &'static str,
        wrapped_select_limit_var: &'static str,
        wrapped_select_offset_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let limit_var = var!(limit_var);
        let offset_var = var!(offset_var);
        let wrapped_select_limit_var = var!(wrapped_select_limit_var);
        let wrapped_select_offset_var = var!(wrapped_select_offset_var);
        move |egraph, subst| {
            for limit in var_iter!(egraph[subst[limit_var]], LimitFetch).cloned() {
                for offset in var_iter!(egraph[subst[offset_var]], LimitSkip).cloned() {
                    subst.insert(
                        wrapped_select_limit_var,
                        egraph.add(LogicalPlanLanguage::WrappedSelectLimit(WrappedSelectLimit(
                            limit,
                        ))),
                    );

                    subst.insert(
                        wrapped_select_offset_var,
                        egraph.add(LogicalPlanLanguage::WrappedSelectOffset(
                            WrappedSelectOffset(offset),
                        )),
                    );
                    return true;
                }
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

    fn transform_fun_expr(
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

    fn transform_case_expr(
        &self,
        alias_to_cube_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
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
                        .contains_key("expressions/case")
                    {
                        return true;
                    }
                }
            }
            false
        }
    }

    fn transform_binary_expr(
        &self,
        _operator_var: &'static str,
        alias_to_cube_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        // let operator_var = var!(operator_var);
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
                        .contains_key("expressions/binary")
                    {
                        // TODO check supported operators
                        return true;
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

    fn expr_list_pushdown_pullup_rules(
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
        rule_name: &str,
        list_node: &str,
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
            list_node,
            |node| wrapper_pullup_replacer(node, "?alias_to_cube"),
        ));

        rules.extend(vec![rewrite(
            rule_name,
            wrapper_pushdown_replacer(list_node, "?alias_to_cube"),
            wrapper_pullup_replacer(list_node, "?alias_to_cube"),
        )]);
    }
}
