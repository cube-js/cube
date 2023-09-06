use crate::{
    compile::{
        engine::provider::CubeContext,
        rewrite::{
            agg_fun_expr, aggregate, alias_expr,
            analysis::LogicalPlanAnalysis,
            binary_expr, case_expr_var_arg, column_expr, column_name_to_member_vec, cube_scan,
            cube_scan_wrapper, fun_expr_var_arg, limit, literal_expr, original_expr_name,
            projection, rewrite,
            rewriter::RewriteRules,
            rules::{members::MemberRules, replacer_pull_up_node, replacer_push_down_node},
            scalar_fun_expr_args, scalar_fun_expr_args_empty_tail, transforming_chain_rewrite,
            transforming_rewrite, wrapped_select, wrapped_select_aggr_expr_empty_tail,
            wrapped_select_filter_expr_empty_tail, wrapped_select_group_expr_empty_tail,
            wrapped_select_having_expr_empty_tail, wrapped_select_joins_empty_tail,
            wrapped_select_order_expr_empty_tail, wrapped_select_projection_expr_empty_tail,
            wrapper_pullup_replacer, wrapper_pushdown_replacer, AggregateFunctionExprDistinct,
            AggregateFunctionExprFun, AliasExprAlias, ColumnExprColumn, CubeScanAliasToCube,
            CubeScanUngrouped, LimitFetch, LimitSkip, LogicalPlanLanguage, ProjectionAlias,
            ScalarFunctionExprFun, WrappedSelectAlias, WrappedSelectLimit, WrappedSelectOffset,
            WrappedSelectSelectType, WrappedSelectType, WrappedSelectUngrouped,
            WrapperPullupReplacerAliasToCube, WrapperPullupReplacerUngrouped,
        },
    },
    transport::V1CubeMetaMeasureExt,
    var, var_iter, var_list_iter,
};
use datafusion::{logical_plan::Column, physical_plan::aggregates::AggregateFunction};
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
                    "?ungrouped",
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
                            "?ungrouped",
                        ),
                        "?alias_to_cube_out",
                        "?ungrouped_out",
                        "?members",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                self.transform_wrap_cube_scan(
                    "?alias_to_cube",
                    "?ungrouped",
                    "?alias_to_cube_out",
                    "?ungrouped_out",
                ),
            ),
            rewrite(
                "wrapper-finalize-pull-up-replacer",
                cube_scan_wrapper(
                    wrapper_pullup_replacer(
                        "?cube_scan_input",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
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
                        wrapper_pullup_replacer(
                            "?projection_expr",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?cube_members",
                        ),
                        wrapper_pullup_replacer(
                            "?group_expr",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?cube_members",
                        ),
                        wrapper_pullup_replacer(
                            "?aggr_expr",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?cube_members",
                        ),
                        wrapper_pullup_replacer(
                            "?cube_scan_input",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?cube_members",
                        ),
                        wrapped_select_joins_empty_tail(),
                        wrapped_select_filter_expr_empty_tail(),
                        wrapped_select_having_expr_empty_tail(),
                        "WrappedSelectLimit:None",
                        "WrappedSelectOffset:None",
                        wrapped_select_order_expr_empty_tail(),
                        "?select_alias",
                        "?select_ungrouped",
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
                            "?select_ungrouped",
                        ),
                        "?alias_to_cube",
                        // TODO in fact ungrouped flag is being used not only to indicate that underlying query is ungrouped however to indicate that WrappedSelect won't push down Cube members. Do we need separate flags?
                        "WrapperPullupReplacerUngrouped:false",
                        "?cube_members",
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
                        wrapper_pullup_replacer(
                            "?projection_expr",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?cube_members",
                        ),
                        wrapper_pullup_replacer(
                            "?group_expr",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?cube_members",
                        ),
                        wrapper_pullup_replacer(
                            "?aggr_expr",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?cube_members",
                        ),
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
                                "?inner_ungrouped",
                            ),
                            "?alias_to_cube",
                            "?ungrouped",
                            "?cube_members",
                        ),
                        wrapped_select_joins_empty_tail(),
                        wrapped_select_filter_expr_empty_tail(),
                        wrapped_select_having_expr_empty_tail(),
                        "WrappedSelectLimit:None",
                        "WrappedSelectOffset:None",
                        wrapped_select_order_expr_empty_tail(),
                        "?select_alias",
                        "?select_ungrouped",
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
                                "?inner_ungrouped",
                            ),
                            wrapped_select_joins_empty_tail(),
                            wrapped_select_filter_expr_empty_tail(),
                            wrapped_select_having_expr_empty_tail(),
                            "WrappedSelectLimit:None",
                            "WrappedSelectOffset:None",
                            wrapped_select_order_expr_empty_tail(),
                            "?select_alias",
                            "?select_ungrouped",
                        ),
                        "?alias_to_cube",
                        "WrapperPullupReplacerUngrouped:false",
                        "?cube_members",
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
            transforming_rewrite(
                "wrapper-push-down-aggregate-to-cube-scan",
                aggregate(
                    cube_scan_wrapper(
                        wrapper_pullup_replacer(
                            "?cube_scan_input",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?cube_members",
                        ),
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
                            "?ungrouped",
                            "?cube_members",
                        ),
                        wrapper_pushdown_replacer(
                            "?group_expr",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?cube_members",
                        ),
                        wrapper_pushdown_replacer(
                            "?aggr_expr",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?cube_members",
                        ),
                        wrapper_pullup_replacer(
                            "?cube_scan_input",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?cube_members",
                        ),
                        wrapped_select_joins_empty_tail(),
                        wrapped_select_filter_expr_empty_tail(),
                        wrapped_select_having_expr_empty_tail(),
                        "WrappedSelectLimit:None",
                        "WrappedSelectOffset:None",
                        wrapped_select_order_expr_empty_tail(),
                        "WrappedSelectAlias:None",
                        "?select_ungrouped",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                self.transform_aggregate("?ungrouped", "?select_ungrouped"),
            ),
            // Projection
            transforming_rewrite(
                "wrapper-push-down-projection-to-cube-scan",
                projection(
                    "?expr",
                    cube_scan_wrapper(
                        wrapper_pullup_replacer(
                            "?cube_scan_input",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?cube_members",
                        ),
                        "CubeScanWrapperFinalized:false",
                    ),
                    "?projection_alias",
                    "ProjectionSplit:false",
                ),
                cube_scan_wrapper(
                    wrapped_select(
                        "WrappedSelectSelectType:Projection",
                        wrapper_pushdown_replacer(
                            "?expr",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?cube_members",
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_group_expr_empty_tail(),
                            "?alias_to_cube",
                            "?ungrouped",
                            "?cube_members",
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_aggr_expr_empty_tail(),
                            "?alias_to_cube",
                            "?ungrouped",
                            "?cube_members",
                        ),
                        wrapper_pullup_replacer(
                            "?cube_scan_input",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?cube_members",
                        ),
                        wrapped_select_joins_empty_tail(),
                        wrapped_select_filter_expr_empty_tail(),
                        wrapped_select_having_expr_empty_tail(),
                        "WrappedSelectLimit:None",
                        "WrappedSelectOffset:None",
                        wrapped_select_order_expr_empty_tail(),
                        "?select_alias",
                        "?select_ungrouped",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                self.transform_projection(
                    "?projection_alias",
                    "?ungrouped",
                    "?select_alias",
                    "?select_ungrouped",
                ),
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
                                "?select_ungrouped",
                            ),
                            "?alias_to_cube",
                            "?ungrouped",
                            "?cube_members",
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
                            "?select_ungrouped",
                        ),
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
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
                    "?ungrouped",
                    "?cube_members",
                ),
                agg_fun_expr(
                    "?fun",
                    vec![wrapper_pushdown_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    )],
                    "?distinct",
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
                        "?cube_members",
                    )],
                    "?distinct",
                ),
                wrapper_pullup_replacer(
                    agg_fun_expr("?fun", vec!["?expr"], "?distinct"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?cube_members",
                ),
                self.transform_agg_fun_expr("?fun", "?distinct", "?alias_to_cube"),
            ),
            // Scalar function
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
            // Alias
            rewrite(
                "wrapper-push-down-alias",
                wrapper_pushdown_replacer(
                    alias_expr("?expr", "?alias"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?cube_members",
                ),
                alias_expr(
                    wrapper_pushdown_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                    "?alias",
                ),
            ),
            rewrite(
                "wrapper-pull-up-alias",
                alias_expr(
                    wrapper_pullup_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                    "?alias",
                ),
                wrapper_pullup_replacer(
                    alias_expr("?expr", "?alias"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?cube_members",
                ),
            ),
            // Case
            rewrite(
                "wrapper-push-down-case",
                wrapper_pushdown_replacer(
                    case_expr_var_arg("?when", "?then", "?else"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?cube_members",
                ),
                case_expr_var_arg(
                    wrapper_pushdown_replacer(
                        "?when",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                    wrapper_pushdown_replacer(
                        "?then",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                    wrapper_pushdown_replacer(
                        "?else",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                ),
            ),
            transforming_rewrite(
                "wrapper-pull-up-case",
                case_expr_var_arg(
                    wrapper_pullup_replacer(
                        "?when",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        "?then",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        "?else",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                ),
                wrapper_pullup_replacer(
                    case_expr_var_arg("?when", "?then", "?else"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?cube_members",
                ),
                self.transform_case_expr("?alias_to_cube"),
            ),
            // Binary Expr
            rewrite(
                "wrapper-push-down-binary-expr",
                wrapper_pushdown_replacer(
                    binary_expr("?left", "?op", "?right"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?cube_members",
                ),
                binary_expr(
                    wrapper_pushdown_replacer(
                        "?left",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                    "?op",
                    wrapper_pushdown_replacer(
                        "?right",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                ),
            ),
            transforming_rewrite(
                "wrapper-pull-up-binary-expr",
                binary_expr(
                    wrapper_pullup_replacer(
                        "?left",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                    "?op",
                    wrapper_pullup_replacer(
                        "?right",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                ),
                wrapper_pullup_replacer(
                    binary_expr("?left", "?op", "?right"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?cube_members",
                ),
                self.transform_binary_expr("?op", "?alias_to_cube"),
            ),
            // Column
            rewrite(
                "wrapper-push-down-column",
                wrapper_pushdown_replacer(
                    column_expr("?name"),
                    "?alias_to_cube",
                    "WrapperPullupReplacerUngrouped:false",
                    "?cube_members",
                ),
                wrapper_pullup_replacer(
                    column_expr("?name"),
                    "?alias_to_cube",
                    "WrapperPullupReplacerUngrouped:false",
                    "?cube_members",
                ),
            ),
            transforming_rewrite(
                "wrapper-push-down-dimension",
                wrapper_pushdown_replacer(
                    column_expr("?name"),
                    "?alias_to_cube",
                    "WrapperPullupReplacerUngrouped:true",
                    "?cube_members",
                ),
                wrapper_pullup_replacer(
                    "?dimension",
                    "?alias_to_cube",
                    "WrapperPullupReplacerUngrouped:true",
                    "?cube_members",
                ),
                self.pushdown_dimension("?name", "?cube_members", "?dimension"),
            ),
            // Literal
            rewrite(
                "wrapper-push-down-literal",
                wrapper_pushdown_replacer(
                    literal_expr("?value"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?cube_members",
                ),
                wrapper_pullup_replacer(
                    literal_expr("?value"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?cube_members",
                ),
            ),
        ];

        // TODO add flag to disable dimension rules
        MemberRules::measure_rewrites(
            &mut rules,
            |name: &'static str,
             aggr_expr: String,
             _measure_expr: String,
             fun_name: Option<&'static str>,
             distinct: Option<&'static str>,
             cast_data_type: Option<&'static str>,
             column: Option<&'static str>| {
                transforming_chain_rewrite(
                    &format!("wrapper-{}", name),
                    wrapper_pushdown_replacer(
                        "?aggr_expr",
                        "?alias_to_cube",
                        "WrapperPullupReplacerUngrouped:true",
                        "?cube_members",
                    ),
                    vec![("?aggr_expr", aggr_expr)],
                    wrapper_pullup_replacer(
                        "?measure",
                        "?alias_to_cube",
                        "WrapperPullupReplacerUngrouped:true",
                        "?cube_members",
                    ),
                    self.pushdown_measure(
                        "?aggr_expr",
                        column,
                        fun_name,
                        distinct,
                        cast_data_type,
                        "?cube_members",
                        "?measure",
                    ),
                )
            },
        );

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
        ungrouped_cube_var: &'static str,
        alias_to_cube_var_out: &'static str,
        ungrouped_cube_var_out: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let ungrouped_cube_var = var!(ungrouped_cube_var);
        let alias_to_cube_var_out = var!(alias_to_cube_var_out);
        let ungrouped_cube_var_out = var!(ungrouped_cube_var_out);
        move |egraph, subst| {
            for alias_to_cube in
                var_iter!(egraph[subst[alias_to_cube_var]], CubeScanAliasToCube).cloned()
            {
                for ungrouped in
                    var_iter!(egraph[subst[ungrouped_cube_var]], CubeScanUngrouped).cloned()
                {
                    subst.insert(
                        ungrouped_cube_var_out,
                        egraph.add(LogicalPlanLanguage::WrapperPullupReplacerUngrouped(
                            WrapperPullupReplacerUngrouped(ungrouped),
                        )),
                    );
                    subst.insert(
                        alias_to_cube_var_out,
                        egraph.add(LogicalPlanLanguage::WrapperPullupReplacerAliasToCube(
                            WrapperPullupReplacerAliasToCube(alias_to_cube),
                        )),
                    );
                    return true;
                }
            }
            false
        }
    }

    fn transform_aggregate(
        &self,
        ungrouped_var: &'static str,
        select_ungrouped_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let ungrouped_var = var!(ungrouped_var);
        let select_ungrouped_var = var!(select_ungrouped_var);
        move |egraph, subst| {
            for ungrouped in
                var_iter!(egraph[subst[ungrouped_var]], WrapperPullupReplacerUngrouped).cloned()
            {
                subst.insert(
                    select_ungrouped_var,
                    egraph.add(LogicalPlanLanguage::WrappedSelectUngrouped(
                        WrappedSelectUngrouped(ungrouped),
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
        ungrouped_var: &'static str,
        select_alias_var: &'static str,
        select_ungrouped_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let projection_alias_var = var!(projection_alias_var);
        let ungrouped_var = var!(ungrouped_var);
        let select_alias_var = var!(select_alias_var);
        let select_ungrouped_var = var!(select_ungrouped_var);
        move |egraph, subst| {
            for projection_alias in
                var_iter!(egraph[subst[projection_alias_var]], ProjectionAlias).cloned()
            {
                for ungrouped in
                    var_iter!(egraph[subst[ungrouped_var]], WrapperPullupReplacerUngrouped).cloned()
                {
                    subst.insert(
                        select_ungrouped_var,
                        egraph.add(LogicalPlanLanguage::WrappedSelectUngrouped(
                            WrappedSelectUngrouped(ungrouped),
                        )),
                    );
                    subst.insert(
                        select_alias_var,
                        egraph.add(LogicalPlanLanguage::WrappedSelectAlias(WrappedSelectAlias(
                            projection_alias,
                        ))),
                    );
                    return true;
                }
            }
            false
        }
    }

    fn pushdown_measure(
        &self,
        original_expr_var: &'static str,
        column_var: Option<&'static str>,
        fun_name_var: Option<&'static str>,
        distinct_var: Option<&'static str>,
        cast_data_type_var: Option<&'static str>,
        cube_members_var: &'static str,
        measure_out_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let original_expr_var = var!(original_expr_var);
        let column_var = column_var.map(|v| var!(v));
        let fun_name_var = fun_name_var.map(|v| var!(v));
        let distinct_var = distinct_var.map(|v| var!(v));
        // let cast_data_type_var = cast_data_type_var.map(|v| var!(v));
        let cube_members_var = var!(cube_members_var);
        let measure_out_var = var!(measure_out_var);
        let cube_context = self.cube_context.clone();
        move |egraph, subst| {
            if let Some(alias) = original_expr_name(egraph, subst[original_expr_var]) {
                for fun in fun_name_var
                    .map(|fun_var| {
                        var_iter!(egraph[subst[fun_var]], AggregateFunctionExprFun)
                            .map(|fun| Some(fun))
                            .collect()
                    })
                    .unwrap_or(vec![None])
                {
                    for distinct in distinct_var
                        .map(|distinct_var| {
                            var_iter!(egraph[subst[distinct_var]], AggregateFunctionExprDistinct)
                                .map(|d| *d)
                                .collect()
                        })
                        .unwrap_or(vec![false])
                    {
                        let call_agg_type = MemberRules::get_agg_type(fun, distinct);

                        let column_iter = if let Some(column_var) = column_var {
                            var_iter!(egraph[subst[column_var]], ColumnExprColumn)
                                .cloned()
                                .collect()
                        } else {
                            vec![Column::from_name(MemberRules::default_count_measure_name())]
                        };

                        if let Some(member_name_to_expr) = egraph[subst[cube_members_var]]
                            .data
                            .member_name_to_expr
                            .clone()
                        {
                            let column_name_to_member_name =
                                column_name_to_member_vec(member_name_to_expr);
                            for column in column_iter {
                                if let Some((_, Some(member))) = column_name_to_member_name
                                    .iter()
                                    .find(|(cn, _)| cn == &column.name)
                                {
                                    if let Some(measure) =
                                        cube_context.meta.find_measure_with_name(member.to_string())
                                    {
                                        if call_agg_type.is_none()
                                            || measure
                                                .is_same_agg_type(call_agg_type.as_ref().unwrap())
                                        {
                                            let column_expr_column =
                                                egraph.add(LogicalPlanLanguage::ColumnExprColumn(
                                                    ColumnExprColumn(column.clone()),
                                                ));

                                            let column_expr =
                                                egraph.add(LogicalPlanLanguage::ColumnExpr([
                                                    column_expr_column,
                                                ]));
                                            let alias_expr_alias =
                                                egraph.add(LogicalPlanLanguage::AliasExprAlias(
                                                    AliasExprAlias(alias.clone()),
                                                ));

                                            let alias_expr =
                                                egraph.add(LogicalPlanLanguage::AliasExpr([
                                                    column_expr,
                                                    alias_expr_alias,
                                                ]));

                                            subst.insert(measure_out_var, alias_expr);

                                            return true;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            false
        }
    }

    fn pushdown_dimension(
        &self,
        column_name_var: &'static str,
        members_var: &'static str,
        dimension_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let column_name_var = var!(column_name_var);
        let members_var = var!(members_var);
        let dimension_var = var!(dimension_var);
        let cube_context = self.cube_context.clone();
        move |egraph, subst| {
            for column in var_iter!(egraph[subst[column_name_var]], ColumnExprColumn).cloned() {
                if let Some(member_name_to_expr) =
                    egraph[subst[members_var]].data.member_name_to_expr.clone()
                {
                    let column_name_to_member_name = column_name_to_member_vec(member_name_to_expr);
                    if let Some((_, Some(member))) = column_name_to_member_name
                        .iter()
                        .find(|(cn, _)| cn == &column.name)
                    {
                        if let Some(dimension) = cube_context
                            .meta
                            .find_dimension_with_name(member.to_string())
                        {
                            let column_expr_column =
                                egraph.add(LogicalPlanLanguage::ColumnExprColumn(
                                    ColumnExprColumn(column.clone()),
                                ));

                            let column_expr =
                                egraph.add(LogicalPlanLanguage::ColumnExpr([column_expr_column]));
                            subst.insert(dimension_var, column_expr);
                            return true;
                        }
                    }
                }
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
            |node| wrapper_pushdown_replacer(node, "?alias_to_cube", "?ungrouped", "?cube_members"),
            false,
        ));

        rules.extend(replacer_pull_up_node(
            rule_name,
            list_node,
            substitute_list_node,
            |node| wrapper_pullup_replacer(node, "?alias_to_cube", "?ungrouped", "?cube_members"),
        ));

        rules.extend(vec![rewrite(
            rule_name,
            wrapper_pushdown_replacer(list_node, "?alias_to_cube", "?ungrouped", "?cube_members"),
            wrapper_pullup_replacer(
                substitute_list_node,
                "?alias_to_cube",
                "?ungrouped",
                "?cube_members",
            ),
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
            |node| wrapper_pushdown_replacer(node, "?alias_to_cube", "?ungrouped", "?cube_members"),
            false,
        ));

        rules.extend(replacer_pull_up_node(
            rule_name,
            list_node,
            list_node,
            |node| wrapper_pullup_replacer(node, "?alias_to_cube", "?ungrouped", "?cube_members"),
        ));

        rules.extend(vec![rewrite(
            rule_name,
            wrapper_pushdown_replacer(list_node, "?alias_to_cube", "?ungrouped", "?cube_members"),
            wrapper_pullup_replacer(list_node, "?alias_to_cube", "?ungrouped", "?cube_members"),
        )]);
    }
}
