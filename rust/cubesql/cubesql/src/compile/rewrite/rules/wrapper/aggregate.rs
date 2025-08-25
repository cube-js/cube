use crate::{
    compile::{
        engine::udf::{MEASURE_UDAF_NAME, PATCH_MEASURE_UDAF_NAME},
        rewrite::{
            agg_fun_expr, agg_fun_expr_within_group_empty_tail, aggregate, alias_expr,
            analysis::ConstantFolding,
            binary_expr, case_expr, column_expr, cube_scan_wrapper, grouping_set_expr,
            literal_null, original_expr_name, rewrite,
            rewriter::{CubeEGraph, CubeRewrite},
            rules::{members::MemberRules, wrapper::WrapperRules},
            subquery, transforming_chain_rewrite, transforming_rewrite, udaf_expr, wrapped_select,
            wrapped_select_aggr_expr_empty_tail, wrapped_select_filter_expr_empty_tail,
            wrapped_select_group_expr_empty_tail, wrapped_select_having_expr_empty_tail,
            wrapped_select_joins_empty_tail, wrapped_select_order_expr_empty_tail,
            wrapped_select_projection_expr_empty_tail, wrapped_select_subqueries_empty_tail,
            wrapped_select_window_expr_empty_tail, wrapper_pullup_replacer,
            wrapper_pushdown_replacer, wrapper_replacer_context, AggregateFunctionExprDistinct,
            AggregateFunctionExprFun, AggregateUDFExprFun, AliasExprAlias, ColumnExprColumn,
            ListType, LiteralExprValue, LogicalPlanData, LogicalPlanLanguage,
            WrappedSelectPushToCube, WrapperReplacerContextAliasToCube,
            WrapperReplacerContextPushToCube,
        },
    },
    copy_flag,
    transport::{MetaContext, V1CubeMetaMeasureExt},
    var, var_iter,
};
use datafusion::{logical_plan::Column, scalar::ScalarValue};
use egg::{Subst, Var};
use std::{collections::HashSet, ops::IndexMut};

impl WrapperRules {
    pub fn aggregate_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            transforming_rewrite(
                "wrapper-push-down-aggregate-to-cube-scan",
                aggregate(
                    cube_scan_wrapper(
                        wrapper_pullup_replacer(
                            "?cube_scan_input",
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
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "?push_to_cube",
                                "WrapperReplacerContextInProjection:false",
                                "?cube_members",
                                "?grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_subqueries_empty_tail(),
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "?push_to_cube",
                                "WrapperReplacerContextInProjection:false",
                                "?cube_members",
                                "?grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pushdown_replacer(
                            "?group_expr",
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "?push_to_cube",
                                "WrapperReplacerContextInProjection:false",
                                "?cube_members",
                                "?grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pushdown_replacer(
                            "?aggr_expr",
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "?push_to_cube",
                                "WrapperReplacerContextInProjection:false",
                                "?cube_members",
                                "?grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_window_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "?push_to_cube",
                                "WrapperReplacerContextInProjection:false",
                                "?cube_members",
                                "?grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            "?cube_scan_input",
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "?push_to_cube",
                                "WrapperReplacerContextInProjection:false",
                                "?cube_members",
                                "?grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_joins_empty_tail(),
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "?push_to_cube",
                                "WrapperReplacerContextInProjection:false",
                                "?cube_members",
                                "?grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_filter_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "?push_to_cube",
                                "WrapperReplacerContextInProjection:false",
                                "?cube_members",
                                "?grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                                "?input_data_source",
                            ),
                        ),
                        wrapped_select_having_expr_empty_tail(),
                        "WrappedSelectLimit:None",
                        "WrappedSelectOffset:None",
                        wrapper_pullup_replacer(
                            wrapped_select_order_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "?push_to_cube",
                                "WrapperReplacerContextInProjection:false",
                                "?cube_members",
                                "?grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                                "?input_data_source",
                            ),
                        ),
                        "WrappedSelectAlias:None",
                        "WrappedSelectDistinct:false",
                        "?select_push_to_cube",
                        "WrappedSelectUngroupedScan:false",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                self.transform_aggregate(
                    "?group_expr",
                    "?aggr_expr",
                    "?push_to_cube",
                    "?select_push_to_cube",
                ),
            ),
            transforming_rewrite(
                "wrapper-groupping-set-push-down",
                wrapper_pushdown_replacer(
                    grouping_set_expr("?rollout_members", "?type"),
                    wrapper_replacer_context(
                        "?alias_to_cube",
                        "?push_to_cube",
                        "WrapperReplacerContextInProjection:false",
                        "?cube_members",
                        "?grouped_subqueries",
                        "?ungrouped_scan",
                        "?input_data_source",
                    ),
                ),
                grouping_set_expr(
                    wrapper_pushdown_replacer(
                        "?rollout_members",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                    "?type",
                ),
                self.check_rollup_allowed("?input_data_source"),
            ),
            rewrite(
                "wrapper-groupping-set-pull-up",
                grouping_set_expr(
                    wrapper_pullup_replacer(
                        "?rollout_members",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                    "?type",
                ),
                wrapper_pullup_replacer(
                    grouping_set_expr("?rollout_members", "?type"),
                    wrapper_replacer_context(
                        "?alias_to_cube",
                        "?push_to_cube",
                        "WrapperReplacerContextInProjection:false",
                        "?cube_members",
                        "?grouped_subqueries",
                        "?ungrouped_scan",
                        "?input_data_source",
                    ),
                ),
            ),
        ]);

        // TODO add flag to disable dimension rules
        MemberRules::measure_rewrites(
            rules,
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
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "WrapperReplacerContextPushToCube:true",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                    vec![("?aggr_expr", aggr_expr)],
                    wrapper_pullup_replacer(
                        alias_expr("?out_measure_expr", "?out_measure_alias"),
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "WrapperReplacerContextPushToCube:true",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                    self.pushdown_measure(
                        "?aggr_expr",
                        column,
                        fun_name,
                        distinct,
                        cast_data_type,
                        "?cube_members",
                        "?out_measure_expr",
                        "?out_measure_alias",
                        "?alias_to_cube",
                    ),
                )
            },
        );

        if self.config_obj.push_down_pull_up_split() {
            Self::flat_list_pushdown_pullup_rules(
                rules,
                "wrapper-aggregate-aggr-expr",
                ListType::AggregateAggrExpr,
                ListType::WrappedSelectAggrExpr,
            );

            Self::flat_list_pushdown_pullup_rules(
                rules,
                "wrapper-aggregate-group-expr",
                ListType::AggregateGroupExpr,
                ListType::WrappedSelectGroupExpr,
            );
            Self::flat_list_pushdown_pullup_rules(
                rules,
                "wrapper-grouping-set-members",
                ListType::GroupingSetExprMembers,
                ListType::GroupingSetExprMembers,
            );
        } else {
            Self::list_pushdown_pullup_rules(
                rules,
                "wrapper-aggregate-aggr-expr",
                "AggregateAggrExpr",
                "WrappedSelectAggrExpr",
            );

            Self::list_pushdown_pullup_rules(
                rules,
                "wrapper-aggregate-group-expr",
                "AggregateGroupExpr",
                "WrappedSelectGroupExpr",
            );
            Self::list_pushdown_pullup_rules(
                rules,
                "wrapper-grouping-set-members",
                "GroupingSetExprMembers",
                "GroupingSetExprMembers",
            );
        }

        // incoming structure: agg_fun(?name, case(?cond, (?when_value, measure_column)))
        // optional "else null" is fine
        // only single when-then
        rules.extend(vec![
            transforming_chain_rewrite(
                "wrapper-push-down-aggregation-over-filtered-measure",
                wrapper_pushdown_replacer("?aggr_expr", "?context"),
                vec![
                    (
                        "?aggr_expr",
                        agg_fun_expr(
                            "?fun",
                            vec![case_expr(
                                Some("?case_expr".to_string()),
                                vec![("?literal".to_string(), column_expr("?measure_column"))],
                                // TODO make `ELSE NULL` optional and/or add generic rewrite to normalize it
                                Some(literal_null()),
                            )],
                            "?distinct",
                            agg_fun_expr_within_group_empty_tail(),
                        ),
                    ),
                    (
                        "?context",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "WrapperReplacerContextPushToCube:true",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                ],
                alias_expr(
                    udaf_expr(
                        PATCH_MEASURE_UDAF_NAME,
                        vec![
                            column_expr("?measure_column"),
                            "?replace_agg_type".to_string(),
                            wrapper_pushdown_replacer(
                                // = is a proper way to filter here:
                                // CASE NULL WHEN ... will return null
                                // So NULL in ?case_expr is equivalent to hitting ELSE branch
                                // TODO add "is not null" to cond? just to make is always boolean
                                binary_expr("?case_expr", "=", "?literal"),
                                "?context",
                            ),
                        ],
                    ),
                    "?out_measure_alias",
                ),
                self.transform_filtered_measure(
                    "?aggr_expr",
                    "?literal",
                    "?measure_column",
                    "?fun",
                    "?cube_members",
                    "?replace_agg_type",
                    "?out_measure_alias",
                ),
            ),
            rewrite(
                "wrapper-pull-up-aggregation-over-filtered-measure",
                udaf_expr(
                    PATCH_MEASURE_UDAF_NAME,
                    vec![
                        column_expr("?measure_column"),
                        "?new_agg_type".to_string(),
                        wrapper_pullup_replacer("?filter_expr", "?context"),
                    ],
                ),
                wrapper_pullup_replacer(
                    udaf_expr(
                        PATCH_MEASURE_UDAF_NAME,
                        vec![
                            column_expr("?measure_column"),
                            "?new_agg_type".to_string(),
                            "?filter_expr".to_string(),
                        ],
                    ),
                    "?context",
                ),
            ),
        ]);
    }

    pub fn aggregate_rules_subquery(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![transforming_rewrite(
            "wrapper-push-down-aggregate-and-subquery-to-cube-scan",
            aggregate(
                subquery(
                    cube_scan_wrapper(
                        wrapper_pullup_replacer(
                            "?cube_scan_input",
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
                        "CubeScanWrapperFinalized:false",
                    ),
                    "?subqueries",
                    "?types",
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
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:false",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pushdown_replacer(
                        "?subqueries",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:false",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pushdown_replacer(
                        "?group_expr",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:false",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pushdown_replacer(
                        "?aggr_expr",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:false",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_window_expr_empty_tail(),
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:false",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        "?cube_scan_input",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:false",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_joins_empty_tail(),
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:false",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_filter_expr_empty_tail(),
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:false",
                            "?input_data_source",
                        ),
                    ),
                    wrapped_select_having_expr_empty_tail(),
                    "WrappedSelectLimit:None",
                    "WrappedSelectOffset:None",
                    wrapper_pullup_replacer(
                        wrapped_select_order_expr_empty_tail(),
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:false",
                            "?input_data_source",
                        ),
                    ),
                    "WrappedSelectAlias:None",
                    "WrappedSelectDistinct:false",
                    "?select_push_to_cube",
                    "WrappedSelectUngroupedScan:false",
                ),
                "CubeScanWrapperFinalized:false",
            ),
            self.transform_aggregate_subquery(
                "?input_data_source",
                "?group_expr",
                "?aggr_expr",
                "?push_to_cube",
                "?select_push_to_cube",
            ),
        )]);
    }

    pub fn aggregate_merge_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![rewrite(
            "wrapper-merge-aggregation-with-inner-wrapped-select",
            // Input is not a finished wrapper_pullup_replacer, but WrappedSelect just before pullup
            // After pullup replacer would disable push to cube, because any node on top would have WrappedSelect in `from`
            // So there would be no CubeScan to push to
            // Instead, this rule tries to catch `from` before pulling up, and merge outer Aggregate into inner WrappedSelect
            aggregate(
                cube_scan_wrapper(
                    wrapped_select(
                        "WrappedSelectSelectType:Projection",
                        wrapper_pullup_replacer(
                            wrapped_select_projection_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "WrapperReplacerContextPushToCube:true",
                                "WrapperReplacerContextInProjection:false",
                                "?cube_members",
                                "?grouped_subqueries",
                                "?ungrouped_scan",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            "?inner_subqueries",
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "WrapperReplacerContextPushToCube:true",
                                "WrapperReplacerContextInProjection:false",
                                "?cube_members",
                                "?grouped_subqueries",
                                "?ungrouped_scan",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_group_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "WrapperReplacerContextPushToCube:true",
                                "WrapperReplacerContextInProjection:false",
                                "?cube_members",
                                "?grouped_subqueries",
                                "?ungrouped_scan",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_aggr_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "WrapperReplacerContextPushToCube:true",
                                "WrapperReplacerContextInProjection:false",
                                "?cube_members",
                                "?grouped_subqueries",
                                "?ungrouped_scan",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_window_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "WrapperReplacerContextPushToCube:true",
                                "WrapperReplacerContextInProjection:false",
                                "?cube_members",
                                "?grouped_subqueries",
                                "?ungrouped_scan",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            "?inner_from",
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "WrapperReplacerContextPushToCube:true",
                                "WrapperReplacerContextInProjection:false",
                                "?cube_members",
                                "?grouped_subqueries",
                                "?ungrouped_scan",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            "?inner_joins",
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "WrapperReplacerContextPushToCube:true",
                                "WrapperReplacerContextInProjection:false",
                                "?cube_members",
                                "?grouped_subqueries",
                                "?ungrouped_scan",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            "?inner_filters",
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "WrapperReplacerContextPushToCube:true",
                                "WrapperReplacerContextInProjection:false",
                                "?cube_members",
                                "?grouped_subqueries",
                                "?ungrouped_scan",
                                "?input_data_source",
                            ),
                        ),
                        wrapped_select_having_expr_empty_tail(),
                        // Inner must not have limit and offset, because they are not commutative with aggregation
                        "WrappedSelectLimit:None",
                        "WrappedSelectOffset:None",
                        wrapper_pullup_replacer(
                            wrapped_select_order_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "WrapperReplacerContextPushToCube:true",
                                "WrapperReplacerContextInProjection:false",
                                "?cube_members",
                                "?grouped_subqueries",
                                "?ungrouped_scan",
                                "?input_data_source",
                            ),
                        ),
                        "WrappedSelectAlias:None",
                        "WrappedSelectDistinct:false",
                        "WrappedSelectPushToCube:true",
                        "WrappedSelectUngroupedScan:true",
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
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "WrapperReplacerContextPushToCube:true",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:false",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        "?inner_subqueries",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "WrapperReplacerContextPushToCube:true",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:false",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pushdown_replacer(
                        "?group_expr",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "WrapperReplacerContextPushToCube:true",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:false",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pushdown_replacer(
                        "?aggr_expr",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "WrapperReplacerContextPushToCube:true",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:false",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_window_expr_empty_tail(),
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "WrapperReplacerContextPushToCube:true",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:false",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        "?inner_from",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "WrapperReplacerContextPushToCube:true",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:false",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        "?inner_joins",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "WrapperReplacerContextPushToCube:true",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:false",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        "?inner_filters",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "WrapperReplacerContextPushToCube:true",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:false",
                            "?input_data_source",
                        ),
                    ),
                    wrapped_select_having_expr_empty_tail(),
                    "WrappedSelectLimit:None",
                    "WrappedSelectOffset:None",
                    wrapper_pullup_replacer(
                        wrapped_select_order_expr_empty_tail(),
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "WrapperReplacerContextPushToCube:true",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:false",
                            "?input_data_source",
                        ),
                    ),
                    "WrappedSelectAlias:None",
                    "WrappedSelectDistinct:false",
                    "WrappedSelectPushToCube:true",
                    "WrappedSelectUngroupedScan:false",
                ),
                "CubeScanWrapperFinalized:false",
            ),
        )]);
    }

    fn transform_aggregate(
        &self,
        group_expr_var: &'static str,
        aggr_expr_var: &'static str,
        push_to_cube_var: &'static str,
        select_push_to_cube_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let group_expr_var = var!(group_expr_var);
        let aggr_expr_var = var!(aggr_expr_var);
        let push_to_cube_var = var!(push_to_cube_var);
        let select_push_to_cube_var = var!(select_push_to_cube_var);
        move |egraph, subst| {
            Self::transform_aggregate_impl(
                egraph,
                subst,
                group_expr_var,
                aggr_expr_var,
                push_to_cube_var,
                select_push_to_cube_var,
            )
        }
    }

    fn transform_aggregate_subquery(
        &self,
        input_data_source_var: &'static str,
        group_expr_var: &'static str,
        aggr_expr_var: &'static str,
        push_to_cube_var: &'static str,
        select_push_to_cube_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let input_data_source_var = var!(input_data_source_var);
        let group_expr_var = var!(group_expr_var);
        let aggr_expr_var = var!(aggr_expr_var);
        let push_to_cube_var = var!(push_to_cube_var);
        let select_push_to_cube_var = var!(select_push_to_cube_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            if Self::transform_check_subquery_allowed(egraph, subst, &meta, input_data_source_var) {
                Self::transform_aggregate_impl(
                    egraph,
                    subst,
                    group_expr_var,
                    aggr_expr_var,
                    push_to_cube_var,
                    select_push_to_cube_var,
                )
            } else {
                false
            }
        }
    }

    fn transform_aggregate_impl(
        egraph: &mut CubeEGraph,
        subst: &mut Subst,
        group_expr_var: Var,
        aggr_expr_var: Var,
        push_to_cube_var: Var,
        select_push_to_cube_var: Var,
    ) -> bool {
        if egraph[subst[group_expr_var]].data.referenced_expr.is_none() {
            return false;
        }
        if egraph[subst[aggr_expr_var]].data.referenced_expr.is_none() {
            return false;
        }

        if !copy_flag!(
            egraph,
            subst,
            push_to_cube_var,
            WrapperReplacerContextPushToCube,
            select_push_to_cube_var,
            WrappedSelectPushToCube
        ) {
            return false;
        }

        true
    }

    fn check_rollup_allowed(
        &self,
        input_data_source_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let input_data_source_var = var!(input_data_source_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            let Ok(data_source) = Self::get_data_source(egraph, subst, input_data_source_var)
            else {
                return false;
            };

            // TODO check supported operators
            Self::can_rewrite_template(&data_source, &meta, "expressions/rollup")
        }
    }

    fn insert_regular_measure(
        egraph: &mut CubeEGraph,
        subst: &mut Subst,
        column: Column,
        alias: String,
        out_expr_var: Var,
        out_alias_var: Var,
    ) {
        let column_expr_column = egraph.add(LogicalPlanLanguage::ColumnExprColumn(
            ColumnExprColumn(column),
        ));
        let column_expr = egraph.add(LogicalPlanLanguage::ColumnExpr([column_expr_column]));
        let udaf_name_expr = egraph.add(LogicalPlanLanguage::AggregateUDFExprFun(
            AggregateUDFExprFun(MEASURE_UDAF_NAME.to_string()),
        ));
        let udaf_args_expr =
            egraph.add(LogicalPlanLanguage::AggregateUDFExprArgs(vec![column_expr]));
        let udaf_expr = egraph.add(LogicalPlanLanguage::AggregateUDFExpr([
            udaf_name_expr,
            udaf_args_expr,
        ]));

        subst.insert(out_expr_var, udaf_expr);

        let alias_expr_alias =
            egraph.add(LogicalPlanLanguage::AliasExprAlias(AliasExprAlias(alias)));
        subst.insert(out_alias_var, alias_expr_alias);
    }

    fn insert_patch_measure(
        egraph: &mut CubeEGraph,
        subst: &mut Subst,
        column: Column,
        call_agg_type: Option<String>,
        alias: String,
        out_expr_var: Option<Var>,
        out_replace_agg_type: Option<Var>,
        out_alias_var: Var,
    ) {
        let column_expr_column = egraph.add(LogicalPlanLanguage::ColumnExprColumn(
            ColumnExprColumn(column),
        ));
        let column_expr = egraph.add(LogicalPlanLanguage::ColumnExpr([column_expr_column]));
        let new_aggregation_value = match call_agg_type {
            Some(call_agg_type) => egraph.add(LogicalPlanLanguage::LiteralExprValue(
                LiteralExprValue(ScalarValue::Utf8(Some(call_agg_type))),
            )),
            None => egraph.add(LogicalPlanLanguage::LiteralExprValue(LiteralExprValue(
                ScalarValue::Null,
            ))),
        };
        let new_aggregation_expr =
            egraph.add(LogicalPlanLanguage::LiteralExpr([new_aggregation_value]));

        if let Some(out_replace_agg_type) = out_replace_agg_type {
            subst.insert(out_replace_agg_type, new_aggregation_expr);
        }

        let add_filters_value = egraph.add(LogicalPlanLanguage::LiteralExprValue(
            LiteralExprValue(ScalarValue::Null),
        ));
        let add_filters_expr = egraph.add(LogicalPlanLanguage::LiteralExpr([add_filters_value]));
        let udaf_name_expr = egraph.add(LogicalPlanLanguage::AggregateUDFExprFun(
            AggregateUDFExprFun(PATCH_MEASURE_UDAF_NAME.to_string()),
        ));
        let udaf_args_expr = egraph.add(LogicalPlanLanguage::AggregateUDFExprArgs(vec![
            column_expr,
            new_aggregation_expr,
            add_filters_expr,
        ]));
        let udaf_expr = egraph.add(LogicalPlanLanguage::AggregateUDFExpr([
            udaf_name_expr,
            udaf_args_expr,
        ]));

        if let Some(out_expr_var) = out_expr_var {
            subst.insert(out_expr_var, udaf_expr);
        }

        let alias_expr_alias = egraph.add(LogicalPlanLanguage::AliasExprAlias(AliasExprAlias(
            alias.clone(),
        )));
        subst.insert(out_alias_var, alias_expr_alias);
    }

    fn pushdown_measure_impl(
        egraph: &mut CubeEGraph,
        subst: &mut Subst,
        original_expr_var: Var,
        column_var: Option<Var>,
        fun_name_var: Option<Var>,
        distinct_var: Option<Var>,
        cube_members_var: Var,
        out_expr_var: Var,
        out_alias_var: Var,
        alias_to_cube_var: Var,
        meta: &MetaContext,
        disable_strict_agg_type_match: bool,
    ) -> bool {
        let Some(alias) = original_expr_name(egraph, subst[original_expr_var]) else {
            return false;
        };

        for alias_to_cube in var_iter!(
            egraph[subst[alias_to_cube_var]],
            WrapperReplacerContextAliasToCube
        )
        .cloned()
        .collect::<Vec<_>>()
        {
            // Do not push down COUNT(*) if there are joined cubes
            let is_count_rows = column_var.is_none();
            if is_count_rows {
                let joined_cubes = alias_to_cube
                    .iter()
                    .map(|(_, cube_name)| cube_name)
                    .collect::<HashSet<_>>();
                if joined_cubes.len() > 1 {
                    continue;
                }
            }

            for fun in fun_name_var
                .map(|fun_var| {
                    var_iter!(egraph[subst[fun_var]], AggregateFunctionExprFun)
                        .map(|fun| Some(fun.clone()))
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
                    let call_agg_type = MemberRules::get_agg_type(fun.as_ref(), distinct);

                    let column_iter = if let Some(column_var) = column_var {
                        var_iter!(egraph[subst[column_var]], ColumnExprColumn)
                            .cloned()
                            .collect()
                    } else {
                        vec![Column::from_name(MemberRules::default_count_measure_name())]
                    };

                    if let Some(member_names_to_expr) = &mut egraph
                        .index_mut(subst[cube_members_var])
                        .data
                        .member_name_to_expr
                    {
                        for column in column_iter {
                            if let Some((&(Some(ref member), _, _), _)) =
                                LogicalPlanData::do_find_member_by_alias(
                                    member_names_to_expr,
                                    &column.name,
                                )
                            {
                                if let Some(measure) = meta.find_measure_with_name(member) {
                                    let Some(call_agg_type) = &call_agg_type else {
                                        // call_agg_type is None, rewrite as is
                                        Self::insert_regular_measure(
                                            egraph,
                                            subst,
                                            column,
                                            alias,
                                            out_expr_var,
                                            out_alias_var,
                                        );

                                        return true;
                                    };

                                    if measure.is_same_agg_type(
                                        call_agg_type,
                                        disable_strict_agg_type_match,
                                    ) {
                                        Self::insert_regular_measure(
                                            egraph,
                                            subst,
                                            column,
                                            alias,
                                            out_expr_var,
                                            out_alias_var,
                                        );

                                        return true;
                                    }

                                    if measure.allow_replace_agg_type(
                                        call_agg_type,
                                        disable_strict_agg_type_match,
                                    ) {
                                        Self::insert_patch_measure(
                                            egraph,
                                            subst,
                                            column,
                                            Some(call_agg_type.clone()),
                                            alias,
                                            Some(out_expr_var),
                                            None,
                                            out_alias_var,
                                        );

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

    fn pushdown_measure(
        &self,
        original_expr_var: &'static str,
        column_var: Option<&'static str>,
        fun_name_var: Option<&'static str>,
        distinct_var: Option<&'static str>,
        // TODO support cast push downs
        _cast_data_type_var: Option<&'static str>,
        cube_members_var: &'static str,
        out_expr_var: &'static str,
        out_alias_var: &'static str,
        alias_to_cube_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let original_expr_var = var!(original_expr_var);
        let column_var = column_var.map(|v| var!(v));
        let fun_name_var = fun_name_var.map(|v| var!(v));
        let distinct_var = distinct_var.map(|v| var!(v));
        // let cast_data_type_var = cast_data_type_var.map(|v| var!(v));
        let cube_members_var = var!(cube_members_var);
        let out_expr_var = var!(out_expr_var);
        let out_alias_var = var!(out_alias_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let meta = self.meta_context.clone();
        let disable_strict_agg_type_match = self.config_obj.disable_strict_agg_type_match();
        move |egraph, subst| {
            Self::pushdown_measure_impl(
                egraph,
                subst,
                original_expr_var,
                column_var,
                fun_name_var,
                distinct_var,
                cube_members_var,
                out_expr_var,
                out_alias_var,
                alias_to_cube_var,
                &meta,
                disable_strict_agg_type_match,
            )
        }
    }

    fn transform_filtered_measure(
        &self,
        aggr_expr_var: &'static str,
        literal_var: &'static str,
        column_var: &'static str,
        fun_name_var: &'static str,
        cube_members_var: &'static str,
        replace_agg_type_var: &'static str,
        out_measure_alias_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let aggr_expr_var = var!(aggr_expr_var);
        let literal_var = var!(literal_var);
        let column_var = var!(column_var);
        let fun_name_var = var!(fun_name_var);
        let cube_members_var = var!(cube_members_var);
        let replace_agg_type_var = var!(replace_agg_type_var);
        let out_measure_alias_var = var!(out_measure_alias_var);

        let meta = self.meta_context.clone();
        let disable_strict_agg_type_match = self.config_obj.disable_strict_agg_type_match();

        move |egraph, subst| {
            match &egraph[subst[literal_var]].data.constant {
                Some(ConstantFolding::Scalar(_)) => {
                    // Do nothing
                }
                _ => {
                    return false;
                }
            }

            let Some(alias) = original_expr_name(egraph, subst[aggr_expr_var]) else {
                return false;
            };

            for fun in var_iter!(egraph[subst[fun_name_var]], AggregateFunctionExprFun)
                .cloned()
                .collect::<Vec<_>>()
            {
                let call_agg_type = MemberRules::get_agg_type(Some(&fun), false);

                let column_iter = var_iter!(egraph[subst[column_var]], ColumnExprColumn)
                    .cloned()
                    .collect::<Vec<_>>();

                if let Some(member_names_to_expr) = &mut egraph
                    .index_mut(subst[cube_members_var])
                    .data
                    .member_name_to_expr
                {
                    for column in column_iter {
                        if let Some((&(Some(ref member), _, _), _)) =
                            LogicalPlanData::do_find_member_by_alias(
                                member_names_to_expr,
                                &column.name,
                            )
                        {
                            if let Some(measure) = meta.find_measure_with_name(member) {
                                if !measure.allow_add_filter(call_agg_type.as_deref()) {
                                    continue;
                                }

                                let Some(call_agg_type) = &call_agg_type else {
                                    // call_agg_type is None, rewrite as is
                                    Self::insert_patch_measure(
                                        egraph,
                                        subst,
                                        column,
                                        None,
                                        alias,
                                        None,
                                        Some(replace_agg_type_var),
                                        out_measure_alias_var,
                                    );

                                    return true;
                                };

                                if measure
                                    .is_same_agg_type(call_agg_type, disable_strict_agg_type_match)
                                {
                                    Self::insert_patch_measure(
                                        egraph,
                                        subst,
                                        column,
                                        None,
                                        alias,
                                        None,
                                        Some(replace_agg_type_var),
                                        out_measure_alias_var,
                                    );

                                    return true;
                                }

                                if measure.allow_replace_agg_type(
                                    call_agg_type,
                                    disable_strict_agg_type_match,
                                ) {
                                    Self::insert_patch_measure(
                                        egraph,
                                        subst,
                                        column,
                                        Some(call_agg_type.clone()),
                                        alias,
                                        None,
                                        Some(replace_agg_type_var),
                                        out_measure_alias_var,
                                    );

                                    return true;
                                }
                            }
                        }
                    }
                }
            }

            false

            // TODO share code with Self::pushdown_measure: locate cube and measure, check that ?fun matches measure, etc
        }
    }
}
