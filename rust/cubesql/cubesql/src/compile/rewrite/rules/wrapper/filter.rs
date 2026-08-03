use crate::{
    compile::rewrite::{
        cube_scan_wrapper, filter, rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        subquery, transforming_rewrite, wrapped_select, wrapped_select_aggr_expr_empty_tail,
        wrapped_select_filter_expr, wrapped_select_filter_expr_empty_tail,
        wrapped_select_group_expr_empty_tail, wrapped_select_having_expr_empty_tail,
        wrapped_select_joins_empty_tail, wrapped_select_order_expr_empty_tail,
        wrapped_select_projection_expr_empty_tail, wrapped_select_subqueries_empty_tail,
        wrapped_select_window_expr_empty_tail, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        wrapper_replacer_context, WrappedSelectPushToCube, WrappedSelectUngroupedScan,
        WrapperReplacerContextPushToCube, WrapperReplacerContextUngroupedScan,
    },
    copy_flag, var,
};
use egg::{Subst, Var};

impl WrapperRules {
    pub fn filter_rules(&self, rules: &mut Vec<CubeRewrite>) {
        // TODO respect having filter for push down to wrapped select
        // rules.extend(vec![rewrite(
        //     "wrapper-push-down-filter-to-wrapped-select",
        //     filter(
        //         "?filter_expr",
        //         cube_scan_wrapper(
        //             wrapper_pullup_replacer(
        //                 wrapped_select(
        //                     "?select_type",
        //                     "?projection_expr",
        //                     "?group_expr",
        //                     "?aggr_expr",
        //                     "?window_expr",
        //                     "?cube_scan_input",
        //                     "?joins",
        //                     "?old_filter_expr",
        //                     "?having_expr",
        //                     "?wrapped_select_limit",
        //                     "?wrapped_select_offset",
        //                     "?order_expr",
        //                     "?select_alias",
        //                     "?select_ungrouped",
        //                 ),
        //                 "?alias_to_cube",
        //                 "?ungrouped",
        //                 "?cube_members",
        //             ),
        //             "CubeScanWrapperFinalized:false".to_string(),
        //         ),
        //     ),
        //     cube_scan_wrapper(
        //         wrapped_select(
        //             "?select_type",
        //             wrapper_pullup_replacer(
        //                 "?projection_expr",
        //                 "?alias_to_cube",
        //                 "?ungrouped",
        //                 "?cube_members",
        //             ),
        //             wrapper_pullup_replacer(
        //                 "?group_expr",
        //                 "?alias_to_cube",
        //                 "?ungrouped",
        //                 "?cube_members",
        //             ),
        //             wrapper_pullup_replacer(
        //                 "?aggr_expr",
        //                 "?alias_to_cube",
        //                 "?ungrouped",
        //                 "?cube_members",
        //             ),
        //             wrapper_pullup_replacer(
        //                 "?window_expr",
        //                 "?alias_to_cube",
        //                 "?ungrouped",
        //                 "?cube_members",
        //             ),
        //             wrapper_pullup_replacer(
        //                 "?cube_scan_input",
        //                 "?alias_to_cube",
        //                 "?ungrouped",
        //                 "?cube_members",
        //             ),
        //             "?joins",
        //             wrapped_select_filter_expr(
        //                 wrapper_pullup_replacer(
        //                     "?old_filter_expr",
        //                     "?alias_to_cube",
        //                     "?ungrouped",
        //                     "?cube_members",
        //                 ),
        //                 wrapper_pushdown_replacer(
        //                     "?filter_expr",
        //                     "?alias_to_cube",
        //                     "?ungrouped",
        //                     "?cube_members",
        //                 ),
        //             ),
        //             "?having_expr",
        //             "?wrapped_select_limit",
        //             "?wrapped_select_offset",
        //             wrapper_pullup_replacer(
        //                 "?order_expr",
        //                 "?alias_to_cube",
        //                 "?ungrouped",
        //                 "?cube_members",
        //             ),
        //             "?select_alias",
        //             "?select_ungrouped",
        //         ),
        //         "CubeScanWrapperFinalized:false",
        //     ),
        // )]);

        rules.extend(vec![transforming_rewrite(
            "wrapper-push-down-filter-to-cube-scan",
            filter(
                "?filter_expr",
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
            ),
            cube_scan_wrapper(
                wrapped_select(
                    "WrappedSelectSelectType:Projection",
                    wrapper_pullup_replacer(
                        wrapped_select_projection_expr_empty_tail(),
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
                        wrapped_select_subqueries_empty_tail(),
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
                        wrapped_select_group_expr_empty_tail(),
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
                        wrapped_select_aggr_expr_empty_tail(),
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
                        wrapped_select_window_expr_empty_tail(),
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
                    wrapper_pullup_replacer(
                        wrapped_select_joins_empty_tail(),
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
                    wrapped_select_filter_expr(
                        wrapper_pushdown_replacer(
                            "?filter_expr",
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
                            wrapped_select_filter_expr_empty_tail(),
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
                    wrapped_select_having_expr_empty_tail(),
                    "WrappedSelectLimit:None",
                    "WrappedSelectOffset:None",
                    wrapper_pullup_replacer(
                        wrapped_select_order_expr_empty_tail(),
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
                    "WrappedSelectAlias:None",
                    "WrappedSelectDistinct:false",
                    "?select_push_to_cube",
                    "?select_ungrouped_scan",
                ),
                "CubeScanWrapperFinalized:false",
            ),
            self.transform_filter(
                "?push_to_cube",
                "?ungrouped_scan",
                "?select_push_to_cube",
                "?select_ungrouped_scan",
            ),
        )]);

        Self::list_pushdown_pullup_rules(
            rules,
            "wrapper-filter-expr",
            "WrappedSelectFilterExpr",
            "WrappedSelectFilterExpr",
        );
    }

    pub fn filter_rules_subquery(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![transforming_rewrite(
            "wrapper-push-down-filter-and-subquery-to-cube-scan",
            filter(
                "?filter_expr",
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
            ),
            cube_scan_wrapper(
                wrapped_select(
                    "WrappedSelectSelectType:Projection",
                    wrapper_pullup_replacer(
                        wrapped_select_projection_expr_empty_tail(),
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
                    wrapper_pushdown_replacer(
                        "?subqueries",
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
                        wrapped_select_group_expr_empty_tail(),
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
                        wrapped_select_aggr_expr_empty_tail(),
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
                        wrapped_select_window_expr_empty_tail(),
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
                    wrapper_pullup_replacer(
                        wrapped_select_joins_empty_tail(),
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
                    wrapped_select_filter_expr(
                        wrapper_pushdown_replacer(
                            "?filter_expr",
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
                            wrapped_select_filter_expr_empty_tail(),
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
                    wrapped_select_having_expr_empty_tail(),
                    "WrappedSelectLimit:None",
                    "WrappedSelectOffset:None",
                    wrapper_pullup_replacer(
                        wrapped_select_order_expr_empty_tail(),
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
                    "WrappedSelectAlias:None",
                    "WrappedSelectDistinct:false",
                    "?select_push_to_cube",
                    "?select_ungrouped_scan",
                ),
                "CubeScanWrapperFinalized:false",
            ),
            self.transform_filter_subquery(
                "?input_data_source",
                "?push_to_cube",
                "?ungrouped_scan",
                "?select_push_to_cube",
                "?select_ungrouped_scan",
            ),
        )]);
    }

    pub fn filter_merge_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![rewrite(
            "wrapper-merge-filter-with-inner-wrapped-select",
            // Input is not a finished wrapper_pullup_replacer, but WrappedSelect just before pullup
            // After pullup replacer would disable push to cube, because any node on top would have WrappedSelect in `from`
            // So there would be no CubeScan to push to
            // Instead, this rule tries to catch `from` before pulling up, and merge outer Filter into inner WrappedSelect
            filter(
                "?filter_expr",
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
                            wrapped_select_subqueries_empty_tail(),
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
                            wrapped_select_filter_expr_empty_tail(),
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
            ),
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
                            "WrapperReplacerContextUngroupedScan:true",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_subqueries_empty_tail(),
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "WrapperReplacerContextPushToCube:true",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:true",
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
                            "WrapperReplacerContextUngroupedScan:true",
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
                            "WrapperReplacerContextUngroupedScan:true",
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
                            "WrapperReplacerContextUngroupedScan:true",
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
                            "WrapperReplacerContextUngroupedScan:true",
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
                            "WrapperReplacerContextUngroupedScan:true",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pushdown_replacer(
                        "?filter_expr",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "WrapperReplacerContextPushToCube:true",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:true",
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
                            "WrapperReplacerContextUngroupedScan:true",
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
        )]);
    }

    fn transform_filter(
        &self,
        push_to_cube_var: &'static str,
        ungrouped_scan_var: &'static str,
        select_push_to_cube_var: &'static str,
        select_ungrouped_scan_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let push_to_cube_var = var!(push_to_cube_var);
        let ungrouped_scan_var = var!(ungrouped_scan_var);
        let select_push_to_cube_var = var!(select_push_to_cube_var);
        let select_ungrouped_scan_var = var!(select_ungrouped_scan_var);
        move |egraph, subst| {
            Self::transform_filter_impl(
                egraph,
                subst,
                push_to_cube_var,
                ungrouped_scan_var,
                select_push_to_cube_var,
                select_ungrouped_scan_var,
            )
        }
    }

    fn transform_filter_subquery(
        &self,
        input_data_source_var: &'static str,
        push_to_cube_var: &'static str,
        ungrouped_scan_var: &'static str,
        select_push_to_cube_var: &'static str,
        select_ungrouped_scan_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let input_data_source_var = var!(input_data_source_var);
        let push_to_cube_var = var!(push_to_cube_var);
        let ungrouped_scan_var = var!(ungrouped_scan_var);
        let select_push_to_cube_var = var!(select_push_to_cube_var);
        let select_ungrouped_scan_var = var!(select_ungrouped_scan_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            if Self::transform_check_subquery_allowed(egraph, subst, &meta, input_data_source_var) {
                Self::transform_filter_impl(
                    egraph,
                    subst,
                    push_to_cube_var,
                    ungrouped_scan_var,
                    select_push_to_cube_var,
                    select_ungrouped_scan_var,
                )
            } else {
                false
            }
        }
    }

    fn transform_filter_impl(
        egraph: &mut CubeEGraph,
        subst: &mut Subst,
        push_to_cube_var: Var,
        ungrouped_scan_var: Var,
        select_push_to_cube_var: Var,
        select_ungrouped_scan_var: Var,
    ) -> bool {
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

        if !copy_flag!(
            egraph,
            subst,
            ungrouped_scan_var,
            WrapperReplacerContextUngroupedScan,
            select_ungrouped_scan_var,
            WrappedSelectUngroupedScan
        ) {
            return false;
        }

        true
    }
}
