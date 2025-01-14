use crate::{
    compile::rewrite::{
        cube_scan_wrapper, filter,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        subquery, transforming_rewrite, wrapped_select, wrapped_select_aggr_expr_empty_tail,
        wrapped_select_filter_expr, wrapped_select_filter_expr_empty_tail,
        wrapped_select_group_expr_empty_tail, wrapped_select_having_expr_empty_tail,
        wrapped_select_joins_empty_tail, wrapped_select_order_expr_empty_tail,
        wrapped_select_projection_expr_empty_tail, wrapped_select_subqueries_empty_tail,
        wrapped_select_window_expr_empty_tail, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        LogicalPlanLanguage, WrappedSelectPushToCube, WrappedSelectUngroupedScan,
        WrapperPullupReplacerGroupedSubqueries, WrapperPullupReplacerPushToCube,
        WrapperPushdownReplacerGroupedSubqueries, WrapperPushdownReplacerPushToCube,
    },
    copy_flag, copy_value, var, var_iter,
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
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
            ),
            cube_scan_wrapper(
                wrapped_select(
                    "WrappedSelectSelectType:Projection",
                    wrapper_pullup_replacer(
                        wrapped_select_projection_expr_empty_tail(),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_subqueries_empty_tail(),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_group_expr_empty_tail(),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_aggr_expr_empty_tail(),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_window_expr_empty_tail(),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        "?cube_scan_input",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_joins_empty_tail(),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapped_select_filter_expr(
                        wrapper_pushdown_replacer(
                            "?filter_expr",
                            "?alias_to_cube",
                            "?pushdown_push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?pushdown_grouped_subqueries",
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_filter_expr_empty_tail(),
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                        ),
                    ),
                    wrapped_select_having_expr_empty_tail(),
                    "WrappedSelectLimit:None",
                    "WrappedSelectOffset:None",
                    wrapper_pullup_replacer(
                        wrapped_select_order_expr_empty_tail(),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
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
                "?pushdown_push_to_cube",
                "?select_push_to_cube",
                "?select_ungrouped_scan",
                "?grouped_subqueries",
                "?pushdown_grouped_subqueries",
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
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
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
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pushdown_replacer(
                        "?subqueries",
                        "?alias_to_cube",
                        "?pushdown_push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?pushdown_grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_group_expr_empty_tail(),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_aggr_expr_empty_tail(),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_window_expr_empty_tail(),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        "?cube_scan_input",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_joins_empty_tail(),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapped_select_filter_expr(
                        wrapper_pushdown_replacer(
                            "?filter_expr",
                            "?alias_to_cube",
                            "?pushdown_push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?pushdown_grouped_subqueries",
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_filter_expr_empty_tail(),
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                        ),
                    ),
                    wrapped_select_having_expr_empty_tail(),
                    "WrappedSelectLimit:None",
                    "WrappedSelectOffset:None",
                    wrapper_pullup_replacer(
                        wrapped_select_order_expr_empty_tail(),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    "WrappedSelectAlias:None",
                    "WrappedSelectDistinct:false",
                    "?select_push_to_cube",
                    "?select_ungrouped_scan",
                ),
                "CubeScanWrapperFinalized:false",
            ),
            self.transform_filter_subquery(
                "?alias_to_cube",
                "?push_to_cube",
                "?pushdown_push_to_cube",
                "?select_push_to_cube",
                "?select_ungrouped_scan",
                "?grouped_subqueries",
                "?pushdown_grouped_subqueries",
            ),
        )]);
    }

    fn transform_filter(
        &self,
        push_to_cube_var: &'static str,
        pushdown_push_to_cube_var: &'static str,
        select_push_to_cube_var: &'static str,
        select_ungrouped_scan_var: &'static str,
        grouped_subqueries_var: &'static str,
        pushdown_grouped_subqueries_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let push_to_cube_var = var!(push_to_cube_var);
        let pushdown_push_to_cube_var = var!(pushdown_push_to_cube_var);
        let select_push_to_cube_var = var!(select_push_to_cube_var);
        let select_ungrouped_scan_var = var!(select_ungrouped_scan_var);
        let grouped_subqueries_var = var!(grouped_subqueries_var);
        let pushdown_grouped_subqueries_var = var!(pushdown_grouped_subqueries_var);
        move |egraph, subst| {
            Self::transform_filter_impl(
                egraph,
                subst,
                push_to_cube_var,
                pushdown_push_to_cube_var,
                select_push_to_cube_var,
                select_ungrouped_scan_var,
                grouped_subqueries_var,
                pushdown_grouped_subqueries_var,
            )
        }
    }

    fn transform_filter_subquery(
        &self,
        alias_to_cube_var: &'static str,
        push_to_cube_var: &'static str,
        pushdown_push_to_cube_var: &'static str,
        select_push_to_cube_var: &'static str,
        select_ungrouped_scan_var: &'static str,
        grouped_subqueries_var: &'static str,
        pushdown_grouped_subqueries_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let push_to_cube_var = var!(push_to_cube_var);
        let pushdown_push_to_cube_var = var!(pushdown_push_to_cube_var);
        let select_push_to_cube_var = var!(select_push_to_cube_var);
        let select_ungrouped_scan_var = var!(select_ungrouped_scan_var);
        let grouped_subqueries_var = var!(grouped_subqueries_var);
        let pushdown_grouped_subqueries_var = var!(pushdown_grouped_subqueries_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            if Self::transform_check_subquery_allowed(
                egraph,
                subst,
                meta.clone(),
                alias_to_cube_var,
            ) {
                Self::transform_filter_impl(
                    egraph,
                    subst,
                    push_to_cube_var,
                    pushdown_push_to_cube_var,
                    select_push_to_cube_var,
                    select_ungrouped_scan_var,
                    grouped_subqueries_var,
                    pushdown_grouped_subqueries_var,
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
        pushdown_push_to_cube_var: Var,
        select_push_to_cube_var: Var,
        select_ungrouped_scan_var: Var,
        grouped_subqueries_var: Var,
        pushdown_grouped_subqueries_var: Var,
    ) -> bool {
        if !copy_flag!(
            egraph,
            subst,
            push_to_cube_var,
            WrapperPullupReplacerPushToCube,
            pushdown_push_to_cube_var,
            WrapperPushdownReplacerPushToCube
        ) {
            return false;
        }

        if !copy_value!(
            egraph,
            subst,
            Vec<String>,
            grouped_subqueries_var,
            WrapperPullupReplacerGroupedSubqueries,
            pushdown_grouped_subqueries_var,
            WrapperPushdownReplacerGroupedSubqueries
        ) {
            return false;
        }

        for push_to_cube in var_iter!(
            egraph[subst[push_to_cube_var]],
            WrapperPullupReplacerPushToCube
        )
        .cloned()
        {
            subst.insert(
                select_push_to_cube_var,
                egraph.add(LogicalPlanLanguage::WrappedSelectPushToCube(
                    WrappedSelectPushToCube(push_to_cube),
                )),
            );

            subst.insert(
                select_ungrouped_scan_var,
                egraph.add(LogicalPlanLanguage::WrappedSelectUngroupedScan(
                    WrappedSelectUngroupedScan(push_to_cube),
                )),
            );
            return true;
        }
        false
    }
}
