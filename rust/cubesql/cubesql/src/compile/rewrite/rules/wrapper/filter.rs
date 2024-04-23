use crate::{
    compile::rewrite::{
        analysis::LogicalPlanAnalysis, cube_scan_wrapper, filter, rules::wrapper::WrapperRules,
        subquery, transforming_rewrite, wrapped_select, wrapped_select_aggr_expr_empty_tail,
        wrapped_select_filter_expr, wrapped_select_filter_expr_empty_tail,
        wrapped_select_group_expr_empty_tail, wrapped_select_having_expr_empty_tail,
        wrapped_select_joins_empty_tail, wrapped_select_order_expr_empty_tail,
        wrapped_select_projection_expr_empty_tail, wrapped_select_subqueries_empty_tail,
        wrapped_select_window_expr_empty_tail, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        LogicalPlanLanguage, WrappedSelectUngrouped, WrappedSelectUngroupedScan,
        WrapperPullupReplacerUngrouped,
    },
    var, var_iter,
};
use egg::{EGraph, Rewrite, Subst, Var};

impl WrapperRules {
    pub fn filter_rules(&self, rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>) {
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
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
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
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_subqueries_empty_tail(),
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_group_expr_empty_tail(),
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_aggr_expr_empty_tail(),
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_window_expr_empty_tail(),
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        "?cube_scan_input",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    wrapped_select_joins_empty_tail(),
                    wrapped_select_filter_expr(
                        wrapper_pushdown_replacer(
                            "?filter_expr",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_filter_expr_empty_tail(),
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                    ),
                    wrapped_select_having_expr_empty_tail(),
                    "WrappedSelectLimit:None",
                    "WrappedSelectOffset:None",
                    wrapper_pullup_replacer(
                        wrapped_select_order_expr_empty_tail(),
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    "WrappedSelectAlias:None",
                    "WrappedSelectDistinct:false",
                    "?select_ungrouped",
                    "?select_ungrouped_scan",
                ),
                "CubeScanWrapperFinalized:false",
            ),
            self.transform_filter("?ungrouped", "?select_ungrouped", "?select_ungrouped_scan"),
        )]);

        Self::list_pushdown_pullup_rules(
            rules,
            "wrapper-filter-expr",
            "WrappedSelectFilterExpr",
            "WrappedSelectFilterExpr",
        );
    }

    pub fn filter_rules_subquery(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        rules.extend(vec![transforming_rewrite(
            "wrapper-push-down-filter-and-subquery-to-cube-scan",
            filter(
                "?filter_expr",
                subquery(
                    cube_scan_wrapper(
                        wrapper_pullup_replacer(
                            "?cube_scan_input",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
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
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    wrapper_pushdown_replacer(
                        "?subqueries",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_group_expr_empty_tail(),
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_aggr_expr_empty_tail(),
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_window_expr_empty_tail(),
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        "?cube_scan_input",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    wrapped_select_joins_empty_tail(),
                    wrapped_select_filter_expr(
                        wrapper_pushdown_replacer(
                            "?filter_expr",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_filter_expr_empty_tail(),
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                    ),
                    wrapped_select_having_expr_empty_tail(),
                    "WrappedSelectLimit:None",
                    "WrappedSelectOffset:None",
                    wrapper_pullup_replacer(
                        wrapped_select_order_expr_empty_tail(),
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    "WrappedSelectAlias:None",
                    "WrappedSelectDistinct:false",
                    "?select_ungrouped",
                    "?select_ungrouped_scan",
                ),
                "CubeScanWrapperFinalized:false",
            ),
            self.transform_filter_subquery(
                "?alias_to_cube",
                "?ungrouped",
                "?select_ungrouped",
                "?select_ungrouped_scan",
            ),
        )]);
    }

    fn transform_filter(
        &self,
        ungrouped_var: &'static str,
        select_ungrouped_var: &'static str,
        select_ungrouped_scan_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let ungrouped_var = var!(ungrouped_var);
        let select_ungrouped_var = var!(select_ungrouped_var);
        let select_ungrouped_scan_var = var!(select_ungrouped_scan_var);
        move |egraph, subst| {
            Self::transform_filter_impl(
                egraph,
                subst,
                ungrouped_var,
                select_ungrouped_var,
                select_ungrouped_scan_var,
            )
        }
    }

    fn transform_filter_subquery(
        &self,
        alias_to_cube_var: &'static str,
        ungrouped_var: &'static str,
        select_ungrouped_var: &'static str,
        select_ungrouped_scan_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let ungrouped_var = var!(ungrouped_var);
        let select_ungrouped_var = var!(select_ungrouped_var);
        let select_ungrouped_scan_var = var!(select_ungrouped_scan_var);
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
                    ungrouped_var,
                    select_ungrouped_var,
                    select_ungrouped_scan_var,
                )
            } else {
                false
            }
        }
    }

    fn transform_filter_impl(
        egraph: &mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
        subst: &mut Subst,
        ungrouped_var: Var,
        select_ungrouped_var: Var,
        select_ungrouped_scan_var: Var,
    ) -> bool {
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
                select_ungrouped_scan_var,
                egraph.add(LogicalPlanLanguage::WrappedSelectUngroupedScan(
                    WrappedSelectUngroupedScan(ungrouped),
                )),
            );
            return true;
        }
        false
    }
}
