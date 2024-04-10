use crate::{
    compile::rewrite::{
        analysis::LogicalPlanAnalysis, cube_scan_wrapper, filter, rewrite,
        rules::wrapper::WrapperRules, subquery, subquery_pullup_replacer,
        subquery_pushdown_replacer, transforming_rewrite, wrapped_select,
        wrapped_select_aggr_expr_empty_tail, wrapped_select_filter_expr,
        wrapped_select_filter_expr_empty_tail, wrapped_select_group_expr_empty_tail,
        wrapped_select_having_expr_empty_tail, wrapped_select_joins_empty_tail,
        wrapped_select_order_expr_empty_tail, wrapped_select_projection_expr_empty_tail,
        wrapped_select_subqueries_empty_tail, wrapped_select_window_expr_empty_tail,
        wrapped_subquery, wrapper_pullup_replacer, wrapper_pushdown_replacer, LogicalPlanLanguage,
        WrappedSelectUngrouped, WrappedSelectUngroupedScan, WrapperPullupReplacerUngrouped,
    },
    var, var_iter, var_list_iter,
};
use egg::{EGraph, Rewrite, Subst};

impl WrapperRules {
    pub fn subquery_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        rules.extend(vec![
            transforming_rewrite(
                "wrapper-subqueries-subquery-push-down",
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
                cube_scan_wrapper(
                    wrapped_select(
                        "WrappedSelectSelectType:Subquery",
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
                        wrapper_pullup_replacer(
                            wrapped_select_filter_expr_empty_tail(),
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
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
                        "?select_ungrouped",
                        "?select_ungrouped_scan",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                self.transform_subquery(
                    "?ungrouped",
                    "?select_ungrouped",
                    "?select_ungrouped_scan",
                ),
            ),
            rewrite(
                "wrapper-subqueries-pull-up-to-wrapped-select",
                cube_scan_wrapper(
                    wrapped_select(
                        "?select_type",
                        "?projection_expr",
                        wrapper_pullup_replacer(
                            wrapped_select_subqueries_empty_tail(),
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        "?group_expr",
                        "?aggr_expr",
                        "?wihdow_expr",
                        wrapper_pullup_replacer(
                            wrapped_select(
                                "WrappedSelectSelectType:Subquery",
                                wrapped_select_projection_expr_empty_tail(),
                                "?subqueries",
                                wrapped_select_group_expr_empty_tail(),
                                wrapped_select_aggr_expr_empty_tail(),
                                wrapped_select_window_expr_empty_tail(),
                                "?cube_scan_input",
                                wrapped_select_joins_empty_tail(),
                                wrapped_select_filter_expr_empty_tail(),
                                wrapped_select_having_expr_empty_tail(),
                                "WrappedSelectLimit:None",
                                "WrappedSelectOffset:None",
                                wrapped_select_order_expr_empty_tail(),
                                "WrappedSelectAlias:None",
                                "?inner_select_ungrouped",
                                "?inner_select_ungrouped_scan",
                            ),
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        "?joins",
                        "?filter_expr",
                        "?having_expr",
                        "?limit",
                        "?offset",
                        "?order_expr",
                        "?select_alias",
                        "?select_ungrouped",
                        "?select_ungrouped_scan",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                cube_scan_wrapper(
                    wrapped_select(
                        "?select_type",
                        "?projection_expr",
                        wrapper_pullup_replacer(
                            "?subqueries",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        "?group_expr",
                        "?aggr_expr",
                        "?wihdow_expr",
                        wrapper_pullup_replacer(
                            "?cube_scan_input",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        "?joins",
                        "?filter_expr",
                        "?having_expr",
                        "?limit",
                        "?offset",
                        "?order_expr",
                        "?select_alias",
                        "?select_ungrouped",
                        "?select_ungrouped_scan",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
            ),
            transforming_rewrite(
                "wrapper-subqueries-wrapped-scan-to-pull-up",
                wrapper_pushdown_replacer(
                    cube_scan_wrapper(
                        wrapper_pullup_replacer(
                            "?cube_scan_input",
                            "?inner_alias_to_cube",
                            "?nner_ungrouped",
                            "?inner_in_projection",
                            "?inner_cube_members",
                        ),
                        "CubeScanWrapperFinalized:false",
                    ),
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
                self.transform_check_subquery_wrapped("?cube_scan_input"),
            ),
        ]);
        Self::list_pushdown_pullup_rules(
            rules,
            "wrapper-subqueries",
            "SubquerySubqueries",
            "WrappedSelectSubqueries",
        );
    }

    fn transform_subquery(
        &self,
        ungrouped_var: &'static str,
        select_ungrouped_var: &'static str,
        select_ungrouped_scan_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let ungrouped_var = var!(ungrouped_var);
        let select_ungrouped_var = var!(select_ungrouped_var);
        let select_ungrouped_scan_var = var!(select_ungrouped_scan_var);
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

    fn transform_check_subquery_wrapped(
        &self,
        cube_scan_input_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_scan_input_var = var!(cube_scan_input_var);
        move |egraph, subst| {
            for _ in var_list_iter!(egraph[subst[cube_scan_input_var]], WrappedSelect).cloned() {
                return true;
            }
            false
        }
    }
}
