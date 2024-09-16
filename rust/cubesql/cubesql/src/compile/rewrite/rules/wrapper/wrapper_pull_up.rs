use crate::{
    compile::rewrite::{
        analysis::LogicalPlanAnalysis, cube_scan_wrapper, rules::wrapper::WrapperRules,
        transforming_rewrite, wrapped_select, wrapped_select_having_expr_empty_tail,
        wrapped_select_joins_empty_tail, wrapper_pullup_replacer, LogicalPlanLanguage,
        WrappedSelectSelectType, WrappedSelectType,
    },
    var, var_iter, var_list_iter,
};
use egg::{EGraph, Rewrite, Subst};

impl WrapperRules {
    pub fn wrapper_pull_up_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        rules.extend(vec![
            transforming_rewrite(
                "wrapper-pull-up-to-cube-scan-non-wrapped-select",
                cube_scan_wrapper(
                    wrapped_select(
                        "?select_type",
                        wrapper_pullup_replacer(
                            "?projection_expr",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        wrapper_pullup_replacer(
                            "?subqueries",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        wrapper_pullup_replacer(
                            "?group_expr",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        wrapper_pullup_replacer(
                            "?aggr_expr",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        wrapper_pullup_replacer(
                            "?window_expr",
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
                            "?filter_expr",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        wrapped_select_having_expr_empty_tail(),
                        "WrappedSelectLimit:None",
                        "WrappedSelectOffset:None",
                        wrapper_pullup_replacer(
                            "?order_expr",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        "?select_alias",
                        "?select_distinct",
                        "?select_ungrouped",
                        "?select_ungrouped_scan",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                cube_scan_wrapper(
                    wrapper_pullup_replacer(
                        wrapped_select(
                            "?select_type",
                            "?projection_expr",
                            "?subqueries",
                            "?group_expr",
                            "?aggr_expr",
                            "?window_expr",
                            "?cube_scan_input",
                            wrapped_select_joins_empty_tail(),
                            "?filter_expr",
                            wrapped_select_having_expr_empty_tail(),
                            "WrappedSelectLimit:None",
                            "WrappedSelectOffset:None",
                            "?order_expr",
                            "?select_alias",
                            "?select_distinct",
                            "?select_ungrouped",
                            "?select_ungrouped_scan",
                        ),
                        "?alias_to_cube",
                        // TODO in fact ungrouped flag is being used not only to indicate that underlying query is ungrouped however to indicate that WrappedSelect won't push down Cube members. Do we need separate flags?
                        "WrapperPullupReplacerUngrouped:false",
                        "?in_projection",
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
                            "?in_projection",
                            "?cube_members",
                        ),
                        wrapper_pullup_replacer(
                            "?subqueries",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        wrapper_pullup_replacer(
                            "?group_expr",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        wrapper_pullup_replacer(
                            "?aggr_expr",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        wrapper_pullup_replacer(
                            "?window_expr",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select(
                                "?inner_select_type",
                                "?inner_projection_expr",
                                "?inner_subqueries",
                                "?inner_group_expr",
                                "?inner_aggr_expr",
                                "?inner_window_expr",
                                "?inner_cube_scan_input",
                                "?inner_joins",
                                "?inner_filter_expr",
                                "?inner_having_expr",
                                "?inner_limit",
                                "?inner_offset",
                                "?inner_order_expr",
                                "?inner_alias",
                                "?inner_distinct",
                                "?inner_ungrouped",
                                "?inner_ungrouped_scan",
                            ),
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        wrapped_select_joins_empty_tail(),
                        wrapper_pullup_replacer(
                            "?filter_expr",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        wrapped_select_having_expr_empty_tail(),
                        "WrappedSelectLimit:None",
                        "WrappedSelectOffset:None",
                        wrapper_pullup_replacer(
                            "?order_expr",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        "?select_alias",
                        "?select_distinct",
                        "?select_ungrouped",
                        "?select_ungrouped_scan",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                cube_scan_wrapper(
                    wrapper_pullup_replacer(
                        wrapped_select(
                            "?select_type",
                            "?projection_expr",
                            "?subqueries",
                            "?group_expr",
                            "?aggr_expr",
                            "?window_expr",
                            wrapped_select(
                                "?inner_select_type",
                                "?inner_projection_expr",
                                "?inner_subqueries",
                                "?inner_group_expr",
                                "?inner_aggr_expr",
                                "?inner_window_expr",
                                "?inner_cube_scan_input",
                                "?inner_joins",
                                "?inner_filter_expr",
                                "?inner_having_expr",
                                "?inner_limit",
                                "?inner_offset",
                                "?inner_order_expr",
                                "?inner_alias",
                                "?inner_distinct",
                                "?inner_ungrouped",
                                "?inner_ungrouped_scan",
                            ),
                            wrapped_select_joins_empty_tail(),
                            "?filter_expr",
                            wrapped_select_having_expr_empty_tail(),
                            "WrappedSelectLimit:None",
                            "WrappedSelectOffset:None",
                            "?order_expr",
                            "?select_alias",
                            "?select_distinct",
                            "?select_ungrouped",
                            "?select_ungrouped_scan",
                        ),
                        "?alias_to_cube",
                        "WrapperPullupReplacerUngrouped:false",
                        "?inner_projection_expr",
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
        ]);
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
}
