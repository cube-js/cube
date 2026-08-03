use crate::{
    compile::rewrite::{
        cube_scan_wrapper,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::{members::MemberRules, wrapper::WrapperRules},
        transforming_rewrite, wrapped_select, wrapped_select_having_expr_empty_tail,
        wrapped_select_joins_empty_tail, wrapper_pullup_replacer, wrapper_replacer_context,
        LogicalPlanLanguage, WrappedSelectAlias, WrappedSelectSelectType, WrappedSelectType,
        WrapperReplacerContextAliasToCube,
    },
    var, var_iter, var_list_iter,
};
use egg::{Subst, Var};

impl WrapperRules {
    pub fn wrapper_pull_up_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            transforming_rewrite(
                "wrapper-pull-up-to-cube-scan-non-wrapped-select",
                cube_scan_wrapper(
                    wrapped_select(
                        "?select_type",
                        wrapper_pullup_replacer(
                            "?projection_expr",
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
                            "?group_expr",
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
                            "?aggr_expr",
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
                            "?window_expr",
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
                            "?joins",
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
                        wrapped_select_having_expr_empty_tail(),
                        "WrappedSelectLimit:None",
                        "WrappedSelectOffset:None",
                        wrapper_pullup_replacer(
                            "?order_expr",
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
                        "?select_alias",
                        "?select_distinct",
                        "?select_push_to_cube",
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
                            "?joins",
                            "?filter_expr",
                            wrapped_select_having_expr_empty_tail(),
                            "WrappedSelectLimit:None",
                            "WrappedSelectOffset:None",
                            "?order_expr",
                            "?select_alias",
                            "?select_distinct",
                            "?select_push_to_cube",
                            "?select_ungrouped_scan",
                        ),
                        wrapper_replacer_context(
                            "?alias_to_cube_out",
                            // This is fixed to false for any LHS because we should only allow to push to Cube when from is ungrouped CubeScan
                            // And after pulling replacer over this node it will be WrappedSelect(from=CubeScan), so it should not allow to push for whatever LP is on top of it
                            "WrapperReplacerContextPushToCube:false",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                self.transform_pull_up_wrapper_select(
                    "?cube_scan_input",
                    "?alias_to_cube",
                    "?select_alias",
                    "?alias_to_cube_out",
                ),
            ),
            transforming_rewrite(
                "wrapper-pull-up-to-cube-scan-non-trivial-wrapped-select",
                cube_scan_wrapper(
                    wrapped_select(
                        "?select_type",
                        wrapper_pullup_replacer(
                            "?projection_expr",
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
                            "?group_expr",
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
                            "?aggr_expr",
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
                            "?window_expr",
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
                                "?inner_push_to_cube",
                                "?inner_ungrouped_scan",
                            ),
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
                            // TODO handle non-empty joins
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
                        wrapper_pullup_replacer(
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
                        wrapped_select_having_expr_empty_tail(),
                        "WrappedSelectLimit:None",
                        "WrappedSelectOffset:None",
                        wrapper_pullup_replacer(
                            "?order_expr",
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
                        "?select_alias",
                        "?select_distinct",
                        // This node has a WrappedSelect in from, so it's not allowed to use push to Cube
                        "WrappedSelectPushToCube:false",
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
                                "?inner_push_to_cube",
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
                            "WrappedSelectPushToCube:false",
                            "?select_ungrouped_scan",
                        ),
                        wrapper_replacer_context(
                            "?alias_to_cube_out",
                            // This is fixed to false for any LHS because we should only allow to push to Cube when from is ungrouped CubeSCan
                            // And after pulling replacer over this node it will be WrappedSelect(from=WrappedSelect), so it should not allow to push for whatever LP is on top of it
                            "WrapperReplacerContextPushToCube:false",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
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
                    "?alias_to_cube",
                    "?select_alias",
                    "?alias_to_cube_out",
                ),
            ),
        ]);
    }

    fn replace_aliases(
        egraph: &mut CubeEGraph,
        subst: &mut Subst,
        alias_to_cube_var: Var,
        select_alias_var: Var,
        alias_to_cube_out_var: Var,
    ) -> bool {
        for alias_to_cube in var_iter!(
            egraph[subst[alias_to_cube_var]],
            WrapperReplacerContextAliasToCube
        ) {
            for projection_alias in var_iter!(egraph[subst[select_alias_var]], WrappedSelectAlias) {
                let replaced_alias_to_cube =
                    MemberRules::replace_alias(&alias_to_cube, &projection_alias);
                let new_alias_to_cube =
                    egraph.add(LogicalPlanLanguage::WrapperReplacerContextAliasToCube(
                        WrapperReplacerContextAliasToCube(replaced_alias_to_cube),
                    ));
                subst.insert(alias_to_cube_out_var, new_alias_to_cube);
                return true;
            }
        }

        false
    }

    fn transform_pull_up_wrapper_select(
        &self,
        cube_scan_input_var: &'static str,
        alias_to_cube_var: &'static str,
        select_alias_var: &'static str,
        alias_to_cube_out_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let cube_scan_input_var = var!(cube_scan_input_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let select_alias_var = var!(select_alias_var);
        let alias_to_cube_out_var = var!(alias_to_cube_out_var);
        move |egraph, subst| {
            for _ in var_list_iter!(egraph[subst[cube_scan_input_var]], WrappedSelect).cloned() {
                return false;
            }

            if !Self::replace_aliases(
                egraph,
                subst,
                alias_to_cube_var,
                select_alias_var,
                alias_to_cube_out_var,
            ) {
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
        alias_to_cube_var: &'static str,
        select_alias_var: &'static str,
        alias_to_cube_out_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let select_type_var = var!(select_type_var);
        let projection_expr_var = var!(projection_expr_var);
        let inner_select_type_var = var!(inner_select_type_var);
        let inner_projection_expr_var = var!(inner_projection_expr_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let select_alias_var = var!(select_alias_var);
        let alias_to_cube_out_var = var!(alias_to_cube_out_var);
        move |egraph, subst| {
            if !Self::replace_aliases(
                egraph,
                subst,
                alias_to_cube_var,
                select_alias_var,
                alias_to_cube_out_var,
            ) {
                return false;
            }

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
