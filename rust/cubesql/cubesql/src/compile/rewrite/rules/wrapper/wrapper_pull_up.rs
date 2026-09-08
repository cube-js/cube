use crate::{
    compile::rewrite::{
        cube_scan_wrapper,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::{members::MemberRules, wrapper::WrapperRules},
        transforming_rewrite, wrapped_select, wrapped_select_having_expr_empty_tail,
        wrapped_select_joins_empty_tail, wrapper_pullup_replacer, wrapper_replacer_context,
        LogicalPlanLanguage, WrappedSelectAlias, WrappedSelectLimit, WrappedSelectOffset,
        WrappedSelectSelectType, WrappedSelectType, WrapperReplacerContextAliasToCube,
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
                    "?window_expr",
                    "?filter_expr",
                    "?order_expr",
                    "?inner_select_type",
                    "?inner_projection_expr",
                    "?inner_group_expr",
                    "?inner_aggr_expr",
                    "?inner_window_expr",
                    "?inner_filter_expr",
                    "?inner_order_expr",
                    "?inner_joins",
                    "?inner_limit",
                    "?inner_offset",
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
        window_expr_var: &'static str,
        filter_expr_var: &'static str,
        order_expr_var: &'static str,
        inner_select_type_var: &'static str,
        inner_projection_expr_var: &'static str,
        _inner_group_expr_var: &'static str,
        _inner_aggr_expr_var: &'static str,
        inner_window_expr_var: &'static str,
        inner_filter_expr_var: &'static str,
        inner_order_expr_var: &'static str,
        inner_joins_var: &'static str,
        inner_limit_var: &'static str,
        inner_offset_var: &'static str,
        alias_to_cube_var: &'static str,
        select_alias_var: &'static str,
        alias_to_cube_out_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let select_type_var = var!(select_type_var);
        let projection_expr_var = var!(projection_expr_var);
        let window_expr_var = var!(window_expr_var);
        let filter_expr_var = var!(filter_expr_var);
        let order_expr_var = var!(order_expr_var);
        let inner_select_type_var = var!(inner_select_type_var);
        let inner_projection_expr_var = var!(inner_projection_expr_var);
        let inner_window_expr_var = var!(inner_window_expr_var);
        let inner_filter_expr_var = var!(inner_filter_expr_var);
        let inner_order_expr_var = var!(inner_order_expr_var);
        let inner_joins_var = var!(inner_joins_var);
        let inner_limit_var = var!(inner_limit_var);
        let inner_offset_var = var!(inner_offset_var);
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
                            if subst[projection_expr_var] != subst[inner_projection_expr_var] {
                                return true;
                            }

                            // Identical projections can still be a non-trivial nesting:
                            // outer select can filter, order or window over an inner select
                            // that can't be merged into (it has joins, limit or offset).
                            // Comparing same-purpose eclasses converges: repeated rule
                            // applications produce equal components, and get rejected here.
                            if subst[filter_expr_var] != subst[inner_filter_expr_var] {
                                return true;
                            }
                            if subst[order_expr_var] != subst[inner_order_expr_var] {
                                return true;
                            }
                            if subst[window_expr_var] != subst[inner_window_expr_var] {
                                return true;
                            }

                            let inner_has_joins =
                                var_list_iter!(egraph[subst[inner_joins_var]], WrappedSelectJoins)
                                    .any(|joins| !joins.is_empty());
                            if inner_has_joins {
                                return true;
                            }

                            let inner_has_limit =
                                var_iter!(egraph[subst[inner_limit_var]], WrappedSelectLimit)
                                    .any(|limit| limit.is_some());
                            let inner_has_offset =
                                var_iter!(egraph[subst[inner_offset_var]], WrappedSelectOffset)
                                    .any(|offset| offset.is_some());
                            inner_has_limit || inner_has_offset
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
