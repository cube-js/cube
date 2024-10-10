use crate::{
    compile::rewrite::{
        analysis::LogicalPlanAnalysis, cube_scan_wrapper, rewrite, rules::wrapper::WrapperRules,
        transforming_rewrite, window, wrapped_select, wrapped_select_window_expr_empty_tail,
        wrapper_pullup_replacer, wrapper_pushdown_replacer, ListType, LogicalPlanLanguage,
    },
    var,
};
use egg::{EGraph, Rewrite, Subst};

impl WrapperRules {
    pub fn window_rules(&self, rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>) {
        rules.extend(vec![
            rewrite(
                "wrapper-push-down-window-to-cube-scan",
                window(
                    cube_scan_wrapper(
                        wrapper_pullup_replacer(
                            wrapped_select(
                                "?select_type",
                                "?projection_expr",
                                "?subqueries",
                                "?group_expr",
                                "?aggr_expr",
                                wrapped_select_window_expr_empty_tail(),
                                "?cube_scan_input",
                                "?joins",
                                "?filter_expr",
                                "?having_expr",
                                "?limit",
                                "?offset",
                                "?order_expr",
                                "?select_alias",
                                "?select_distinct",
                                "?select_ungrouped",
                                "?select_ungrouped_scan",
                            ),
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        "CubeScanWrapperFinalized:false",
                    ),
                    "?window_expr",
                ),
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
                        wrapper_pushdown_replacer(
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
                        "?joins",
                        wrapper_pullup_replacer(
                            "?filter_expr",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        "?having_expr",
                        "?limit",
                        "?offset",
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
            ),
            transforming_rewrite(
                "wrapper-push-down-window-combined-to-cube-scan",
                window(
                    cube_scan_wrapper(
                        wrapper_pullup_replacer(
                            wrapped_select(
                                "?select_type",
                                "?projection_expr",
                                "?subqueries",
                                "?group_expr",
                                "?aggr_expr",
                                "?wrapped_window_expr",
                                "?cube_scan_input",
                                "?joins",
                                "?filter_expr",
                                "?having_expr",
                                "?limit",
                                "?offset",
                                "?order_expr",
                                "?select_alias",
                                "?select_distinct",
                                "?select_ungrouped",
                                "?select_ungrouped_scan",
                            ),
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        "CubeScanWrapperFinalized:false",
                    ),
                    "?window_expr",
                ),
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
                        "?new_window_expr",
                        wrapper_pullup_replacer(
                            "?cube_scan_input",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        "?joins",
                        wrapper_pullup_replacer(
                            "?filter_expr",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        "?having_expr",
                        "?limit",
                        "?offset",
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
                self.transform_window_combined(
                    "?wrapped_window_expr",
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                    "?window_expr",
                    "?new_window_expr",
                ),
            ),
        ]);

        if self.config_obj.push_down_pull_up_split() {
            Self::flat_list_pushdown_pullup_rules(
                rules,
                "wrapper-window-expr",
                ListType::WindowWindowExpr,
                ListType::WrappedSelectWindowExpr,
            );
        } else {
            Self::list_pushdown_pullup_rules(
                rules,
                "wrapper-window-expr",
                "WindowWindowExpr",
                "WrappedSelectWindowExpr",
            );
        }
    }

    fn transform_window_combined(
        &self,
        wrapped_window_expr_var: &'static str,
        alias_to_cube_var: &'static str,
        ungrouped_var: &'static str,
        in_projection_var: &'static str,
        cube_members_var: &'static str,
        window_expr_var: &'static str,
        new_window_expr_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let wrapped_window_expr_var = var!(wrapped_window_expr_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let ungrouped_var = var!(ungrouped_var);
        let in_projection_var = var!(in_projection_var);
        let cube_members_var = var!(cube_members_var);
        let window_expr_var = var!(window_expr_var);
        let new_window_expr_var = var!(new_window_expr_var);
        let push_down_pull_up_split = self.config_obj.push_down_pull_up_split();
        move |egraph, subst| {
            for wrapped_node in &egraph[subst[wrapped_window_expr_var]].nodes {
                let LogicalPlanLanguage::WrappedSelectWindowExpr(wrapped_ids) = wrapped_node else {
                    continue;
                };
                if wrapped_ids.is_empty() {
                    continue;
                }
                for window_node in &egraph[subst[window_expr_var]].nodes {
                    let LogicalPlanLanguage::WindowWindowExpr(window_ids) = window_node else {
                        continue;
                    };

                    if !push_down_pull_up_split {
                        let left = egraph.add(LogicalPlanLanguage::WrapperPullupReplacer([
                            subst[wrapped_window_expr_var],
                            subst[alias_to_cube_var],
                            subst[ungrouped_var],
                            subst[in_projection_var],
                            subst[cube_members_var],
                        ]));
                        let right = egraph.add(LogicalPlanLanguage::WrapperPushdownReplacer([
                            subst[window_expr_var],
                            subst[alias_to_cube_var],
                            subst[ungrouped_var],
                            subst[in_projection_var],
                            subst[cube_members_var],
                        ]));

                        subst.insert(
                            new_window_expr_var,
                            egraph.add(LogicalPlanLanguage::WindowWindowExpr(vec![left, right])),
                        );
                        return true;
                    }

                    let wrapped_ids = wrapped_ids.clone();
                    let window_ids = window_ids.clone();

                    let mut new_window_expr_ids = Vec::new();
                    for id in wrapped_ids {
                        new_window_expr_ids.push(egraph.add(
                            LogicalPlanLanguage::WrapperPullupReplacer([
                                id,
                                subst[alias_to_cube_var],
                                subst[ungrouped_var],
                                subst[in_projection_var],
                                subst[cube_members_var],
                            ]),
                        ));
                    }
                    for id in window_ids {
                        new_window_expr_ids.push(egraph.add(
                            LogicalPlanLanguage::WrapperPushdownReplacer([
                                id,
                                subst[alias_to_cube_var],
                                subst[ungrouped_var],
                                subst[in_projection_var],
                                subst[cube_members_var],
                            ]),
                        ));
                    }

                    subst.insert(
                        new_window_expr_var,
                        egraph.add(LogicalPlanLanguage::WindowWindowExpr(new_window_expr_ids)),
                    );
                    return true;
                }
            }
            false
        }
    }
}
