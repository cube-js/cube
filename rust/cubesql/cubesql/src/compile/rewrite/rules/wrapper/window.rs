use egg::{EGraph, Subst};

use crate::{
    compile::rewrite::{
        cube_scan_wrapper, rewrite, rewriter::CubeRewrite, rules::wrapper::WrapperRules,
        transforming_rewrite, window, wrapped_select, wrapped_select_window_expr_empty_tail,
        wrapper_pullup_replacer, wrapper_pushdown_replacer, ListType, LogicalPlanAnalysis,
        LogicalPlanLanguage,
    },
    var,
};

impl WrapperRules {
    pub fn window_rules(&self, rules: &mut Vec<CubeRewrite>) {
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
                                "?select_push_to_cube",
                                "?select_ungrouped_scan",
                            ),
                            "?context",
                        ),
                        "CubeScanWrapperFinalized:false",
                    ),
                    "?window_expr",
                ),
                cube_scan_wrapper(
                    wrapped_select(
                        "?select_type",
                        wrapper_pullup_replacer("?projection_expr", "?context"),
                        wrapper_pullup_replacer("?subqueries", "?context"),
                        wrapper_pullup_replacer("?group_expr", "?context"),
                        wrapper_pullup_replacer("?aggr_expr", "?context"),
                        wrapper_pushdown_replacer("?window_expr", "?context"),
                        wrapper_pullup_replacer("?cube_scan_input", "?context"),
                        wrapper_pullup_replacer("?joins", "?context"),
                        wrapper_pullup_replacer("?filter_expr", "?context"),
                        "?having_expr",
                        "?limit",
                        "?offset",
                        wrapper_pullup_replacer("?order_expr", "?context"),
                        "?select_alias",
                        "?select_distinct",
                        "?select_push_to_cube",
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
                            "?context",
                        ),
                        "CubeScanWrapperFinalized:false",
                    ),
                    "?window_expr",
                ),
                cube_scan_wrapper(
                    wrapped_select(
                        "?select_type",
                        wrapper_pullup_replacer("?projection_expr", "?context"),
                        wrapper_pullup_replacer("?subqueries", "?context"),
                        wrapper_pullup_replacer("?group_expr", "?context"),
                        wrapper_pullup_replacer("?aggr_expr", "?context"),
                        "?new_window_expr",
                        wrapper_pullup_replacer("?cube_scan_input", "?context"),
                        wrapper_pullup_replacer("?joins", "?context"),
                        wrapper_pullup_replacer("?filter_expr", "?context"),
                        "?having_expr",
                        "?limit",
                        "?offset",
                        wrapper_pullup_replacer("?order_expr", "?context"),
                        "?select_alias",
                        "?select_distinct",
                        "?select_ungrouped",
                        "?select_ungrouped_scan",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                self.transform_window_combined(
                    "?wrapped_window_expr",
                    "?window_expr",
                    "?context",
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
        window_expr_var: &'static str,
        context_var: &'static str,
        new_window_expr_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let wrapped_window_expr_var = var!(wrapped_window_expr_var);
        let window_expr_var = var!(window_expr_var);
        let context_var = var!(context_var);
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
                            subst[context_var],
                        ]));
                        let right = egraph.add(LogicalPlanLanguage::WrapperPushdownReplacer([
                            subst[window_expr_var],
                            subst[context_var],
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
                            LogicalPlanLanguage::WrapperPullupReplacer([id, subst[context_var]]),
                        ));
                    }
                    for id in window_ids {
                        new_window_expr_ids.push(egraph.add(
                            LogicalPlanLanguage::WrapperPushdownReplacer([id, subst[context_var]]),
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
