use crate::compile::rewrite::{
    analysis::LogicalPlanAnalysis, cube_scan_wrapper, rewrite, rules::wrapper::WrapperRules,
    window, wrapped_select, wrapped_select_window_expr_empty_tail, wrapper_pullup_replacer,
    wrapper_pushdown_replacer, LogicalPlanLanguage,
};
use egg::Rewrite;

impl WrapperRules {
    pub fn window_rules(&self, rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>) {
        rules.extend(vec![rewrite(
            "wrapper-push-down-window-to-cube-scan",
            window(
                cube_scan_wrapper(
                    wrapper_pullup_replacer(
                        wrapped_select(
                            "?select_type",
                            "?projection_expr",
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
                            "?select_ungrouped",
                        ),
                        "?alias_to_cube",
                        "?ungrouped",
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
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        "?group_expr",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        "?aggr_expr",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                    wrapper_pushdown_replacer(
                        "?window_expr",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        "?cube_scan_input",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                    "?joins",
                    "?filter_expr",
                    "?having_expr",
                    "?limit",
                    "?offset",
                    wrapper_pullup_replacer(
                        "?order_expr",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                    "?select_alias",
                    "?select_ungrouped",
                ),
                "CubeScanWrapperFinalized:false",
            ),
        )]);

        Self::list_pushdown_pullup_rules(
            rules,
            "wrapper-window-expr",
            "WindowWindowExpr",
            "WrappedSelectWindowExpr",
        );
    }
}
