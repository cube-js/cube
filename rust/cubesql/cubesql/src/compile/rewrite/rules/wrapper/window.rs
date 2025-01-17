use crate::compile::rewrite::{
    cube_scan_wrapper, rewrite, rewriter::CubeRewrite, rules::wrapper::WrapperRules, window,
    wrapped_select, wrapped_select_window_expr_empty_tail, wrapper_pullup_replacer,
    wrapper_pushdown_replacer, ListType,
};

impl WrapperRules {
    pub fn window_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![rewrite(
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
        )]);

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
}
