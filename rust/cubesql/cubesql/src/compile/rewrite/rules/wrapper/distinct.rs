use crate::compile::rewrite::{
    analysis::LogicalPlanAnalysis, cube_scan_wrapper, distinct, rewrite,
    rules::wrapper::WrapperRules, wrapped_select, wrapper_pullup_replacer, LogicalPlanLanguage,
};
use egg::Rewrite;

impl WrapperRules {
    pub fn distinct_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        rules.extend(vec![rewrite(
            "wrapper-push-down-distinct-to-cube-scan",
            distinct(cube_scan_wrapper(
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
                        "?having_expr",
                        "?limit",
                        "?offset",
                        "?order_expr",
                        "?select_alias",
                        "?select_distinct",
                        "WrappedSelectUngrouped:false",
                        "?select_ungrouped_scan",
                    ),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                "CubeScanWrapperFinalized:false".to_string(),
            )),
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
                        "?having_expr",
                        "?limit",
                        "?offset",
                        "?order_expr",
                        "?select_alias",
                        "WrappedSelectDistinct:true",
                        "WrappedSelectUngrouped:false",
                        "?select_ungrouped_scan",
                    ),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                "CubeScanWrapperFinalized:false",
            ),
        )])
    }
}
