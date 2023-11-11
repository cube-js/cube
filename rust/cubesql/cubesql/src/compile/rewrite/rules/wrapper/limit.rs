use crate::{
    compile::rewrite::{
        analysis::LogicalPlanAnalysis, cube_scan_wrapper, limit, rules::wrapper::WrapperRules,
        transforming_rewrite, wrapped_select, wrapper_pullup_replacer, LimitFetch, LimitSkip,
        LogicalPlanLanguage, WrappedSelectLimit, WrappedSelectOffset,
    },
    var, var_iter,
};
use egg::{EGraph, Rewrite, Subst};

impl WrapperRules {
    pub fn limit_rules(&self, rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>) {
        rules.extend(vec![transforming_rewrite(
            "wrapper-push-down-limit-to-cube-scan",
            limit(
                "?offset",
                "?limit",
                cube_scan_wrapper(
                    wrapper_pullup_replacer(
                        wrapped_select(
                            "?select_type",
                            "?projection_expr",
                            "?group_expr",
                            "?aggr_expr",
                            "?window_expr",
                            "?cube_scan_input",
                            "?joins",
                            "?filter_expr",
                            "?having_expr",
                            "WrappedSelectLimit:None",
                            "WrappedSelectOffset:None",
                            "?order_expr",
                            "?select_alias",
                            "?select_ungrouped",
                        ),
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                    "CubeScanWrapperFinalized:false".to_string(),
                ),
            ),
            cube_scan_wrapper(
                wrapper_pullup_replacer(
                    wrapped_select(
                        "?select_type",
                        "?projection_expr",
                        "?group_expr",
                        "?aggr_expr",
                        "?window_expr",
                        "?cube_scan_input",
                        "?joins",
                        "?filter_expr",
                        "?having_expr",
                        "?wrapped_select_limit",
                        "?wrapped_select_offset",
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
            self.transform_limit(
                "?limit",
                "?offset",
                "?wrapped_select_limit",
                "?wrapped_select_offset",
            ),
        )])
    }

    fn transform_limit(
        &self,
        limit_var: &'static str,
        offset_var: &'static str,
        wrapped_select_limit_var: &'static str,
        wrapped_select_offset_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let limit_var = var!(limit_var);
        let offset_var = var!(offset_var);
        let wrapped_select_limit_var = var!(wrapped_select_limit_var);
        let wrapped_select_offset_var = var!(wrapped_select_offset_var);
        move |egraph, subst| {
            for limit in var_iter!(egraph[subst[limit_var]], LimitFetch).cloned() {
                for offset in var_iter!(egraph[subst[offset_var]], LimitSkip).cloned() {
                    subst.insert(
                        wrapped_select_limit_var,
                        egraph.add(LogicalPlanLanguage::WrappedSelectLimit(WrappedSelectLimit(
                            limit,
                        ))),
                    );

                    subst.insert(
                        wrapped_select_offset_var,
                        egraph.add(LogicalPlanLanguage::WrappedSelectOffset(
                            WrappedSelectOffset(offset),
                        )),
                    );
                    return true;
                }
            }
            false
        }
    }
}
