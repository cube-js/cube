use crate::{
    compile::rewrite::{
        analysis::LogicalPlanAnalysis, cube_scan_wrapper, projection, rules::wrapper::WrapperRules,
        transforming_rewrite, wrapped_select, wrapped_select_aggr_expr_empty_tail,
        wrapped_select_filter_expr_empty_tail, wrapped_select_group_expr_empty_tail,
        wrapped_select_having_expr_empty_tail, wrapped_select_joins_empty_tail,
        wrapped_select_order_expr_empty_tail, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        LogicalPlanLanguage, ProjectionAlias, WrappedSelectAlias, WrappedSelectUngrouped,
        WrapperPullupReplacerUngrouped,
    },
    var, var_iter,
};
use egg::{EGraph, Rewrite, Subst};

impl WrapperRules {
    pub fn projection_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        rules.extend(vec![transforming_rewrite(
            "wrapper-push-down-projection-to-cube-scan",
            projection(
                "?expr",
                cube_scan_wrapper(
                    wrapper_pullup_replacer(
                        "?cube_scan_input",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                "?projection_alias",
                "ProjectionSplit:false",
            ),
            cube_scan_wrapper(
                wrapped_select(
                    "WrappedSelectSelectType:Projection",
                    wrapper_pushdown_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_group_expr_empty_tail(),
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_aggr_expr_empty_tail(),
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
                    wrapped_select_joins_empty_tail(),
                    wrapped_select_filter_expr_empty_tail(),
                    wrapped_select_having_expr_empty_tail(),
                    "WrappedSelectLimit:None",
                    "WrappedSelectOffset:None",
                    wrapper_pullup_replacer(
                        wrapped_select_order_expr_empty_tail(),
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                    "?select_alias",
                    "?select_ungrouped",
                ),
                "CubeScanWrapperFinalized:false",
            ),
            self.transform_projection(
                "?projection_alias",
                "?ungrouped",
                "?select_alias",
                "?select_ungrouped",
            ),
        )]);

        Self::list_pushdown_pullup_rules(
            rules,
            "wrapper-projection-expr",
            "ProjectionExpr",
            "WrappedSelectProjectionExpr",
        );
    }

    fn transform_projection(
        &self,
        projection_alias_var: &'static str,
        ungrouped_var: &'static str,
        select_alias_var: &'static str,
        select_ungrouped_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let projection_alias_var = var!(projection_alias_var);
        let ungrouped_var = var!(ungrouped_var);
        let select_alias_var = var!(select_alias_var);
        let select_ungrouped_var = var!(select_ungrouped_var);
        move |egraph, subst| {
            for projection_alias in
                var_iter!(egraph[subst[projection_alias_var]], ProjectionAlias).cloned()
            {
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
                        select_alias_var,
                        egraph.add(LogicalPlanLanguage::WrappedSelectAlias(WrappedSelectAlias(
                            projection_alias,
                        ))),
                    );
                    return true;
                }
            }
            false
        }
    }
}
