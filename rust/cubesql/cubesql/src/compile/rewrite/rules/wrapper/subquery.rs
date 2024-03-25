use crate::{
    compile::rewrite::{
        analysis::LogicalPlanAnalysis, cube_scan_wrapper, filter, rewrite,
        rules::wrapper::WrapperRules, subquery, transforming_rewrite, wrapped_select,
        wrapped_select_aggr_expr_empty_tail, wrapped_select_filter_expr,
        wrapped_select_filter_expr_empty_tail, wrapped_select_group_expr_empty_tail,
        wrapped_select_having_expr_empty_tail, wrapped_select_joins_empty_tail,
        wrapped_select_order_expr_empty_tail, wrapped_select_projection_expr_empty_tail,
        wrapped_select_subqueries_empty_tail, wrapped_select_window_expr_empty_tail,
        wrapper_pullup_replacer, wrapper_pushdown_replacer, LogicalPlanLanguage,
        WrappedSelectUngrouped, WrappedSelectUngroupedScan, WrapperPullupReplacerUngrouped,
    },
    var, var_iter,
};
use egg::{EGraph, Rewrite, Subst};

impl WrapperRules {
    pub fn subquery_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        /* rules.extend(vec![
            transforming_rewrite(
                "wrapper-push-down-subquery-to-cube-scan",
                subquery(
                    cube_scan_wrapper(
                        wrapper_pullup_replacer(
                            "?cube_scan_input",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        "CubeScanWrapperFinalized:false",
                    ),
                    "?subqueries",
                    "?types",
                ),
                cube_scan_wrapper(
                    wrapped_select(
                        "WrappedSelectSelectType:Projection",
                        wrapper_pullup_replacer(
                            wrapped_select_projection_expr_empty_tail(),
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        wrapper_pushdown_replacer(
                            "?subqueries",
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_group_expr_empty_tail(),
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_aggr_expr_empty_tail(),
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_window_expr_empty_tail(),
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
                            wrapped_select_filter_expr_empty_tail(),
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        wrapped_select_having_expr_empty_tail(),
                        "WrappedSelectLimit:None",
                        "WrappedSelectOffset:None",
                        wrapper_pullup_replacer(
                            wrapped_select_order_expr_empty_tail(),
                            "?alias_to_cube",
                            "?ungrouped",
                            "?in_projection",
                            "?cube_members",
                        ),
                        "WrappedSelectAlias:None",
                        "?select_ungrouped",
                        "?select_ungrouped_scan",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                self.transform_subquery(
                    "?ungrouped",
                    "?select_ungrouped",
                    "?select_ungrouped_scan",
                ),
            ),
            rewrite(
                "wrapper-subqueries-wrapped-scan-to-pull-up",
                wrapper_pushdown_replacer(
                    cube_scan_wrapper("?input", "CubeScanWrapperFinalized:true"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                wrapper_pullup_replacer(
                    cube_scan_wrapper("?input", "CubeScanWrapperFinalized:true"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
            ),
        ]); */

        rules.extend(vec![rewrite(
            "wrapper-subqueries-push-down",
                subquery(
                    "?input",
                    "?subqueries",
                    "?types",
                ),
                subquery(
                    "?input"
                    "?subqueries",
                    "?types",
                ),
        )]);
        Self::list_pushdown_pullup_rules(
            rules,
            "wrapper-subqueries",
            "SubquerySubqueries",
            "WrappedSelectSubqueries",
        );
    }

    fn transform_subquery(
        &self,
        ungrouped_var: &'static str,
        select_ungrouped_var: &'static str,
        select_ungrouped_scan_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        println!("====!!!!---!!!====");
        let ungrouped_var = var!(ungrouped_var);
        let select_ungrouped_var = var!(select_ungrouped_var);
        let select_ungrouped_scan_var = var!(select_ungrouped_scan_var);
        println!("!!! select ungrouped var: {:?}", select_ungrouped_var);
        move |egraph, subst| {
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
                    select_ungrouped_scan_var,
                    egraph.add(LogicalPlanLanguage::WrappedSelectUngroupedScan(
                        WrappedSelectUngroupedScan(ungrouped),
                    )),
                );
                return true;
            }
            false
        }
    }
}
