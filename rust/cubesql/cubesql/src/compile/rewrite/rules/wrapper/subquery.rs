use crate::{
    compile::rewrite::{
        analysis::LogicalPlanAnalysis, cube_scan_wrapper, rewrite, rules::wrapper::WrapperRules,
        subquery, transforming_rewrite, wrapped_select, wrapped_select_aggr_expr_empty_tail,
        wrapped_select_filter_expr_empty_tail, wrapped_select_group_expr_empty_tail,
        wrapped_select_having_expr_empty_tail, wrapped_select_joins_empty_tail,
        wrapped_select_order_expr_empty_tail, wrapped_select_projection_expr_empty_tail,
        wrapped_select_subqueries_empty_tail, wrapped_select_window_expr_empty_tail,
        wrapper_pullup_replacer, wrapper_pushdown_replacer, LogicalPlanLanguage,
        WrappedSelectUngrouped, WrappedSelectUngroupedScan, WrapperPullupReplacerUngrouped,
    },
    var, var_iter, var_list_iter,
};
use egg::{EGraph, Rewrite, Subst};

impl WrapperRules {
    pub fn subquery_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        rules.extend(vec![transforming_rewrite(
            "wrapper-subqueries-wrapped-scan-to-pull-up",
            wrapper_pushdown_replacer(
                cube_scan_wrapper(
                    wrapper_pullup_replacer(
                        "?cube_scan_input",
                        "?inner_alias_to_cube",
                        "?nner_ungrouped",
                        "?inner_in_projection",
                        "?inner_cube_members",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
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
            self.transform_check_subquery_wrapped("?cube_scan_input"),
        )]);
        Self::list_pushdown_pullup_rules(
            rules,
            "wrapper-subqueries",
            "SubquerySubqueries",
            "WrappedSelectSubqueries",
        );
    }

    fn transform_check_subquery_wrapped(
        &self,
        cube_scan_input_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_scan_input_var = var!(cube_scan_input_var);
        move |egraph, subst| {
            for _ in var_list_iter!(egraph[subst[cube_scan_input_var]], WrappedSelect).cloned() {
                return true;
            }
            false
        }
    }
}
