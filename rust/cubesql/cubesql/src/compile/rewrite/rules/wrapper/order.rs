use crate::{
    compile::rewrite::{
        cube_scan_wrapper,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        sort, transforming_rewrite, wrapped_select, wrapped_select_order_expr_empty_tail,
        wrapper_pullup_replacer, wrapper_pushdown_replacer, WrapperPullupReplacerGroupedSubqueries,
        WrapperPullupReplacerPushToCube, WrapperPushdownReplacerGroupedSubqueries,
        WrapperPushdownReplacerPushToCube,
    },
    copy_flag, copy_value, var,
};
use egg::Subst;

impl WrapperRules {
    pub fn order_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![transforming_rewrite(
            "wrapper-push-down-order-to-cube-scan",
            sort(
                "?order_expr",
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
                            wrapped_select_order_expr_empty_tail(),
                            "?select_alias",
                            "?select_distinct",
                            "?select_push_to_cube",
                            "?select_ungrouped_scan",
                        ),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
            ),
            cube_scan_wrapper(
                wrapped_select(
                    "?select_type",
                    wrapper_pullup_replacer(
                        "?projection_expr",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        "?subqueries",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        "?group_expr",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        "?aggr_expr",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        "?window_expr",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        "?cube_scan_input",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        "?joins",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        "?filter_expr",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    "?having_expr",
                    "?limit",
                    "?offset",
                    wrapper_pushdown_replacer(
                        "?order_expr",
                        "?alias_to_cube",
                        "?pushdown_push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?pushdown_grouped_subqueries",
                    ),
                    "?select_alias",
                    "?select_distinct",
                    "?select_push_to_cube",
                    "?select_ungrouped_scan",
                ),
                "CubeScanWrapperFinalized:false",
            ),
            self.transform_order(
                "?push_to_cube",
                "?pushdown_push_to_cube",
                "?grouped_subqueries",
                "?pushdown_grouped_subqueries",
            ),
        )]);

        Self::list_pushdown_pullup_rules(
            rules,
            "wrapper-order-expr",
            "SortExp",
            "WrappedSelectOrderExpr",
        );
    }

    fn transform_order(
        &self,
        push_to_cube_var: &'static str,
        pushdown_push_to_cube_var: &'static str,
        grouped_subqueries_var: &'static str,
        pushdown_grouped_subqueries_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let push_to_cube_var = var!(push_to_cube_var);
        let pushdown_push_to_cube_var = var!(pushdown_push_to_cube_var);
        let grouped_subqueries_var = var!(grouped_subqueries_var);
        let pushdown_grouped_subqueries_var = var!(pushdown_grouped_subqueries_var);
        move |egraph, subst| {
            if !copy_flag!(
                egraph,
                subst,
                push_to_cube_var,
                WrapperPullupReplacerPushToCube,
                pushdown_push_to_cube_var,
                WrapperPushdownReplacerPushToCube
            ) {
                return false;
            }
            if !copy_value!(
                egraph,
                subst,
                Vec<String>,
                grouped_subqueries_var,
                WrapperPullupReplacerGroupedSubqueries,
                pushdown_grouped_subqueries_var,
                WrapperPushdownReplacerGroupedSubqueries
            ) {
                return false;
            }
            true
        }
    }
}
