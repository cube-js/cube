use crate::{
    compile::rewrite::{
        insubquery_expr, rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        transforming_rewrite, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        WrapperPullupReplacerGroupedSubqueries, WrapperPullupReplacerPushToCube,
        WrapperPushdownReplacerGroupedSubqueries, WrapperPushdownReplacerPushToCube,
    },
    copy_flag, copy_value, var,
};
use egg::Subst;

impl WrapperRules {
    pub fn in_subquery_expr_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            transforming_rewrite(
                "wrapper-in-subquery-push-down",
                wrapper_pushdown_replacer(
                    insubquery_expr("?expr", "?subquery", "?negated"),
                    "?alias_to_cube",
                    "?push_to_cube",
                    "?in_projection",
                    "?cube_members",
                    "?grouped_subqueries",
                ),
                insubquery_expr(
                    wrapper_pushdown_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        "?subquery",
                        "?alias_to_cube",
                        "?pullup_push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?pullup_grouped_subqueries",
                    ),
                    "?negated",
                ),
                self.transform_in_subquery_pushdown(
                    "?push_to_cube",
                    "?pullup_push_to_cube",
                    "?grouped_subqueries",
                    "?pullup_grouped_subqueries",
                ),
            ),
            rewrite(
                "wrapper-in-subquery-pull-up",
                insubquery_expr(
                    wrapper_pullup_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        "?subquery",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    "?negated",
                ),
                wrapper_pullup_replacer(
                    insubquery_expr("?expr", "?subquery", "?negated"),
                    "?alias_to_cube",
                    "?push_to_cube",
                    "?in_projection",
                    "?cube_members",
                    "?grouped_subqueries",
                ),
            ),
        ]);
    }

    fn transform_in_subquery_pushdown(
        &self,
        push_to_cube_var: &'static str,
        pullup_push_to_cube_var: &'static str,
        grouped_subqueries_var: &'static str,
        pullup_grouped_subqueries_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let push_to_cube_var = var!(push_to_cube_var);
        let pullup_push_to_cube_var = var!(pullup_push_to_cube_var);
        let grouped_subqueries_var = var!(grouped_subqueries_var);
        let pullup_grouped_subqueries_var = var!(pullup_grouped_subqueries_var);
        move |egraph: &mut CubeEGraph, subst| {
            if !copy_flag!(
                egraph,
                subst,
                push_to_cube_var,
                WrapperPushdownReplacerPushToCube,
                pullup_push_to_cube_var,
                WrapperPullupReplacerPushToCube
            ) {
                return false;
            }
            if !copy_value!(
                egraph,
                subst,
                Vec<String>,
                grouped_subqueries_var,
                WrapperPushdownReplacerGroupedSubqueries,
                pullup_grouped_subqueries_var,
                WrapperPullupReplacerGroupedSubqueries
            ) {
                return false;
            }
            true
        }
    }
}
