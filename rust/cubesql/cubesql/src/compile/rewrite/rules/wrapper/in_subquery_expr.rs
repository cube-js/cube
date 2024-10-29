use crate::{
    compile::rewrite::{
        insubquery_expr, rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        transforming_rewrite, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        WrapperPullupReplacerUngrouped, WrapperPushdownReplacerUngrouped,
    },
    copy_flag, var,
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
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                insubquery_expr(
                    wrapper_pushdown_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        "?subquery",
                        "?alias_to_cube",
                        "?pullup_ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    "?negated",
                ),
                self.transform_in_subquery_pushdown("?ungrouped", "?pullup_ungrouped"),
            ),
            rewrite(
                "wrapper-in-subquery-pull-up",
                insubquery_expr(
                    wrapper_pullup_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        "?subquery",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    "?negated",
                ),
                wrapper_pullup_replacer(
                    insubquery_expr("?expr", "?subquery", "?negated"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
            ),
        ]);
    }

    fn transform_in_subquery_pushdown(
        &self,
        ungrouped_var: &'static str,
        pullup_ungrouped_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let ungrouped_var = var!(ungrouped_var);
        let pullup_ungrouped_var = var!(pullup_ungrouped_var);
        move |egraph: &mut CubeEGraph, subst| {
            if !copy_flag!(
                egraph,
                subst,
                ungrouped_var,
                WrapperPushdownReplacerUngrouped,
                pullup_ungrouped_var,
                WrapperPullupReplacerUngrouped
            ) {
                return false;
            }
            true
        }
    }
}
