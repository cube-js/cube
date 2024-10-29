use crate::{
    compile::rewrite::{
        inlist_expr, rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        transforming_rewrite, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        WrapperPullupReplacerAliasToCube, WrapperPullupReplacerPushToCube,
        WrapperPushdownReplacerPushToCube,
    },
    copy_flag, var, var_iter,
};
use egg::Subst;

impl WrapperRules {
    pub fn in_list_expr_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            transforming_rewrite(
                "wrapper-in-list-only-consts-push-down",
                wrapper_pushdown_replacer(
                    inlist_expr("?expr", "?list", "?negated"),
                    "?alias_to_cube",
                    "?push_to_cube",
                    "?in_projection",
                    "?cube_members",
                ),
                inlist_expr(
                    wrapper_pushdown_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        "?list",
                        "?alias_to_cube",
                        "?pullup_push_to_cube",
                        "?in_projection",
                        "?cube_members",
                    ),
                    "?negated",
                ),
                self.transform_in_list_only_consts(
                    "?list",
                    "?push_to_cube",
                    "?pullup_push_to_cube",
                ),
            ),
            rewrite(
                "wrapper-in-list-push-down",
                wrapper_pushdown_replacer(
                    inlist_expr("?expr", "?list", "?negated"),
                    "?alias_to_cube",
                    "?push_to_cube",
                    "?in_projection",
                    "?cube_members",
                ),
                inlist_expr(
                    wrapper_pushdown_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                    ),
                    wrapper_pushdown_replacer(
                        "?list",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                    ),
                    "?negated",
                ),
            ),
            transforming_rewrite(
                "wrapper-in-list-pull-up",
                inlist_expr(
                    wrapper_pullup_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        "?list",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                    ),
                    "?negated",
                ),
                wrapper_pullup_replacer(
                    inlist_expr("?expr", "?list", "?negated"),
                    "?alias_to_cube",
                    "?push_to_cube",
                    "?in_projection",
                    "?cube_members",
                ),
                self.transform_in_list_expr("?alias_to_cube"),
            ),
        ]);

        // TODO: support for flatten list
        Self::expr_list_pushdown_pullup_rules(rules, "wrapper-in-list-exprs", "InListExprList");
    }

    fn transform_in_list_expr(
        &self,
        alias_to_cube_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            for alias_to_cube in var_iter!(
                egraph[subst[alias_to_cube_var]],
                WrapperPullupReplacerAliasToCube
            )
            .cloned()
            {
                if let Some(sql_generator) = meta.sql_generator_by_alias_to_cube(&alias_to_cube) {
                    if sql_generator
                        .get_sql_templates()
                        .templates
                        .contains_key("expressions/in_list")
                    {
                        return true;
                    }
                }
            }
            false
        }
    }

    fn transform_in_list_only_consts(
        &self,
        list_var: &'static str,
        push_to_cube_var: &'static str,
        pullup_push_to_cube_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let list_var = var!(list_var);
        let push_to_cube_var = var!(push_to_cube_var);
        let pullup_push_to_cube_var = var!(pullup_push_to_cube_var);
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
            return egraph[subst[list_var]].data.constant_in_list.is_some();
        }
    }
}
