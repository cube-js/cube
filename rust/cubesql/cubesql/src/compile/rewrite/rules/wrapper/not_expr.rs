use crate::{
    compile::rewrite::{
        not_expr, rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        transforming_rewrite, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        WrapperPullupReplacerAliasToCube,
    },
    var, var_iter,
};
use egg::Subst;

impl WrapperRules {
    pub fn not_expr_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            rewrite(
                "wrapper-not-push-down",
                wrapper_pushdown_replacer(
                    not_expr("?expr"),
                    "?alias_to_cube",
                    "?push_to_cube",
                    "?in_projection",
                    "?cube_members",
                    "?grouped_subqueries",
                ),
                not_expr(wrapper_pushdown_replacer(
                    "?expr",
                    "?alias_to_cube",
                    "?push_to_cube",
                    "?in_projection",
                    "?cube_members",
                    "?grouped_subqueries",
                )),
            ),
            transforming_rewrite(
                "wrapper-not-pull-up",
                not_expr(wrapper_pullup_replacer(
                    "?expr",
                    "?alias_to_cube",
                    "?push_to_cube",
                    "?in_projection",
                    "?cube_members",
                    "?grouped_subqueries",
                )),
                wrapper_pullup_replacer(
                    not_expr("?expr"),
                    "?alias_to_cube",
                    "?push_to_cube",
                    "?in_projection",
                    "?cube_members",
                    "?grouped_subqueries",
                ),
                self.transform_not_expr("?alias_to_cube"),
            ),
        ]);
    }

    fn transform_not_expr(
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
                        .contains_key("expressions/not")
                    {
                        return true;
                    }
                }
            }
            false
        }
    }
}
