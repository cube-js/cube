use crate::{
    compile::rewrite::{
        binary_expr, rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        transforming_rewrite, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        WrapperPullupReplacerAliasToCube,
    },
    var, var_iter,
};
use egg::Subst;

impl WrapperRules {
    pub fn binary_expr_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            rewrite(
                "wrapper-push-down-binary-expr",
                wrapper_pushdown_replacer(
                    binary_expr("?left", "?op", "?right"),
                    "?alias_to_cube",
                    "?push_to_cube",
                    "?in_projection",
                    "?cube_members",
                    "?grouped_subqueries",
                ),
                binary_expr(
                    wrapper_pushdown_replacer(
                        "?left",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    "?op",
                    wrapper_pushdown_replacer(
                        "?right",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                ),
            ),
            transforming_rewrite(
                "wrapper-pull-up-binary-expr",
                binary_expr(
                    wrapper_pullup_replacer(
                        "?left",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    "?op",
                    wrapper_pullup_replacer(
                        "?right",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                ),
                wrapper_pullup_replacer(
                    binary_expr("?left", "?op", "?right"),
                    "?alias_to_cube",
                    "?push_to_cube",
                    "?in_projection",
                    "?cube_members",
                    "?grouped_subqueries",
                ),
                self.transform_binary_expr("?op", "?alias_to_cube"),
            ),
        ]);
    }

    fn transform_binary_expr(
        &self,
        _operator_var: &'static str,
        alias_to_cube_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        // let operator_var = var!(operator_var);
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
                        .contains_key("expressions/binary")
                    {
                        // TODO check supported operators
                        return true;
                    }
                }
            }
            false
        }
    }
}
