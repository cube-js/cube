use crate::{
    compile::rewrite::{
        analysis::LogicalPlanAnalysis, is_not_null_expr, is_null_expr, rewrite,
        rules::wrapper::WrapperRules, transforming_rewrite, wrapper_pullup_replacer,
        wrapper_pushdown_replacer, LogicalPlanLanguage, WrapperPullupReplacerAliasToCube,
    },
    var, var_iter,
};
use egg::{EGraph, Rewrite, Subst};

impl WrapperRules {
    pub fn is_null_expr_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        rules.extend(vec![
            rewrite(
                "wrapper-push-down-is-null-expr",
                wrapper_pushdown_replacer(
                    is_null_expr("?expr"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                is_null_expr(wrapper_pushdown_replacer(
                    "?expr",
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                )),
            ),
            transforming_rewrite(
                "wrapper-pull-up-is-null-expr",
                is_null_expr(wrapper_pullup_replacer(
                    "?expr",
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                )),
                wrapper_pullup_replacer(
                    is_null_expr("?expr"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                self.transform_is_null_expr("?alias_to_cube"),
            ),
            rewrite(
                "wrapper-push-down-is-not-null-expr",
                wrapper_pushdown_replacer(
                    is_not_null_expr("?expr"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                is_not_null_expr(wrapper_pushdown_replacer(
                    "?expr",
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                )),
            ),
            transforming_rewrite(
                "wrapper-pull-up-is-not-null-expr",
                is_not_null_expr(wrapper_pullup_replacer(
                    "?expr",
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                )),
                wrapper_pullup_replacer(
                    is_not_null_expr("?expr"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                self.transform_is_null_expr("?alias_to_cube"),
            ),
        ]);
    }

    fn transform_is_null_expr(
        &self,
        alias_to_cube_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
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
                        .contains_key("expressions/is_null")
                    {
                        return true;
                    }
                }
            }
            false
        }
    }
}
