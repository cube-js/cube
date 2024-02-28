use crate::{
    compile::rewrite::{
        analysis::LogicalPlanAnalysis, binary_expr, rewrite, rules::wrapper::WrapperRules,
        transforming_rewrite, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        LogicalPlanLanguage, WrapperPullupReplacerAliasToCube,
    },
    var, var_iter,
};
use egg::{EGraph, Rewrite, Subst};

impl WrapperRules {
    pub fn binary_expr_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        rules.extend(vec![
            rewrite(
                "wrapper-push-down-binary-expr",
                wrapper_pushdown_replacer(
                    binary_expr("?left", "?op", "?right"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                binary_expr(
                    wrapper_pushdown_replacer(
                        "?left",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    "?op",
                    wrapper_pushdown_replacer(
                        "?right",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                ),
            ),
            transforming_rewrite(
                "wrapper-pull-up-binary-expr",
                binary_expr(
                    wrapper_pullup_replacer(
                        "?left",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    "?op",
                    wrapper_pullup_replacer(
                        "?right",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                ),
                wrapper_pullup_replacer(
                    binary_expr("?left", "?op", "?right"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                self.transform_binary_expr("?op", "?alias_to_cube"),
            ),
        ]);
    }

    fn transform_binary_expr(
        &self,
        _operator_var: &'static str,
        alias_to_cube_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
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
