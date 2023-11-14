use crate::{
    compile::rewrite::{
        analysis::LogicalPlanAnalysis, inlist_expr, rewrite, rules::wrapper::WrapperRules,
        transforming_rewrite, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        LogicalPlanLanguage, WrapperPullupReplacerAliasToCube,
    },
    var, var_iter,
};
use egg::{EGraph, Rewrite, Subst};

impl WrapperRules {
    pub fn in_list_expr_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        rules.extend(vec![
            rewrite(
                "wrapper-in-list-push-down",
                wrapper_pushdown_replacer(
                    inlist_expr("?expr", "?list", "?negated"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?cube_members",
                ),
                inlist_expr(
                    wrapper_pushdown_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                    wrapper_pushdown_replacer(
                        "?list",
                        "?alias_to_cube",
                        "?ungrouped",
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
                        "?ungrouped",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        "?list",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?cube_members",
                    ),
                    "?negated",
                ),
                wrapper_pullup_replacer(
                    inlist_expr("?expr", "?list", "?negated"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?cube_members",
                ),
                self.transform_in_list_expr("?alias_to_cube"),
            ),
        ]);

        Self::expr_list_pushdown_pullup_rules(rules, "wrapper-in-list-exprs", "InListExprList");
    }

    fn transform_in_list_expr(
        &self,
        alias_to_cube_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let meta = self.cube_context.meta.clone();
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
}
