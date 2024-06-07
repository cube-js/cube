use crate::{
    compile::rewrite::{
        analysis::LogicalPlanAnalysis, literal_expr, rules::wrapper::WrapperRules,
        transforming_rewrite, wrapper_pullup_replacer, LogicalPlanLanguage,
        WrapperPullupReplacerAliasToCube,
    },
    var, var_iter,
};
use egg::{EGraph, Rewrite, Subst};

impl WrapperRules {
    pub fn extract_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        rules.extend(vec![transforming_rewrite(
            "wrapper-pull-up-extract",
            self.fun_expr(
                "DatePart",
                vec![
                    wrapper_pullup_replacer(
                        literal_expr("?date_part"),
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        "?date",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                ],
            ),
            wrapper_pullup_replacer(
                self.fun_expr(
                    "DatePart",
                    vec![literal_expr("?date_part"), "?date".to_string()],
                ),
                "?alias_to_cube",
                "?ungrouped",
                "?in_projection",
                "?cube_members",
            ),
            self.transform_date_part_expr("?alias_to_cube"),
        )]);
    }

    fn transform_date_part_expr(
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
                        .contains_key("expressions/extract")
                    {
                        return true;
                    }
                }
            }
            false
        }
    }
}
