use crate::{
    compile::rewrite::{
        literal_expr,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        transforming_rewrite, wrapper_pullup_replacer, wrapper_replacer_context,
        WrapperReplacerContextAliasToCube,
    },
    var, var_iter,
};
use egg::Subst;

impl WrapperRules {
    pub fn extract_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![transforming_rewrite(
            "wrapper-pull-up-extract",
            self.fun_expr(
                "DatePart",
                vec![
                    wrapper_pullup_replacer(
                        literal_expr("?date_part"),
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        "?date",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                        ),
                    ),
                ],
            ),
            wrapper_pullup_replacer(
                self.fun_expr(
                    "DatePart",
                    vec![literal_expr("?date_part"), "?date".to_string()],
                ),
                wrapper_replacer_context(
                    "?alias_to_cube",
                    "?push_to_cube",
                    "?in_projection",
                    "?cube_members",
                    "?grouped_subqueries",
                    "?ungrouped_scan",
                ),
            ),
            self.transform_date_part_expr("?alias_to_cube"),
        )]);
    }

    fn transform_date_part_expr(
        &self,
        alias_to_cube_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            for alias_to_cube in var_iter!(
                egraph[subst[alias_to_cube_var]],
                WrapperReplacerContextAliasToCube
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
