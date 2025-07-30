use crate::{
    compile::rewrite::{
        between_expr, rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        transforming_rewrite, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        wrapper_replacer_context,
    },
    var,
};
use egg::Subst;

impl WrapperRules {
    pub fn between_expr_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            rewrite(
                "wrapper-push-down-between-expr",
                wrapper_pushdown_replacer(
                    between_expr("?expr", "?negated", "?low", "?high"),
                    "?context",
                ),
                between_expr(
                    wrapper_pushdown_replacer("?expr", "?context"),
                    "?negated",
                    wrapper_pushdown_replacer("?low", "?context"),
                    wrapper_pushdown_replacer("?high", "?context"),
                ),
            ),
            transforming_rewrite(
                "wrapper-pull-up-between-expr",
                between_expr(
                    wrapper_pullup_replacer(
                        "?expr",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                    "?negated",
                    wrapper_pullup_replacer(
                        "?low",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        "?high",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                ),
                wrapper_pullup_replacer(
                    between_expr("?expr", "?negated", "?low", "?high"),
                    wrapper_replacer_context(
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                        "?ungrouped_scan",
                        "?input_data_source",
                    ),
                ),
                self.transform_between_expr("?input_data_source"),
            ),
        ]);
    }

    fn transform_between_expr(
        &self,
        input_data_source_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let input_data_source_var = var!(input_data_source_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            let Ok(data_source) = Self::get_data_source(egraph, subst, input_data_source_var)
            else {
                return false;
            };

            Self::can_rewrite_template(&data_source, &meta, "expressions/between")
        }
    }
}
