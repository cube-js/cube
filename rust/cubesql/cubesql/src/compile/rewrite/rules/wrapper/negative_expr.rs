use crate::{
    compile::rewrite::{
        negative_expr, rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        transforming_rewrite, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        wrapper_replacer_context,
    },
    var,
};
use egg::Subst;

impl WrapperRules {
    pub fn negative_expr_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            rewrite(
                "wrapper-negative-push-down",
                wrapper_pushdown_replacer(negative_expr("?expr"), "?context"),
                negative_expr(wrapper_pushdown_replacer("?expr", "?context")),
            ),
            transforming_rewrite(
                "wrapper-negative-pull-up",
                negative_expr(wrapper_pullup_replacer(
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
                )),
                wrapper_pullup_replacer(
                    negative_expr("?expr"),
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
                self.transform_negative_expr("?input_data_source"),
            ),
        ]);
    }

    fn transform_negative_expr(
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

            Self::can_rewrite_template(&data_source, &meta, "expressions/negative")
        }
    }
}
