use crate::{
    compile::rewrite::{
        binary_expr, rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        transforming_rewrite, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        wrapper_replacer_context,
    },
    var,
};
use egg::Subst;

impl WrapperRules {
    pub fn binary_expr_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            rewrite(
                "wrapper-push-down-binary-expr",
                wrapper_pushdown_replacer(binary_expr("?left", "?op", "?right"), "?context"),
                binary_expr(
                    wrapper_pushdown_replacer("?left", "?context"),
                    "?op",
                    wrapper_pushdown_replacer("?right", "?context"),
                ),
            ),
            transforming_rewrite(
                "wrapper-pull-up-binary-expr",
                binary_expr(
                    wrapper_pullup_replacer(
                        "?left",
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
                    "?op",
                    wrapper_pullup_replacer(
                        "?right",
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
                    binary_expr("?left", "?op", "?right"),
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
                self.transform_binary_expr("?op", "?input_data_source"),
            ),
        ]);
    }

    fn transform_binary_expr(
        &self,
        _operator_var: &'static str,
        input_data_source_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let input_data_source_var = var!(input_data_source_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            let Ok(data_source) = Self::get_data_source(egraph, subst, input_data_source_var)
            else {
                return false;
            };

            // TODO check supported operators
            Self::can_rewrite_template(&data_source, &meta, "expressions/binary")
        }
    }
}
