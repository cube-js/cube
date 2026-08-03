use crate::{
    compile::rewrite::{
        literal_expr,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        transforming_rewrite, wrapper_pullup_replacer, wrapper_replacer_context,
    },
    var,
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
                            "?input_data_source",
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
                            "?input_data_source",
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
                    "?input_data_source",
                ),
            ),
            self.transform_date_part_expr("?input_data_source"),
        )]);
    }

    fn transform_date_part_expr(
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

            Self::can_rewrite_template(&data_source, &meta, "expressions/extract")
        }
    }
}
