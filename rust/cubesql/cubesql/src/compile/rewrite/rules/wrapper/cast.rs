use crate::{
    compile::rewrite::{
        cast_expr, rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        transforming_rewrite, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        wrapper_replacer_context, CastExprDataType,
    },
    var, var_iter,
};
use egg::Subst;
use std::ops::ControlFlow;

impl WrapperRules {
    pub fn cast_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            rewrite(
                "wrapper-push-down-cast",
                wrapper_pushdown_replacer(cast_expr("?expr", "?data_type"), "?context"),
                cast_expr(wrapper_pushdown_replacer("?expr", "?context"), "?data_type"),
            ),
            transforming_rewrite(
                "wrapper-pull-up-cast",
                cast_expr(
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
                    "?data_type",
                ),
                wrapper_pullup_replacer(
                    cast_expr("?expr", "?data_type"),
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
                self.transform_cast_expr("?data_type", "?input_data_source"),
            ),
        ]);
    }

    fn transform_cast_expr(
        &self,
        data_type_var: &'static str,
        input_data_source_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let data_type_var = var!(data_type_var);
        let input_data_source_var = var!(input_data_source_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            let Ok(data_source) = Self::get_data_source(egraph, subst, input_data_source_var)
            else {
                return false;
            };

            // Rendering a cast needs both the cast template and the template of its type
            let sql_generator = match Self::template_sql_generator(&data_source, &meta) {
                ControlFlow::Continue(sql_generator) => sql_generator,
                ControlFlow::Break(verdict) => return verdict,
            };
            let templates = sql_generator.get_sql_templates();
            if !templates.contains_template("expressions/cast") {
                return false;
            }
            var_iter!(egraph[subst[data_type_var]], CastExprDataType)
                .any(|data_type| templates.contains_sql_type(data_type))
        }
    }
}
