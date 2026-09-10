use crate::compile::rewrite::{
    cast_expr, rewrite, rewriter::CubeRewrite, rules::wrapper::WrapperRules, transforming_rewrite,
    wrapper_pullup_replacer, wrapper_pushdown_replacer, wrapper_replacer_context, CastExprDataType,
};
use crate::{compile::rewrite::rewriter::CubeEGraph, var, var_iter};
use datafusion::arrow::datatypes::DataType;
use egg::Subst;

impl WrapperRules {
    pub fn cast_rules(&self, rules: &mut Vec<CubeRewrite>) {
        let context = wrapper_replacer_context(
            "?alias_to_cube",
            "?push_to_cube",
            "?in_projection",
            "?cube_members",
            "?grouped_subqueries",
            "?ungrouped_scan",
            "?input_data_source",
        );
        rules.extend(vec![
            rewrite(
                "wrapper-push-down-cast",
                wrapper_pushdown_replacer(cast_expr("?expr", "?data_type"), "?context"),
                cast_expr(wrapper_pushdown_replacer("?expr", "?context"), "?data_type"),
            ),
            transforming_rewrite(
                "wrapper-pull-up-cast",
                cast_expr(wrapper_pullup_replacer("?expr", &context), "?data_type"),
                wrapper_pullup_replacer(cast_expr("?expr", "?data_type"), &context),
                self.transform_float_cast("?input_data_source", "?data_type"),
            ),
        ]);
    }

    fn transform_float_cast(
        &self,
        input_data_source_var: &str,
        data_type_var: &str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let input_data_source_var = var!(input_data_source_var);
        let data_type_var = var!(data_type_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            for data_type in var_iter!(egraph[subst[data_type_var]], CastExprDataType) {
                let type_template = match data_type {
                    DataType::Float32 => "types/float",
                    DataType::Float64 => "types/double",
                    _ => return true,
                };
                let Ok(data_source) = Self::get_data_source(egraph, subst, input_data_source_var)
                else {
                    return false;
                };
                // A cast can survive as an alternative to a folded float literal.
                // It must not bypass the literal gate when cast templates are missing.
                return Self::can_rewrite_template(&data_source, &meta, type_template)
                    && Self::can_rewrite_template(&data_source, &meta, "expressions/cast");
            }
            false
        }
    }
}
