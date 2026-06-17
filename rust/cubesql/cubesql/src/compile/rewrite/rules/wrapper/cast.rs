use crate::{
    compile::rewrite::{
        cast_expr, rewrite, rewriter::CubeEGraph, rewriter::CubeRewrite,
        rules::wrapper::WrapperRules, transforming_rewrite, wrapper_pullup_replacer,
        wrapper_pushdown_replacer, wrapper_replacer_context, CastExprDataType, LiteralExprValue,
        LogicalPlanLanguage,
    },
    transport::DataSource,
    utils::{parse_named_timezone_timestamp, TIMESTAMP_TZ_NAMED_TIMEZONE_CAST_TEMPLATE},
};
use crate::{var, var_iter};
use datafusion::{arrow::datatypes::DataType, scalar::ScalarValue};
use egg::{Id, Subst};

impl WrapperRules {
    pub fn cast_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            transforming_rewrite(
                "wrapper-push-down-cast",
                wrapper_pushdown_replacer(
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
                cast_expr(
                    wrapper_pushdown_replacer(
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
                self.transform_cast_pushdown("?input_data_source", "?expr", "?data_type"),
            ),
            rewrite(
                "wrapper-pull-up-cast",
                cast_expr(wrapper_pullup_replacer("?expr", "?context"), "?data_type"),
                wrapper_pullup_replacer(cast_expr("?expr", "?data_type"), "?context"),
            ),
        ]);
    }

    fn transform_cast_pushdown(
        &self,
        input_data_source_var: &str,
        expr_var: &str,
        data_type_var: &str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let input_data_source_var = var!(input_data_source_var);
        let expr_var = var!(expr_var);
        let data_type_var = var!(data_type_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            let is_named_timezone_timestamp_cast =
                var_iter!(egraph[subst[data_type_var]], CastExprDataType).any(|data_type| {
                    // DataFusion erases WITH TIME ZONE for these literals.
                    matches!(data_type, DataType::Timestamp(_, _))
                        && Self::expr_is_named_timezone_string_literal(egraph, subst[expr_var])
                });

            if !is_named_timezone_timestamp_cast {
                return true;
            }

            let Ok(data_source) = Self::get_data_source(egraph, subst, input_data_source_var)
            else {
                return false;
            };

            match data_source {
                DataSource::Specific(_) => Self::can_rewrite_template(
                    &data_source,
                    &meta,
                    TIMESTAMP_TZ_NAMED_TIMEZONE_CAST_TEMPLATE,
                ),
                // This template is target-specific.
                DataSource::Unrestricted => false,
            }
        }
    }

    fn expr_is_named_timezone_string_literal(egraph: &CubeEGraph, expr_id: Id) -> bool {
        egraph[expr_id].nodes.iter().any(|node| {
            if let LogicalPlanLanguage::LiteralExpr([value_id]) = node {
                var_iter!(egraph[*value_id], LiteralExprValue).any(|literal| {
                    matches!(literal, ScalarValue::Utf8(Some(value)) if parse_named_timezone_timestamp(value).is_some())
                })
            } else {
                false
            }
        })
    }
}
