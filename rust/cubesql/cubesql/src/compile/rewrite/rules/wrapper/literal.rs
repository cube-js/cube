use crate::{
    compile::rewrite::{
        literal_expr, rules::wrapper::WrapperRules, transforming_rewrite, wrapper_pullup_replacer,
        wrapper_pushdown_replacer, LiteralExprValue, LogicalPlanLanguage,
    },
    var, var_iter,
};

use crate::compile::rewrite::{
    rewriter::{CubeEGraph, CubeRewrite},
    rules::utils::{DecomposedDayTime, DecomposedMonthDayNano},
    wrapper_replacer_context,
};
use datafusion::scalar::ScalarValue;
use egg::Subst;

impl WrapperRules {
    pub fn literal_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            transforming_rewrite(
                "wrapper-push-down-literal",
                wrapper_pushdown_replacer(
                    literal_expr("?value"),
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
                    literal_expr("?value"),
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
                self.transform_literal("?input_data_source", "?value"),
            ),
            transforming_rewrite(
                "wrapper-push-down-interval-literal",
                wrapper_pushdown_replacer(
                    literal_expr("?value"),
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
                    "?new_value",
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
                self.transform_interval_literal("?input_data_source", "?value", "?new_value"),
            ),
        ]);
    }

    fn transform_literal(
        &self,
        input_data_source_var: &str,
        value_var: &str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let input_data_source_var = var!(input_data_source_var);
        let value_var = var!(value_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            let Ok(data_source) = Self::get_data_source(egraph, subst, input_data_source_var)
            else {
                return false;
            };

            for literal in var_iter!(egraph[subst[value_var]], LiteralExprValue) {
                match literal {
                    ScalarValue::TimestampNanosecond(_, _)
                    | ScalarValue::TimestampMillisecond(_, _)
                    | ScalarValue::TimestampMicrosecond(_, _)
                    | ScalarValue::TimestampSecond(_, _) => {
                        return Self::can_rewrite_template(
                            &data_source,
                            &meta,
                            "expressions/timestamp_literal",
                        );
                    }

                    // transform_inteval_literal
                    ScalarValue::IntervalYearMonth(_) => return false,
                    ScalarValue::IntervalDayTime(_) => return false,
                    ScalarValue::IntervalMonthDayNano(_) => return false,

                    _ => return true,
                }
            }
            false
        }
    }

    fn transform_interval_literal(
        &self,
        input_data_source_var: &str,
        value_var: &str,
        new_value_var: &str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let input_data_source_var = var!(input_data_source_var);
        let value_var = var!(value_var);
        let new_value_var = var!(new_value_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            let Ok(data_source) = Self::get_data_source(egraph, subst, input_data_source_var)
            else {
                return false;
            };

            let contains_template = |name| Self::can_rewrite_template(&data_source, &meta, name);

            let id = subst[value_var];

            macro_rules! ret {
                () => {{
                    // return without changing:
                    // id is `LiteralExprValue`
                    // literal_expr("?value") --> literal_expr("?value")
                    let id = egraph.add(LogicalPlanLanguage::LiteralExpr([id]));
                    subst.insert(new_value_var, id);
                    return true;
                }};

                ($interval:ident; $DecomposeTy:ty) => {{
                    if contains_template("expressions/interval") {
                        // we can use nondecomposed intervals
                        ret!()
                    }
                    let decomposed = <$DecomposeTy>::from_raw_interval_value(*$interval);
                    if decomposed.is_single_part() {
                        // interval already decomposed (only one date part)
                        ret!()
                    }
                    let id = decomposed.add_decomposed_to_egraph(egraph);
                    subst.insert(new_value_var, id);
                    return true;
                }};
            }

            for literal in var_iter!(egraph[id], LiteralExprValue) {
                match literal {
                    ScalarValue::IntervalYearMonth(_)
                    | ScalarValue::IntervalDayTime(None)
                    | ScalarValue::IntervalMonthDayNano(None) => ret!(),

                    ScalarValue::IntervalDayTime(Some(interval)) => {
                        ret!(interval; DecomposedDayTime)
                    }
                    ScalarValue::IntervalMonthDayNano(Some(interval)) => {
                        ret!(interval; DecomposedMonthDayNano)
                    }

                    _ => return false,
                }
            }
            false
        }
    }
}
