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

            let supports_float_literal = |type_template| {
                Self::can_rewrite_template(&data_source, &meta, "expressions/float_literal")
                    || (Self::can_rewrite_template(&data_source, &meta, type_template)
                        && Self::can_rewrite_template(&data_source, &meta, "expressions/cast"))
            };

            for literal in var_iter!(egraph[subst[value_var]], LiteralExprValue) {
                match literal {
                    // NaN and infinity need dialect-specific syntax; neither a bare
                    // identifier in a cast nor an exponent literal can represent them.
                    ScalarValue::Float32(value) => {
                        return value.is_none_or(|value| value.is_finite())
                            && supports_float_literal("types/float");
                    }
                    ScalarValue::Float64(value) => {
                        return value.is_none_or(|value| value.is_finite())
                            && supports_float_literal("types/double");
                    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compile::{
        rewrite::{analysis::LogicalPlanAnalysis, WrapperReplacerContextInputDataSource},
        test::{get_test_session, get_test_tenant_ctx_customized},
        CubeContext, DatabaseProtocol,
    };
    use crate::config::ConfigObjImpl;
    use datafusion::{
        execution::context::SessionContext, physical_plan::planner::DefaultPhysicalPlanner,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn test_float_literal_gate() {
        for missing in [
            None,
            Some("types/float"),
            Some("types/double"),
            Some("expressions/cast"),
        ] {
            for has_override in [false, true] {
                let mut templates = missing
                    .into_iter()
                    .map(|name| (name.to_string(), String::new()))
                    .collect::<Vec<_>>();
                if has_override {
                    templates.push((
                        "expressions/float_literal".to_string(),
                        "{{ value }}".to_string(),
                    ));
                }
                let meta = get_test_tenant_ctx_customized(templates);
                let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
                let context = Arc::new(CubeContext::new(
                    Arc::new(SessionContext::new().state.read().clone()),
                    meta.clone(),
                    session.session_manager.clone(),
                    session.state.clone(),
                ));
                let mut graph = CubeEGraph::new(LogicalPlanAnalysis::new(
                    context,
                    Arc::new(DefaultPhysicalPlanner::default()),
                ));
                let mut subst = Subst::default();
                subst.insert(
                    var!("?source"),
                    graph.add(LogicalPlanLanguage::WrapperReplacerContextInputDataSource(
                        WrapperReplacerContextInputDataSource(Some("default".to_string())),
                    )),
                );
                let rules = WrapperRules::new(meta, Arc::new(ConfigObjImpl::default()));
                for (literal, type_template, finite) in [
                    (ScalarValue::Float32(Some(100.0)), "types/float", true),
                    (ScalarValue::Float64(Some(100.0)), "types/double", true),
                    (ScalarValue::Float32(None), "types/float", true),
                    (ScalarValue::Float64(None), "types/double", true),
                    (ScalarValue::Float32(Some(f32::NAN)), "types/float", false),
                    (ScalarValue::Float64(Some(f64::NAN)), "types/double", false),
                    (
                        ScalarValue::Float32(Some(f32::INFINITY)),
                        "types/float",
                        false,
                    ),
                    (
                        ScalarValue::Float64(Some(f64::INFINITY)),
                        "types/double",
                        false,
                    ),
                    (
                        ScalarValue::Float32(Some(f32::NEG_INFINITY)),
                        "types/float",
                        false,
                    ),
                    (
                        ScalarValue::Float64(Some(f64::NEG_INFINITY)),
                        "types/double",
                        false,
                    ),
                ] {
                    subst.insert(
                        var!("?value"),
                        graph.add(LogicalPlanLanguage::LiteralExprValue(LiteralExprValue(
                            literal.clone(),
                        ))),
                    );
                    let expected = finite
                        && (has_override
                            || (missing != Some(type_template)
                                && missing != Some("expressions/cast")));
                    assert_eq!(
                        rules.transform_literal("?source", "?value")(&mut graph, &mut subst),
                        expected,
                        "{:?}, missing {:?}, override={}",
                        literal,
                        missing,
                        has_override
                    );
                }
            }
        }
    }
}
