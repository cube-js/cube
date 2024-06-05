use crate::{
    compile::rewrite::{
        analysis::LogicalPlanAnalysis, literal_expr, rules::wrapper::WrapperRules,
        transforming_rewrite, wrapper_pullup_replacer, wrapper_pushdown_replacer, LiteralExprValue,
        LogicalPlanLanguage, WrapperPullupReplacerAliasToCube,
    },
    var, var_iter,
};

use crate::compile::rewrite::rules::utils::{DecomposedDayTime, DecomposedMonthDayNano};
use datafusion::scalar::ScalarValue;
use egg::{EGraph, Rewrite, Subst};

impl WrapperRules {
    pub fn literal_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        rules.extend(vec![
            transforming_rewrite(
                "wrapper-push-down-literal",
                wrapper_pushdown_replacer(
                    literal_expr("?value"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                wrapper_pullup_replacer(
                    literal_expr("?value"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                self.transform_literal("?alias_to_cube", "?value"),
            ),
            transforming_rewrite(
                "wrapper-push-down-interval-literal",
                wrapper_pushdown_replacer(
                    literal_expr("?value"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                wrapper_pullup_replacer(
                    "?new_value",
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                self.transform_inteval_literal("?alias_to_cube", "?value", "?new_value"),
            ),
        ]);
    }

    fn transform_literal(
        &self,
        alias_to_cube_var: &str,
        value_var: &str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let value_var = var!(value_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            for alias_to_cube in var_iter!(
                egraph[subst[alias_to_cube_var]],
                WrapperPullupReplacerAliasToCube
            ) {
                if let Some(sql_generator) = meta.sql_generator_by_alias_to_cube(alias_to_cube) {
                    for literal in var_iter!(egraph[subst[value_var]], LiteralExprValue) {
                        match literal {
                            ScalarValue::TimestampNanosecond(_, _)
                            | ScalarValue::TimestampMillisecond(_, _)
                            | ScalarValue::TimestampMicrosecond(_, _)
                            | ScalarValue::TimestampSecond(_, _) => {
                                return sql_generator
                                    .get_sql_templates()
                                    .contains_template("expressions/timestamp_literal");
                            }

                            // transform_inteval_literal
                            ScalarValue::IntervalYearMonth(_) => return false,
                            ScalarValue::IntervalDayTime(_) => return false,
                            ScalarValue::IntervalMonthDayNano(_) => return false,

                            _ => return true,
                        }
                    }
                }
            }
            false
        }
    }

    fn transform_inteval_literal(
        &self,
        alias_to_cube_var: &str,
        value_var: &str,
        new_value_var: &str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let value_var = var!(value_var);
        let new_value_var = var!(new_value_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            for alias_to_cube in var_iter!(
                egraph[subst[alias_to_cube_var]],
                WrapperPullupReplacerAliasToCube
            ) {
                if let Some(sql_generator) = meta.sql_generator_by_alias_to_cube(alias_to_cube) {
                    let contains_template =
                        |name| sql_generator.get_sql_templates().contains_template(name);

                    let id = subst[value_var];

                    macro_rules! ret {
                        () => {{
                            let id = egraph.add(LogicalPlanLanguage::LiteralExpr([id]));
                            subst.insert(new_value_var, id);
                            return true;
                        }};

                        ($interval:ident; $DecomposeTy:ty) => {{
                            if contains_template("expressions/interval") {
                                ret!()
                            }
                            let decomposed = <$DecomposeTy>::from_interval(*$interval);
                            if decomposed.is_single_part() {
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
                }
            }
            false
        }
    }
}
