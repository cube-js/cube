use crate::{
    compile::rewrite::{
        analysis::LogicalPlanAnalysis, literal_expr, rules::wrapper::WrapperRules,
        transforming_rewrite, wrapper_pullup_replacer, wrapper_pushdown_replacer, LiteralExprValue,
        LogicalPlanLanguage, WrapperPullupReplacerAliasToCube,
    },
    var, var_iter,
};

use datafusion::scalar::ScalarValue;
use egg::{EGraph, Rewrite, Subst};

impl WrapperRules {
    pub fn literal_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        rules.extend(vec![transforming_rewrite(
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
        )]);
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
            )
            .cloned()
            {
                if let Some(sql_generator) = meta.sql_generator_by_alias_to_cube(&alias_to_cube) {
                    for literal in var_iter!(egraph[subst[value_var]], LiteralExprValue) {
                        match literal {
                            ScalarValue::TimestampNanosecond(_, _)
                            | ScalarValue::TimestampMillisecond(_, _)
                            | ScalarValue::TimestampMicrosecond(_, _)
                            | ScalarValue::TimestampSecond(_, _) => {
                                return sql_generator
                                    .get_sql_templates()
                                    .templates
                                    .contains_key("expressions/timestamp_literal");
                            }
                            _ => return true,
                        }
                    }
                }
            }
            false
        }
    }
}
