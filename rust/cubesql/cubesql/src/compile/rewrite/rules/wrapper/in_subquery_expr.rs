use crate::compile::rewrite::{
    insubquery_expr, rewrite, rewriter::CubeRewrite, rules::wrapper::WrapperRules,
    wrapper_pullup_replacer, wrapper_pushdown_replacer,
};

impl WrapperRules {
    pub fn in_subquery_expr_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            rewrite(
                "wrapper-in-subquery-push-down",
                wrapper_pushdown_replacer(
                    insubquery_expr("?expr", "?subquery", "?negated"),
                    "?context",
                ),
                insubquery_expr(
                    wrapper_pushdown_replacer("?expr", "?context"),
                    wrapper_pullup_replacer("?subquery", "?context"),
                    "?negated",
                ),
            ),
            rewrite(
                "wrapper-in-subquery-pull-up",
                insubquery_expr(
                    wrapper_pullup_replacer("?expr", "?context"),
                    wrapper_pullup_replacer("?subquery", "?context"),
                    "?negated",
                ),
                wrapper_pullup_replacer(
                    insubquery_expr("?expr", "?subquery", "?negated"),
                    "?context",
                ),
            ),
        ]);
    }
}
