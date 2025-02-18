use crate::compile::rewrite::{
    rewrite, rewriter::CubeRewrite, rules::wrapper::WrapperRules, sort_expr,
    wrapper_pullup_replacer, wrapper_pushdown_replacer,
};

impl WrapperRules {
    pub fn sort_expr_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            rewrite(
                "wrapper-push-down-sort-expr",
                wrapper_pushdown_replacer(sort_expr("?expr", "?asc", "?nulls_first"), "?context"),
                sort_expr(
                    wrapper_pushdown_replacer("?expr", "?context"),
                    "?asc",
                    "?nulls_first",
                ),
            ),
            rewrite(
                "wrapper-pull-up-sort-expr",
                sort_expr(
                    wrapper_pullup_replacer("?expr", "?context"),
                    "?asc",
                    "?nulls_first",
                ),
                wrapper_pullup_replacer(sort_expr("?expr", "?asc", "?nulls_first"), "?context"),
            ),
        ]);
    }
}
