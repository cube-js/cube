use crate::compile::rewrite::{
    cast_expr, rewrite, rewriter::CubeRewrite, rules::wrapper::WrapperRules,
    wrapper_pullup_replacer, wrapper_pushdown_replacer,
};

impl WrapperRules {
    pub fn cast_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            rewrite(
                "wrapper-push-down-cast",
                wrapper_pushdown_replacer(cast_expr("?expr", "?data_type"), "?context"),
                cast_expr(wrapper_pushdown_replacer("?expr", "?context"), "?data_type"),
            ),
            rewrite(
                "wrapper-pull-up-cast",
                cast_expr(wrapper_pullup_replacer("?expr", "?context"), "?data_type"),
                wrapper_pullup_replacer(cast_expr("?expr", "?data_type"), "?context"),
            ),
        ]);
    }
}
