use crate::compile::rewrite::{
    alias_expr, rewrite, rewriter::CubeRewrite, rules::wrapper::WrapperRules,
    wrapper_pullup_replacer, wrapper_pushdown_replacer,
};

impl WrapperRules {
    pub fn alias_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            rewrite(
                "wrapper-push-down-alias",
                wrapper_pushdown_replacer(
                    alias_expr("?expr", "?alias"),
                    "?alias_to_cube",
                    "?push_to_cube",
                    "?in_projection",
                    "?cube_members",
                ),
                alias_expr(
                    wrapper_pushdown_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                    ),
                    "?alias",
                ),
            ),
            rewrite(
                "wrapper-pull-up-alias",
                alias_expr(
                    wrapper_pullup_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                    ),
                    "?alias",
                ),
                wrapper_pullup_replacer(
                    alias_expr("?expr", "?alias"),
                    "?alias_to_cube",
                    "?push_to_cube",
                    "?in_projection",
                    "?cube_members",
                ),
            ),
        ]);
    }
}
