use crate::compile::rewrite::{
    rewrite, rewriter::CubeRewrite, rules::wrapper::WrapperRules, wrapped_select_join,
    wrapper_pullup_replacer,
};

impl WrapperRules {
    pub fn join_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![rewrite(
            "wrapper-pull-up-single-select-join",
            wrapped_select_join(
                wrapper_pullup_replacer(
                    "?input",
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                    "?grouped_subqueries",
                ),
                wrapper_pullup_replacer(
                    "?join_expr",
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                    "?grouped_subqueries",
                ),
                "?out_join_type",
            ),
            wrapper_pullup_replacer(
                wrapped_select_join("?input", "?join_expr", "?out_join_type"),
                "?alias_to_cube",
                "?ungrouped",
                "?in_projection",
                "?cube_members",
                "?grouped_subqueries",
            ),
        )]);
    }
}
