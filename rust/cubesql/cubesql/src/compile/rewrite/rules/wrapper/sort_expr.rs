use crate::compile::rewrite::{
    analysis::LogicalPlanAnalysis, rewrite, rules::wrapper::WrapperRules, sort_expr,
    wrapper_pullup_replacer, wrapper_pushdown_replacer, LogicalPlanLanguage,
};
use egg::Rewrite;

impl WrapperRules {
    pub fn sort_expr_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        rules.extend(vec![
            rewrite(
                "wrapper-push-down-sort-expr",
                wrapper_pushdown_replacer(
                    sort_expr("?expr", "?asc", "?nulls_first"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                sort_expr(
                    wrapper_pushdown_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    "?asc",
                    "?nulls_first",
                ),
            ),
            rewrite(
                "wrapper-pull-up-sort-expr",
                sort_expr(
                    wrapper_pullup_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    "?asc",
                    "?nulls_first",
                ),
                wrapper_pullup_replacer(
                    sort_expr("?expr", "?asc", "?nulls_first"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
            ),
        ]);
    }
}
