use crate::compile::rewrite::{
    alias_expr, analysis::LogicalPlanAnalysis, rewrite, rules::wrapper::WrapperRules,
    wrapper_pullup_replacer, wrapper_pushdown_replacer, LogicalPlanLanguage,
};
use egg::Rewrite;

impl WrapperRules {
    pub fn alias_rules(&self, rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>) {
        rules.extend(vec![
            rewrite(
                "wrapper-push-down-alias",
                wrapper_pushdown_replacer(
                    alias_expr("?expr", "?alias"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                alias_expr(
                    wrapper_pushdown_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?ungrouped",
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
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    "?alias",
                ),
                wrapper_pullup_replacer(
                    alias_expr("?expr", "?alias"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
            ),
        ]);
    }
}
