use crate::compile::rewrite::{
    analysis::LogicalPlanAnalysis, cast_expr, rewrite, rules::wrapper::WrapperRules,
    wrapper_pullup_replacer, wrapper_pushdown_replacer, LogicalPlanLanguage,
};
use egg::Rewrite;

impl WrapperRules {
    pub fn cast_rules(&self, rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>) {
        rules.extend(vec![
            rewrite(
                "wrapper-push-down-cast",
                wrapper_pushdown_replacer(
                    cast_expr("?expr", "?data_type"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                cast_expr(
                    wrapper_pushdown_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    "?data_type",
                ),
            ),
            rewrite(
                "wrapper-pull-up-cast",
                cast_expr(
                    wrapper_pullup_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    "?data_type",
                ),
                wrapper_pullup_replacer(
                    cast_expr("?expr", "?data_type"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
            ),
        ]);
    }
}
