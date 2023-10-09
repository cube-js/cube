use crate::compile::rewrite::{
    analysis::LogicalPlanAnalysis, literal_expr, rewrite, rules::wrapper::WrapperRules,
    wrapper_pullup_replacer, wrapper_pushdown_replacer, LogicalPlanLanguage,
};
use egg::Rewrite;

impl WrapperRules {
    pub fn literal_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        rules.extend(vec![rewrite(
            "wrapper-push-down-literal",
            wrapper_pushdown_replacer(
                literal_expr("?value"),
                "?alias_to_cube",
                "?ungrouped",
                "?cube_members",
            ),
            wrapper_pullup_replacer(
                literal_expr("?value"),
                "?alias_to_cube",
                "?ungrouped",
                "?cube_members",
            ),
        )]);
    }
}
