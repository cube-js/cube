use crate::compile::rewrite::{
    aggregate_split_pullup_replacer, aggregate_split_pushdown_replacer,
    analysis::LogicalPlanAnalysis, binary_expr, rewrite, rules::split::SplitRules,
    LogicalPlanLanguage,
};
use egg::Rewrite;

impl SplitRules {
    pub fn binary_expr_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        rules.extend(vec![
            rewrite(
                "split-binary-push-down-aggregate",
                aggregate_split_pushdown_replacer(
                    binary_expr("?left".to_string(), "?op".to_string(), "?right".to_string()),
                    "?list_node",
                    "?alias_to_cube",
                ),
                binary_expr(
                    aggregate_split_pushdown_replacer("?left", "?list_node", "?alias_to_cube"),
                    "?op".to_string(),
                    aggregate_split_pushdown_replacer("?right", "?list_node", "?alias_to_cube"),
                ),
            ),
            // TODO projection
        ]);
        for inner_list_node in Self::possible_inner_list_nodes() {
            rules.push(rewrite(
                &format!("split-binary-pull-up-{}-aggregate", inner_list_node),
                binary_expr(
                    aggregate_split_pullup_replacer(
                        "?left_inner_expr".to_string(),
                        "?left_outer_expr".to_string(),
                        inner_list_node.to_string(),
                        "?alias_to_cube",
                    ),
                    "?op".to_string(),
                    aggregate_split_pullup_replacer(
                        "?right_inner_expr".to_string(),
                        "?right_outer_expr".to_string(),
                        inner_list_node.to_string(),
                        "?alias_to_cube",
                    ),
                ),
                aggregate_split_pullup_replacer(
                    format!("({} ?left_inner_expr ?right_inner_expr)", inner_list_node),
                    binary_expr("?left_outer_expr", "?op".to_string(), "?right_outer_expr"),
                    inner_list_node.to_string(),
                    "?alias_to_cube",
                ),
            ));
        }
    }
}
