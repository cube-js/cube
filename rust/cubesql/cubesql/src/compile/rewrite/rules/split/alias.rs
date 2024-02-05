use crate::compile::rewrite::{
    aggregate_split_pullup_replacer, aggregate_split_pushdown_replacer, alias_expr,
    alias_expr_split_replacer, analysis::LogicalPlanAnalysis, column_expr,
    projection_split_pullup_replacer, projection_split_pushdown_replacer, rewrite,
    rules::split::SplitRules, transforming_chain_rewrite, transforming_rewrite,
    LogicalPlanLanguage,
};
use egg::Rewrite;

impl SplitRules {
    pub fn alias_rules(&self, rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>) {
        rules.extend(vec![
            rewrite(
                "split-alias-push-down-aggregate",
                aggregate_split_pushdown_replacer(
                    alias_expr("?expr".to_string(), "?alias"),
                    "?list_node",
                    "?alias_to_cube",
                ),
                alias_expr(
                    aggregate_split_pushdown_replacer("?expr", "?list_node", "?alias_to_cube"),
                    "?alias",
                ),
            ),
            rewrite(
                "split-alias-pull-up-aggregate",
                alias_expr(
                    aggregate_split_pullup_replacer(
                        "?inner_expr".to_string(),
                        "?outer_expr".to_string(),
                        "?list_node",
                        "?alias_to_cube",
                    ),
                    "?alias",
                ),
                aggregate_split_pullup_replacer(
                    "?inner_expr",
                    alias_expr("?outer_expr".to_string(), "?alias"),
                    "?list_node",
                    "?alias_to_cube",
                ),
            ),
            // rewrite(
            //     &format!("split-{}-push-down-projection", name),
            //     projection_split_pushdown_replacer(
            //         node("?expr".to_string()),
            //         "?list_node",
            //         "?alias_to_cube",
            //     ),
            //     node(projection_split_pushdown_replacer(
            //         "?expr".to_string(),
            //         "?list_node",
            //         "?alias_to_cube",
            //     )),
            // ),
            // rewrite(
            //     &format!("split-{}-pull-up-projection", name),
            //     node(projection_split_pullup_replacer(
            //         "?inner_expr".to_string(),
            //         "?outer_expr".to_string(),
            //         "?list_node",
            //         "?alias_to_cube",
            //     )),
            //     projection_split_pullup_replacer(
            //         "?inner_expr",
            //         node("?outer_expr".to_string()),
            //         "?list_node",
            //         "?alias_to_cube",
            //     ),
            // )
        ]);
    }
}
