use crate::compile::rewrite::{
    aggregate_split_pullup_replacer, aggregate_split_pushdown_replacer,
    analysis::LogicalPlanAnalysis, case_expr_var_arg, rewrite, rules::split::SplitRules,
    LogicalPlanLanguage,
};
use egg::Rewrite;

impl SplitRules {
    pub fn case_rules(&self, rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>) {
        rules.extend(vec![
            rewrite(
                "split-case-push-down-aggregate",
                aggregate_split_pushdown_replacer(
                    case_expr_var_arg("?expr", "?when_then", "?else"),
                    "?list_node",
                    "?alias_to_cube",
                ),
                case_expr_var_arg(
                    aggregate_split_pushdown_replacer("?expr", "?list_node", "?alias_to_cube"),
                    aggregate_split_pushdown_replacer("?when_then", "?list_node", "?alias_to_cube"),
                    aggregate_split_pushdown_replacer("?else", "?list_node", "?alias_to_cube"),
                ),
            ),
            // TODO projection
        ]);
        for inner_list_node in Self::possible_inner_list_nodes() {
            rules.push(rewrite(
                &format!("split-case-pull-up-{}-aggregate", inner_list_node),
                case_expr_var_arg(
                    aggregate_split_pullup_replacer(
                        "?inner_expr".to_string(),
                        "?outer_expr".to_string(),
                        inner_list_node.to_string(),
                        "?alias_to_cube",
                    ),
                    aggregate_split_pullup_replacer(
                        "?inner_when_then".to_string(),
                        "?outer_when_then".to_string(),
                        inner_list_node.to_string(),
                        "?alias_to_cube",
                    ),
                    aggregate_split_pullup_replacer(
                        "?inner_else".to_string(),
                        "?outer_else".to_string(),
                        inner_list_node.to_string(),
                        "?alias_to_cube",
                    ),
                ),
                aggregate_split_pullup_replacer(
                    format!(
                        "({} ({} ?inner_expr ?inner_when_then) ?inner_else)",
                        inner_list_node, inner_list_node
                    ),
                    case_expr_var_arg("?outer_expr", "?outer_when_then", "?outer_else"),
                    inner_list_node.to_string(),
                    "?alias_to_cube",
                ),
            ));
        }
        Self::list_pushdown_pullup_rules("case-expr", "CaseExprExpr", rules);
        Self::list_pushdown_pullup_rules("case-when-then-expr", "CaseExprWhenThenExpr", rules);
        Self::list_pushdown_pullup_rules("case-else-expr", "CaseExprElseExpr", rules);
    }
}
