use crate::compile::rewrite::{
    aggregate_split_pullup_replacer, aggregate_split_pushdown_replacer, column_expr,
    projection_split_pullup_replacer, projection_split_pushdown_replacer, rewrite,
    rewriter::CubeRewrite, rules::split::SplitRules,
};

impl SplitRules {
    pub fn column_rules(&self, rules: &mut Vec<CubeRewrite>) {
        // TODO check for measures?
        rules.push(rewrite(
            "split-column-point-aggregate",
            aggregate_split_pushdown_replacer(
                column_expr("?column"),
                "?list_node",
                "?alias_to_cube",
            ),
            aggregate_split_pullup_replacer(
                column_expr("?column"),
                column_expr("?column"),
                "?list_node",
                "?alias_to_cube",
            ),
        ));
        rules.push(rewrite(
            "split-column-point-projection",
            projection_split_pushdown_replacer(
                column_expr("?column"),
                "?list_node",
                "?alias_to_cube",
            ),
            projection_split_pullup_replacer(
                column_expr("?column"),
                column_expr("?column"),
                "?list_node",
                "?alias_to_cube",
            ),
        ));
    }
}
