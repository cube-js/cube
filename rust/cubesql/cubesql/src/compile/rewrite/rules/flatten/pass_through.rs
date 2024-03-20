use crate::compile::rewrite::{
    agg_fun_expr, alias_expr, analysis::LogicalPlanAnalysis, binary_expr, cast_expr,
    flatten_pushdown_replacer, fun_expr_var_arg, is_not_null_expr, is_null_expr, rewrite,
    rules::flatten::FlattenRules, udf_expr_var_arg, LogicalPlanLanguage,
};
use egg::Rewrite;

impl FlattenRules {
    pub fn pass_through_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        self.single_arg_pass_through_rules("alias", |expr| alias_expr(expr, "?alias"), rules);
        self.single_arg_pass_through_rules("cast", |expr| cast_expr(expr, "?data_type"), rules);
        self.single_arg_pass_through_rules(
            "scalar-function",
            |expr| fun_expr_var_arg("?fun", expr),
            rules,
        );
        self.single_arg_pass_through_rules(
            "agg-function",
            |expr| agg_fun_expr("?fun", vec![expr], "?distinct"),
            rules,
        );
        self.single_arg_pass_through_rules(
            "udf-function",
            |expr| udf_expr_var_arg("?fun", expr),
            rules,
        );
        self.single_arg_pass_through_rules("is-null", |expr| is_null_expr(expr), rules);
        self.single_arg_pass_through_rules("is-not-null", |expr| is_not_null_expr(expr), rules);
        rules.push(rewrite(
            "flatten-binary-expr",
            flatten_pushdown_replacer(
                binary_expr("?left", "?op", "?right"),
                "?inner_expr",
                "?inner_alias",
                "?top_level",
            ),
            binary_expr(
                flatten_pushdown_replacer(
                    "?left",
                    "?inner_expr",
                    "?inner_alias",
                    "FlattenPushdownReplacerTopLevel:false",
                ),
                "?op",
                flatten_pushdown_replacer(
                    "?right",
                    "?inner_expr",
                    "?inner_alias",
                    "FlattenPushdownReplacerTopLevel:false",
                ),
            ),
        ))
    }

    pub fn single_arg_pass_through_rules(
        &self,
        name: &str,
        node: impl Fn(String) -> String,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        rules.extend(vec![rewrite(
            &format!("flatten-{}-push-down", name),
            flatten_pushdown_replacer(
                node("?expr".to_string()),
                "?inner_expr",
                "?inner_alias",
                "?top_level",
            ),
            node(flatten_pushdown_replacer(
                "?expr".to_string(),
                "?inner_expr",
                "?inner_alias",
                "FlattenPushdownReplacerTopLevel:false",
            )),
        )]);
    }
}
