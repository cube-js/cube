use crate::compile::rewrite::{
    analysis::LogicalPlanAnalysis, cast_expr, is_not_null_expr, is_null_expr, literal_expr,
    negative_expr, rules::split::SplitRules, udf_expr, LogicalPlanLanguage,
};
use egg::Rewrite;

impl SplitRules {
    pub fn functions_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        self.single_arg_pass_through_rules(
            "cast",
            |expr| cast_expr(expr, "?data_type"),
            false,
            rules,
        );
        self.single_arg_pass_through_rules(
            "trunc",
            |expr| self.fun_expr("Trunc", vec![expr]),
            false,
            rules,
        );
        self.single_arg_pass_through_rules(
            "lower",
            |expr| self.fun_expr("Lower", vec![expr]),
            false,
            rules,
        );
        self.single_arg_pass_through_rules(
            "upper",
            |expr| self.fun_expr("Upper", vec![expr]),
            false,
            rules,
        );
        self.single_arg_pass_through_rules(
            "ceil",
            |expr| self.fun_expr("Ceil", vec![expr]),
            false,
            rules,
        );
        self.single_arg_pass_through_rules(
            "floor",
            |expr| self.fun_expr("Floor", vec![expr]),
            false,
            rules,
        );
        self.single_arg_pass_through_rules(
            "char-length",
            |expr| self.fun_expr("CharacterLength", vec![expr]),
            false,
            rules,
        );
        self.single_arg_pass_through_rules(
            "to-char",
            |expr| udf_expr("to_char", vec![expr, "?format".to_string()]),
            false,
            rules,
        );
        self.single_arg_pass_through_rules(
            "substring",
            |expr| {
                self.fun_expr(
                    "Substr",
                    vec![expr, "?from".to_string(), "?for".to_string()],
                )
            },
            false,
            rules,
        );
        self.single_arg_pass_through_rules(
            "lpad",
            |expr| {
                self.fun_expr(
                    "Lpad",
                    vec![expr, "?length".to_string(), "?char".to_string()],
                )
            },
            false,
            rules,
        );
        self.single_arg_pass_through_rules(
            "rpad",
            |expr| {
                self.fun_expr(
                    "Rpad",
                    vec![expr, "?length".to_string(), "?char".to_string()],
                )
            },
            false,
            rules,
        );
        self.single_arg_pass_through_rules("is-null", |expr| is_null_expr(expr), false, rules);
        self.single_arg_pass_through_rules(
            "is-not-null",
            |expr| is_not_null_expr(expr),
            false,
            rules,
        );
        self.single_arg_pass_through_rules(
            "coalesce-constant",
            |expr| self.fun_expr("Coalesce", vec![expr, literal_expr("?literal")]),
            true,
            rules,
        );
        self.single_arg_pass_through_rules(
            "nullif-constant",
            |expr| self.fun_expr("NullIf", vec![expr, literal_expr("?literal")]),
            true,
            rules,
        );
        self.single_arg_pass_through_rules(
            "left-constant",
            |expr| self.fun_expr("Left", vec![expr, literal_expr("?literal")]),
            true,
            rules,
        );
        self.single_arg_pass_through_rules(
            "right-constant",
            |expr| self.fun_expr("Right", vec![expr, literal_expr("?literal")]),
            true,
            rules,
        );
        self.single_arg_pass_through_rules("negative", |expr| negative_expr(expr), true, rules);
        self.single_arg_split_point_rules(
            "literal",
            || literal_expr("?value".to_string()),
            || literal_expr("?value".to_string()),
            |alias_column| alias_column,
            |_, _, _| true,
            true,
            rules,
        );
    }
}
