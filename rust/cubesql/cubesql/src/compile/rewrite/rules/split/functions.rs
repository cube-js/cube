use crate::compile::rewrite::{
    analysis::LogicalPlanAnalysis, is_not_null_expr, is_null_expr, literal_expr, negative_expr,
    rules::split::SplitRules, udf_expr, ListType, LogicalPlanLanguage,
};
use egg::Rewrite;

impl SplitRules {
    pub fn functions_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        // Universal rule to traverse any number of function arguments
        // PushDown(ScalarFunctionExprArgs(..., arg, ...)) => ScalarFunctionExprArgs(..., PushDown(arg), ...)
        // ScalarFunctionExprArgs(..., PullUp(arg), ...) => PullUp(ScalarFunctionExprArgs(..., arg, ...))
        // If your function arguments requires special treatment, avoid generic rewrite like this
        // PushDown(ScalarFunctionExpr ?fun ?args) => ScalarFunctionExpr ?fun PushDown(?args)
        // Instead, use direct rewrite like this
        // PushDown(ScalarFunctionExpr ?fun ScalarFunctionExprArgs(arg1 arg2)) => ScalarFunctionExpr ?fun ScalarFunctionExprArgs(PushDown(arg1) arg2)
        Self::flat_list_pushdown_pullup_rules(
            "scalar-fun-var-args",
            ListType::ScalarFunctionExprArgs,
            rules,
        );

        let fns = [
            ("Trunc", false),
            ("Lower", false),
            ("Upper", false),
            ("Ceil", false),
            ("Floor", false),
            ("CharacterLength", false),
            ("Substr", false),
            ("Lpad", false),
            ("Rpad", false),
            ("Coalesce", false),
            ("NullIf", false),
            ("Left", false),
            ("Right", false),
        ];

        for (fn_name, with_projection) in fns {
            self.scalar_fn_args_pass_through_rules(fn_name, with_projection, rules);
        }

        // TODO udf function have different list type for args, accomodate it
        self.single_arg_pass_through_rules(
            "to-char",
            |expr| udf_expr("to_char", vec![expr, "?format".to_string()]),
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
