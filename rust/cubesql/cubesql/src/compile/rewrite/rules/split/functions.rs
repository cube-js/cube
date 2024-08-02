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

        self.scalar_fn_args_pass_through_rules("Trunc", false, rules);
        self.scalar_fn_args_pass_through_rules("Lower", false, rules);
        self.scalar_fn_args_pass_through_rules("Upper", false, rules);
        self.scalar_fn_args_pass_through_rules("Ceil", false, rules);
        self.scalar_fn_args_pass_through_rules("Floor", false, rules);
        self.scalar_fn_args_pass_through_rules("CharacterLength", false, rules);
        // TODO udf function have different list type for args, accomodate it
        self.single_arg_pass_through_rules(
            "to-char",
            |expr| udf_expr("to_char", vec![expr, "?format".to_string()]),
            false,
            rules,
        );
        self.scalar_fn_args_pass_through_rules("Substr", false, rules);
        self.scalar_fn_args_pass_through_rules("Lpad", false, rules);
        self.scalar_fn_args_pass_through_rules("Rpad", false, rules);
        self.single_arg_pass_through_rules("is-null", |expr| is_null_expr(expr), false, rules);
        self.single_arg_pass_through_rules(
            "is-not-null",
            |expr| is_not_null_expr(expr),
            false,
            rules,
        );
        // coalesce, nullif, left and right are a bit harder, variadic rule breaks some tests
        // And projection split seems wrong here
        // TODO Support them properly
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
