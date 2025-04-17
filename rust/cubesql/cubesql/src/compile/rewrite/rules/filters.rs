use super::utils;
use crate::{
    compile::rewrite::{
        alias_expr,
        analysis::{ConstantFolding, Member, OriginalExpr},
        between_expr, binary_expr, case_expr, case_expr_var_arg, cast_expr, change_user_member,
        column_expr, cube_scan, cube_scan_filters, cube_scan_filters_empty_tail, cube_scan_members,
        dimension_expr, expr_column_name, filter, filter_member, filter_op, filter_op_filters,
        filter_op_filters_empty_tail, filter_replacer, filter_simplify_pull_up_replacer,
        filter_simplify_push_down_replacer, fun_expr, fun_expr_args_legacy, fun_expr_var_arg,
        inlist_expr, inlist_expr_list, is_not_null_expr, is_null_expr, like_expr, limit,
        list_rewrite, literal_bool, literal_expr, literal_int, literal_string, measure_expr,
        negative_expr, not_expr, projection, rewrite,
        rewriter::{CubeEGraph, CubeRewrite, RewriteRules},
        scalar_fun_expr_args_empty_tail, segment_member, time_dimension_date_range_replacer,
        time_dimension_expr, transform_original_expr_to_alias, transforming_chain_rewrite,
        transforming_rewrite, transforming_rewrite_with_root, udf_expr, udf_expr_var_arg,
        udf_fun_expr_args, udf_fun_expr_args_empty_tail, BetweenExprNegated, BinaryExprOp,
        CastExprDataType, ChangeUserMemberValue, ColumnExprColumn, CubeScanAliasToCube,
        CubeScanLimit, FilterMemberMember, FilterMemberOp, FilterMemberValues, FilterOpOp,
        FilterReplacerAliasToCube, FilterReplacerAliases, InListExprNegated, LikeExprEscapeChar,
        LikeExprNegated, LimitFetch, LimitSkip, ListPattern, ListType, LiteralExprValue,
        LogicalPlanLanguage, SegmentMemberMember, TimeDimensionDateRange,
        TimeDimensionDateRangeReplacerDateRange, TimeDimensionDateRangeReplacerMember,
        TimeDimensionGranularity, TimeDimensionName,
    },
    config::ConfigObj,
    copy_value,
    transport::{ext::V1CubeMetaExt, MemberType, MetaContext},
    var, var_iter,
};
use bigdecimal::{num_bigint::BigInt, BigDecimal};
use chrono::{
    format::{
        Fixed, Item,
        Numeric::{Day, Hour, Minute, Month, Second, Year},
        Pad::Zero,
    },
    DateTime, Datelike, Days, Duration, Months, NaiveDate, NaiveDateTime, Timelike, Weekday,
};
use cubeclient::models::V1CubeMeta;
use datafusion::{
    arrow::{
        array::{Date32Array, Date64Array, TimestampNanosecondArray},
        datatypes::{DataType, IntervalDayTimeType},
    },
    logical_plan::{Column, Expr, Operator},
    scalar::ScalarValue,
};
use egg::{Subst, Var};
use std::{
    collections::HashSet,
    fmt::Display,
    ops::{Index, IndexMut},
    sync::Arc,
};

pub struct FilterRules {
    meta_context: Arc<MetaContext>,
    config_obj: Arc<dyn ConfigObj>,
    eval_stable_functions: bool,
}

impl FilterRules {
    fn inlist_expr_list(&self, exprs: Vec<impl Display>) -> String {
        inlist_expr_list(exprs, self.config_obj.push_down_pull_up_split())
    }
}

impl RewriteRules for FilterRules {
    fn rewrite_rules(&self) -> Vec<CubeRewrite> {
        let mut rules = vec![
            transforming_rewrite(
                "push-down-filter-simplify",
                filter(
                    "?expr",
                    cube_scan(
                        "?alias_to_cube",
                        "?members",
                        "?filters",
                        "?order",
                        "?limit",
                        "?offset",
                        "?split",
                        "?can_pushdown_join",
                        "?wrapped",
                        "?ungrouped",
                    ),
                ),
                cube_scan(
                    "?alias_to_cube",
                    "?members",
                    cube_scan_filters("?filters", filter_simplify_push_down_replacer("?expr")),
                    "?order",
                    "?limit",
                    "?offset",
                    "?split",
                    "?can_pushdown_join",
                    "?wrapped",
                    "?ungrouped",
                ),
                self.push_down_filter_simplify("?expr"),
            ),
            // Transform Filter: Boolean(False)
            transforming_rewrite(
                "push-down-limit-filter",
                filter(
                    "?literal_false",
                    cube_scan(
                        "?source_table_name",
                        "?members",
                        "?filters",
                        "?order",
                        "?limit",
                        "?offset",
                        "?split",
                        "?can_pushdown_join",
                        "?wrapped",
                        "?ungrouped",
                    ),
                ),
                limit(
                    "?new_limit_skip",
                    "?new_limit_fetch",
                    cube_scan(
                        "?source_table_name",
                        "?members",
                        "?filters",
                        "?order",
                        "?new_limit",
                        "?offset",
                        "?split",
                        "?can_pushdown_join",
                        "?wrapped",
                        "?ungrouped",
                    ),
                ),
                self.push_down_limit_filter(
                    "?literal_false",
                    "?new_limit",
                    "?new_limit_skip",
                    "?new_limit_fetch",
                ),
            ),
            // Transform Filter: Boolean(true)
            // It's safe to push down filter under projection, next filter-truncate-true will truncate it
            // TODO: Find a better solution how to drop filter node at all once
            transforming_rewrite(
                "push-down-filter-projection",
                filter(
                    "?literal_true",
                    projection("?expr", "?input", "?alias", "?projection_split"),
                ),
                projection(
                    "?expr",
                    filter("?literal_true", "?input"),
                    "?alias",
                    "?projection_split",
                ),
                self.transform_literal_true("?literal_true"),
            ),
            rewrite(
                "swap-limit-filter",
                filter(
                    "?filter",
                    limit(
                        "?limit_skip",
                        "LimitFetch:0",
                        cube_scan(
                            "?source_table_name",
                            "?members",
                            "?filters",
                            "?order",
                            "?limit",
                            "?offset",
                            "?split",
                            "?can_pushdown_join",
                            "?wrapped",
                            "?ungrouped",
                        ),
                    ),
                ),
                limit(
                    "?limit_skip",
                    "LimitFetch:0",
                    filter(
                        "?filter",
                        cube_scan(
                            "?source_table_name",
                            "?members",
                            "?filters",
                            "?order",
                            "?limit",
                            "?offset",
                            "?split",
                            "?can_pushdown_join",
                            "?wrapped",
                            "?ungrouped",
                        ),
                    ),
                ),
            ),
            transforming_rewrite(
                "limit-push-down-projection",
                limit(
                    "?skip",
                    "?fetch",
                    projection("?expr", "?input", "?alias", "?projection_split"),
                ),
                projection(
                    "?expr",
                    limit("?skip", "?fetch", "?input"),
                    "?alias",
                    "?projection_split",
                ),
                self.push_down_limit_projection("?input"),
            ),
            // Limit to top node
            rewrite(
                "swap-limit-projection",
                projection(
                    "?filter",
                    limit(
                        "?limit_skip",
                        "LimitFetch:0",
                        cube_scan(
                            "?source_table_name",
                            "?members",
                            "?filters",
                            "?order",
                            "?limit",
                            "?offset",
                            "?split",
                            "?can_pushdown_join",
                            "?wrapped",
                            "?ungrouped",
                        ),
                    ),
                    "?alias",
                    "?projection_split",
                ),
                limit(
                    "?limit_skip",
                    "LimitFetch:0",
                    projection(
                        "?filter",
                        cube_scan(
                            "?source_table_name",
                            "?members",
                            "?filters",
                            "?order",
                            "?limit",
                            "?offset",
                            "?split",
                            "?can_pushdown_join",
                            "?wrapped",
                            "?ungrouped",
                        ),
                        "?alias",
                        "?projection_split",
                    ),
                ),
            ),
            transforming_rewrite(
                "push-down-filter-pickup-simplified",
                cube_scan(
                    "?alias_to_cube",
                    "?members",
                    cube_scan_filters("?filters", filter_simplify_pull_up_replacer("?filter")),
                    "?order",
                    "?limit",
                    "?offset",
                    "?split",
                    "?can_pushdown_join",
                    "?wrapped",
                    "?ungrouped",
                ),
                cube_scan(
                    "?alias_to_cube",
                    "?members",
                    cube_scan_filters(
                        "?filters",
                        filter_replacer(
                            "?filter",
                            "?filter_alias_to_cube",
                            "?members",
                            "?filter_aliases",
                        ),
                    ),
                    "?order",
                    "?limit",
                    "?offset",
                    "?split",
                    "?can_pushdown_join",
                    "?wrapped",
                    "?ungrouped",
                ),
                self.push_down_filter("?alias_to_cube", "?filter_alias_to_cube", "?filter_aliases"),
            ),
            // Transform Filter: Boolean(True) same as TRUE = TRUE, which is useless
            transforming_rewrite(
                "filter-truncate-true",
                filter_replacer(
                    "?literal_true",
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                cube_scan_filters_empty_tail(),
                self.transform_literal_true("?literal_true"),
            ),
            // We use this rule to transform: (?expr IN (?list..)) = TRUE and ((?expr IN (?list..)) = TRUE) = TRUE
            transforming_rewrite(
                "filter-truncate-in-list-true",
                filter_replacer(
                    binary_expr("?expr", "=", "?literal_true"),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer("?expr", "?alias_to_cube", "?members", "?filter_aliases"),
                self.transform_literal_true("?literal_true"),
            ),
            transforming_rewrite(
                "filter-replacer",
                filter_replacer(
                    binary_expr(column_expr("?column"), "?op", "?constant"),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_filter(
                    "?column",
                    "?op",
                    "?constant",
                    "?alias_to_cube",
                    "?members",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                    "?filter_aliases",
                ),
            ),
            transforming_rewrite(
                "segment-replacer",
                filter_replacer(
                    binary_expr(column_expr("?column"), "?op", literal_expr("?literal")),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                segment_member("?segment"),
                self.transform_segment(
                    "?column",
                    "?op",
                    "?literal",
                    "?alias_to_cube",
                    "?members",
                    "?segment",
                    "?filter_aliases",
                ),
            ),
            transforming_rewrite(
                "change-user-lower-equal-filter",
                filter_replacer(
                    binary_expr(
                        self.fun_expr("Lower", vec![column_expr("?column")]),
                        "=",
                        literal_expr("?literal"),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                change_user_member("?user"),
                self.transform_change_user_eq(
                    "?column",
                    "?literal",
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                    "?user",
                ),
            ),
            transforming_rewrite(
                "change-user-equal-filter",
                filter_replacer(
                    binary_expr(column_expr("?column"), "=", literal_expr("?literal")),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                change_user_member("?user"),
                self.transform_change_user_eq(
                    "?column",
                    "?literal",
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                    "?user",
                ),
            ),
            transforming_rewrite(
                "user-is-not-null-filter",
                filter_replacer(
                    is_not_null_expr(column_expr("?column")),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    literal_bool(true),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                self.transform_user_is_not_null(
                    "?column",
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
            ),
            transforming_rewrite(
                "join-field-filter-eq",
                filter_replacer(
                    binary_expr(
                        column_expr("?column_left"),
                        "=",
                        column_expr("?column_right"),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                // TODO worth introducing always true filter member
                filter_op(filter_op_filters_empty_tail(), "FilterOpOp:and"),
                self.transform_join_field(
                    "?column_left",
                    "?column_right",
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
            ),
            transforming_rewrite(
                "join-field-filter-is-null",
                filter_replacer(
                    is_null_expr(column_expr("?column")),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                // TODO worth introducing always true filter member
                // TODO we might want actually return always false here in case actual check conflicts with previous rule
                filter_op(filter_op_filters_empty_tail(), "FilterOpOp:and"),
                self.transform_join_field_is_null(
                    "?column",
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
            ),
            transforming_rewrite(
                "in-filter-equal",
                filter_replacer(
                    inlist_expr("?expr", self.inlist_expr_list(vec!["?elem"]), "?negated"),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    binary_expr("?expr", "?op", "?elem"),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                self.transform_filter_in_to_equal("?negated", "?op"),
            ),
            transforming_rewrite(
                "filter-in-list-datetrunc",
                filter_replacer(
                    inlist_expr(
                        self.fun_expr(
                            "DateTrunc",
                            vec!["?granularity".to_string(), column_expr("?column")],
                        ),
                        "?list",
                        "?negated",
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                "?new_filter".to_string(),
                self.transform_filter_in_list_datetrunc(
                    "?granularity",
                    "?column",
                    "?list",
                    "?negated",
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                    "?new_filter",
                ),
            ),
            rewrite(
                "filter-in-place-filter-to-true-filter",
                filter_replacer(
                    column_expr("?column"),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    binary_expr(column_expr("?column"), "=", literal_bool(true)),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
            ),
            rewrite(
                "filter-in-place-filter-to-false-filter",
                filter_replacer(
                    not_expr(column_expr("?column")),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    binary_expr(column_expr("?column"), "=", literal_bool(false)),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
            ),
            transforming_rewrite(
                "filter-replacer-in-filter",
                filter_replacer(
                    inlist_expr(column_expr("?column"), "?list", "?negated"),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_in_filter(
                    "?column",
                    "?list",
                    "?negated",
                    "?alias_to_cube",
                    "?members",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                    "?filter_aliases",
                ),
            ),
            transforming_rewrite(
                "filter-replacer-not-in-filter-to-negated-in-filter",
                filter_replacer(
                    not_expr(inlist_expr("?expr", "?list", "?negated")),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    inlist_expr("?expr", "?list", "?new_negated"),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                self.transform_negate_inlist("?negated", "?new_negated"),
            ),
            rewrite(
                "filter-replacer-not-or-to-not-and",
                filter_replacer(
                    not_expr(binary_expr("?left", "OR", "?right")),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    binary_expr(not_expr("?left"), "AND", not_expr("?right")),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
            ),
            transforming_rewrite(
                "filter-replacer-is-null",
                filter_replacer(
                    is_null_expr(column_expr("?column")),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_is_null(
                    "?column",
                    "?alias_to_cube",
                    "?members",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                    "?filter_aliases",
                    true,
                ),
            ),
            transforming_rewrite(
                "filter-replacer-is-not-null",
                filter_replacer(
                    is_not_null_expr(column_expr("?column")),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_is_null(
                    "?column",
                    "?alias_to_cube",
                    "?members",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                    "?filter_aliases",
                    false,
                ),
            ),
            transforming_rewrite(
                "filter-replacer-binary-swap",
                filter_replacer(
                    binary_expr(literal_expr("?literal"), "?op", column_expr("?column")),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    binary_expr(column_expr("?column"), "?new_op", literal_expr("?literal")),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                self.transform_filter_binary_swap("?literal", "?op", "?new_op"),
            ),
            rewrite(
                "filter-replacer-equals-negation",
                filter_replacer(
                    not_expr(binary_expr(
                        column_expr("?column"),
                        "=",
                        literal_expr("?literal"),
                    )),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    binary_expr(column_expr("?column"), "!=", literal_expr("?literal")),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
            ),
            rewrite(
                "filter-replacer-not-equals-negation",
                filter_replacer(
                    not_expr(binary_expr(
                        column_expr("?column"),
                        "!=",
                        literal_expr("?literal"),
                    )),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    binary_expr(column_expr("?column"), "=", literal_expr("?literal")),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
            ),
            rewrite(
                "filter-replacer-is-null-negation",
                filter_replacer(
                    not_expr(is_null_expr("?expr")),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    is_not_null_expr("?expr"),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
            ),
            rewrite(
                "filter-replacer-is-not-null-negation",
                filter_replacer(
                    not_expr(is_not_null_expr("?expr")),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    is_null_expr("?expr"),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
            ),
            rewrite(
                "filter-replacer-double-negation",
                filter_replacer(
                    not_expr(not_expr("?expr")),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer("?expr", "?alias_to_cube", "?members", "?filter_aliases"),
            ),
            transforming_rewrite(
                "filter-replacer-between-dates",
                filter_replacer(
                    between_expr(column_expr("?column"), "?negated", "?low", "?high"),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_between_dates(
                    "?column",
                    "?negated",
                    "?low",
                    "?high",
                    "?alias_to_cube",
                    "?members",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                    "?filter_aliases",
                ),
            ),
            transforming_rewrite(
                "filter-replacer-between-numbers",
                filter_replacer(
                    between_expr(column_expr("?column"), "?negated", "?low", "?high"),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    binary_expr(
                        binary_expr(column_expr("?column"), ">=", "?low"),
                        "AND",
                        binary_expr(column_expr("?column"), "<=", "?high"),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                self.transform_between_numbers(
                    "?column",
                    "?negated",
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                    false,
                ),
            ),
            transforming_rewrite(
                "filter-replacer-not-between-numbers",
                filter_replacer(
                    between_expr(column_expr("?column"), "?negated", "?low", "?high"),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    binary_expr(
                        binary_expr(column_expr("?column"), "<", "?low"),
                        "OR",
                        binary_expr(column_expr("?column"), ">", "?high"),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                self.transform_between_numbers(
                    "?column",
                    "?negated",
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                    true,
                ),
            ),
            rewrite(
                "filter-replacer-and",
                filter_replacer(
                    binary_expr("?left", "AND", "?right"),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_op(
                    filter_op_filters(
                        filter_replacer("?left", "?alias_to_cube", "?members", "?filter_aliases"),
                        filter_replacer("?right", "?alias_to_cube", "?members", "?filter_aliases"),
                    ),
                    "FilterOpOp:and",
                ),
            ),
            // OR filters presents an issue: it must be a single filter with LogicalOp inside, so it can't have both measures and dimensions together
            // There's no need to check AND operation for measure-dimension mixup
            // Any number of AND's between root and terminal filter will be split to separate filters in CubeScan
            // It's enough to stop on first OR, because FilterReplacer goes top-down
            transforming_rewrite(
                "filter-replacer-or",
                filter_replacer(
                    binary_expr("?left", "OR", "?right"),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_op(
                    filter_op_filters(
                        filter_replacer("?left", "?alias_to_cube", "?members", "?filter_aliases"),
                        filter_replacer("?right", "?alias_to_cube", "?members", "?filter_aliases"),
                    ),
                    "FilterOpOp:or",
                ),
                self.transform_filter_or(
                    "?left",
                    "?right",
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
            ),
            // Unwrap lower for case-insensitive operators
            transforming_rewrite(
                "filter-replacer-lower-unwrap",
                filter_replacer(
                    binary_expr(self.fun_expr("Lower", vec!["?param"]), "?op", "?right"),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    binary_expr("?param", "?op", "?right"),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                self.unwrap_lower_or_upper("?op"),
            ),
            rewrite(
                "filter-replacer-lower-is-null-unwrap",
                filter_replacer(
                    is_null_expr(self.fun_expr("Lower", vec!["?expr"])),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    is_null_expr("?expr"),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
            ),
            rewrite(
                "filter-replacer-lower-is-not-null-unwrap",
                filter_replacer(
                    is_not_null_expr(self.fun_expr("Lower", vec!["?expr"])),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    is_not_null_expr("?expr"),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
            ),
            // Unwrap upper for case-insensitive operators
            transforming_rewrite(
                "filter-replacer-upper-unwrap",
                filter_replacer(
                    binary_expr(self.fun_expr("Upper", vec!["?param"]), "?op", "?right"),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    binary_expr("?param", "?op", "?right"),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                self.unwrap_lower_or_upper("?op"),
            ),
            rewrite(
                "filter-replacer-upper-is-null-unwrap",
                filter_replacer(
                    is_null_expr(self.fun_expr("Upper", vec!["?expr"])),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    is_null_expr("?expr"),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
            ),
            rewrite(
                "filter-replacer-upper-is-not-null-unwrap",
                filter_replacer(
                    is_not_null_expr(self.fun_expr("Upper", vec!["?expr"])),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    is_not_null_expr("?expr"),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
            ),
            rewrite(
                "filter-str-pos-to-like",
                filter_replacer(
                    binary_expr(
                        self.fun_expr(
                            "Strpos",
                            vec![column_expr("?column"), literal_expr("?value")],
                        ),
                        ">",
                        literal_int(0),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    binary_expr(
                        column_expr("?column"),
                        "LIKE",
                        binary_expr(
                            binary_expr(literal_string("%"), "||", literal_expr("?value")),
                            "||",
                            literal_string("%"),
                        ),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
            ),
            rewrite(
                "filter-str-lower-to-column",
                filter_replacer(
                    binary_expr(
                        self.fun_expr(
                            "Strpos",
                            vec![
                                self.fun_expr("Lower", vec![column_expr("?column")]),
                                literal_expr("?value"),
                            ],
                        ),
                        ">",
                        literal_expr("?zero"),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    binary_expr(
                        self.fun_expr(
                            "Strpos",
                            vec![column_expr("?column"), literal_expr("?value")],
                        ),
                        ">",
                        literal_expr("?zero"),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
            ),
            rewrite(
                "filter-str-not-null-case-to-column",
                filter_replacer(
                    binary_expr(
                        self.fun_expr(
                            "Strpos",
                            vec![
                                case_expr(
                                    None,
                                    vec![(
                                        is_not_null_expr(column_expr("?column")),
                                        column_expr("?column"),
                                    )],
                                    Some(literal_string("")),
                                ),
                                literal_expr("?value"),
                            ],
                        ),
                        ">",
                        literal_expr("?zero"),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    binary_expr(
                        self.fun_expr(
                            "Strpos",
                            vec![column_expr("?column"), literal_expr("?value")],
                        ),
                        ">",
                        literal_expr("?zero"),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
            ),
            transforming_chain_rewrite(
                "filter-left",
                filter_replacer(
                    binary_expr(
                        self.fun_expr(
                            "Left",
                            vec!["?expr".to_string(), literal_expr("?literal_length")],
                        ),
                        "=",
                        "?literal",
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                vec![("?expr", column_expr("?column"))],
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_filter_prefix(
                    "?column",
                    "startsWith",
                    "?literal",
                    Some("?literal_length"),
                    "?alias_to_cube",
                    "?members",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                    "?filter_aliases",
                ),
            ),
            transforming_chain_rewrite(
                "filter-right",
                filter_replacer(
                    binary_expr(
                        self.fun_expr(
                            "Right",
                            vec!["?expr".to_string(), literal_expr("?literal_length")],
                        ),
                        "=",
                        "?literal",
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                vec![("?expr", column_expr("?column"))],
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_filter_prefix(
                    "?column",
                    "endsWith",
                    "?literal",
                    Some("?literal_length"),
                    "?alias_to_cube",
                    "?members",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                    "?filter_aliases",
                ),
            ),
            transforming_rewrite(
                "filter-starts-with",
                filter_replacer(
                    self.fun_expr(
                        "StartsWith",
                        vec![column_expr("?column"), "?literal".to_string()],
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_filter_prefix(
                    "?column",
                    "startsWith",
                    "?literal",
                    None,
                    "?alias_to_cube",
                    "?members",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                    "?filter_aliases",
                ),
            ),
            transforming_rewrite(
                "filter-not-starts-with",
                filter_replacer(
                    not_expr(self.fun_expr(
                        "StartsWith",
                        vec![column_expr("?column"), "?literal".to_string()],
                    )),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_filter_prefix(
                    "?column",
                    "notStartsWith",
                    "?literal",
                    None,
                    "?alias_to_cube",
                    "?members",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                    "?filter_aliases",
                ),
            ),
            transforming_rewrite(
                "filter-not-ends-with",
                filter_replacer(
                    not_expr(udf_expr(
                        "ends_with",
                        vec![column_expr("?column"), "?literal".to_string()],
                    )),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_filter_prefix(
                    "?column",
                    "notEndsWith",
                    "?literal",
                    None,
                    "?alias_to_cube",
                    "?members",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                    "?filter_aliases",
                ),
            ),
            transforming_rewrite(
                "filter-ends-with",
                filter_replacer(
                    udf_expr(
                        "ends_with",
                        vec![column_expr("?column"), "?literal".to_string()],
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_filter_prefix(
                    "?column",
                    "endsWith",
                    "?literal",
                    None,
                    "?alias_to_cube",
                    "?members",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                    "?filter_aliases",
                ),
            ),
            transforming_chain_rewrite(
                "filter-quicksight-str-num-contains",
                filter_replacer(
                    binary_expr(
                        case_expr(
                            None,
                            vec![(
                                binary_expr(
                                    self.fun_expr(
                                        "Strpos",
                                        vec![
                                            self.fun_expr(
                                                "Substr",
                                                vec!["?expr".to_string(), literal_int(1)],
                                            ),
                                            literal_expr("?literal"),
                                        ],
                                    ),
                                    ">",
                                    literal_int(0),
                                ),
                                alias_expr(
                                    self.fun_expr(
                                        "Strpos",
                                        vec![
                                            self.fun_expr(
                                                "Substr",
                                                vec!["?expr".to_string(), literal_int(1)],
                                            ),
                                            literal_expr("?literal"),
                                        ],
                                    ),
                                    "?plus_minus_one_alias",
                                ),
                            )],
                            Some(literal_int(0)),
                        ),
                        "?op",
                        literal_int(0),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                vec![("?expr", column_expr("?column"))],
                filter_replacer(
                    binary_expr(
                        column_expr("?column"),
                        "?output_op",
                        literal_expr("?new_literal"),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                self.transform_filter_quicksight_case(
                    "?op",
                    "?output_op",
                    "?literal",
                    "?new_literal",
                ),
            ),
            transforming_rewrite(
                "filter-sigma-str-contains",
                filter_replacer(
                    binary_expr(
                        udf_expr(
                            "position",
                            vec![
                                "?literal".to_string(),
                                self.fun_expr("Lower", vec![column_expr("?column")]),
                            ],
                        ),
                        ">",
                        literal_int(0),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_filter_prefix(
                    "?column",
                    "contains",
                    "?literal",
                    None,
                    "?alias_to_cube",
                    "?members",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                    "?filter_aliases",
                ),
            ),
            transforming_rewrite(
                "filter-sigma-str-not-contains",
                filter_replacer(
                    binary_expr(
                        udf_expr(
                            "position",
                            vec![
                                "?literal".to_string(),
                                self.fun_expr("Lower", vec![column_expr("?column")]),
                            ],
                        ),
                        "<=",
                        literal_int(0),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_filter_prefix(
                    "?column",
                    "notContains",
                    "?literal",
                    None,
                    "?alias_to_cube",
                    "?members",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                    "?filter_aliases",
                ),
            ),
            transforming_rewrite(
                "filter-sigma-str-starts-with",
                filter_replacer(
                    binary_expr(
                        udf_expr(
                            "position",
                            vec![
                                "?literal".to_string(),
                                self.fun_expr("Lower", vec![column_expr("?column")]),
                            ],
                        ),
                        "=",
                        literal_int(1),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_filter_prefix(
                    "?column",
                    "startsWith",
                    "?literal",
                    None,
                    "?alias_to_cube",
                    "?members",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                    "?filter_aliases",
                ),
            ),
            transforming_rewrite(
                "filter-sigma-str-not-starts-with",
                filter_replacer(
                    binary_expr(
                        udf_expr(
                            "position",
                            vec![
                                "?literal".to_string(),
                                self.fun_expr("Lower", vec![column_expr("?column")]),
                            ],
                        ),
                        "!=",
                        literal_int(1),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_filter_prefix(
                    "?column",
                    "notStartsWith",
                    "?literal",
                    None,
                    "?alias_to_cube",
                    "?members",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                    "?filter_aliases",
                ),
            ),
            transforming_rewrite(
                "filter-sigma-str-ends-with",
                filter_replacer(
                    binary_expr(
                        udf_expr(
                            "position",
                            vec![
                                self.fun_expr("Reverse", vec!["?literal".to_string()]),
                                self.fun_expr(
                                    "Reverse",
                                    vec![self.fun_expr("Lower", vec![column_expr("?column")])],
                                ),
                            ],
                        ),
                        "=",
                        literal_int(1),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_filter_prefix(
                    "?column",
                    "endsWith",
                    "?literal",
                    None,
                    "?alias_to_cube",
                    "?members",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                    "?filter_aliases",
                ),
            ),
            transforming_rewrite(
                "filter-sigma-str-not-ends-with",
                filter_replacer(
                    binary_expr(
                        udf_expr(
                            "position",
                            vec![
                                self.fun_expr("Reverse", vec!["?literal".to_string()]),
                                self.fun_expr(
                                    "Reverse",
                                    vec![self.fun_expr("Lower", vec![column_expr("?column")])],
                                ),
                            ],
                        ),
                        "!=",
                        literal_int(1),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_filter_prefix(
                    "?column",
                    "notEndsWith",
                    "?literal",
                    None,
                    "?alias_to_cube",
                    "?members",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                    "?filter_aliases",
                ),
            ),
            transforming_rewrite(
                "filter-thoughtspot-like-escape-contains",
                filter_replacer(
                    like_expr(
                        "LikeExprLikeType:Like",
                        "?negated",
                        self.fun_expr("Lower", vec![column_expr("?column")]),
                        binary_expr(
                            binary_expr(
                                literal_string("%"),
                                "||",
                                self.fun_expr(
                                    "Replace",
                                    vec![
                                        self.fun_expr(
                                            "Replace",
                                            vec![
                                                self.fun_expr(
                                                    "Replace",
                                                    vec![
                                                        literal_expr("?literal"),
                                                        literal_string("!"),
                                                        literal_string("!!"),
                                                    ],
                                                ),
                                                literal_string("%"),
                                                literal_string("!%"),
                                            ],
                                        ),
                                        literal_string("_"),
                                        literal_string("!_"),
                                    ],
                                ),
                            ),
                            "||",
                            literal_string("%"),
                        ),
                        "?escape_char",
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_like_escape(
                    "contains",
                    "?negated",
                    "?column",
                    "?literal",
                    "?escape_char",
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                ),
            ),
            transforming_rewrite(
                "filter-thoughtspot-like-escape-starts-with",
                filter_replacer(
                    like_expr(
                        "LikeExprLikeType:Like",
                        "?negated",
                        self.fun_expr("Lower", vec![column_expr("?column")]),
                        binary_expr(
                            self.fun_expr(
                                "Replace",
                                vec![
                                    self.fun_expr(
                                        "Replace",
                                        vec![
                                            self.fun_expr(
                                                "Replace",
                                                vec![
                                                    literal_expr("?literal"),
                                                    literal_string("!"),
                                                    literal_string("!!"),
                                                ],
                                            ),
                                            literal_string("%"),
                                            literal_string("!%"),
                                        ],
                                    ),
                                    literal_string("_"),
                                    literal_string("!_"),
                                ],
                            ),
                            "||",
                            literal_string("%"),
                        ),
                        "?escape_char",
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_like_escape(
                    "startsWith",
                    "?negated",
                    "?column",
                    "?literal",
                    "?escape_char",
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                ),
            ),
            transforming_rewrite(
                "filter-thoughtspot-like-escape-ends-with",
                filter_replacer(
                    like_expr(
                        "LikeExprLikeType:Like",
                        "?negated",
                        self.fun_expr("Lower", vec![column_expr("?column")]),
                        binary_expr(
                            literal_string("%"),
                            "||",
                            self.fun_expr(
                                "Replace",
                                vec![
                                    self.fun_expr(
                                        "Replace",
                                        vec![
                                            self.fun_expr(
                                                "Replace",
                                                vec![
                                                    literal_expr("?literal"),
                                                    literal_string("!"),
                                                    literal_string("!!"),
                                                ],
                                            ),
                                            literal_string("%"),
                                            literal_string("!%"),
                                        ],
                                    ),
                                    literal_string("_"),
                                    literal_string("!_"),
                                ],
                            ),
                        ),
                        "?escape_char",
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_like_escape(
                    "endsWith",
                    "?negated",
                    "?column",
                    "?literal",
                    "?escape_char",
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                ),
            ),
            rewrite(
                "filter-thoughtspot-date-add-column-comp-date",
                filter_replacer(
                    binary_expr(
                        // TODO unwrap alias for filter_replacer?
                        alias_expr(
                            udf_expr(
                                "date_add",
                                vec!["?expr".to_string(), "?interval".to_string()],
                            ),
                            "?fun_alias",
                        ),
                        "?op",
                        "?date",
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    binary_expr(
                        "?expr",
                        "?op",
                        udf_expr(
                            "date_add",
                            vec![
                                udf_expr("date_to_timestamp", vec!["?date".to_string()]),
                                negative_expr("?interval"),
                            ],
                        ),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
            ),
            transforming_rewrite(
                "extract-year-equals",
                filter_replacer(
                    binary_expr(
                        self.fun_expr(
                            "Trunc",
                            vec![self.fun_expr(
                                "DatePart",
                                vec![literal_string("YEAR"), column_expr("?column")],
                            )],
                        ),
                        "=",
                        literal_expr("?year"),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_member("?member", "FilterMemberOp:inDateRange", "?values"),
                self.transform_filter_extract_year_equals(
                    "?year",
                    "?column",
                    "?alias_to_cube",
                    "?members",
                    "?member",
                    "?values",
                    "?filter_aliases",
                ),
            ),
            transforming_rewrite(
                "filter-date-trunc-sub-leeq",
                filter_replacer(
                    binary_expr(
                        binary_expr(
                            self.fun_expr(
                                "DateTrunc",
                                vec![
                                    literal_expr("?granularity"),
                                    binary_expr(column_expr("?column"), "+", "?same_interval"),
                                ],
                            ),
                            "-",
                            "?same_interval",
                        ),
                        "<=",
                        binary_expr(
                            self.fun_expr(
                                "DateTrunc",
                                vec![literal_expr("?granularity"), "?literal_expr".to_string()],
                            ),
                            "-",
                            "?same_interval",
                        ),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    binary_expr(
                        column_expr("?column"),
                        "<",
                        binary_expr(
                            binary_expr(
                                self.fun_expr(
                                    "DateTrunc",
                                    vec![literal_expr("?granularity"), "?literal_expr".to_string()],
                                ),
                                "-",
                                "?same_interval",
                            ),
                            "+",
                            literal_expr("?interval"),
                        ),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                self.transform_granularity_to_interval("?granularity", "?interval"),
            ),
            transforming_rewrite(
                "filter-binary-expr-date-trunc-column-with-literal",
                filter_replacer(
                    binary_expr(
                        self.fun_expr(
                            "DateTrunc",
                            vec![literal_expr("?granularity"), column_expr("?column")],
                        ),
                        "?op",
                        "?date",
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    binary_expr(
                        column_expr("?column"),
                        "?new_op",
                        self.fun_expr(
                            "DateTrunc",
                            vec![
                                literal_expr("?granularity"),
                                udf_expr(
                                    "date_sub",
                                    vec![
                                        udf_expr(
                                            "date_add",
                                            vec![
                                                "?date".to_string(),
                                                literal_expr("?date_add_interval"),
                                            ],
                                        ),
                                        literal_expr("?date_sub_interval"),
                                    ],
                                ),
                            ],
                        ),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                self.transform_binary_expr_date_trunc_column_with_literal(
                    "?granularity",
                    "?op",
                    "?new_op",
                    "?date_add_interval",
                    "?date_sub_interval",
                ),
            ),
            transforming_rewrite(
                "filter-date-trunc-eq-literal",
                filter_replacer(
                    binary_expr(
                        self.fun_expr(
                            "DateTrunc",
                            vec!["?granularity".to_string(), column_expr("?column")],
                        ),
                        "=",
                        "?date".to_string(),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    binary_expr(
                        binary_expr(column_expr("?column"), ">=", literal_expr("?start_date")),
                        "AND",
                        binary_expr(column_expr("?column"), "<", literal_expr("?end_date")),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                self.transform_date_trunc_eq_literal(
                    "?granularity",
                    "?date",
                    "?start_date",
                    "?end_date",
                ),
            ),
            rewrite(
                "between-move-interval-beyond-equal-sign",
                between_expr(
                    binary_expr("?expr", "+", "?interval"),
                    "?negated",
                    "?low",
                    "?high",
                ),
                between_expr(
                    "?expr",
                    "?negated",
                    binary_expr("?low", "-", "?interval"),
                    binary_expr("?high", "-", "?interval"),
                ),
            ),
            // TODO: second is minimum cube granularity, so we can unwrap it until cube has smaller granularity
            transforming_rewrite(
                "between-unwrap-datetrunc",
                filter_replacer(
                    between_expr(
                        self.fun_expr(
                            "DateTrunc",
                            vec![literal_expr("?granularity"), "?expr".to_string()],
                        ),
                        "?negated",
                        "?low",
                        "?high",
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    between_expr("?expr", "?negated", "?low", "?high"),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                self.unwrap_datetrunc("?granularity", "second"),
            ),
            rewrite(
                "not-expr-ilike-to-expr-not-ilike",
                not_expr(binary_expr("?left", "ILIKE", "?right")),
                binary_expr("?left", "NOT_ILIKE", "?right"),
            ),
            rewrite(
                "not-expr-like-to-expr-not-like",
                not_expr(binary_expr("?left", "LIKE", "?right")),
                binary_expr("?left", "NOT_LIKE", "?right"),
            ),
            transforming_rewrite(
                "not-like-expr-to-like-negated-expr",
                not_expr(like_expr(
                    "?like_type",
                    "?negated",
                    "?expr",
                    "?pattern",
                    "?escape_char",
                )),
                like_expr(
                    "?like_type",
                    "?new_negated",
                    "?expr",
                    "?pattern",
                    "?escape_char",
                ),
                self.transform_negate_like_expr("?negated", "?new_negated"),
            ),
            transforming_rewrite_with_root(
                "plus-value-minus-value",
                binary_expr(
                    binary_expr("?expr", "+", literal_expr("?value")),
                    "-",
                    literal_expr("?value"),
                ),
                alias_expr("?expr", "?alias"),
                transform_original_expr_to_alias("?alias"),
            ),
            // Every expression should be handled by filter cast unwrap replacer otherwise other rules just won't work
            // Simplify rules
            transforming_rewrite(
                "filter-simplify-cast-unwrap",
                filter_simplify_push_down_replacer(cast_expr("?expr", "?data_type")),
                filter_simplify_push_down_replacer("?expr"),
                self.transform_filter_cast_unwrap("?expr", "?data_type", false),
            ),
            transforming_rewrite(
                "filter-simplify-cast-push-down",
                filter_simplify_push_down_replacer(cast_expr("?expr", "?data_type")),
                cast_expr(filter_simplify_push_down_replacer("?expr"), "?data_type"),
                self.transform_filter_cast_unwrap("?expr", "?data_type", true),
            ),
            rewrite(
                "filter-simplify-cast-pull-up",
                cast_expr(filter_simplify_pull_up_replacer("?expr"), "?data_type"),
                filter_simplify_pull_up_replacer(cast_expr("?expr", "?data_type")),
            ),
            // Alias
            // TODO remove alias completely during simplification, they should be irrelevant in filters
            rewrite(
                "filter-simplify-alias-push-down",
                filter_simplify_push_down_replacer(alias_expr("?expr", "?alias")),
                alias_expr(filter_simplify_push_down_replacer("?expr"), "?alias"),
            ),
            rewrite(
                "filter-simplify-alias-pull-up",
                alias_expr(filter_simplify_pull_up_replacer("?expr"), "?alias"),
                filter_simplify_pull_up_replacer(alias_expr("?expr", "?alias")),
            ),
            // Binary expr
            rewrite(
                "filter-simplify-binary-push-down",
                filter_simplify_push_down_replacer(binary_expr("?left", "?op", "?right")),
                binary_expr(
                    filter_simplify_push_down_replacer("?left"),
                    "?op",
                    filter_simplify_push_down_replacer("?right"),
                ),
            ),
            rewrite(
                "filter-simplify-binary-pull-up",
                binary_expr(
                    filter_simplify_pull_up_replacer("?left"),
                    "?op",
                    filter_simplify_pull_up_replacer("?right"),
                ),
                filter_simplify_pull_up_replacer(binary_expr("?left", "?op", "?right")),
            ),
            rewrite(
                "filter-simplify-like-push-down",
                filter_simplify_push_down_replacer(like_expr(
                    "?like_type",
                    "?negated",
                    "?expr",
                    "?pattern",
                    "?escape_char",
                )),
                like_expr(
                    "?like_type",
                    "?negated",
                    filter_simplify_push_down_replacer("?expr"),
                    filter_simplify_push_down_replacer("?pattern"),
                    "?escape_char",
                ),
            ),
            rewrite(
                "filter-simplify-like-pull-up",
                like_expr(
                    "?like_type",
                    "?negated",
                    filter_simplify_pull_up_replacer("?expr"),
                    filter_simplify_pull_up_replacer("?pattern"),
                    "?escape_char",
                ),
                filter_simplify_pull_up_replacer(like_expr(
                    "?like_type",
                    "?negated",
                    "?expr",
                    "?pattern",
                    "?escape_char",
                )),
            ),
            rewrite(
                "filter-simplify-not-push-down",
                filter_simplify_push_down_replacer(not_expr("?expr")),
                not_expr(filter_simplify_push_down_replacer("?expr")),
            ),
            rewrite(
                "filter-simplify-not-pull-up",
                not_expr(filter_simplify_pull_up_replacer("?expr")),
                filter_simplify_pull_up_replacer(not_expr("?expr")),
            ),
            rewrite(
                "filter-simplify-inlist-push-down",
                filter_simplify_push_down_replacer(inlist_expr("?expr", "?list", "?negated")),
                // TODO unwrap list as well
                inlist_expr(
                    filter_simplify_push_down_replacer("?expr"),
                    "?list",
                    "?negated",
                ),
            ),
            rewrite(
                "filter-simplify-inlist-pull-up",
                // TODO unwrap list as well
                inlist_expr(
                    filter_simplify_pull_up_replacer("?expr"),
                    "?list",
                    "?negated",
                ),
                filter_simplify_pull_up_replacer(inlist_expr("?expr", "?list", "?negated")),
            ),
            rewrite(
                "filter-simplify-is-null-push-down",
                filter_simplify_push_down_replacer(is_null_expr("?expr")),
                is_null_expr(filter_simplify_push_down_replacer("?expr")),
            ),
            rewrite(
                "filter-simplify-is-null-pull-up",
                is_null_expr(filter_simplify_pull_up_replacer("?expr")),
                filter_simplify_pull_up_replacer(is_null_expr("?expr")),
            ),
            rewrite(
                "filter-simplify-is-not-null-push-down",
                filter_simplify_push_down_replacer(is_not_null_expr("?expr")),
                is_not_null_expr(filter_simplify_push_down_replacer("?expr")),
            ),
            rewrite(
                "filter-simplify-is-not-null-pull-up",
                is_not_null_expr(filter_simplify_pull_up_replacer("?expr")),
                filter_simplify_pull_up_replacer(is_not_null_expr("?expr")),
            ),
            rewrite(
                "filter-simplify-literal",
                filter_simplify_push_down_replacer(literal_expr("?literal")),
                filter_simplify_pull_up_replacer(literal_expr("?literal")),
            ),
            rewrite(
                "filter-simplify-column",
                filter_simplify_push_down_replacer(column_expr("?column")),
                filter_simplify_pull_up_replacer(column_expr("?column")),
            ),
            // scalar
            rewrite(
                "filter-simplify-scalar-fun-push-down",
                filter_simplify_push_down_replacer(fun_expr_var_arg("?fun", "?args")),
                fun_expr_var_arg("?fun", filter_simplify_push_down_replacer("?args")),
            ),
            rewrite(
                "filter-simplify-scalar-fun-pull-up",
                fun_expr_var_arg("?fun", filter_simplify_pull_up_replacer("?args")),
                filter_simplify_pull_up_replacer(fun_expr_var_arg("?fun", "?args")),
            ),
            rewrite(
                "filter-simplify-scalar-args-empty-tail",
                filter_simplify_push_down_replacer(scalar_fun_expr_args_empty_tail()),
                filter_simplify_pull_up_replacer(scalar_fun_expr_args_empty_tail()),
            ),
            // udf
            rewrite(
                "filter-simplify-udf-fun-push-down",
                filter_simplify_push_down_replacer(udf_expr_var_arg("?fun", "?args")),
                udf_expr_var_arg("?fun", filter_simplify_push_down_replacer("?args")),
            ),
            rewrite(
                "filter-simplify-udf-fun-pull-up",
                udf_expr_var_arg("?fun", filter_simplify_pull_up_replacer("?args")),
                filter_simplify_pull_up_replacer(udf_expr_var_arg("?fun", "?args")),
            ),
            rewrite(
                "filter-simplify-udf-args-push-down",
                filter_simplify_push_down_replacer(udf_fun_expr_args("?left", "?right")),
                udf_fun_expr_args(
                    filter_simplify_push_down_replacer("?left"),
                    filter_simplify_push_down_replacer("?right"),
                ),
            ),
            rewrite(
                "filter-simplify-udf-args-pull-up",
                udf_fun_expr_args(
                    filter_simplify_pull_up_replacer("?left"),
                    filter_simplify_pull_up_replacer("?right"),
                ),
                filter_simplify_pull_up_replacer(udf_fun_expr_args("?left", "?right")),
            ),
            rewrite(
                "filter-simplify-udf-args-empty-tail",
                filter_simplify_push_down_replacer(udf_fun_expr_args_empty_tail()),
                filter_simplify_pull_up_replacer(udf_fun_expr_args_empty_tail()),
            ),
            // case
            rewrite(
                "filter-simplify-case-push-down",
                filter_simplify_push_down_replacer(case_expr_var_arg(
                    "?expr",
                    "?when_then",
                    "?else",
                )),
                case_expr_var_arg(
                    filter_simplify_push_down_replacer("?expr"),
                    filter_simplify_push_down_replacer("?when_then"),
                    filter_simplify_push_down_replacer("?else"),
                ),
            ),
            rewrite(
                "filter-simplify-case-pull-up",
                case_expr_var_arg(
                    filter_simplify_pull_up_replacer("?expr"),
                    filter_simplify_pull_up_replacer("?when_then"),
                    filter_simplify_pull_up_replacer("?else"),
                ),
                filter_simplify_pull_up_replacer(case_expr_var_arg("?expr", "?when_then", "?else")),
            ),
            rewrite(
                "filter-simplify-between-push-down",
                filter_simplify_push_down_replacer(between_expr(
                    "?expr", "?negated", "?low", "?high",
                )),
                between_expr(
                    // TODO why expr is not simplified?
                    "?expr",
                    "?negated",
                    filter_simplify_push_down_replacer("?low"),
                    filter_simplify_push_down_replacer("?high"),
                ),
            ),
            rewrite(
                "filter-simplify-between-pull-up",
                between_expr(
                    // TODO why expr is not simplified?
                    "?expr",
                    "?negated",
                    filter_simplify_pull_up_replacer("?low"),
                    filter_simplify_pull_up_replacer("?high"),
                ),
                filter_simplify_pull_up_replacer(between_expr(
                    "?expr", "?negated", "?low", "?high",
                )),
            ),
            filter_simplify_push_down("CaseExprExpr"),
            filter_simplify_pull_up("CaseExprExpr"),
            filter_simplify_tail("CaseExprExpr"),
            filter_simplify_push_down("CaseExprWhenThenExpr"),
            filter_simplify_pull_up("CaseExprWhenThenExpr"),
            filter_simplify_tail("CaseExprWhenThenExpr"),
            filter_simplify_push_down("CaseExprElseExpr"),
            filter_simplify_pull_up("CaseExprElseExpr"),
            filter_simplify_tail("CaseExprElseExpr"),
            rewrite(
                "filter-flatten-upper-and-left",
                cube_scan_filters(
                    filter_op(filter_op_filters("?left", "?right"), "FilterOpOp:and"),
                    "?tail",
                ),
                cube_scan_filters(cube_scan_filters("?left", "?right"), "?tail"),
            ),
            rewrite(
                "filter-flatten-upper-and-right",
                cube_scan_filters(
                    "?tail",
                    filter_op(filter_op_filters("?left", "?right"), "FilterOpOp:and"),
                ),
                cube_scan_filters("?tail", cube_scan_filters("?left", "?right")),
            ),
            rewrite(
                "filter-flatten-upper-and-left-reduce",
                cube_scan_filters(filter_op_filters("?left", "?right"), "?tail"),
                cube_scan_filters(cube_scan_filters("?left", "?right"), "?tail"),
            ),
            rewrite(
                "filter-flatten-upper-and-right-reduce",
                cube_scan_filters("?tail", filter_op_filters("?left", "?right")),
                cube_scan_filters("?tail", cube_scan_filters("?left", "?right")),
            ),
            rewrite(
                "filter-flatten-op-left",
                filter_op(
                    filter_op_filters(filter_op("?filters", "?op"), "?tail"),
                    "?op",
                ),
                filter_op(filter_op_filters("?filters", "?tail"), "?op"),
            ),
            rewrite(
                "filter-flatten-op-right",
                filter_op(
                    filter_op_filters("?tail", filter_op("?filters", "?op")),
                    "?op",
                ),
                filter_op(filter_op_filters("?tail", "?filters"), "?op"),
            ),
            transforming_rewrite(
                "filter-flatten-empty-filter-op-left",
                filter_op(
                    filter_op_filters(filter_op("?filter_ops_filters", "?op"), "?tail"),
                    "?outer_op",
                ),
                filter_op(
                    filter_op_filters(filter_op_filters_empty_tail(), "?tail"),
                    "?outer_op",
                ),
                self.is_empty_filter_ops_filters("?filter_ops_filters"),
            ),
            transforming_rewrite(
                "filter-flatten-empty-filter-op-right",
                filter_op(
                    filter_op_filters("?tail", filter_op("?filter_ops_filters", "?op")),
                    "?outer_op",
                ),
                filter_op(
                    filter_op_filters("?tail", filter_op_filters_empty_tail()),
                    "?outer_op",
                ),
                self.is_empty_filter_ops_filters("?filter_ops_filters"),
            ),
            transforming_rewrite(
                "filter-flatten-empty-filter-op-left-cube-scan-filters",
                cube_scan_filters(filter_op("?filter_ops_filters", "?op"), "?tail"),
                cube_scan_filters(cube_scan_filters_empty_tail(), "?tail"),
                self.is_empty_filter_ops_filters("?filter_ops_filters"),
            ),
            transforming_rewrite(
                "filter-flatten-empty-filter-op-right-cube-scan-filters",
                cube_scan_filters("?tail", filter_op("?filter_ops_filters", "?op")),
                cube_scan_filters("?tail", cube_scan_filters_empty_tail()),
                self.is_empty_filter_ops_filters("?filter_ops_filters"),
            ),
            // TODO changes filter ordering which fail tests
            // rewrite(
            //     "filter-commute",
            //     cube_scan_filters("?left", "?right"),
            //     cube_scan_filters("?right", "?left"),
            // ),
            transforming_rewrite(
                "filter-replacer-in-date-range",
                filter_op(
                    filter_op_filters(
                        filter_member("?member", "?date_range_start_op", "?date_range_start"),
                        filter_member("?member", "?date_range_end_op", "?date_range_end"),
                    ),
                    "FilterOpOp:and",
                ),
                filter_member("?member", "FilterMemberOp:inDateRange", "?date_range"),
                self.merge_date_range(
                    "?date_range_start",
                    "?date_range_end",
                    "?date_range",
                    "?date_range_start_op",
                    "?date_range_end_op",
                ),
            ),
            transforming_chain_rewrite(
                "filter-replacer-rotate-filter-and-date-range-left",
                filter_op(
                    filter_op_filters(
                        "?time_dimension_filter",
                        filter_op(filter_op_filters("?left", "?right"), "FilterOpOp:and"),
                    ),
                    "FilterOpOp:and",
                ),
                vec![(
                    "?time_dimension_filter",
                    filter_member(
                        "?time_dimension_member",
                        "?time_dimension_op",
                        "?time_dimension_value",
                    ),
                )],
                filter_op(
                    filter_op_filters(
                        "?pull_up_member",
                        filter_op(
                            filter_op_filters("?left_out", "?right_out"),
                            "FilterOpOp:and",
                        ),
                    ),
                    "FilterOpOp:and",
                ),
                self.rotate_filter_and_date_range(
                    "?time_dimension_filter",
                    "?time_dimension_member",
                    "?time_dimension_op",
                    "?left",
                    "?right",
                    "?pull_up_member",
                    "?left_out",
                    "?right_out",
                ),
            ),
            transforming_chain_rewrite(
                "filter-replacer-rotate-filter-and-date-range-right",
                filter_op(
                    filter_op_filters(
                        filter_op(filter_op_filters("?left", "?right"), "FilterOpOp:and"),
                        "?time_dimension_filter",
                    ),
                    "FilterOpOp:and",
                ),
                vec![(
                    "?time_dimension_filter",
                    filter_member(
                        "?time_dimension_member",
                        "?time_dimension_op",
                        "?time_dimension_value",
                    ),
                )],
                filter_op(
                    filter_op_filters(
                        filter_op(
                            filter_op_filters("?left_out", "?right_out"),
                            "FilterOpOp:and",
                        ),
                        "?pull_up_member",
                    ),
                    "FilterOpOp:and",
                ),
                self.rotate_filter_and_date_range(
                    "?time_dimension_filter",
                    "?time_dimension_member",
                    "?time_dimension_op",
                    "?left",
                    "?right",
                    "?pull_up_member",
                    "?left_out",
                    "?right_out",
                ),
            ),
            transforming_rewrite(
                "filter-domo-date-column-compare-date-str",
                filter_replacer(
                    binary_expr(
                        udf_expr("date", vec![column_expr("?column")]),
                        "?op",
                        literal_expr("?literal"),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    binary_expr(
                        self.fun_expr(
                            "DateTrunc",
                            vec![literal_string("day"), column_expr("?column")],
                        ),
                        "?op",
                        udf_expr(
                            "to_date",
                            vec![literal_expr("?literal"), literal_string("yyyy-MM-dd")],
                        ),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                self.transform_date_column_compare_date_str("?op", "?literal"),
            ),
            rewrite(
                "filter-domo-date-column-between",
                filter_replacer(
                    between_expr(
                        udf_expr("date", vec![column_expr("?column")]),
                        "?negated",
                        "?low",
                        "?high",
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    between_expr(column_expr("?column"), "?negated", "?low", "?high"),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
            ),
            transforming_rewrite(
                "filter-domo-not-column-equals-date",
                filter_replacer(
                    not_expr(binary_expr(
                        udf_expr("date", vec![column_expr("?column")]),
                        "=",
                        literal_expr("?literal"),
                    )),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    binary_expr(
                        binary_expr(
                            column_expr("?column"),
                            "<",
                            udf_expr(
                                "to_date",
                                vec![literal_expr("?literal"), literal_string("yyyy-MM-dd")],
                            ),
                        ),
                        "OR",
                        binary_expr(
                            column_expr("?column"),
                            ">=",
                            binary_expr(
                                udf_expr(
                                    "to_date",
                                    vec![literal_expr("?literal"), literal_string("yyyy-MM-dd")],
                                ),
                                "+",
                                literal_expr("?one_day"),
                            ),
                        ),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                self.transform_not_column_equals_date("?literal", "?one_day"),
            ),
            rewrite(
                "in-date-range-to-time-dimension-pull-up-left",
                cube_scan_filters(
                    time_dimension_date_range_replacer(
                        "?filters",
                        "?time_dimension_member",
                        "?time_dimension_date_range",
                    ),
                    "?right",
                ),
                time_dimension_date_range_replacer(
                    cube_scan_filters("?filters", "?right"),
                    "?time_dimension_member",
                    "?time_dimension_date_range",
                ),
            ),
            rewrite(
                "in-date-range-to-time-dimension-pull-up-right",
                cube_scan_filters(
                    "?left",
                    time_dimension_date_range_replacer(
                        "?filters",
                        "?time_dimension_member",
                        "?time_dimension_date_range",
                    ),
                ),
                time_dimension_date_range_replacer(
                    cube_scan_filters("?left", "?filters"),
                    "?time_dimension_member",
                    "?time_dimension_date_range",
                ),
            ),
            rewrite(
                "in-date-range-to-time-dimension-swap-to-members",
                cube_scan(
                    "?source_table_name",
                    "?members",
                    time_dimension_date_range_replacer(
                        "?filters",
                        "?time_dimension_member",
                        "?time_dimension_date_range",
                    ),
                    "?order",
                    "?limit",
                    "?offset",
                    "CubeScanSplit:false",
                    "?can_pushdown_join",
                    "?wrapped",
                    "?ungrouped",
                ),
                cube_scan(
                    "?source_table_name",
                    time_dimension_date_range_replacer(
                        "?members",
                        "?time_dimension_member",
                        "?time_dimension_date_range",
                    ),
                    "?filters",
                    "?order",
                    "?limit",
                    "?offset",
                    "CubeScanSplit:false",
                    "?can_pushdown_join",
                    "?wrapped",
                    "?ungrouped",
                ),
            ),
            transforming_rewrite(
                "time-dimension-date-range-replacer-push-down-left",
                time_dimension_date_range_replacer(
                    cube_scan_members("?left", "?right"),
                    "?time_dimension_member",
                    "?time_dimension_date_range",
                ),
                cube_scan_members(
                    time_dimension_date_range_replacer(
                        "?left",
                        "?time_dimension_member",
                        "?time_dimension_date_range",
                    ),
                    "?right",
                ),
                self.push_down_time_dimension_replacer("?left", "?time_dimension_member"),
            ),
            transforming_rewrite(
                "time-dimension-date-range-replacer-push-down-right",
                time_dimension_date_range_replacer(
                    cube_scan_members("?left", "?right"),
                    "?time_dimension_member",
                    "?time_dimension_date_range",
                ),
                cube_scan_members(
                    "?left",
                    time_dimension_date_range_replacer(
                        "?right",
                        "?time_dimension_member",
                        "?time_dimension_date_range",
                    ),
                ),
                self.push_down_time_dimension_replacer("?right", "?time_dimension_member"),
            ),
            transforming_rewrite(
                "time-dimension-date-range-replacer-push-down-new-time-dimension",
                time_dimension_date_range_replacer(
                    "?members",
                    "?time_dimension_member",
                    "?time_dimension_date_range",
                ),
                cube_scan_members(
                    time_dimension_expr("?member", "?granularity", "?date_range", "?expr"),
                    "?members",
                ),
                self.push_down_time_dimension_replacer_new_time_dimension(
                    "?members",
                    "?time_dimension_member",
                    "?time_dimension_date_range",
                    "?member",
                    "?granularity",
                    "?date_range",
                    "?expr",
                ),
            ),
            rewrite(
                "time-dimension-date-range-replacer-skip-measure",
                time_dimension_date_range_replacer(
                    cube_scan_members(measure_expr("?measure", "?expr"), "?tail"),
                    "?time_dimension_member",
                    "?time_dimension_date_range",
                ),
                cube_scan_members(
                    measure_expr("?measure", "?expr"),
                    time_dimension_date_range_replacer(
                        "?tail",
                        "?time_dimension_member",
                        "?time_dimension_date_range",
                    ),
                ),
            ),
            rewrite(
                "time-dimension-date-range-replacer-skip-dimension",
                time_dimension_date_range_replacer(
                    cube_scan_members(dimension_expr("?dimension", "?expr"), "?tail"),
                    "?time_dimension_member",
                    "?time_dimension_date_range",
                ),
                cube_scan_members(
                    dimension_expr("?dimension", "?expr"),
                    time_dimension_date_range_replacer(
                        "?tail",
                        "?time_dimension_member",
                        "?time_dimension_date_range",
                    ),
                ),
            ),
            transforming_rewrite(
                "time-dimension-date-range-replacer-time-dimension",
                time_dimension_date_range_replacer(
                    cube_scan_members(
                        time_dimension_expr("?member", "?granularity", "?date_range", "?expr"),
                        "?tail",
                    ),
                    "?time_dimension_member",
                    "?time_dimension_date_range",
                ),
                cube_scan_members(
                    time_dimension_expr("?member", "?granularity", "?output_date_range", "?expr"),
                    "?tail",
                ),
                self.replace_time_dimension_date_range(
                    "?member",
                    "?date_range",
                    "?time_dimension_member",
                    "?time_dimension_date_range",
                    "?output_date_range",
                ),
            ),
        ];
        if self.config_obj.push_down_pull_up_split() {
            rules.push(list_rewrite(
                "filter-simplify-scalar-args-push-down",
                ListType::ScalarFunctionExprArgs,
                ListPattern {
                    pattern: filter_simplify_push_down_replacer("?args"),
                    list_var: "?args".to_string(),
                    elem: "?arg".to_string(),
                },
                ListPattern {
                    pattern: "?new_args".to_string(),
                    list_var: "?new_args".to_string(),
                    elem: filter_simplify_push_down_replacer("?arg"),
                },
            ));
            rules.push(list_rewrite(
                "filter-simplify-scalar-args-pull-up",
                ListType::ScalarFunctionExprArgs,
                ListPattern {
                    pattern: "?args".to_string(),
                    list_var: "?args".to_string(),
                    elem: filter_simplify_pull_up_replacer("?arg"),
                },
                ListPattern {
                    pattern: filter_simplify_pull_up_replacer("?new_args"),
                    list_var: "?new_args".to_string(),
                    elem: "?arg".to_string(),
                },
            ));
        } else {
            rules.push(rewrite(
                "filter-simplify-scalar-args-push-down",
                filter_simplify_push_down_replacer(fun_expr_args_legacy("?left", "?right")),
                fun_expr_args_legacy(
                    filter_simplify_push_down_replacer("?left"),
                    filter_simplify_push_down_replacer("?right"),
                ),
            ));
            rules.push(rewrite(
                "filter-simplify-scalar-args-pull-up",
                fun_expr_args_legacy(
                    filter_simplify_pull_up_replacer("?left"),
                    filter_simplify_pull_up_replacer("?right"),
                ),
                filter_simplify_pull_up_replacer(fun_expr_args_legacy("?left", "?right")),
            ));
        }
        if self.eval_stable_functions {
            rules.extend(vec![
                rewrite(
                    "filter-simplify-now",
                    filter_simplify_push_down_replacer(self.fun_expr("Now", Vec::<String>::new())),
                    filter_simplify_pull_up_replacer(udf_expr("eval_now", Vec::<String>::new())),
                ),
                rewrite(
                    "filter-simplify-utc-timestamp",
                    filter_simplify_push_down_replacer(
                        self.fun_expr("UtcTimestamp", Vec::<String>::new()),
                    ),
                    filter_simplify_pull_up_replacer(udf_expr(
                        "eval_utc_timestamp",
                        Vec::<String>::new(),
                    )),
                ),
                rewrite(
                    "filter-simplify-current-date",
                    filter_simplify_push_down_replacer(
                        self.fun_expr("CurrentDate", Vec::<String>::new()),
                    ),
                    filter_simplify_pull_up_replacer(udf_expr(
                        "eval_current_date",
                        Vec::<String>::new(),
                    )),
                ),
            ]);
        }
        rules
    }
}

impl FilterRules {
    pub fn new(
        meta_context: Arc<MetaContext>,
        config_obj: Arc<dyn ConfigObj>,
        eval_stable_functions: bool,
    ) -> Self {
        Self {
            meta_context,
            config_obj,
            eval_stable_functions,
        }
    }

    fn fun_expr(&self, fun_name: impl Display, args: Vec<impl Display>) -> String {
        fun_expr(fun_name, args, self.config_obj.push_down_pull_up_split())
    }

    fn push_down_filter_simplify(
        &self,
        exp_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let exp_var = var!(exp_var);
        move |egraph, subst| {
            // TODO check referenced_expr
            egraph.index(subst[exp_var]).data.referenced_expr.is_some()
        }
    }

    fn push_down_filter(
        &self,
        alias_to_cube_var: &'static str,
        filter_alias_to_cube_var: &'static str,
        filter_aliases_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let filter_aliases_var = var!(filter_aliases_var);
        let filter_alias_to_cube_var = var!(filter_alias_to_cube_var);
        move |egraph, subst| {
            if !copy_value!(
                egraph,
                subst,
                Vec<(String, String)>,
                alias_to_cube_var,
                CubeScanAliasToCube,
                filter_alias_to_cube_var,
                FilterReplacerAliasToCube
            ) {
                return false;
            }

            let filter_replacer_aliases = egraph.add(LogicalPlanLanguage::FilterReplacerAliases(
                FilterReplacerAliases(vec![]),
            ));
            subst.insert(filter_aliases_var, filter_replacer_aliases);

            true
        }
    }

    fn push_down_limit_filter(
        &self,
        literal_var: &'static str,
        new_limit_var: &'static str,
        new_limit_skip_var: &'static str,
        new_limit_fetch_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let literal_var = var!(literal_var);
        let new_limit_var = var!(new_limit_var);
        let new_limit_skip_var = var!(new_limit_skip_var);
        let new_limit_fetch_var = var!(new_limit_fetch_var);
        move |egraph, subst| {
            if let Some(ConstantFolding::Scalar(literal_value)) =
                &egraph[subst[literal_var]].data.constant
            {
                if let ScalarValue::Boolean(Some(false)) = literal_value {
                    subst.insert(
                        new_limit_var,
                        egraph.add(LogicalPlanLanguage::CubeScanLimit(CubeScanLimit(Some(0)))),
                    );
                    subst.insert(
                        new_limit_skip_var,
                        egraph.add(LogicalPlanLanguage::LimitSkip(LimitSkip(Some(0)))),
                    );
                    subst.insert(
                        new_limit_fetch_var,
                        egraph.add(LogicalPlanLanguage::LimitFetch(LimitFetch(Some(0)))),
                    );
                    return true;
                }
            }
            false
        }
    }

    fn transform_literal_true(
        &self,
        literal_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let literal_var = var!(literal_var);
        move |egraph, subst| {
            if let Some(ConstantFolding::Scalar(literal)) =
                &egraph[subst[literal_var]].data.constant
            {
                if let ScalarValue::Boolean(Some(true)) = literal {
                    return true;
                }
            }
            false
        }
    }

    fn push_down_limit_projection(
        &self,
        input_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let input_var = var!(input_var);
        move |egraph, subst| {
            for node in egraph[subst[input_var]].nodes.iter() {
                match node {
                    LogicalPlanLanguage::Limit(_) => return false,
                    _ => (),
                }
            }
            true
        }
    }

    fn transform_filter_or(
        &self,
        left_var: &'static str,
        right_var: &'static str,
        alias_to_cube_var: &'static str,
        members_var: &'static str,
        filter_aliases_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let left_var = var!(left_var);
        let right_var = var!(right_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let members_var = var!(members_var);
        let filter_aliases_var = var!(filter_aliases_var);
        let meta_context = self.meta_context.clone();
        move |egraph, subst| {
            let Some(left_columns) = &egraph[subst[left_var]].data.referenced_expr else {
                return false;
            };
            let Some(right_columns) = &egraph[subst[right_var]].data.referenced_expr else {
                return false;
            };
            let columns = left_columns
                .iter()
                .chain(right_columns.iter())
                .cloned()
                .collect::<Vec<_>>();

            let aliases_es: Vec<Vec<(String, String)>> =
                var_iter!(egraph[subst[filter_aliases_var]], FilterReplacerAliases)
                    .cloned()
                    .collect();
            for aliases in aliases_es {
                let mut has_dimensions = false;
                let mut has_measures = false;

                for column in &columns {
                    let Expr::Column(column) = column else {
                        // Unexpected non-column in referenced_expr
                        return false;
                    };

                    let Some((member_name, _, cube)) = Self::filter_member_name_on_columns(
                        egraph,
                        subst,
                        &meta_context,
                        alias_to_cube_var,
                        &[column.clone()],
                        members_var,
                        &aliases,
                    ) else {
                        // TODO is this necessary? When predicate in a filter references column that is not-a-member, is it ok to push it?
                        return false;
                    };

                    has_dimensions |= cube.lookup_dimension_by_member_name(&member_name).is_some();
                    has_measures |= cube.lookup_measure_by_member_name(&member_name).is_some();
                }
                if has_dimensions && has_measures {
                    // This filter references both measure and dimension in a single OR
                    // It is not supported by Cube.js
                    return false;
                }
            }

            true
        }
    }

    fn unwrap_lower_or_upper(
        &self,
        op_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let op_var = op_var.parse().unwrap();

        move |egraph, subst| {
            for expr_op in var_iter!(egraph[subst[op_var]], BinaryExprOp) {
                match expr_op {
                    Operator::Like | Operator::NotLike | Operator::ILike | Operator::NotILike => {
                        return true;
                    }
                    _ => {
                        continue;
                    }
                };
            }

            return false;
        }
    }

    fn transform_filter(
        &self,
        column_var: &'static str,
        op_var: &'static str,
        constant_var: &'static str,
        alias_to_cube_var: &'static str,
        members_var: &'static str,
        filter_member_var: &'static str,
        filter_op_var: &'static str,
        filter_values_var: &'static str,
        filter_aliases_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let column_var = column_var.parse().unwrap();
        let op_var = op_var.parse().unwrap();
        let constant_var = var!(constant_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let members_var = var!(members_var);
        let filter_member_var = filter_member_var.parse().unwrap();
        let filter_op_var = filter_op_var.parse().unwrap();
        let filter_values_var = filter_values_var.parse().unwrap();
        let filter_aliases_var = filter_aliases_var.parse().unwrap();
        let meta_context = self.meta_context.clone();
        move |egraph, subst| {
            let expr_ops: Vec<_> = var_iter!(egraph[subst[op_var]], BinaryExprOp)
                .cloned()
                .collect();
            for expr_op in expr_ops {
                if let Some(ConstantFolding::Scalar(literal)) =
                    &egraph[subst[constant_var]].data.constant.clone()
                {
                    let aliases_es: Vec<_> =
                        var_iter!(egraph[subst[filter_aliases_var]], FilterReplacerAliases)
                            .cloned()
                            .collect();
                    for aliases in aliases_es {
                        if let Some((member_name, granularity, cube)) =
                            Self::filter_member_name_with_granularity(
                                egraph,
                                subst,
                                &meta_context,
                                alias_to_cube_var,
                                column_var,
                                members_var,
                                &aliases,
                            )
                        {
                            if let Some(member_type) = cube.member_type(&member_name) {
                                // Segments + __user are handled by separate rule
                                if cube.lookup_measure_by_member_name(&member_name).is_some()
                                    || cube.lookup_dimension_by_member_name(&member_name).is_some()
                                {
                                    let op = match expr_op {
                                        Operator::Eq => "equals",
                                        Operator::NotEq => "notEquals",
                                        Operator::Lt => "lt",
                                        Operator::LtEq => "lte",
                                        Operator::Gt => "gt",
                                        Operator::GtEq => "gte",
                                        Operator::Like => "contains",
                                        Operator::ILike => "contains",
                                        Operator::NotLike => "notContains",
                                        Operator::NotILike => "notContains",
                                        // TODO: support regex totally
                                        Operator::RegexMatch => "startsWith",
                                        _ => {
                                            continue;
                                        }
                                    };

                                    let op = match member_type {
                                        MemberType::String => op,
                                        MemberType::Number => op,
                                        MemberType::Boolean => op,
                                        MemberType::Time => match expr_op {
                                            Operator::Lt => "beforeDate",
                                            Operator::LtEq => "beforeOrOnDate",
                                            Operator::Gt => "afterDate",
                                            Operator::GtEq => "afterOrOnDate",
                                            Operator::Eq => "inDateRange",
                                            _ => op,
                                        },
                                    };

                                    let op = match literal {
                                        ScalarValue::Utf8(Some(value)) => match op {
                                            "contains" => {
                                                let starts_with_pcnt = value.starts_with("%");
                                                let ends_with_pcnt = value.ends_with("%");
                                                match (starts_with_pcnt, ends_with_pcnt) {
                                                    (false, false) => "equals",
                                                    (false, true) => "startsWith",
                                                    (true, false) => "endsWith",
                                                    (true, true) => "contains",
                                                }
                                            }
                                            "notContains" => {
                                                let starts_with_pcnt = value.starts_with("%");
                                                let ends_with_pcnt = value.ends_with("%");
                                                match (starts_with_pcnt, ends_with_pcnt) {
                                                    (false, false) => "notEquals",
                                                    (false, true) => "notStartsWith",
                                                    (true, false) => "notEndsWith",
                                                    (true, true) => "notContains",
                                                }
                                            }
                                            _ => op,
                                        },
                                        _ => op,
                                    };

                                    let values = match literal {
                                        ScalarValue::Utf8(Some(value)) => vec![{
                                            if op == "startsWith"
                                                && value.starts_with("^^")
                                                && value.ends_with(".*$")
                                            {
                                                value[2..value.len() - 3].to_string()
                                            } else if op == "contains" || op == "notContains" {
                                                if value.starts_with("%") && value.ends_with("%") {
                                                    let without_wildcard =
                                                        value[1..value.len() - 1].to_string();
                                                    if without_wildcard.contains("%") {
                                                        continue;
                                                    }
                                                    without_wildcard
                                                } else {
                                                    value.to_string()
                                                }
                                            } else if op == "startsWith" || op == "notStartsWith" {
                                                if value.ends_with("%") {
                                                    let without_wildcard =
                                                        value[..value.len() - 1].to_string();
                                                    if without_wildcard.contains("%") {
                                                        continue;
                                                    }
                                                    without_wildcard
                                                } else {
                                                    value.to_string()
                                                }
                                            } else if op == "endsWith" || op == "notEndsWith" {
                                                if let Some(without_wildcard) =
                                                    value.strip_prefix("%")
                                                {
                                                    if without_wildcard.contains("%") {
                                                        continue;
                                                    }
                                                    without_wildcard.to_string()
                                                } else {
                                                    value.to_string()
                                                }
                                            } else {
                                                value.to_string()
                                            }
                                        }],
                                        ScalarValue::Int64(Some(value)) => vec![value.to_string()],
                                        ScalarValue::Boolean(Some(value)) => {
                                            vec![value.to_string()]
                                        }
                                        ScalarValue::Float64(Some(value)) => {
                                            vec![value.to_string()]
                                        }
                                        ScalarValue::Decimal128(Some(value), _, scale) => {
                                            vec![Decimal::new(*value).to_string(*scale)]
                                        }
                                        ScalarValue::TimestampNanosecond(_, _)
                                        | ScalarValue::Date32(_)
                                        | ScalarValue::Date64(_) => {
                                            if let Ok(Some(timestamp)) =
                                                Self::scalar_to_native_datetime(&literal)
                                            {
                                                let value = format_iso_timestamp(timestamp);

                                                match expr_op {
                                                    // TODO: all other operators need special granularity handlers like Eq
                                                    Operator::Lt => vec![value],
                                                    Operator::LtEq => vec![value],
                                                    Operator::Gt => vec![value],
                                                    Operator::GtEq => vec![value],
                                                    Operator::Eq => {
                                                        if let Some(granularity) = granularity {
                                                            if let Some((_, end)) = Self::naive_datetime_to_range_by_granularity(timestamp, &granularity) {
                                                                let end = format_iso_timestamp(end.checked_sub_signed(Duration::milliseconds(1)).unwrap());
                                                                vec![value, end]
                                                            } else {
                                                                continue;
                                                            }
                                                        } else {
                                                            vec![value.to_string(), value]
                                                        }
                                                    }
                                                    _ => {
                                                        continue;
                                                    }
                                                }
                                            } else {
                                                log::trace!(
                                                    "Can't get timestamp for {:?}",
                                                    literal
                                                );
                                                continue;
                                            }
                                        }
                                        x => {
                                            log::trace!("Unsupported filter scalar: {x:?}");
                                            continue;
                                        }
                                    };

                                    subst.insert(
                                        filter_member_var,
                                        egraph.add(LogicalPlanLanguage::FilterMemberMember(
                                            FilterMemberMember(member_name.to_string()),
                                        )),
                                    );

                                    subst.insert(
                                        filter_op_var,
                                        egraph.add(LogicalPlanLanguage::FilterMemberOp(
                                            FilterMemberOp(op.to_string()),
                                        )),
                                    );

                                    subst.insert(
                                        filter_values_var,
                                        egraph.add(LogicalPlanLanguage::FilterMemberValues(
                                            FilterMemberValues(values),
                                        )),
                                    );

                                    return true;
                                }
                            }
                        }
                    }
                }
            }

            false
        }
    }

    fn transform_filter_prefix(
        &self,
        column_var: &'static str,
        filter_member_op: &'static str,
        literal_var: &'static str,
        literal_length_var: Option<&'static str>,
        alias_to_cube_var: &'static str,
        members_var: &'static str,
        filter_member_var: &'static str,
        filter_op_var: &'static str,
        filter_values_var: &'static str,
        filter_aliases_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let column_var = var!(column_var);
        let literal_var = var!(literal_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let members_var = var!(members_var);
        let filter_member_var = var!(filter_member_var);
        let filter_op_var = var!(filter_op_var);
        let filter_values_var = var!(filter_values_var);
        let filter_aliases_var = var!(filter_aliases_var);
        let meta_context = self.meta_context.clone();
        move |egraph, subst| {
            if let Some(ConstantFolding::Scalar(literal)) =
                &egraph[subst[literal_var]].data.constant.clone()
            {
                let aliases_es: Vec<Vec<(String, String)>> =
                    var_iter!(egraph[subst[filter_aliases_var]], FilterReplacerAliases)
                        .cloned()
                        .collect();
                for aliases in aliases_es {
                    let literal_value = match literal {
                        ScalarValue::Utf8(Some(literal_value)) => literal_value.to_string(),
                        _ => continue,
                    };

                    let mut found_correct_length = None;

                    if let Some(literal_length_var) = literal_length_var {
                        let literal_length_var = var!(literal_length_var);
                        found_correct_length = Some(false);

                        for literal_length in
                            var_iter!(egraph[subst[literal_length_var]], LiteralExprValue)
                        {
                            let literal_length = match literal_length {
                                ScalarValue::Int64(Some(literal_length)) => literal_length,
                                _ => continue,
                            };

                            if literal_value.len() != *literal_length as usize {
                                continue;
                            };

                            found_correct_length = Some(true);
                        }
                    }

                    if let Some(found_correct_length) = found_correct_length {
                        if !found_correct_length {
                            return false;
                        }
                    }

                    if let Some((member_name, cube)) = Self::filter_member_name(
                        egraph,
                        subst,
                        &meta_context,
                        alias_to_cube_var,
                        column_var,
                        members_var,
                        &aliases,
                    ) {
                        if !(cube.lookup_measure_by_member_name(&member_name).is_some()
                            || cube.lookup_dimension_by_member_name(&member_name).is_some())
                        {
                            continue;
                        }

                        subst.insert(
                            filter_member_var,
                            egraph.add(LogicalPlanLanguage::FilterMemberMember(
                                FilterMemberMember(member_name.to_string()),
                            )),
                        );

                        subst.insert(
                            filter_op_var,
                            egraph.add(LogicalPlanLanguage::FilterMemberOp(FilterMemberOp(
                                filter_member_op.to_string(),
                            ))),
                        );

                        subst.insert(
                            filter_values_var,
                            egraph.add(LogicalPlanLanguage::FilterMemberValues(
                                FilterMemberValues(vec![literal_value.to_string()]),
                            )),
                        );

                        return true;
                    }
                }
            }

            false
        }
    }

    fn transform_filter_binary_swap(
        &self,
        literal_var: &'static str,
        op_var: &'static str,
        new_op_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let literal_var = var!(literal_var);
        let op_var = var!(op_var);
        let new_op_var = var!(new_op_var);
        move |egraph, subst| {
            for literal in var_iter!(egraph[subst[literal_var]], LiteralExprValue) {
                match literal {
                    ScalarValue::Decimal128(_, _, _)
                    | ScalarValue::Float32(_)
                    | ScalarValue::Float64(_)
                    | ScalarValue::Int8(_)
                    | ScalarValue::Int16(_)
                    | ScalarValue::Int32(_)
                    | ScalarValue::Int64(_)
                    | ScalarValue::UInt8(_)
                    | ScalarValue::UInt16(_)
                    | ScalarValue::UInt32(_)
                    | ScalarValue::UInt64(_)
                    | ScalarValue::Date32(_)
                    | ScalarValue::Date64(_)
                    | ScalarValue::TimestampSecond(_, _)
                    | ScalarValue::TimestampMillisecond(_, _)
                    | ScalarValue::TimestampMicrosecond(_, _)
                    | ScalarValue::TimestampNanosecond(_, _) => (),
                    _ => continue,
                };

                for op in var_iter!(egraph[subst[op_var]], BinaryExprOp) {
                    let new_op = match op {
                        Operator::Gt => Operator::Lt,
                        Operator::GtEq => Operator::LtEq,
                        Operator::Lt => Operator::Gt,
                        Operator::LtEq => Operator::GtEq,
                        Operator::Eq => Operator::Eq,
                        Operator::NotEq => Operator::NotEq,
                        _ => continue,
                    };

                    subst.insert(
                        new_op_var,
                        egraph.add(LogicalPlanLanguage::BinaryExprOp(BinaryExprOp(new_op))),
                    );

                    return true;
                }
            }

            false
        }
    }

    fn transform_join_field(
        &self,
        column_left_var: &'static str,
        column_right_var: &'static str,
        alias_to_cube_var: &'static str,
        members_var: &'static str,
        filter_aliases_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let column_left_var = var!(column_left_var);
        let column_right_var = var!(column_right_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let members_var = var!(members_var);
        let filter_aliases_var = var!(filter_aliases_var);
        let meta_context = self.meta_context.clone();
        move |egraph, subst| {
            let aliases_es: Vec<Vec<(String, String)>> =
                var_iter!(egraph[subst[filter_aliases_var]], FilterReplacerAliases)
                    .cloned()
                    .collect();
            for aliases in aliases_es {
                if let Some((left_member_name, _)) = Self::filter_member_name(
                    egraph,
                    subst,
                    &meta_context,
                    alias_to_cube_var,
                    column_left_var,
                    members_var,
                    &aliases,
                ) {
                    if left_member_name.ends_with(".__cubeJoinField") {
                        if let Some((right_member_name, _)) = Self::filter_member_name(
                            egraph,
                            subst,
                            &meta_context,
                            alias_to_cube_var,
                            column_right_var,
                            members_var,
                            &aliases,
                        ) {
                            if right_member_name.ends_with("__cubeJoinField") {
                                return true;
                            }
                        }
                    }
                }
            }

            false
        }
    }

    fn transform_join_field_is_null(
        &self,
        column_var: &'static str,
        alias_to_cube_var: &'static str,
        members_var: &'static str,
        filter_aliases_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let column_var = var!(column_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let members_var = var!(members_var);
        let filter_aliases_var = var!(filter_aliases_var);
        let meta_context = self.meta_context.clone();
        move |egraph, subst| {
            let aliases_es: Vec<Vec<(String, String)>> =
                var_iter!(egraph[subst[filter_aliases_var]], FilterReplacerAliases)
                    .cloned()
                    .collect();
            for aliases in aliases_es {
                if let Some((left_member_name, _)) = Self::filter_member_name(
                    egraph,
                    subst,
                    &meta_context,
                    alias_to_cube_var,
                    column_var,
                    members_var,
                    &aliases,
                ) {
                    if left_member_name.ends_with(".__cubeJoinField") {
                        return true;
                    }
                }
            }

            false
        }
    }

    fn transform_filter_quicksight_case(
        &self,
        op_var: &'static str,
        output_op_var: &'static str,
        literal_var: &'static str,
        new_literal_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let op_var = var!(op_var);
        let output_op_var = var!(output_op_var);
        let literal_var = var!(literal_var);
        let new_literal_var = var!(new_literal_var);
        move |egraph, subst| {
            for op in var_iter!(egraph[subst[op_var]], BinaryExprOp) {
                let output_op = match op {
                    Operator::Gt => Operator::Like,
                    Operator::Eq => Operator::NotLike,
                    _ => continue,
                };

                for literal in var_iter!(egraph[subst[literal_var]], LiteralExprValue) {
                    let new_literal = match output_op {
                        Operator::Like | Operator::NotLike => match literal {
                            ScalarValue::Utf8(Some(literal)) => {
                                // Replacers for [NOT] LIKE pattern
                                let replaced = literal
                                    .replace("\\", "\\\\")
                                    .replace("%", "\\%")
                                    .replace("_", "\\_");
                                ScalarValue::Utf8(Some(format!("%{}%", replaced)))
                            }
                            x => x.clone(),
                        },
                        _ => literal.clone(),
                    };

                    subst.insert(
                        output_op_var,
                        egraph.add(LogicalPlanLanguage::BinaryExprOp(BinaryExprOp(output_op))),
                    );

                    subst.insert(
                        new_literal_var,
                        egraph.add(LogicalPlanLanguage::LiteralExprValue(LiteralExprValue(
                            new_literal,
                        ))),
                    );

                    return true;
                }
            }

            false
        }
    }

    fn transform_filter_extract_year_equals(
        &self,
        year_var: &'static str,
        column_var: &'static str,
        alias_to_cube_var: &'static str,
        members_var: &'static str,
        member_var: &'static str,
        values_var: &'static str,
        filter_aliases_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let year_var = var!(year_var);
        let column_var = var!(column_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let members_var = var!(members_var);
        let member_var = var!(member_var);
        let values_var = var!(values_var);
        let filter_aliases_var = var!(filter_aliases_var);
        let meta_context = self.meta_context.clone();
        move |egraph, subst| {
            let years: Vec<ScalarValue> = var_iter!(egraph[subst[year_var]], LiteralExprValue)
                .cloned()
                .collect();
            if years.is_empty() {
                return false;
            }
            let aliases_es: Vec<Vec<(String, String)>> =
                var_iter!(egraph[subst[filter_aliases_var]], FilterReplacerAliases)
                    .cloned()
                    .collect();
            for year in years {
                for aliases in aliases_es.iter() {
                    if let ScalarValue::Int64(Some(year)) = year {
                        if !(1000..=9999).contains(&year) {
                            continue;
                        }

                        if let Some((member_name, cube)) = Self::filter_member_name(
                            egraph,
                            subst,
                            &meta_context,
                            alias_to_cube_var,
                            column_var,
                            members_var,
                            &aliases,
                        ) {
                            if !cube.contains_member(&member_name) {
                                continue;
                            }

                            subst.insert(
                                member_var,
                                egraph.add(LogicalPlanLanguage::FilterMemberMember(
                                    FilterMemberMember(member_name.to_string()),
                                )),
                            );

                            subst.insert(
                                values_var,
                                egraph.add(LogicalPlanLanguage::FilterMemberValues(
                                    FilterMemberValues(vec![
                                        format!("{}-01-01", year),
                                        format!("{}-12-31", year),
                                    ]),
                                )),
                            );

                            return true;
                        }
                    }
                }
            }

            false
        }
    }

    fn transform_segment(
        &self,
        column_var: &'static str,
        op_var: &'static str,
        literal_var: &'static str,
        alias_to_cube_var: &'static str,
        members_var: &'static str,
        segment_member_var: &'static str,
        filter_aliases_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let column_var = column_var.parse().unwrap();
        let op_var = op_var.parse().unwrap();
        let literal_var = literal_var.parse().unwrap();
        let alias_to_cube_var = var!(alias_to_cube_var);
        let members_var = var!(members_var);
        let segment_member_var = segment_member_var.parse().unwrap();
        let filter_aliases_var = filter_aliases_var.parse().unwrap();
        let meta_context = self.meta_context.clone();
        move |egraph, subst| {
            let expr_ops: Vec<Operator> = var_iter!(egraph[subst[op_var]], BinaryExprOp)
                .cloned()
                .collect();
            if expr_ops.is_empty() {
                return false;
            }
            let literals: Vec<ScalarValue> =
                var_iter!(egraph[subst[literal_var]], LiteralExprValue)
                    .cloned()
                    .collect();
            if literals.is_empty() {
                return false;
            }
            let aliases_es: Vec<Vec<(String, String)>> =
                var_iter!(egraph[subst[filter_aliases_var]], FilterReplacerAliases)
                    .cloned()
                    .collect();
            for expr_op in expr_ops {
                for literal in literals.iter() {
                    for aliases in aliases_es.iter() {
                        if expr_op == Operator::Eq {
                            if literal == &ScalarValue::Boolean(Some(true))
                                || literal == &ScalarValue::Utf8(Some("true".to_string()))
                            {
                                if let Some((member_name, cube)) = Self::filter_member_name(
                                    egraph,
                                    subst,
                                    &meta_context,
                                    alias_to_cube_var,
                                    column_var,
                                    members_var,
                                    &aliases,
                                ) {
                                    if let Some(_) = cube
                                        .segments
                                        .iter()
                                        .find(|s| s.name.eq_ignore_ascii_case(&member_name))
                                    {
                                        subst.insert(
                                            segment_member_var,
                                            egraph.add(LogicalPlanLanguage::SegmentMemberMember(
                                                SegmentMemberMember(member_name.to_string()),
                                            )),
                                        );

                                        return true;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            false
        }
    }

    fn transform_change_user_eq(
        &self,
        column_var: &'static str,
        literal_var: &'static str,
        alias_to_cube_var: &'static str,
        members_var: &'static str,
        filter_aliases_var: &'static str,
        change_user_member_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let column_var = var!(column_var);
        let literal_var = var!(literal_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let members_var = var!(members_var);
        let filter_aliases_var = var!(filter_aliases_var);
        let change_user_member_var = var!(change_user_member_var);
        let meta_context = self.meta_context.clone();
        move |egraph, subst| {
            let literals = var_iter!(egraph[subst[literal_var]], LiteralExprValue)
                .cloned()
                .collect::<Vec<_>>();
            for literal in literals {
                let ScalarValue::Utf8(Some(user_name)) = literal else {
                    continue;
                };

                let aliases_es =
                    var_iter!(egraph[subst[filter_aliases_var]], FilterReplacerAliases)
                        .cloned()
                        .collect::<Vec<_>>();
                for aliases in aliases_es {
                    let Some((member_name, cube)) = Self::filter_member_name(
                        egraph,
                        subst,
                        &meta_context,
                        alias_to_cube_var,
                        column_var,
                        members_var,
                        &aliases,
                    ) else {
                        continue;
                    };

                    let user_member_name = format!("{}.__user", cube.name);
                    if !member_name.eq_ignore_ascii_case(&user_member_name) {
                        continue;
                    }

                    subst.insert(
                        change_user_member_var,
                        egraph.add(LogicalPlanLanguage::ChangeUserMemberValue(
                            ChangeUserMemberValue(user_name.clone()),
                        )),
                    );
                    return true;
                }
            }

            false
        }
    }

    fn transform_user_is_not_null(
        &self,
        column_var: &'static str,
        alias_to_cube_var: &'static str,
        members_var: &'static str,
        filter_aliases_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let column_var = var!(column_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let members_var = var!(members_var);
        let filter_aliases_var = var!(filter_aliases_var);
        let meta_context = self.meta_context.clone();
        move |egraph, subst| {
            let aliases_es = var_iter!(egraph[subst[filter_aliases_var]], FilterReplacerAliases)
                .cloned()
                .collect::<Vec<_>>();
            for aliases in aliases_es {
                let Some((member_name, cube)) = Self::filter_member_name(
                    egraph,
                    subst,
                    &meta_context,
                    alias_to_cube_var,
                    column_var,
                    members_var,
                    &aliases,
                ) else {
                    continue;
                };

                let user_member_name = format!("{}.__user", cube.name);
                if !member_name.eq_ignore_ascii_case(&user_member_name) {
                    continue;
                }

                return true;
            }
            false
        }
    }

    // Transform ?expr IN (?literal) to ?expr = ?literal
    // TODO it's incorrect: inner expr can be null, or can be non-literal (and domain in not clear)
    fn transform_filter_in_to_equal(
        &self,
        negated_var: &'static str,
        op_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let negated_var = var!(negated_var);
        let op_var = var!(op_var);

        move |egraph, subst| {
            for negated in var_iter!(egraph[subst[negated_var]], InListExprNegated) {
                let operator = if *negated {
                    Operator::NotEq
                } else {
                    Operator::Eq
                };

                subst.insert(
                    op_var,
                    egraph.add(LogicalPlanLanguage::BinaryExprOp(BinaryExprOp(operator))),
                );
                return true;
            }

            false
        }
    }

    fn transform_in_filter(
        &self,
        column_var: &'static str,
        list_var: &'static str,
        negated_var: &'static str,
        alias_to_cube_var: &'static str,
        members_var: &'static str,
        filter_member_var: &'static str,
        filter_op_var: &'static str,
        filter_values_var: &'static str,
        filter_aliases_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let column_var = var!(column_var);
        let list_var = var!(list_var);
        let negated_var = var!(negated_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let members_var = var!(members_var);
        let filter_member_var = var!(filter_member_var);
        let filter_op_var = var!(filter_op_var);
        let filter_values_var = var!(filter_values_var);
        let filter_aliases_var = var!(filter_aliases_var);
        let meta_context = self.meta_context.clone();
        move |egraph, subst| {
            let aliases_es: Vec<_> =
                var_iter!(egraph[subst[filter_aliases_var]], FilterReplacerAliases)
                    .cloned()
                    .collect();
            for aliases in aliases_es {
                if let Some(list) = &egraph[subst[list_var]].data.constant_in_list {
                    let values = list
                        .iter()
                        .map(|literal| FilterRules::scalar_to_value(literal))
                        .collect::<Result<Vec<_>, _>>();
                    let Ok(values) = values else {
                        return false;
                    };

                    if let Some((member_name, cube)) = Self::filter_member_name(
                        egraph,
                        subst,
                        &meta_context,
                        alias_to_cube_var,
                        column_var,
                        members_var,
                        &aliases,
                    ) {
                        if cube.contains_member(&member_name) {
                            for negated in var_iter!(egraph[subst[negated_var]], InListExprNegated)
                            {
                                let negated = *negated;
                                subst.insert(
                                    filter_member_var,
                                    egraph.add(LogicalPlanLanguage::FilterMemberMember(
                                        FilterMemberMember(member_name.to_string()),
                                    )),
                                );

                                subst.insert(
                                    filter_op_var,
                                    egraph.add(LogicalPlanLanguage::FilterMemberOp(
                                        FilterMemberOp(if negated {
                                            "notEquals".to_string()
                                        } else {
                                            "equals".to_string()
                                        }),
                                    )),
                                );

                                subst.insert(
                                    filter_values_var,
                                    egraph.add(LogicalPlanLanguage::FilterMemberValues(
                                        FilterMemberValues(values),
                                    )),
                                );

                                return true;
                            }
                        }
                    }
                }
            }

            false
        }
    }

    fn scalar_to_value(literal: &ScalarValue) -> Result<String, &'static str> {
        Ok(match literal {
            ScalarValue::Utf8(Some(value)) => value.to_string(),
            ScalarValue::Int64(Some(value)) => value.to_string(),
            ScalarValue::Boolean(Some(value)) => value.to_string(),
            ScalarValue::Float64(Some(value)) => value.to_string(),
            ScalarValue::Decimal128(Some(value), _, scale) => {
                Decimal::new(*value).to_string(*scale)
            }
            ScalarValue::TimestampNanosecond(_, _)
            | ScalarValue::Date32(_)
            | ScalarValue::Date64(_) => {
                if let Some(timestamp) = Self::scalar_to_native_datetime(literal)? {
                    format_iso_timestamp(timestamp)
                } else {
                    log::trace!("Unsupported filter scalar: {literal:?}");
                    return Err("Unsupported filter scalar");
                }
            }
            x => {
                log::trace!("Unsupported filter scalar: {x:?}");
                return Err("Unsupported filter scalar");
            }
        })
    }

    fn scalar_to_native_datetime(
        literal: &ScalarValue,
    ) -> Result<Option<NaiveDateTime>, &'static str> {
        Ok(match literal {
            ScalarValue::TimestampNanosecond(_, _)
            | ScalarValue::Date32(_)
            | ScalarValue::Date64(_) => {
                let array = literal.to_array();
                let timestamp = if let Some(array) =
                    array.as_any().downcast_ref::<TimestampNanosecondArray>()
                {
                    array.value_as_datetime(0)
                } else if let Some(array) = array.as_any().downcast_ref::<Date32Array>() {
                    array.value_as_datetime(0)
                } else if let Some(array) = array.as_any().downcast_ref::<Date64Array>() {
                    array.value_as_datetime(0)
                } else {
                    log::trace!("Unexpected array type: {:?}", array.data_type());
                    return Err("Unexpected array type");
                };

                timestamp
            }
            x => {
                log::trace!("Unsupported filter scalar: {x:?}");
                return Err("Unsupported filter scalar");
            }
        })
    }

    fn transform_is_null(
        &self,
        column_var: &'static str,
        alias_to_cube_var: &'static str,
        members_var: &'static str,
        filter_member_var: &'static str,
        filter_op_var: &'static str,
        filter_values_var: &'static str,
        filter_aliases_var: &'static str,
        is_null_op: bool,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let column_var = var!(column_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let members_var = var!(members_var);
        let filter_member_var = var!(filter_member_var);
        let filter_op_var = var!(filter_op_var);
        let filter_values_var = var!(filter_values_var);
        let filter_aliases_var = var!(filter_aliases_var);
        let meta_context = self.meta_context.clone();
        move |egraph, subst| {
            let aliases_es: Vec<_> =
                var_iter!(egraph[subst[filter_aliases_var]], FilterReplacerAliases)
                    .cloned()
                    .collect();
            for aliases in aliases_es {
                if let Some((member_name, cube)) = Self::filter_member_name(
                    egraph,
                    subst,
                    &meta_context,
                    alias_to_cube_var,
                    column_var,
                    members_var,
                    &aliases,
                ) {
                    if cube.contains_member(&member_name) {
                        subst.insert(
                            filter_member_var,
                            egraph.add(LogicalPlanLanguage::FilterMemberMember(
                                FilterMemberMember(member_name.to_string()),
                            )),
                        );

                        subst.insert(
                            filter_op_var,
                            egraph.add(LogicalPlanLanguage::FilterMemberOp(FilterMemberOp(
                                if is_null_op {
                                    "notSet".to_string()
                                } else {
                                    "set".to_string()
                                },
                            ))),
                        );

                        subst.insert(
                            filter_values_var,
                            egraph.add(LogicalPlanLanguage::FilterMemberValues(
                                FilterMemberValues(Vec::new()),
                            )),
                        );

                        return true;
                    }
                }
            }

            false
        }
    }

    fn transform_negate_inlist(
        &self,
        negated_var: &'static str,
        new_negated_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let negated_var = var!(negated_var);
        let new_negated_var = var!(new_negated_var);
        move |egraph, subst| {
            for negated in var_iter!(egraph[subst[negated_var]], InListExprNegated).cloned() {
                subst.insert(
                    new_negated_var,
                    egraph.add(LogicalPlanLanguage::InListExprNegated(InListExprNegated(
                        !negated,
                    ))),
                );

                return true;
            }

            false
        }
    }

    fn transform_negate_like_expr(
        &self,
        negated_var: &'static str,
        new_negated_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let negated_var = var!(negated_var);
        let new_negated_var = var!(new_negated_var);
        move |egraph, subst| {
            for negated in var_iter!(egraph[subst[negated_var]], LikeExprNegated).cloned() {
                subst.insert(
                    new_negated_var,
                    egraph.add(LogicalPlanLanguage::LikeExprNegated(LikeExprNegated(
                        !negated,
                    ))),
                );

                return true;
            }

            false
        }
    }

    fn filter_member_name<'meta>(
        egraph: &mut CubeEGraph,
        subst: &Subst,
        meta_context: &'meta MetaContext,
        alias_to_cube_var: Var,
        column_var: Var,
        members_var: Var,
        aliases: &Vec<(String, String)>,
    ) -> Option<(String, &'meta V1CubeMeta)> {
        Self::filter_member_name_with_granularity(
            egraph,
            subst,
            meta_context,
            alias_to_cube_var,
            column_var,
            members_var,
            aliases,
        )
        .map(|(name, _, meta)| (name, meta))
    }

    fn filter_member_name_with_granularity<'meta>(
        egraph: &mut CubeEGraph,
        subst: &Subst,
        meta_context: &'meta MetaContext,
        alias_to_cube_var: Var,
        column_var: Var,
        members_var: Var,
        aliases: &Vec<(String, String)>,
    ) -> Option<(String, Option<String>, &'meta V1CubeMeta)> {
        let columns: Vec<_> = var_iter!(egraph[subst[column_var]], ColumnExprColumn)
            .cloned()
            .collect();

        Self::filter_member_name_on_columns(
            egraph,
            subst,
            meta_context,
            alias_to_cube_var,
            &columns,
            members_var,
            aliases,
        )
    }

    fn filter_member_name_on_columns<'meta>(
        egraph: &mut CubeEGraph,
        subst: &Subst,
        meta_context: &'meta MetaContext,
        alias_to_cube_var: Var,
        columns: &[Column],
        members_var: Var,
        aliases: &Vec<(String, String)>,
    ) -> Option<(String, Option<String>, &'meta V1CubeMeta)> {
        let alias_to_cubes: Vec<_> =
            var_iter!(egraph[subst[alias_to_cube_var]], FilterReplacerAliasToCube)
                .cloned()
                .collect();
        if alias_to_cubes.is_empty() {
            return None;
        }
        for alias_to_cube in alias_to_cubes {
            for column in columns.iter() {
                let alias_name = expr_column_name(&Expr::Column(column.clone()), &None);

                let member_name = aliases
                    .iter()
                    .find(|(a, _)| a == &alias_name)
                    .map(|(_, name)| name.to_string());
                let (member_name, granularity) = if member_name.is_some() {
                    (member_name, None)
                } else {
                    // TODO: aliases are not enough?
                    egraph
                        .index_mut(subst[members_var])
                        .data
                        .find_member_by_alias(&alias_name)
                        .map(|((member_name, member, _), _)| {
                            let member_name: Option<String> = member_name.clone();
                            if let Member::TimeDimension { granularity, .. } = member {
                                (member_name, granularity.clone())
                            } else {
                                (member_name, None)
                            }
                        })
                        .unwrap_or((None, None))
                };

                if let Some(member_name) = member_name {
                    if let Some(cube) =
                        meta_context.find_cube_with_name(&member_name.split(".").next().unwrap())
                    {
                        return Some((member_name, granularity, cube));
                    }
                } else if let Some((_, cube)) =
                    meta_context.find_cube_by_column(&alias_to_cube, &column)
                {
                    if let Some(original_name) = Self::original_member_name(&cube, &column.name) {
                        return Some((original_name, granularity, cube));
                    }
                }
            }
        }

        None
    }

    fn original_member_name(cube: &V1CubeMeta, name: &String) -> Option<String> {
        if let Some(measure) = cube.lookup_measure(name) {
            return Some(measure.name.clone());
        } else if let Some(dimension) = cube.lookup_dimension(name) {
            return Some(dimension.name.clone());
        } else if let Some(dimension) = cube.lookup_segment(name) {
            return Some(dimension.name.clone());
        }

        None
    }

    fn transform_between_dates(
        &self,
        column_var: &'static str,
        negated_var: &'static str,
        low_var: &'static str,
        high_var: &'static str,
        alias_to_cube_var: &'static str,
        members_var: &'static str,
        filter_member_var: &'static str,
        filter_op_var: &'static str,
        filter_values_var: &'static str,
        filter_aliases_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let column_var = var!(column_var);
        let negated_var = var!(negated_var);
        let low_var = var!(low_var);
        let high_var = var!(high_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let members_var = var!(members_var);
        let filter_member_var = var!(filter_member_var);
        let filter_op_var = var!(filter_op_var);
        let filter_values_var = var!(filter_values_var);
        let filter_aliases_var = var!(filter_aliases_var);
        let meta_context = self.meta_context.clone();
        move |egraph, subst| {
            let aliases_es: Vec<_> =
                var_iter!(egraph[subst[filter_aliases_var]], FilterReplacerAliases)
                    .cloned()
                    .collect();
            for aliases in aliases_es {
                if let Some((member_name, cube)) = Self::filter_member_name(
                    egraph,
                    subst,
                    &meta_context,
                    alias_to_cube_var,
                    column_var,
                    members_var,
                    &aliases,
                ) {
                    if cube.lookup_measure_by_member_name(&member_name).is_some()
                        || cube.lookup_dimension_by_member_name(&member_name).is_some()
                    {
                        for negated in var_iter!(egraph[subst[negated_var]], BetweenExprNegated) {
                            let negated = *negated;
                            if let Some(ConstantFolding::Scalar(low)) =
                                &egraph[subst[low_var]].data.constant
                            {
                                if let Some(ConstantFolding::Scalar(high)) =
                                    &egraph[subst[high_var]].data.constant
                                {
                                    match cube.member_type(&member_name) {
                                        Some(MemberType::Time) => (),
                                        _ => continue,
                                    }

                                    let Ok(low) = FilterRules::scalar_to_value(&low) else {
                                        return false;
                                    };
                                    let Ok(high) = FilterRules::scalar_to_value(&high) else {
                                        return false;
                                    };

                                    let values = vec![low, high];

                                    subst.insert(
                                        filter_member_var,
                                        egraph.add(LogicalPlanLanguage::FilterMemberMember(
                                            FilterMemberMember(member_name.to_string()),
                                        )),
                                    );

                                    subst.insert(
                                        filter_op_var,
                                        egraph.add(LogicalPlanLanguage::FilterMemberOp(
                                            FilterMemberOp(if negated {
                                                "notInDateRange".to_string()
                                            } else {
                                                "inDateRange".to_string()
                                            }),
                                        )),
                                    );

                                    subst.insert(
                                        filter_values_var,
                                        egraph.add(LogicalPlanLanguage::FilterMemberValues(
                                            FilterMemberValues(values),
                                        )),
                                    );

                                    return true;
                                }
                            }
                        }
                    }
                }
            }

            false
        }
    }

    fn transform_between_numbers(
        &self,
        column_var: &'static str,
        negated_var: &'static str,
        alias_to_cube_var: &'static str,
        members_var: &'static str,
        filter_aliases_var: &'static str,
        is_negated: bool,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let column_var = var!(column_var);
        let negated_var = var!(negated_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let members_var = var!(members_var);
        let filter_aliases_var = var!(filter_aliases_var);
        let meta_context = self.meta_context.clone();
        move |egraph, subst| {
            let aliases_es: Vec<_> =
                var_iter!(egraph[subst[filter_aliases_var]], FilterReplacerAliases)
                    .cloned()
                    .collect();
            for aliases in aliases_es {
                if let Some((member_name, cube)) = Self::filter_member_name(
                    egraph,
                    subst,
                    &meta_context,
                    alias_to_cube_var,
                    column_var,
                    members_var,
                    &aliases,
                ) {
                    if cube.lookup_measure_by_member_name(&member_name).is_some()
                        || cube.lookup_dimension_by_member_name(&member_name).is_some()
                    {
                        for negated in var_iter!(egraph[subst[negated_var]], BetweenExprNegated) {
                            match cube.member_type(&member_name) {
                                Some(MemberType::Number) if &is_negated == negated => return true,
                                _ => continue,
                            }
                        }
                    }
                }
            }

            false
        }
    }

    fn transform_granularity_to_interval(
        &self,
        granularity_var: &'static str,
        interval_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let granularity_var = var!(granularity_var);
        let interval_var = var!(interval_var);
        move |egraph, subst| {
            for granularity in var_iter!(egraph[subst[granularity_var]], LiteralExprValue) {
                if let Some(interval) = utils::granularity_scalar_to_interval(granularity) {
                    subst.insert(
                        interval_var,
                        egraph.add(LogicalPlanLanguage::LiteralExprValue(LiteralExprValue(
                            interval,
                        ))),
                    );

                    return true;
                }
            }

            false
        }
    }

    // This transform fn's job is to convert `date_trunc(col) ?op ?expr` filter expression
    // into a `col ?new_op ?new_expr`, applying the correct filter to the raw column
    // instead of a date_trunced variant.
    //
    // Here's the expression equivalence reference for day granularity:
    // ```
    // date_trunc('day', dt) < '2024-01-10 00:00:00.000'
    // dt < '2024-01-10 00:00:00.000'
    //
    // date_trunc('day', dt) <= '2024-01-10 00:00:00.000'
    // dt < '2024-01-11 00:00:00.000'
    //
    // date_trunc('day', dt) >= '2024-01-10 00:00:00.000'
    // dt >= '2024-01-10 00:00:00.000'
    //
    // date_trunc('day', dt) > '2024-01-10 00:00:00.000'
    // dt >= '2024-01-11 00:00:00.000'
    //
    // date_trunc('day', dt) < '2024-01-10 00:00:00.001'
    // dt < '2024-01-11 00:00:00.000'
    //
    // date_trunc('day', dt) <= '2024-01-10 00:00:00.001'
    // dt < '2024-01-11 00:00:00.000'
    //
    // date_trunc('day', dt) >= '2024-01-10 00:00:00.001'
    // dt >= '2024-01-11 00:00:00.000'
    //
    // date_trunc('day', dt) > '2024-01-10 00:00:00.001'
    // dt >= '2024-01-11 00:00:00.000'
    // ```
    //
    // In all cases, the expression on the right is being offset forward by an interval
    // of one granularity unit, with the exception of two cases: `<` and `>=` operators being applied
    // with an expression on the right being exactly date_trunced to granularity;
    // since we know that the left side is a date_trunced expression, the change
    // between `>=`/`>` and `<=`/`<` operators only matters if the right side of expression
    // is truncated to the granularity specified on the left side.
    //
    // To replicate this behavior, we add an interval of one granularity unit
    // to the expression on the right side, and then subtract an interval of one minimal
    // granularity unit for two operators, `<` and `>=`, to offset the expression
    // to the previous slice only if it is at the edge of a trunc slice.
    // The resulting expression is then truncated to the same granularity,
    // leading to one of the eight cases listed above.
    fn transform_binary_expr_date_trunc_column_with_literal(
        &self,
        granularity_var: &'static str,
        op_var: &'static str,
        new_op_var: &'static str,
        date_add_interval_var: &'static str,
        date_sub_interval_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let granularity_var = var!(granularity_var);
        let op_var = var!(op_var);
        let new_op_var = var!(new_op_var);
        let date_add_interval_var = var!(date_add_interval_var);
        let date_sub_interval_var = var!(date_sub_interval_var);
        move |egraph, subst| {
            for op in var_iter!(egraph[subst[op_var]], BinaryExprOp) {
                let new_op = match op {
                    Operator::GtEq | Operator::Gt => Operator::GtEq,
                    Operator::LtEq | Operator::Lt => Operator::Lt,
                    _ => continue,
                };

                for granularity in var_iter!(egraph[subst[granularity_var]], LiteralExprValue) {
                    if let ScalarValue::Utf8(Some(granularity)) = granularity {
                        if let (Some(date_add_interval), Some(date_sub_interval)) = (
                            utils::granularity_str_to_interval(&granularity),
                            match op {
                                Operator::GtEq | Operator::Lt => {
                                    utils::granularity_str_to_interval("min_unit")
                                }
                                Operator::Gt | Operator::LtEq => {
                                    Some(ScalarValue::IntervalDayTime(Some(0)))
                                }
                                _ => None,
                            },
                        ) {
                            subst.insert(
                                new_op_var,
                                egraph.add(LogicalPlanLanguage::BinaryExprOp(BinaryExprOp(new_op))),
                            );

                            subst.insert(
                                date_add_interval_var,
                                egraph.add(LogicalPlanLanguage::LiteralExprValue(
                                    LiteralExprValue(date_add_interval),
                                )),
                            );

                            subst.insert(
                                date_sub_interval_var,
                                egraph.add(LogicalPlanLanguage::LiteralExprValue(
                                    LiteralExprValue(date_sub_interval),
                                )),
                            );

                            return true;
                        }
                    }
                }
            }
            false
        }
    }

    fn transform_date_trunc_eq_literal(
        &self,
        granularity_var: &'static str,
        date_var: &'static str,
        start_date_var: &'static str,
        end_date_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let granularity_var = var!(granularity_var);
        let date_var = var!(date_var);
        let start_date_var = var!(start_date_var);
        let end_date_var = var!(end_date_var);
        move |egraph, subst| {
            let Some(ConstantFolding::Scalar(ScalarValue::Utf8(Some(granularity)))) =
                &egraph[subst[granularity_var]].data.constant
            else {
                return false;
            };

            let Some(ConstantFolding::Scalar(date)) = &egraph[subst[date_var]].data.constant else {
                return false;
            };
            let Some(Some(date)) = Self::scalar_dt_to_naive_datetime(date) else {
                return false;
            };

            let Some((start_date, end_date)) =
                Self::naive_datetime_to_range_by_granularity(date, granularity)
            else {
                return false;
            };

            let (Some(start_date), Some(end_date)) = (
                start_date.and_utc().timestamp_nanos_opt(),
                end_date.and_utc().timestamp_nanos_opt(),
            ) else {
                return false;
            };

            subst.insert(
                start_date_var,
                egraph.add(LogicalPlanLanguage::LiteralExprValue(LiteralExprValue(
                    ScalarValue::TimestampNanosecond(Some(start_date), None),
                ))),
            );
            subst.insert(
                end_date_var,
                egraph.add(LogicalPlanLanguage::LiteralExprValue(LiteralExprValue(
                    ScalarValue::TimestampNanosecond(Some(end_date), None),
                ))),
            );
            true
        }
    }

    fn is_empty_filter_ops_filters(
        &self,
        filter_ops_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let filter_ops_var = var!(filter_ops_var);
        move |egraph, subst| {
            if let Some(true) = egraph[subst[filter_ops_var]].data.is_empty_list {
                return true;
            }

            false
        }
    }

    fn merge_date_range(
        &self,
        date_range_start_var: &'static str,
        date_range_end_var: &'static str,
        date_range_var: &'static str,
        date_range_start_op_var: &'static str,
        date_range_end_op_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let date_range_start_var = date_range_start_var.parse().unwrap();
        let date_range_end_var = date_range_end_var.parse().unwrap();
        let date_range_var = date_range_var.parse().unwrap();
        let date_range_start_op_var = date_range_start_op_var.parse().unwrap();
        let date_range_end_op_var = date_range_end_op_var.parse().unwrap();
        move |egraph, subst| {
            fn resolve_time_delta(date_var: &String, op: &String) -> String {
                if op == "afterDate" {
                    return increment_iso_timestamp_time(date_var);
                } else if op == "beforeDate" {
                    return decrement_iso_timestamp_time(date_var);
                } else {
                    return date_var.clone();
                }
            }

            fn increment_iso_timestamp_time(date_var: &String) -> String {
                let timestamp = NaiveDateTime::parse_from_str(date_var, "%Y-%m-%dT%H:%M:%S%.fZ");
                let value = match timestamp {
                    Ok(val) => format_iso_timestamp(
                        val.checked_add_signed(Duration::milliseconds(1)).unwrap(),
                    ),
                    Err(_) => date_var.clone(),
                };
                return value;
            }

            fn decrement_iso_timestamp_time(date_var: &String) -> String {
                let timestamp = NaiveDateTime::parse_from_str(date_var, "%Y-%m-%dT%H:%M:%S%.fZ");
                let value = match timestamp {
                    Ok(val) => format_iso_timestamp(
                        val.checked_sub_signed(Duration::milliseconds(1)).unwrap(),
                    ),
                    Err(_) => date_var.clone(),
                };
                return value;
            }

            for date_range_start in
                var_iter!(egraph[subst[date_range_start_var]], FilterMemberValues)
            {
                for date_range_end in
                    var_iter!(egraph[subst[date_range_end_var]], FilterMemberValues)
                {
                    for date_range_start_op in
                        var_iter!(egraph[subst[date_range_start_op_var]], FilterMemberOp)
                    {
                        for date_range_end_op in
                            var_iter!(egraph[subst[date_range_end_op_var]], FilterMemberOp)
                        {
                            let valid_left_filters = ["afterDate", "afterOrOnDate"];
                            let valid_right_filters = ["beforeDate", "beforeOrOnDate"];

                            let swap_left_and_right;

                            if valid_left_filters.contains(&date_range_start_op.as_str())
                                && valid_right_filters.contains(&date_range_end_op.as_str())
                            {
                                swap_left_and_right = false;
                            } else if valid_left_filters.contains(&date_range_end_op.as_str())
                                && valid_right_filters.contains(&date_range_start_op.as_str())
                            {
                                swap_left_and_right = true;
                            } else {
                                return false;
                            }

                            let mut result = Vec::new();
                            let resolved_start_date =
                                resolve_time_delta(&date_range_start[0], date_range_start_op);
                            let resolved_end_date =
                                resolve_time_delta(&date_range_end[0], date_range_end_op);

                            if swap_left_and_right {
                                result.extend(vec![resolved_end_date]);
                                result.extend(vec![resolved_start_date]);
                            } else {
                                result.extend(vec![resolved_start_date]);
                                result.extend(vec![resolved_end_date]);
                            }

                            subst.insert(
                                date_range_var,
                                egraph.add(LogicalPlanLanguage::FilterMemberValues(
                                    FilterMemberValues(result),
                                )),
                            );
                            return true;
                        }
                    }
                }
            }

            false
        }
    }

    fn push_down_time_dimension_replacer_new_time_dimension(
        &self,
        members_var: &'static str,
        time_dimension_member_var: &'static str,
        time_dimension_date_range_var: &'static str,
        member_var: &'static str,
        granularity_var: &'static str,
        date_range_var: &'static str,
        expr_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let members_var = var!(members_var);
        let time_dimension_member_var = var!(time_dimension_member_var);
        let time_dimension_date_range_var = var!(time_dimension_date_range_var);
        let member_var = var!(member_var);
        let granularity_var = var!(granularity_var);
        let date_range_var = var!(date_range_var);
        let expr_var = var!(expr_var);
        move |egraph, subst| {
            for member in var_iter!(
                egraph[subst[time_dimension_member_var]],
                TimeDimensionDateRangeReplacerMember
            ) {
                let member = member.to_string();
                if let Some(member_name_to_expr) = &egraph
                    .index(subst[members_var])
                    .data
                    .member_name_to_expr
                    .as_ref()
                    .map(|x| &x.list)
                {
                    if member_name_to_expr
                        .iter()
                        .all(|(m, _, _)| m.as_ref() != Some(&member))
                    {
                        let date_range = var_iter!(
                            egraph[subst[time_dimension_date_range_var]],
                            TimeDimensionDateRangeReplacerDateRange
                        )
                        .next()
                        .unwrap()
                        .clone();

                        subst.insert(
                            member_var,
                            egraph.add(LogicalPlanLanguage::TimeDimensionName(TimeDimensionName(
                                member.to_string(),
                            ))),
                        );

                        subst.insert(
                            granularity_var,
                            egraph.add(LogicalPlanLanguage::TimeDimensionGranularity(
                                TimeDimensionGranularity(None),
                            )),
                        );

                        subst.insert(
                            date_range_var,
                            egraph.add(LogicalPlanLanguage::TimeDimensionDateRange(
                                TimeDimensionDateRange(Some(date_range)),
                            )),
                        );

                        let column = egraph.add(LogicalPlanLanguage::ColumnExprColumn(
                            ColumnExprColumn(Column::from_name(member.to_string())),
                        ));

                        subst.insert(
                            expr_var,
                            egraph.add(LogicalPlanLanguage::ColumnExpr([column])),
                        );

                        return true;
                    }
                }
            }

            false
        }
    }

    fn push_down_time_dimension_replacer(
        &self,
        members_var: &'static str,
        time_dimension_member_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let members_var = var!(members_var);
        let time_dimension_member_var = var!(time_dimension_member_var);
        move |egraph, subst| {
            for member in var_iter!(
                egraph[subst[time_dimension_member_var]],
                TimeDimensionDateRangeReplacerMember
            ) {
                if let Some(member_name_to_expr) = &egraph
                    .index(subst[members_var])
                    .data
                    .member_name_to_expr
                    .as_ref()
                    .map(|x| &x.list)
                {
                    if member_name_to_expr
                        .iter()
                        .any(|(m, _, _)| m.as_ref() == Some(member))
                    {
                        return true;
                    }
                }
            }

            false
        }
    }

    fn replace_time_dimension_date_range(
        &self,
        member_var: &'static str,
        date_range_var: &'static str,
        time_dimension_member_var: &'static str,
        time_dimension_date_range_var: &'static str,
        output_date_range_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let member_var = var!(member_var);
        let date_range_var = var!(date_range_var);
        let time_dimension_member_var = var!(time_dimension_member_var);
        let time_dimension_date_range_var = var!(time_dimension_date_range_var);
        let output_date_range_var = var!(output_date_range_var);
        move |egraph, subst| {
            for date_range in var_iter!(egraph[subst[date_range_var]], TimeDimensionDateRange) {
                if date_range.is_none() {
                    for member in var_iter!(egraph[subst[member_var]], TimeDimensionName) {
                        for time_dimension_member in var_iter!(
                            egraph[subst[time_dimension_member_var]],
                            TimeDimensionDateRangeReplacerMember
                        ) {
                            if member == time_dimension_member {
                                for time_dimension_date_range in var_iter!(
                                    egraph[subst[time_dimension_date_range_var]],
                                    TimeDimensionDateRangeReplacerDateRange
                                ) {
                                    let time_dimension_date_range =
                                        time_dimension_date_range.clone();
                                    subst.insert(
                                        output_date_range_var,
                                        egraph.add(LogicalPlanLanguage::TimeDimensionDateRange(
                                            TimeDimensionDateRange(Some(
                                                time_dimension_date_range.clone(),
                                            )),
                                        )),
                                    );

                                    return true;
                                }
                            }
                        }
                    }
                }
            }

            false
        }
    }

    fn unwrap_datetrunc(
        &self,
        granularity_var: &'static str,
        target_granularity: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let granularity_var = var!(granularity_var);
        move |egraph, subst| {
            for granularity in var_iter!(egraph[subst[granularity_var]], LiteralExprValue) {
                match utils::parse_granularity(granularity, false) {
                    Some(granularity)
                        if granularity.to_lowercase() == target_granularity.to_lowercase() =>
                    {
                        return true
                    }
                    _ => (),
                }
            }

            false
        }
    }

    fn transform_like_escape(
        &self,
        filter_op: &'static str,
        negated_var: &'static str,
        column_var: &'static str,
        literal_var: &'static str,
        escape_char_var: &'static str,
        alias_to_cube_var: &'static str,
        members_var: &'static str,
        filter_aliases_var: &'static str,
        filter_member_var: &'static str,
        filter_op_var: &'static str,
        filter_values_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let negated_var = var!(negated_var);
        let column_var = var!(column_var);
        let literal_var = var!(literal_var);
        let escape_char_var = var!(escape_char_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let members_var = var!(members_var);
        let filter_aliases_var = var!(filter_aliases_var);
        let filter_member_var = var!(filter_member_var);
        let filter_op_var = var!(filter_op_var);
        let filter_values_var = var!(filter_values_var);
        let meta_context = self.meta_context.clone();
        move |egraph, subst| {
            let escape_chars: Vec<Option<char>> =
                var_iter!(egraph[subst[escape_char_var]], LikeExprEscapeChar)
                    .cloned()
                    .collect();
            if escape_chars.is_empty() {
                return false;
            }
            let literals: Vec<ScalarValue> =
                var_iter!(egraph[subst[literal_var]], LiteralExprValue)
                    .cloned()
                    .collect();
            if literals.is_empty() {
                return false;
            }
            for escape_char in escape_chars.into_iter().flatten() {
                if escape_char == '!' {
                    for literal in literals.iter() {
                        let literal_value = match &literal {
                            ScalarValue::Utf8(Some(literal_value)) => literal_value.to_string(),
                            _ => continue,
                        };

                        let aliases_es: Vec<_> =
                            var_iter!(egraph[subst[filter_aliases_var]], FilterReplacerAliases)
                                .cloned()
                                .collect();
                        for aliases in aliases_es {
                            if let Some((member_name, cube)) = Self::filter_member_name(
                                egraph,
                                subst,
                                &meta_context,
                                alias_to_cube_var,
                                column_var,
                                members_var,
                                &aliases,
                            ) {
                                if !(cube.lookup_measure_by_member_name(&member_name).is_some()
                                    || cube.lookup_dimension_by_member_name(&member_name).is_some())
                                {
                                    continue;
                                }

                                for negated in
                                    var_iter!(egraph[subst[negated_var]], LikeExprNegated)
                                {
                                    let filter_member_op = match &negated {
                                        true => match utils::negated_cube_filter_op(filter_op) {
                                            Some(op) => op,
                                            None => continue,
                                        },
                                        false => filter_op,
                                    };

                                    subst.insert(
                                        filter_member_var,
                                        egraph.add(LogicalPlanLanguage::FilterMemberMember(
                                            FilterMemberMember(member_name.to_string()),
                                        )),
                                    );

                                    subst.insert(
                                        filter_op_var,
                                        egraph.add(LogicalPlanLanguage::FilterMemberOp(
                                            FilterMemberOp(filter_member_op.to_string()),
                                        )),
                                    );

                                    subst.insert(
                                        filter_values_var,
                                        egraph.add(LogicalPlanLanguage::FilterMemberValues(
                                            FilterMemberValues(vec![literal_value.to_string()]),
                                        )),
                                    );

                                    return true;
                                }
                            }
                        }
                    }
                }
            }
            false
        }
    }

    fn transform_filter_cast_unwrap(
        &self,
        expr_var: &'static str,
        data_type_var: &'static str,
        negative: bool,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let expr_var = var!(expr_var);
        let data_type_var = var!(data_type_var);
        move |egraph, subst| {
            if let Some(OriginalExpr::Expr(expr)) =
                egraph[subst[expr_var]].data.original_expr.clone()
            {
                for data_type in var_iter!(egraph[subst[data_type_var]], CastExprDataType).cloned()
                {
                    return match data_type {
                        // Exclude casts to string for timestamps
                        DataType::Timestamp(_, _) => match expr {
                            Expr::Literal(ScalarValue::Utf8(_)) => negative,
                            _ => !negative,
                        },
                        // Exclude casts to date as those truncate precision and change filter behavior
                        DataType::Date32 | DataType::Date64 => negative,
                        _ => !negative,
                    };
                }
            }

            false
        }
    }

    fn transform_date_column_compare_date_str(
        &self,
        op_var: &'static str,
        literal_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let op_var = var!(op_var);
        let literal_var = var!(literal_var);
        move |egraph, subst| {
            for op in var_iter!(egraph[subst[op_var]], BinaryExprOp) {
                match op {
                    Operator::Gt
                    | Operator::GtEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Eq => (),
                    _ => continue,
                };

                for literal in var_iter!(egraph[subst[literal_var]], LiteralExprValue) {
                    if let ScalarValue::Utf8(_) = literal {
                        return true;
                    }
                }
            }

            false
        }
    }

    fn transform_not_column_equals_date(
        &self,
        literal_var: &'static str,
        one_day_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let literal_var = var!(literal_var);
        let one_day_var = var!(one_day_var);
        move |egraph, subst| {
            for literal in var_iter!(egraph[subst[literal_var]], LiteralExprValue) {
                if let ScalarValue::Utf8(_) = literal {
                    subst.insert(
                        one_day_var,
                        egraph.add(LogicalPlanLanguage::LiteralExprValue(LiteralExprValue(
                            ScalarValue::IntervalDayTime(Some(IntervalDayTimeType::make_value(
                                1, 0,
                            ))),
                        ))),
                    );
                    return true;
                }
            }

            false
        }
    }

    fn transform_filter_in_list_datetrunc(
        &self,
        granularity_var: &'static str,
        column_var: &'static str,
        list_var: &'static str,
        negated_var: &'static str,
        alias_to_cube_var: &'static str,
        members_var: &'static str,
        filter_aliases_var: &'static str,
        new_filter_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let granularity_var = var!(granularity_var);
        let column_var = var!(column_var);
        let list_var = var!(list_var);
        let negated_var = var!(negated_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let members_var = var!(members_var);
        let filter_aliases_var = var!(filter_aliases_var);
        let new_filter_var = var!(new_filter_var);
        let meta_context = self.meta_context.clone();
        move |egraph, subst| {
            let Some(list) = &egraph[subst[list_var]].data.constant_in_list.clone() else {
                return false;
            };
            let Some(ConstantFolding::Scalar(ScalarValue::Utf8(Some(granularity)))) =
                &egraph[subst[granularity_var]].data.constant.clone()
            else {
                return false;
            };

            let aliases_es: Vec<_> =
                var_iter!(egraph[subst[filter_aliases_var]], FilterReplacerAliases)
                    .cloned()
                    .collect();
            for aliases in aliases_es {
                let Some((member_name, cube)) = Self::filter_member_name(
                    egraph,
                    subst,
                    &meta_context,
                    alias_to_cube_var,
                    column_var,
                    members_var,
                    &aliases,
                ) else {
                    continue;
                };

                if !cube.contains_member(&member_name) {
                    continue;
                }

                for negated in var_iter!(egraph[subst[negated_var]], InListExprNegated) {
                    let Some(values) = list
                        .iter()
                        .map(|literal| Self::scalar_dt_to_naive_datetime(literal))
                        .collect::<Option<HashSet<_>>>()
                        .map(|values| {
                            let mut values = values.into_iter().collect::<Vec<_>>();
                            values.sort();
                            values
                        })
                    else {
                        continue;
                    };

                    let values = values.into_iter().filter_map(|value| {
                        let Some(value) = value else {
                            // TODO: NULL values are skipped for now as if they're not there
                            return None;
                        };
                        Self::naive_datetime_to_range_by_granularity(value, granularity)
                    });

                    let mut dts: Vec<(NaiveDateTime, NaiveDateTime)> = vec![];
                    let mut last_value: Option<(NaiveDateTime, NaiveDateTime)> = None;
                    for (next_value_from, next_value_to) in values {
                        let Some((last_value_from, last_value_to)) = last_value else {
                            last_value = Some((next_value_from, next_value_to));
                            continue;
                        };
                        if last_value_to == next_value_from {
                            last_value = Some((last_value_from, next_value_to));
                            continue;
                        }
                        dts.push((last_value_from, last_value_to));
                        last_value = Some((next_value_from, next_value_to));
                    }
                    if let Some(last_value) = last_value {
                        dts.push(last_value);
                    }

                    let format = "%Y-%m-%d %H:%M:%S%.3f";
                    let dts = dts
                        .into_iter()
                        .map(|(dt, new_dt)| {
                            let new_dt = new_dt
                                .checked_sub_signed(Duration::milliseconds(1))
                                .unwrap();
                            (
                                dt.format(format).to_string(),
                                new_dt.format(format).to_string(),
                            )
                        })
                        .collect::<Vec<_>>();
                    let len = dts.len();
                    if len < 1 {
                        continue;
                    }

                    let member_op = if *negated {
                        "notInDateRange"
                    } else {
                        "inDateRange"
                    };

                    let member = egraph.add(LogicalPlanLanguage::FilterMemberMember(
                        FilterMemberMember(member_name.to_string()),
                    ));
                    let op = egraph.add(LogicalPlanLanguage::FilterMemberOp(FilterMemberOp(
                        member_op.to_string(),
                    )));
                    if len == 1 {
                        for (from, to) in dts.into_iter() {
                            let values = egraph.add(LogicalPlanLanguage::FilterMemberValues(
                                FilterMemberValues(vec![from, to]),
                            ));
                            let filter_member =
                                egraph.add(LogicalPlanLanguage::FilterMember([member, op, values]));
                            subst.insert(new_filter_var, filter_member);
                        }
                        return true;
                    }

                    let mut filters = egraph.add(LogicalPlanLanguage::FilterOpFilters(vec![]));
                    for (from, to) in dts.into_iter().rev() {
                        let values = egraph.add(LogicalPlanLanguage::FilterMemberValues(
                            FilterMemberValues(vec![from, to]),
                        ));
                        let filter_member =
                            egraph.add(LogicalPlanLanguage::FilterMember([member, op, values]));
                        filters = egraph.add(LogicalPlanLanguage::FilterOpFilters(vec![
                            filter_member,
                            filters,
                        ]));
                    }

                    let op = egraph.add(LogicalPlanLanguage::FilterOpOp(FilterOpOp(
                        "or".to_string(),
                    )));

                    subst.insert(
                        new_filter_var,
                        egraph.add(LogicalPlanLanguage::FilterOp([filters, op])),
                    );
                    return true;
                }
            }

            false
        }
    }

    // The outer Option's purpose is to signal when the type is incorrect
    // or parsing couldn't interpret the value as a NativeDateTime.
    // The inner Option is None when the ScalarValue is None.
    fn scalar_dt_to_naive_datetime(literal: &ScalarValue) -> Option<Option<NaiveDateTime>> {
        if let ScalarValue::TimestampNanosecond(ts, None) = literal {
            let Some(ts) = ts else {
                return Some(None);
            };
            let ts_seconds = *ts / 1_000_000_000;
            let ts_nanos = (*ts % 1_000_000_000) as u32;
            let dt = DateTime::from_timestamp(ts_seconds, ts_nanos).map(|dt| Some(dt.naive_utc()));
            return dt;
        };

        let ScalarValue::Utf8(str) = literal else {
            return None;
        };
        let Some(str) = str else {
            return Some(None);
        };
        let dt = NaiveDateTime::parse_from_str(str, "%Y-%m-%d %H:%M:%S%.f")
            .or_else(|_| NaiveDateTime::parse_from_str(str, "%Y-%m-%d %H:%M:%S"))
            .or_else(|_| {
                NaiveDate::parse_from_str(str, "%Y-%m-%d")
                    .map(|date| date.and_hms_opt(0, 0, 0).unwrap())
            });
        let Ok(dt) = dt else {
            return None;
        };
        Some(Some(dt))
    }

    fn naive_datetime_to_range_by_granularity(
        dt: NaiveDateTime,
        granularity: &String,
    ) -> Option<(NaiveDateTime, NaiveDateTime)> {
        let granularity = granularity.to_lowercase();

        // Validate that `dt` is indeed the earliest time of a granularity unit.
        // If it's not, it will never match in `IN` with `DATE_TRUNC` as expr.
        let is_earliest = match granularity.as_str() {
            "year" => {
                dt.month() == 1
                    && dt.day() == 1
                    && dt.hour() == 0
                    && dt.minute() == 0
                    && dt.second() == 0
                    && dt.nanosecond() == 0
            }
            "quarter" | "qtr" => {
                matches!(dt.month(), 1 | 4 | 7 | 10)
                    && dt.day() == 1
                    && dt.hour() == 0
                    && dt.minute() == 0
                    && dt.second() == 0
                    && dt.nanosecond() == 0
            }
            "month" => {
                dt.day() == 1
                    && dt.hour() == 0
                    && dt.minute() == 0
                    && dt.second() == 0
                    && dt.nanosecond() == 0
            }
            "week" => {
                dt.weekday() == Weekday::Mon
                    && dt.hour() == 0
                    && dt.minute() == 0
                    && dt.second() == 0
                    && dt.nanosecond() == 0
            }
            "day" => dt.hour() == 0 && dt.minute() == 0 && dt.second() == 0 && dt.nanosecond() == 0,
            "hour" => dt.minute() == 0 && dt.second() == 0 && dt.nanosecond() == 0,
            "minute" => dt.second() == 0 && dt.nanosecond() == 0,
            "second" => dt.nanosecond() == 0,
            _ => return None,
        };
        if !is_earliest {
            return None;
        }

        let new_dt = dt;
        let new_dt = match granularity.as_str() {
            "year" => new_dt.checked_add_months(Months::new(12)),
            "quarter" | "qtr" => new_dt.checked_add_months(Months::new(3)),
            "month" => new_dt.checked_add_months(Months::new(1)),
            "week" => new_dt.checked_add_days(Days::new(7)),
            "day" => new_dt.checked_add_days(Days::new(1)),
            "hour" => new_dt.checked_add_signed(Duration::hours(1)),
            "minute" => new_dt.checked_add_signed(Duration::minutes(1)),
            "second" => new_dt.checked_add_signed(Duration::seconds(1)),
            _ => return None,
        }
        .expect("Unable to add specified duration to new_dt");

        Some((dt, new_dt))
    }

    fn rotate_filter_and_date_range(
        &self,
        time_dimension_filter_var: &'static str,
        time_dimension_member_var: &'static str,
        time_dimension_op_var: &'static str,
        left_var: &'static str,
        right_var: &'static str,
        pull_up_member_var: &'static str,
        left_out_var: &'static str,
        right_out_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let time_dimension_filter_var = var!(time_dimension_filter_var);
        let time_dimension_member_var = var!(time_dimension_member_var);
        let time_dimension_op_var = var!(time_dimension_op_var);
        let left_var = var!(left_var);
        let right_var = var!(right_var);
        let pull_up_member_var = var!(pull_up_member_var);
        let left_out_var = var!(left_out_var);
        let right_out_var = var!(right_out_var);
        move |egraph, subst| {
            for time_dimension_op in
                var_iter!(egraph[subst[time_dimension_op_var]], FilterMemberOp).cloned()
            {
                fn time_dimension_op_score(time_dimension_op: &str) -> i32 {
                    match time_dimension_op {
                        "beforeDate" => -1,
                        "beforeOrOnDate" => -1,
                        "afterDate" => 1,
                        "afterOrOnDate" => 1,
                        _ => 0,
                    }
                }

                let op_score = time_dimension_op_score(&time_dimension_op);
                if op_score == 0 {
                    continue;
                }
                for time_dimension_member in
                    var_iter!(egraph[subst[time_dimension_member_var]], FilterMemberMember).cloned()
                {
                    if let Some(left_filter_operators) =
                        egraph[subst[left_var]].data.filter_operators.clone()
                    {
                        if let Some(right_filter_operators) =
                            egraph[subst[right_var]].data.filter_operators.clone()
                        {
                            let left_filter_operator_score = left_filter_operators
                                .iter()
                                .filter(|(member, _)| member == &time_dimension_member)
                                .map(|(_, op)| time_dimension_op_score(op))
                                .sum::<i32>();

                            let right_filter_operator_score = right_filter_operators
                                .iter()
                                .filter(|(member, _)| member == &time_dimension_member)
                                .map(|(_, op)| time_dimension_op_score(op))
                                .sum::<i32>();

                            if left_filter_operator_score == op_score * -1
                                && right_filter_operator_score != op_score
                            {
                                subst.insert(pull_up_member_var, subst[right_var]);

                                subst.insert(left_out_var, subst[left_var]);

                                subst.insert(right_out_var, subst[time_dimension_filter_var]);

                                return true;
                            }

                            if right_filter_operator_score == op_score * -1
                                && left_filter_operator_score != op_score
                            {
                                subst.insert(pull_up_member_var, subst[left_var]);

                                subst.insert(left_out_var, subst[time_dimension_filter_var]);

                                subst.insert(right_out_var, subst[right_var]);

                                return true;
                            }
                        }
                    }
                }
            }

            false
        }
    }
}

fn filter_simplify_push_down(node_type: impl Display) -> CubeRewrite {
    rewrite(
        &format!("filter-simplify-{}-push-down", node_type),
        filter_simplify_push_down_replacer(format!("({} ?left ?right)", node_type)),
        format!(
            "({} {} {})",
            node_type,
            filter_simplify_push_down_replacer("?left"),
            filter_simplify_push_down_replacer("?right")
        ),
    )
}

fn filter_simplify_pull_up(node_type: impl Display) -> CubeRewrite {
    rewrite(
        &format!("filter-simplify-{}-pull-up", node_type),
        format!(
            "({} {} {})",
            node_type,
            filter_simplify_pull_up_replacer("?left"),
            filter_simplify_pull_up_replacer("?right")
        ),
        filter_simplify_pull_up_replacer(format!("({} ?left ?right)", node_type)),
    )
}

fn filter_simplify_tail(node_type: impl Display) -> CubeRewrite {
    rewrite(
        &format!("filter-simplify-{}-empty-tail", node_type),
        filter_simplify_push_down_replacer(node_type.to_string()),
        filter_simplify_pull_up_replacer(node_type.to_string()),
    )
}

fn format_iso_timestamp(dt: NaiveDateTime) -> String {
    dt.format_with_items(
        [
            Item::Numeric(Year, Zero),
            Item::Literal("-"),
            Item::Numeric(Month, Zero),
            Item::Literal("-"),
            Item::Numeric(Day, Zero),
            Item::Literal("T"),
            Item::Numeric(Hour, Zero),
            Item::Literal(":"),
            Item::Numeric(Minute, Zero),
            Item::Literal(":"),
            Item::Numeric(Second, Zero),
            Item::Fixed(Fixed::Nanosecond3),
            // TODO remove when there're no more non rewrite tests
            Item::Literal("Z"),
        ]
        .iter(),
    )
    .to_string()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Decimal {
    raw_value: i128,
}

impl Decimal {
    pub fn new(raw_value: i128) -> Decimal {
        Decimal { raw_value }
    }

    pub fn to_string(&self, scale: usize) -> String {
        let big_decimal = BigDecimal::new(BigInt::from(self.raw_value), scale as i64);
        let mut res = big_decimal.to_string();
        if res.contains('.') {
            let mut truncate_len = res.len();
            for (i, c) in res.char_indices().rev() {
                if c == '0' {
                    truncate_len = i;
                } else if c == '.' {
                    truncate_len = i;
                    break;
                } else {
                    break;
                }
            }
            res.truncate(truncate_len);
        }
        res
    }

    pub fn format_string(raw_value: i128, scale: usize) -> String {
        Decimal::new(raw_value).to_string(scale)
    }
}
