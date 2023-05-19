use super::utils;
use crate::{
    compile::{
        engine::provider::CubeContext,
        rewrite::{
            analysis::{ConstantFolding, LogicalPlanAnalysis},
            between_expr, binary_expr, case_expr, case_expr_var_arg, cast_expr, change_user_member,
            column_expr, cube_scan, cube_scan_filters, cube_scan_filters_empty_tail,
            cube_scan_members, dimension_expr, expr_column_name, filter,
            filter_cast_unwrap_replacer, filter_member, filter_op, filter_op_filters,
            filter_op_filters_empty_tail, filter_replacer, fun_expr, fun_expr_var_arg, inlist_expr,
            is_not_null_expr, is_null_expr, like_expr, limit, list_expr, literal_bool,
            literal_expr, literal_int, literal_string, measure_expr, member_name_by_alias,
            negative_expr, not_expr, projection, rewrite,
            rewriter::RewriteRules,
            scalar_fun_expr_args, scalar_fun_expr_args_empty_tail, segment_member,
            time_dimension_date_range_replacer, time_dimension_expr, transforming_chain_rewrite,
            transforming_rewrite, udf_expr, udf_expr_var_arg, udf_fun_expr_args,
            udf_fun_expr_args_empty_tail, BetweenExprNegated, BinaryExprOp, ChangeUserMemberValue,
            ColumnExprColumn, CubeScanAliasToCube, CubeScanAliases, CubeScanLimit,
            FilterMemberMember, FilterMemberOp, FilterMemberValues, FilterReplacerAliasToCube,
            FilterReplacerAliases, InListExprNegated, LikeExprEscapeChar, LikeExprNegated,
            LimitFetch, LimitSkip, LiteralExprValue, LogicalPlanLanguage, SegmentMemberMember,
            TimeDimensionDateRange, TimeDimensionDateRangeReplacerDateRange,
            TimeDimensionDateRangeReplacerMember, TimeDimensionGranularity, TimeDimensionName,
        },
    },
    transport::{ext::V1CubeMetaExt, MemberType, MetaContext},
    var, var_iter,
};
use chrono::{
    format::{
        Fixed, Item,
        Numeric::{Day, Hour, Minute, Month, Second, Year},
        Pad::Zero,
    },
    Duration, NaiveDateTime,
};
use cubeclient::models::V1CubeMeta;
use datafusion::{
    arrow::array::{Date32Array, Date64Array, TimestampNanosecondArray},
    logical_plan::{Column, Expr, Operator},
    scalar::ScalarValue,
};
use egg::{EGraph, Rewrite, Subst, Var};
use std::{fmt::Display, ops::Index, sync::Arc};

pub struct FilterRules {
    cube_context: Arc<CubeContext>,
}

impl RewriteRules for FilterRules {
    fn rewrite_rules(&self) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
        vec![
            transforming_rewrite(
                "push-down-filter",
                filter(
                    "?expr",
                    cube_scan(
                        "?alias_to_cube",
                        "?members",
                        "?filters",
                        "?order",
                        "?limit",
                        "?offset",
                        "?aliases",
                        "?split",
                        "?can_pushdown_join",
                        "?wrapped",
                    ),
                ),
                cube_scan(
                    "?alias_to_cube",
                    "?members",
                    cube_scan_filters(
                        "?filters",
                        filter_replacer(
                            filter_cast_unwrap_replacer("?expr"),
                            "?filter_alias_to_cube",
                            "?members",
                            "?filter_aliases",
                        ),
                    ),
                    "?order",
                    "?limit",
                    "?offset",
                    "?aliases",
                    "?split",
                    "?can_pushdown_join",
                    "?wrapped",
                ),
                self.push_down_filter(
                    "?alias_to_cube",
                    "?expr",
                    "?filter_alias_to_cube",
                    "?aliases",
                    "?filter_aliases",
                ),
            ),
            // Transform Filter: Boolean(False)
            transforming_rewrite(
                "push-down-limit-filter",
                filter(
                    literal_expr("?literal"),
                    cube_scan(
                        "?source_table_name",
                        "?members",
                        "?filters",
                        "?order",
                        "?limit",
                        "?offset",
                        "?aliases",
                        "?split",
                        "?can_pushdown_join",
                        "?wrapped",
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
                        "?aliases",
                        "?split",
                        "?can_pushdown_join",
                        "?wrapped",
                    ),
                ),
                self.push_down_limit_filter(
                    "?literal",
                    "?new_limit",
                    "?new_limit_skip",
                    "?new_limit_fetch",
                ),
            ),
            // Transform Filter: Boolean(true)
            // It's safe to push down filter under projection, next filter-truncate-true will truncate it
            // TODO: Find a better solution how to drop filter node at all once
            rewrite(
                "push-down-filter-projection",
                filter(
                    literal_bool(true),
                    projection("?expr", "?input", "?alias", "?projection_split"),
                ),
                projection(
                    "?expr",
                    filter(literal_bool(true), "?input"),
                    "?alias",
                    "?projection_split",
                ),
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
                            "?aliases",
                            "?split",
                            "?can_pushdown_join",
                            "?wrapped",
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
                            "?aliases",
                            "?split",
                            "?can_pushdown_join",
                            "?wrapped",
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
                            "?aliases",
                            "?split",
                            "?can_pushdown_join",
                            "?wrapped",
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
                            "?aliases",
                            "?split",
                            "?can_pushdown_join",
                            "?wrapped",
                        ),
                        "?alias",
                        "?projection_split",
                    ),
                ),
            ),
            // Transform Filter: Boolean(True) same as TRUE = TRUE, which is useless
            rewrite(
                "filter-truncate-true",
                filter_replacer(
                    literal_bool(true),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                cube_scan_filters_empty_tail(),
            ),
            // We use this rule to transform: (?expr IN (?list..)) = TRUE and ((?expr IN (?list..)) = TRUE) = TRUE
            rewrite(
                "filter-truncate-in-list-true",
                filter_replacer(
                    binary_expr("?expr", "=", literal_bool(true)),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer("?expr", "?alias_to_cube", "?members", "?filter_aliases"),
            ),
            transforming_rewrite(
                "filter-replacer",
                filter_replacer(
                    binary_expr(column_expr("?column"), "?op", literal_expr("?literal")),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_filter(
                    "?column",
                    "?op",
                    "?literal",
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
                        fun_expr("Lower", vec![column_expr("?column")]),
                        "=",
                        literal_expr("?literal"),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                change_user_member("?user"),
                self.transform_change_user_eq("?column", "?literal", "?user"),
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
                self.transform_change_user_eq("?column", "?literal", "?user"),
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
                    inlist_expr("?expr", "?list", "?negated"),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    "?binary_expr",
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                self.transform_filter_in_to_equal("?expr", "?list", "?negated", "?binary_expr"),
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
            rewrite(
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
            ),
            // Unwrap lower for case-insensitive operators
            transforming_rewrite(
                "filter-replacer-lower-unwrap",
                filter_replacer(
                    binary_expr(fun_expr("Lower", vec!["?param"]), "?op", "?right"),
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
                    is_null_expr(fun_expr("Lower", vec!["?expr"])),
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
                    is_not_null_expr(fun_expr("Lower", vec!["?expr"])),
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
                "filter-replacer-lower-in-list-unwrap",
                filter_replacer(
                    not_expr(inlist_expr(
                        fun_expr("Lower", vec!["?expr"]),
                        "?list",
                        "?new_negated",
                    )),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    not_expr(inlist_expr("?expr", "?list", "?new_negated")),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
            ),
            // Unwrap upper for case-insensitive operators
            transforming_rewrite(
                "filter-replacer-upper-unwrap",
                filter_replacer(
                    binary_expr(fun_expr("Upper", vec!["?param"]), "?op", "?right"),
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
                    is_null_expr(fun_expr("Upper", vec!["?expr"])),
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
                    is_not_null_expr(fun_expr("Upper", vec!["?expr"])),
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
            // Lower(?column) = 'literal'
            // TODO: Migrate to equalsLower operator, when it will be available in Cube?
            rewrite(
                "filter-replacer-lower-equal-workaround",
                filter_replacer(
                    binary_expr(
                        fun_expr("Lower", vec![column_expr("?column")]),
                        "=",
                        literal_expr("?str"),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    binary_expr(
                        fun_expr(
                            "StartsWith",
                            vec![column_expr("?column"), literal_expr("?str")],
                        ),
                        "AND",
                        udf_expr(
                            "ends_with",
                            vec![column_expr("?column"), literal_expr("?str")],
                        ),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
            ),
            // Lower(?column) = 'literal'
            // TODO: Migrate to equalsLower operator, when it will be available in Cube?
            rewrite(
                "filter-replacer-lower-not-equal-workaround",
                filter_replacer(
                    binary_expr(
                        fun_expr("Lower", vec![column_expr("?column")]),
                        "!=",
                        literal_expr("?str"),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    binary_expr(
                        not_expr(fun_expr(
                            "StartsWith",
                            vec![column_expr("?column"), literal_expr("?str")],
                        )),
                        "AND",
                        not_expr(udf_expr(
                            "ends_with",
                            vec![column_expr("?column"), literal_expr("?str")],
                        )),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
            ),
            transforming_rewrite(
                "filter-replacer-date-trunc-equals",
                filter_replacer(
                    binary_expr(
                        fun_expr(
                            "DateTrunc",
                            vec![literal_expr("?granularity"), column_expr("?column")],
                        ),
                        "=",
                        fun_expr(
                            "DateTrunc",
                            vec![literal_expr("?granularity"), literal_expr("?date")],
                        ),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    binary_expr(
                        binary_expr(
                            column_expr("?column"),
                            ">=",
                            fun_expr(
                                "DateTrunc",
                                vec![literal_expr("?granularity"), literal_expr("?date")],
                            ),
                        ),
                        "AND",
                        binary_expr(
                            column_expr("?column"),
                            "<",
                            binary_expr(
                                fun_expr(
                                    "DateTrunc",
                                    vec![literal_expr("?granularity"), literal_expr("?date")],
                                ),
                                "+",
                                literal_expr("?interval"),
                            ),
                        ),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                self.transform_granularity_to_interval("?granularity", "?interval"),
            ),
            // TODO define zero
            rewrite(
                "filter-str-pos-to-like",
                filter_replacer(
                    binary_expr(
                        fun_expr(
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
                filter_replacer(
                    binary_expr(column_expr("?column"), "LIKE", literal_expr("?value")),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
            ),
            rewrite(
                "filter-str-lower-to-column",
                filter_replacer(
                    binary_expr(
                        fun_expr(
                            "Strpos",
                            vec![
                                fun_expr("Lower", vec![column_expr("?column")]),
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
                        fun_expr(
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
                        fun_expr(
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
                        fun_expr(
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
                        fun_expr(
                            "Left",
                            vec!["?expr".to_string(), literal_expr("?literal_length")],
                        ),
                        "=",
                        literal_expr("?literal"),
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
                        fun_expr(
                            "Right",
                            vec!["?expr".to_string(), literal_expr("?literal_length")],
                        ),
                        "=",
                        literal_expr("?literal"),
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
                    fun_expr(
                        "StartsWith",
                        vec![column_expr("?column"), literal_expr("?literal")],
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
                    not_expr(fun_expr(
                        "StartsWith",
                        vec![column_expr("?column"), literal_expr("?literal")],
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
                        vec![column_expr("?column"), literal_expr("?literal")],
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
                        vec![column_expr("?column"), literal_expr("?literal")],
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
                                    fun_expr(
                                        "Strpos",
                                        vec![
                                            fun_expr(
                                                "Substr",
                                                vec!["?expr".to_string(), literal_int(1)],
                                            ),
                                            literal_expr("?literal"),
                                        ],
                                    ),
                                    ">",
                                    literal_int(0),
                                ),
                                fun_expr(
                                    "Strpos",
                                    vec![
                                        fun_expr(
                                            "Substr",
                                            vec!["?expr".to_string(), literal_int(1)],
                                        ),
                                        literal_expr("?literal"),
                                    ],
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
                                literal_expr("?literal"),
                                fun_expr("Lower", vec![column_expr("?column")]),
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
                                literal_expr("?literal"),
                                fun_expr("Lower", vec![column_expr("?column")]),
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
                                literal_expr("?literal"),
                                fun_expr("Lower", vec![column_expr("?column")]),
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
                                literal_expr("?literal"),
                                fun_expr("Lower", vec![column_expr("?column")]),
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
                                fun_expr("Reverse", vec![literal_expr("?literal")]),
                                fun_expr(
                                    "Reverse",
                                    vec![fun_expr("Lower", vec![column_expr("?column")])],
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
                                fun_expr("Reverse", vec![literal_expr("?literal")]),
                                fun_expr(
                                    "Reverse",
                                    vec![fun_expr("Lower", vec![column_expr("?column")])],
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
                        fun_expr("Lower", vec![column_expr("?column")]),
                        binary_expr(
                            binary_expr(
                                literal_string("%"),
                                "||",
                                fun_expr(
                                    "Replace",
                                    vec![
                                        fun_expr(
                                            "Replace",
                                            vec![
                                                fun_expr(
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
                        fun_expr("Lower", vec![column_expr("?column")]),
                        binary_expr(
                            fun_expr(
                                "Replace",
                                vec![
                                    fun_expr(
                                        "Replace",
                                        vec![
                                            fun_expr(
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
                        fun_expr("Lower", vec![column_expr("?column")]),
                        binary_expr(
                            literal_string("%"),
                            "||",
                            fun_expr(
                                "Replace",
                                vec![
                                    fun_expr(
                                        "Replace",
                                        vec![
                                            fun_expr(
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
                        udf_expr(
                            "date_add",
                            vec!["?expr".to_string(), literal_expr("?interval")],
                        ),
                        "?op",
                        literal_expr("?date"),
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
                                udf_expr("date_to_timestamp", vec![literal_expr("?date")]),
                                negative_expr(literal_expr("?interval")),
                            ],
                        ),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
            ),
            rewrite(
                "filter-thoughtspot-lower-in-true-false",
                filter_replacer(
                    inlist_expr(
                        binary_expr(
                            binary_expr(
                                fun_expr("Lower", vec![column_expr("?column")]),
                                "=",
                                literal_expr("?left_literal"),
                            ),
                            "OR",
                            binary_expr(
                                fun_expr("Lower", vec![column_expr("?column")]),
                                "=",
                                literal_expr("?right_literal"),
                            ),
                        ),
                        list_expr(
                            "InListExprList",
                            vec![literal_bool(true), literal_bool(false)],
                        ),
                        "InListExprNegated:false",
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    binary_expr(
                        binary_expr(
                            binary_expr(
                                fun_expr("Lower", vec![column_expr("?column")]),
                                "=",
                                literal_expr("?left_literal"),
                            ),
                            "OR",
                            binary_expr(
                                fun_expr("Lower", vec![column_expr("?column")]),
                                "=",
                                literal_expr("?right_literal"),
                            ),
                        ),
                        "AND",
                        is_not_null_expr(column_expr("?column")),
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
                        fun_expr(
                            "Trunc",
                            vec![fun_expr(
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
                "filter-date-trunc-leeq",
                filter_replacer(
                    binary_expr(
                        fun_expr(
                            "DateTrunc",
                            vec![literal_expr("?granularity"), column_expr("?column")],
                        ),
                        "<=",
                        fun_expr(
                            "DateTrunc",
                            vec![literal_expr("?granularity"), literal_expr("?expr")],
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
                            fun_expr(
                                "DateTrunc",
                                vec![literal_expr("?granularity"), literal_expr("?expr")],
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
                "filter-date-trunc-sub-leeq",
                filter_replacer(
                    binary_expr(
                        binary_expr(
                            fun_expr(
                                "DateTrunc",
                                vec![
                                    literal_expr("?granularity"),
                                    binary_expr(
                                        column_expr("?column"),
                                        "+",
                                        literal_expr("?same_interval"),
                                    ),
                                ],
                            ),
                            "-",
                            literal_expr("?same_interval"),
                        ),
                        "<=",
                        binary_expr(
                            fun_expr(
                                "DateTrunc",
                                vec![literal_expr("?granularity"), literal_expr("?expr")],
                            ),
                            "-",
                            literal_expr("?same_interval"),
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
                                fun_expr(
                                    "DateTrunc",
                                    vec![literal_expr("?granularity"), literal_expr("?expr")],
                                ),
                                "-",
                                literal_expr("?same_interval"),
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
                        fun_expr(
                            "DateTrunc",
                            vec![literal_expr("?granularity"), column_expr("?column")],
                        ),
                        "?op",
                        literal_expr("?date"),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    binary_expr(
                        column_expr("?column"),
                        "?new_op",
                        fun_expr(
                            "DateTrunc",
                            vec![
                                literal_expr("?granularity"),
                                udf_expr(
                                    "date_sub",
                                    vec![
                                        udf_expr(
                                            "date_add",
                                            vec![
                                                literal_expr("?date"),
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
                "filter-date-trunc-eq-literal-date",
                filter_replacer(
                    binary_expr(
                        fun_expr(
                            "DateTrunc",
                            vec![literal_expr("?granularity"), column_expr("?column")],
                        ),
                        "=",
                        literal_expr("?date"),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                filter_replacer(
                    binary_expr(
                        binary_expr(
                            column_expr("?column"),
                            ">=",
                            fun_expr(
                                "DateTrunc",
                                vec![literal_expr("?granularity"), literal_expr("?date")],
                            ),
                        ),
                        "AND",
                        binary_expr(
                            column_expr("?column"),
                            "<",
                            fun_expr(
                                "DateTrunc",
                                vec![
                                    literal_expr("?granularity"),
                                    udf_expr(
                                        "date_add",
                                        vec![
                                            literal_expr("?date"),
                                            literal_expr("?date_add_interval"),
                                        ],
                                    ),
                                ],
                            ),
                        ),
                    ),
                    "?alias_to_cube",
                    "?members",
                    "?filter_aliases",
                ),
                self.transform_date_trunc_eq_literal_date(
                    "?granularity",
                    "?date",
                    "?date_add_interval",
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
                        fun_expr(
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
            rewrite(
                "plus-value-minus-value",
                binary_expr(
                    binary_expr("?expr", "+", literal_expr("?value")),
                    "-",
                    literal_expr("?value"),
                ),
                "?expr".to_string(),
            ),
            // Every expression should be handled by filter cast unwrap replacer otherwise other rules just won't work
            rewrite(
                "filter-cast-unwrap",
                filter_cast_unwrap_replacer(cast_expr("?expr", "?data_type")),
                filter_cast_unwrap_replacer("?expr"),
            ),
            rewrite(
                "filter-cast-unwrap-binary-push-down",
                filter_cast_unwrap_replacer(binary_expr("?left", "?op", "?right")),
                binary_expr(
                    filter_cast_unwrap_replacer("?left"),
                    "?op",
                    filter_cast_unwrap_replacer("?right"),
                ),
            ),
            rewrite(
                "filter-cast-unwrap-like-push-down",
                filter_cast_unwrap_replacer(like_expr(
                    "?like_type",
                    "?negated",
                    "?expr",
                    "?pattern",
                    "?escape_char",
                )),
                like_expr(
                    "?like_type",
                    "?negated",
                    filter_cast_unwrap_replacer("?expr"),
                    filter_cast_unwrap_replacer("?pattern"),
                    "?escape_char",
                ),
            ),
            rewrite(
                "filter-cast-unwrap-not-push-down",
                filter_cast_unwrap_replacer(not_expr("?expr")),
                not_expr(filter_cast_unwrap_replacer("?expr")),
            ),
            rewrite(
                "filter-cast-unwrap-inlist-push-down",
                filter_cast_unwrap_replacer(inlist_expr("?expr", "?list", "?negated")),
                // TODO unwrap list as well
                inlist_expr(filter_cast_unwrap_replacer("?expr"), "?list", "?negated"),
            ),
            rewrite(
                "filter-cast-unwrap-is-null-push-down",
                filter_cast_unwrap_replacer(is_null_expr("?expr")),
                is_null_expr(filter_cast_unwrap_replacer("?expr")),
            ),
            rewrite(
                "filter-cast-unwrap-is-not-null-push-down",
                filter_cast_unwrap_replacer(is_not_null_expr("?expr")),
                is_not_null_expr(filter_cast_unwrap_replacer("?expr")),
            ),
            rewrite(
                "filter-cast-unwrap-literal-push-down",
                filter_cast_unwrap_replacer(literal_expr("?literal")),
                literal_expr("?literal"),
            ),
            rewrite(
                "filter-cast-unwrap-column-push-down",
                filter_cast_unwrap_replacer(column_expr("?column")),
                column_expr("?column"),
            ),
            // scalar
            rewrite(
                "filter-cast-unwrap-scalar-fun-push-down",
                filter_cast_unwrap_replacer(fun_expr_var_arg("?fun", "?args")),
                fun_expr_var_arg("?fun", filter_cast_unwrap_replacer("?args")),
            ),
            rewrite(
                "filter-cast-unwrap-scalar-args-push-down",
                filter_cast_unwrap_replacer(scalar_fun_expr_args("?left", "?right")),
                scalar_fun_expr_args(
                    filter_cast_unwrap_replacer("?left"),
                    filter_cast_unwrap_replacer("?right"),
                ),
            ),
            rewrite(
                "filter-cast-unwrap-scalar-args-empty-tail-push-down",
                filter_cast_unwrap_replacer(scalar_fun_expr_args_empty_tail()),
                scalar_fun_expr_args_empty_tail(),
            ),
            // udf
            rewrite(
                "filter-cast-unwrap-udf-fun-push-down",
                filter_cast_unwrap_replacer(udf_expr_var_arg("?fun", "?args")),
                udf_expr_var_arg("?fun", filter_cast_unwrap_replacer("?args")),
            ),
            rewrite(
                "filter-cast-unwrap-udf-args-push-down",
                filter_cast_unwrap_replacer(udf_fun_expr_args("?left", "?right")),
                udf_fun_expr_args(
                    filter_cast_unwrap_replacer("?left"),
                    filter_cast_unwrap_replacer("?right"),
                ),
            ),
            rewrite(
                "filter-cast-unwrap-udf-args-empty-tail-push-down",
                filter_cast_unwrap_replacer(udf_fun_expr_args_empty_tail()),
                udf_fun_expr_args_empty_tail(),
            ),
            // case
            rewrite(
                "filter-cast-unwrap-case-push-down",
                filter_cast_unwrap_replacer(case_expr_var_arg("?expr", "?when_then", "?else")),
                case_expr_var_arg(
                    filter_cast_unwrap_replacer("?expr"),
                    filter_cast_unwrap_replacer("?when_then"),
                    filter_cast_unwrap_replacer("?else"),
                ),
            ),
            rewrite(
                "filter-cast-unwrap-between-push-down",
                filter_cast_unwrap_replacer(between_expr("?expr", "?negated", "?low", "?high")),
                between_expr(
                    "?expr",
                    "?negated",
                    filter_cast_unwrap_replacer("?low"),
                    filter_cast_unwrap_replacer("?high"),
                ),
            ),
            filter_unwrap_cast_push_down("CaseExprExpr"),
            filter_unwrap_cast_push_down_tail("CaseExprExpr"),
            filter_unwrap_cast_push_down("CaseExprWhenThenExpr"),
            filter_unwrap_cast_push_down_tail("CaseExprWhenThenExpr"),
            filter_unwrap_cast_push_down("CaseExprElseExpr"),
            filter_unwrap_cast_push_down_tail("CaseExprElseExpr"),
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
                        filter_member("?member", "FilterMemberOp:afterDate", "?date_range_start"),
                        filter_member("?member", "FilterMemberOp:beforeDate", "?date_range_end"),
                    ),
                    "FilterOpOp:and",
                ),
                filter_member("?member", "FilterMemberOp:inDateRange", "?date_range"),
                self.merge_date_range("?date_range_start", "?date_range_end", "?date_range"),
            ),
            rewrite(
                "filter-replacer-in-date-range-inverse",
                filter_op(
                    filter_op_filters(
                        filter_member("?member", "FilterMemberOp:beforeDate", "?date_range_end"),
                        filter_member("?member", "FilterMemberOp:afterDate", "?date_range_start"),
                    ),
                    "FilterOpOp:and",
                ),
                filter_op(
                    filter_op_filters(
                        filter_member("?member", "FilterMemberOp:afterDate", "?date_range_start"),
                        filter_member("?member", "FilterMemberOp:beforeDate", "?date_range_end"),
                    ),
                    "FilterOpOp:and",
                ),
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
            transforming_rewrite(
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
                    "?aliases",
                    "CubeScanSplit:false",
                    "?can_pushdown_join",
                    "?wrapped",
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
                    "?aliases_none",
                    "CubeScanSplit:false",
                    "?can_pushdown_join",
                    "?wrapped",
                ),
                self.transform_cube_scan_aliases_none("?aliases_none"),
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
        ]
    }
}

impl FilterRules {
    pub fn new(cube_context: Arc<CubeContext>) -> Self {
        Self { cube_context }
    }

    fn push_down_filter(
        &self,
        alias_to_cube_var: &'static str,
        exp_var: &'static str,
        filter_alias_to_cube_var: &'static str,
        cube_aliases_var: &'static str,
        filter_aliases_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let exp_var = var!(exp_var);
        let cube_aliases_var = var!(cube_aliases_var);
        let filter_aliases_var = var!(filter_aliases_var);
        let filter_alias_to_cube_var = var!(filter_alias_to_cube_var);
        move |egraph, subst| {
            for alias_to_cube in
                var_iter!(egraph[subst[alias_to_cube_var]], CubeScanAliasToCube).cloned()
            {
                for cube_aliases in
                    var_iter!(egraph[subst[cube_aliases_var]], CubeScanAliases).cloned()
                {
                    if cube_aliases.is_none() {
                        continue;
                    }

                    if let Some(_referenced_expr) =
                        &egraph.index(subst[exp_var]).data.referenced_expr
                    {
                        // TODO check referenced_expr
                        subst.insert(
                            filter_alias_to_cube_var,
                            egraph.add(LogicalPlanLanguage::FilterReplacerAliasToCube(
                                FilterReplacerAliasToCube(alias_to_cube),
                            )),
                        );

                        let filter_replacer_aliases =
                            egraph.add(LogicalPlanLanguage::FilterReplacerAliases(
                                FilterReplacerAliases(cube_aliases.unwrap_or(vec![])),
                            ));
                        subst.insert(filter_aliases_var, filter_replacer_aliases);

                        return true;
                    }
                }
            }

            false
        }
    }

    fn push_down_limit_filter(
        &self,
        literal_var: &'static str,
        new_limit_var: &'static str,
        new_limit_skip_var: &'static str,
        new_limit_fetch_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let literal_var = var!(literal_var);
        let new_limit_var = var!(new_limit_var);
        let new_limit_skip_var = var!(new_limit_skip_var);
        let new_limit_fetch_var = var!(new_limit_fetch_var);
        move |egraph, subst| {
            for literal_value in var_iter!(egraph[subst[literal_var]], LiteralExprValue) {
                if let ScalarValue::Boolean(Some(false)) = literal_value {
                    subst.insert(
                        new_limit_var,
                        egraph.add(LogicalPlanLanguage::CubeScanLimit(CubeScanLimit(Some(1)))),
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

    fn push_down_limit_projection(
        &self,
        input_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
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

    fn unwrap_lower_or_upper(
        &self,
        op_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
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
        literal_var: &'static str,
        alias_to_cube_var: &'static str,
        members_var: &'static str,
        filter_member_var: &'static str,
        filter_op_var: &'static str,
        filter_values_var: &'static str,
        filter_aliases_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let column_var = column_var.parse().unwrap();
        let op_var = op_var.parse().unwrap();
        let literal_var = literal_var.parse().unwrap();
        let alias_to_cube_var = var!(alias_to_cube_var);
        let members_var = var!(members_var);
        let filter_member_var = filter_member_var.parse().unwrap();
        let filter_op_var = filter_op_var.parse().unwrap();
        let filter_values_var = filter_values_var.parse().unwrap();
        let filter_aliases_var = filter_aliases_var.parse().unwrap();
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for expr_op in var_iter!(egraph[subst[op_var]], BinaryExprOp) {
                for literal in var_iter!(egraph[subst[literal_var]], LiteralExprValue) {
                    for aliases in
                        var_iter!(egraph[subst[filter_aliases_var]], FilterReplacerAliases)
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
                                            Operator::LtEq => "beforeDate",
                                            Operator::Gt => "afterDate",
                                            Operator::GtEq => "afterDate",
                                            _ => op,
                                        },
                                    };

                                    let value = match literal {
                                        ScalarValue::Utf8(Some(value)) => {
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
                                            } else {
                                                value.to_string()
                                            }
                                        }
                                        ScalarValue::Int64(Some(value)) => value.to_string(),
                                        ScalarValue::Boolean(Some(value)) => value.to_string(),
                                        ScalarValue::Float64(Some(value)) => value.to_string(),
                                        ScalarValue::TimestampNanosecond(_, _)
                                        | ScalarValue::Date32(_)
                                        | ScalarValue::Date64(_) => {
                                            if let Some(timestamp) =
                                                Self::scalar_to_native_datetime(&literal)
                                            {
                                                let minus_one = format_iso_timestamp(
                                                    timestamp
                                                        .checked_sub_signed(Duration::milliseconds(
                                                            1,
                                                        ))
                                                        .unwrap(),
                                                );
                                                let value = format_iso_timestamp(timestamp);

                                                match expr_op {
                                                    Operator::Lt => minus_one,
                                                    Operator::LtEq => minus_one,
                                                    Operator::Gt => value,
                                                    Operator::GtEq => value,
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
                                        x => panic!("Unsupported filter scalar: {:?}", x),
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
                                            FilterMemberValues(vec![value.to_string()]),
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
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let column_var = var!(column_var);
        let literal_var = var!(literal_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let members_var = var!(members_var);
        let filter_member_var = var!(filter_member_var);
        let filter_op_var = var!(filter_op_var);
        let filter_values_var = var!(filter_values_var);
        let filter_aliases_var = var!(filter_aliases_var);
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for literal in var_iter!(egraph[subst[literal_var]], LiteralExprValue) {
                for aliases in var_iter!(egraph[subst[filter_aliases_var]], FilterReplacerAliases) {
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
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
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
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let column_left_var = var!(column_left_var);
        let column_right_var = var!(column_right_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let members_var = var!(members_var);
        let filter_aliases_var = var!(filter_aliases_var);
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for aliases in var_iter!(egraph[subst[filter_aliases_var]], FilterReplacerAliases) {
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
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let column_var = var!(column_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let members_var = var!(members_var);
        let filter_aliases_var = var!(filter_aliases_var);
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for aliases in var_iter!(egraph[subst[filter_aliases_var]], FilterReplacerAliases) {
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
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
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
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let year_var = var!(year_var);
        let column_var = var!(column_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let members_var = var!(members_var);
        let member_var = var!(member_var);
        let values_var = var!(values_var);
        let filter_aliases_var = var!(filter_aliases_var);
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for year in var_iter!(egraph[subst[year_var]], LiteralExprValue) {
                for aliases in var_iter!(egraph[subst[filter_aliases_var]], FilterReplacerAliases) {
                    if let ScalarValue::Int64(Some(year)) = year {
                        let year = year.clone();
                        if year < 1000 || year > 9999 {
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
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let column_var = column_var.parse().unwrap();
        let op_var = op_var.parse().unwrap();
        let literal_var = literal_var.parse().unwrap();
        let alias_to_cube_var = var!(alias_to_cube_var);
        let members_var = var!(members_var);
        let segment_member_var = segment_member_var.parse().unwrap();
        let filter_aliases_var = filter_aliases_var.parse().unwrap();
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for expr_op in var_iter!(egraph[subst[op_var]], BinaryExprOp) {
                for literal in var_iter!(egraph[subst[literal_var]], LiteralExprValue) {
                    for aliases in
                        var_iter!(egraph[subst[filter_aliases_var]], FilterReplacerAliases)
                    {
                        if expr_op == &Operator::Eq {
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
        change_user_member_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let column_var = column_var.parse().unwrap();
        let literal_var = literal_var.parse().unwrap();
        let change_user_member_var = change_user_member_var.parse().unwrap();

        move |egraph, subst| {
            for literal in var_iter!(egraph[subst[literal_var]], LiteralExprValue) {
                if let ScalarValue::Utf8(Some(change_user)) = literal {
                    let specified_user = change_user.clone();

                    for column in var_iter!(egraph[subst[column_var]], ColumnExprColumn).cloned() {
                        if column.name.eq_ignore_ascii_case("__user") {
                            subst.insert(
                                change_user_member_var,
                                egraph.add(LogicalPlanLanguage::ChangeUserMemberValue(
                                    ChangeUserMemberValue(specified_user),
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

    // Transform ?expr IN (?literal) to ?expr = ?literal
    fn transform_filter_in_to_equal(
        &self,
        expr_val: &'static str,
        list_var: &'static str,
        negated_var: &'static str,
        return_binary_expr_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let expr_val = var!(expr_val);
        let list_var = var!(list_var);
        let negated_var = var!(negated_var);
        let return_binary_expr_var = var!(return_binary_expr_var);

        move |egraph, subst| {
            let expr_id = subst[expr_val];
            let (list, scalar) = match &egraph[subst[list_var]].data.constant_in_list {
                Some(list) if list.len() > 0 => (list.clone(), list[0].clone()),
                _ => return false,
            };

            for negated in var_iter!(egraph[subst[negated_var]], InListExprNegated) {
                let operator = if *negated {
                    Operator::NotEq
                } else {
                    Operator::Eq
                };
                let operator =
                    egraph.add(LogicalPlanLanguage::BinaryExprOp(BinaryExprOp(operator)));

                let literal_expr = egraph.add(LogicalPlanLanguage::LiteralExprValue(
                    LiteralExprValue(scalar),
                ));
                let literal_expr = egraph.add(LogicalPlanLanguage::LiteralExpr([literal_expr]));

                let mut return_binary_expr = egraph.add(LogicalPlanLanguage::BinaryExpr([
                    expr_id,
                    operator,
                    literal_expr,
                ]));

                for scalar in list.into_iter().skip(1) {
                    let literal_expr = egraph.add(LogicalPlanLanguage::LiteralExprValue(
                        LiteralExprValue(scalar),
                    ));
                    let literal_expr = egraph.add(LogicalPlanLanguage::LiteralExpr([literal_expr]));

                    let right_binary_expr = egraph.add(LogicalPlanLanguage::BinaryExpr([
                        expr_id,
                        operator,
                        literal_expr,
                    ]));

                    let or = egraph.add(LogicalPlanLanguage::BinaryExprOp(BinaryExprOp(
                        Operator::Or,
                    )));

                    return_binary_expr = egraph.add(LogicalPlanLanguage::BinaryExpr([
                        return_binary_expr,
                        or,
                        right_binary_expr,
                    ]));
                }

                subst.insert(return_binary_expr_var, return_binary_expr);

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
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let column_var = var!(column_var);
        let list_var = var!(list_var);
        let negated_var = var!(negated_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let members_var = var!(members_var);
        let filter_member_var = var!(filter_member_var);
        let filter_op_var = var!(filter_op_var);
        let filter_values_var = var!(filter_values_var);
        let filter_aliases_var = var!(filter_aliases_var);
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for aliases in var_iter!(egraph[subst[filter_aliases_var]], FilterReplacerAliases) {
                if let Some(list) = &egraph[subst[list_var]].data.constant_in_list {
                    let values = list
                        .into_iter()
                        .map(|literal| FilterRules::scalar_to_value(literal))
                        .collect::<Vec<_>>();

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

    fn scalar_to_value(literal: &ScalarValue) -> String {
        match literal {
            ScalarValue::Utf8(Some(value)) => value.to_string(),
            ScalarValue::Int64(Some(value)) => value.to_string(),
            ScalarValue::Boolean(Some(value)) => value.to_string(),
            ScalarValue::Float64(Some(value)) => value.to_string(),
            ScalarValue::TimestampNanosecond(_, _)
            | ScalarValue::Date32(_)
            | ScalarValue::Date64(_) => {
                if let Some(timestamp) = Self::scalar_to_native_datetime(literal) {
                    return format_iso_timestamp(timestamp);
                }

                panic!("Unsupported filter scalar: {:?}", literal);
            }
            x => panic!("Unsupported filter scalar: {:?}", x),
        }
    }

    fn scalar_to_native_datetime(literal: &ScalarValue) -> Option<NaiveDateTime> {
        match literal {
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
                    panic!("Unexpected array type: {:?}", array.data_type())
                };

                timestamp
            }
            _ => panic!("Unsupported filter scalar: {:?}", literal),
        }
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
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let column_var = var!(column_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let members_var = var!(members_var);
        let filter_member_var = var!(filter_member_var);
        let filter_op_var = var!(filter_op_var);
        let filter_values_var = var!(filter_values_var);
        let filter_aliases_var = var!(filter_aliases_var);
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for aliases in var_iter!(egraph[subst[filter_aliases_var]], FilterReplacerAliases) {
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
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
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
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
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

    fn filter_member_name(
        egraph: &EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
        subst: &Subst,
        meta_context: &Arc<MetaContext>,
        alias_to_cube_var: Var,
        column_var: Var,
        members_var: Var,
        aliases: &Vec<(String, String)>,
    ) -> Option<(String, V1CubeMeta)> {
        for alias_to_cube in var_iter!(egraph[subst[alias_to_cube_var]], FilterReplacerAliasToCube)
        {
            for column in var_iter!(egraph[subst[column_var]], ColumnExprColumn).cloned() {
                let alias_name = expr_column_name(Expr::Column(column.clone()), &None);

                let member_name = aliases
                    .iter()
                    .find(|(a, _)| a == &alias_name)
                    .map(|(_, name)| name.to_string());
                let member_name = if member_name.is_some() {
                    member_name
                } else {
                    // TODO: aliases are not enough?
                    member_name_by_alias(egraph, subst[members_var], &alias_name)
                };

                if let Some(member_name) = member_name {
                    if let Some(cube) =
                        meta_context.find_cube_with_name(&member_name.split(".").next().unwrap())
                    {
                        return Some((member_name, cube));
                    }
                } else if let Some((_, cube)) =
                    meta_context.find_cube_by_column(alias_to_cube, &column)
                {
                    if let Some(original_name) = Self::original_member_name(&cube, &column.name) {
                        return Some((original_name, cube));
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
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
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
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for aliases in var_iter!(egraph[subst[filter_aliases_var]], FilterReplacerAliases) {
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
                                    let values = vec![
                                        FilterRules::scalar_to_value(&low),
                                        FilterRules::scalar_to_value(&high),
                                    ];

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
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let column_var = var!(column_var);
        let negated_var = var!(negated_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let members_var = var!(members_var);
        let filter_aliases_var = var!(filter_aliases_var);
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for aliases in var_iter!(egraph[subst[filter_aliases_var]], FilterReplacerAliases) {
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
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
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

    fn transform_binary_expr_date_trunc_column_with_literal(
        &self,
        granularity_var: &'static str,
        op_var: &'static str,
        new_op_var: &'static str,
        date_add_interval_var: &'static str,
        date_sub_interval_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
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
                                    utils::granularity_str_to_interval("second")
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

    fn transform_date_trunc_eq_literal_date(
        &self,
        granularity_var: &'static str,
        date_var: &'static str,
        date_add_interval_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let granularity_var = var!(granularity_var);
        let date_var = var!(date_var);
        let date_add_interval_var = var!(date_add_interval_var);
        move |egraph, subst| {
            for granularity in var_iter!(egraph[subst[granularity_var]], LiteralExprValue) {
                if let ScalarValue::Utf8(Some(granularity)) = granularity {
                    if let Some(date_add_interval) =
                        utils::granularity_str_to_interval(&granularity)
                    {
                        for date in var_iter!(egraph[subst[date_var]], LiteralExprValue) {
                            if let ScalarValue::TimestampNanosecond(Some(date), None) = date {
                                if let Some(true) =
                                    utils::is_literal_date_trunced(*date, &granularity)
                                {
                                    subst.insert(
                                        date_add_interval_var,
                                        egraph.add(LogicalPlanLanguage::LiteralExprValue(
                                            LiteralExprValue(date_add_interval),
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

    fn is_empty_filter_ops_filters(
        &self,
        filter_ops_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let filter_ops_var = var!(filter_ops_var);
        move |egraph, subst| {
            if let Some(true) = egraph[subst[filter_ops_var]].data.is_empty_list.clone() {
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
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let date_range_start_var = date_range_start_var.parse().unwrap();
        let date_range_end_var = date_range_end_var.parse().unwrap();
        let date_range_var = date_range_var.parse().unwrap();
        move |egraph, subst| {
            for date_range_start in
                var_iter!(egraph[subst[date_range_start_var]], FilterMemberValues)
            {
                for date_range_end in
                    var_iter!(egraph[subst[date_range_end_var]], FilterMemberValues)
                {
                    let mut result = Vec::new();
                    result.extend(date_range_start.clone().into_iter());
                    result.extend(date_range_end.clone().into_iter());
                    subst.insert(
                        date_range_var,
                        egraph.add(LogicalPlanLanguage::FilterMemberValues(FilterMemberValues(
                            result,
                        ))),
                    );
                    return true;
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
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
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
                if let Some(member_name_to_expr) =
                    &egraph.index(subst[members_var]).data.member_name_to_expr
                {
                    if member_name_to_expr
                        .iter()
                        .all(|(m, _)| m.as_ref() != Some(&member))
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
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let members_var = var!(members_var);
        let time_dimension_member_var = var!(time_dimension_member_var);
        move |egraph, subst| {
            for member in var_iter!(
                egraph[subst[time_dimension_member_var]],
                TimeDimensionDateRangeReplacerMember
            ) {
                if let Some(member_name_to_expr) =
                    &egraph.index(subst[members_var]).data.member_name_to_expr
                {
                    if member_name_to_expr
                        .iter()
                        .any(|(m, _)| m.as_ref() == Some(member))
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
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
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
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
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
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
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
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for escape_char in var_iter!(egraph[subst[escape_char_var]], LikeExprEscapeChar) {
                if let Some('!') = escape_char {
                    for literal in var_iter!(egraph[subst[literal_var]], LiteralExprValue) {
                        let literal_value = match &literal {
                            ScalarValue::Utf8(Some(literal_value)) => literal_value.to_string(),
                            _ => continue,
                        };

                        for aliases in
                            var_iter!(egraph[subst[filter_aliases_var]], FilterReplacerAliases)
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

    fn transform_cube_scan_aliases_none(
        &self,
        aliases_none_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let aliases_none_var = var!(aliases_none_var);
        move |egraph, subst| {
            subst.insert(
                aliases_none_var,
                egraph.add(LogicalPlanLanguage::CubeScanAliases(CubeScanAliases(None))),
            );
            true
        }
    }
}

fn filter_unwrap_cast_push_down(
    node_type: impl Display,
) -> Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis> {
    rewrite(
        &format!("filter-cast-unwrap-{}-push-down", node_type),
        filter_cast_unwrap_replacer(format!("({} ?left ?right)", node_type)),
        format!(
            "({} {} {})",
            node_type,
            filter_cast_unwrap_replacer("?left"),
            filter_cast_unwrap_replacer("?right")
        ),
    )
}

fn filter_unwrap_cast_push_down_tail(
    node_type: impl Display,
) -> Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis> {
    rewrite(
        &format!("filter-cast-unwrap-{}-empty-tail-push-down", node_type),
        filter_cast_unwrap_replacer(node_type.to_string()),
        node_type.to_string(),
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
