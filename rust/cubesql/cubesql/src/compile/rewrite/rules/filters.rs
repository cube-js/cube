use crate::{
    compile::{
        engine::provider::CubeContext,
        rewrite::{
            analysis::{ConstantFolding, LogicalPlanAnalysis},
            between_expr, binary_expr, case_expr, case_expr_var_arg, cast_expr, change_user_member,
            column_expr, cube_scan, cube_scan_filters, cube_scan_members, dimension_expr,
            expr_column_name, filter, filter_cast_unwrap_replacer, filter_member, filter_op,
            filter_op_filters, filter_replacer, fun_expr, fun_expr_var_arg, inlist_expr,
            is_not_null_expr, is_null_expr, limit, literal_expr, literal_string, measure_expr,
            member_name_by_alias, not_expr, projection, rewrite,
            rewriter::RewriteRules,
            scalar_fun_expr_args, scalar_fun_expr_args_empty_tail, segment_member,
            time_dimension_date_range_replacer, time_dimension_expr, transforming_rewrite,
            BetweenExprNegated, BinaryExprOp, ChangeUserMemberValue, ColumnExprColumn,
            CubeScanLimit, CubeScanTableName, FilterMemberMember, FilterMemberOp,
            FilterMemberValues, FilterReplacerCube, FilterReplacerTableName, InListExprNegated,
            LimitN, LiteralExprValue, LogicalPlanLanguage, SegmentMemberMember,
            TableScanSourceTableName, TimeDimensionDateRange,
            TimeDimensionDateRangeReplacerDateRange, TimeDimensionDateRangeReplacerMember,
            TimeDimensionGranularity, TimeDimensionName,
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
                        "?source_table_name",
                        "?members",
                        "?filters",
                        "?order",
                        "?limit",
                        "?offset",
                        "?aliases",
                        "?table_name",
                        "?split",
                    ),
                ),
                cube_scan(
                    "?source_table_name",
                    "?members",
                    cube_scan_filters(
                        "?filters",
                        filter_replacer(
                            filter_cast_unwrap_replacer("?expr"),
                            "?cube",
                            "?members",
                            "?filter_table_name",
                        ),
                    ),
                    "?order",
                    "?limit",
                    "?offset",
                    "?aliases",
                    "?table_name",
                    "?split",
                ),
                self.push_down_filter(
                    "?source_table_name",
                    "?table_name",
                    "?expr",
                    "?cube",
                    "?filter_table_name",
                ),
            ),
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
                        "?table_name",
                        "?split",
                    ),
                ),
                limit(
                    "?new_limit_n",
                    cube_scan(
                        "?source_table_name",
                        "?members",
                        "?filters",
                        "?order",
                        "?new_limit",
                        "?offset",
                        "?aliases",
                        "?table_name",
                        "?split",
                    ),
                ),
                self.push_down_limit_filter("?literal", "?new_limit", "?new_limit_n"),
            ),
            rewrite(
                "swap-limit-filter",
                filter(
                    "?filter",
                    limit(
                        "LimitN:0",
                        cube_scan(
                            "?source_table_name",
                            "?members",
                            "?filters",
                            "?order",
                            "?limit",
                            "?offset",
                            "?aliases",
                            "?table_name",
                            "?split",
                        ),
                    ),
                ),
                limit(
                    "LimitN:0",
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
                            "?table_name",
                            "?split",
                        ),
                    ),
                ),
            ),
            rewrite(
                "swap-limit-projection",
                projection(
                    "?filter",
                    limit(
                        "LimitN:0",
                        cube_scan(
                            "?source_table_name",
                            "?members",
                            "?filters",
                            "?order",
                            "?limit",
                            "?offset",
                            "?aliases",
                            "?table_name",
                            "?split",
                        ),
                    ),
                    "?alias",
                ),
                limit(
                    "LimitN:0",
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
                            "?table_name",
                            "?split",
                        ),
                        "?alias",
                    ),
                ),
            ),
            transforming_rewrite(
                "filter-replacer",
                filter_replacer(
                    binary_expr(column_expr("?column"), "?op", literal_expr("?literal")),
                    "?cube",
                    "?members",
                    "?table_name",
                ),
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_filter(
                    "?column",
                    "?op",
                    "?literal",
                    "?cube",
                    "?members",
                    "?table_name",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                ),
            ),
            transforming_rewrite(
                "segment-replacer",
                filter_replacer(
                    binary_expr(column_expr("?column"), "?op", literal_expr("?literal")),
                    "?cube",
                    "?members",
                    "?table_name",
                ),
                segment_member("?segment"),
                self.transform_segment(
                    "?column",
                    "?op",
                    "?literal",
                    "?cube",
                    "?members",
                    "?table_name",
                    "?segment",
                ),
            ),
            transforming_rewrite(
                "change-user-replacer",
                filter_replacer(
                    binary_expr(column_expr("?column"), "?op", literal_expr("?literal")),
                    "?cube",
                    "?members",
                    "?table_name",
                ),
                change_user_member("?user"),
                self.transform_change_user("?column", "?op", "?literal", "?user"),
            ),
            rewrite(
                "filter-in-place-filter-to-true-filter",
                filter_replacer(column_expr("?column"), "?cube", "?members", "?table_name"),
                filter_replacer(
                    binary_expr(column_expr("?column"), "=", literal_string("true")),
                    "?cube",
                    "?members",
                    "?table_name",
                ),
            ),
            rewrite(
                "filter-in-place-filter-to-false-filter",
                filter_replacer(
                    not_expr(column_expr("?column")),
                    "?cube",
                    "?members",
                    "?table_name",
                ),
                filter_replacer(
                    binary_expr(column_expr("?column"), "=", literal_string("false")),
                    "?cube",
                    "?members",
                    "?table_name",
                ),
            ),
            transforming_rewrite(
                "filter-replacer-in-filter",
                filter_replacer(
                    inlist_expr(column_expr("?column"), "?list", "?negated"),
                    "?cube",
                    "?members",
                    "?table_name",
                ),
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_in_filter(
                    "?column",
                    "?list",
                    "?negated",
                    "?cube",
                    "?members",
                    "?table_name",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                ),
            ),
            transforming_rewrite(
                "filter-replacer-is-null",
                filter_replacer(
                    is_null_expr(column_expr("?column")),
                    "?cube",
                    "?members",
                    "?table_name",
                ),
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_is_null(
                    "?column",
                    "?cube",
                    "?members",
                    "?table_name",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                    true,
                ),
            ),
            transforming_rewrite(
                "filter-replacer-is-not-null",
                filter_replacer(
                    is_not_null_expr(column_expr("?column")),
                    "?cube",
                    "?members",
                    "?table_name",
                ),
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_is_null(
                    "?column",
                    "?cube",
                    "?members",
                    "?table_name",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                    false,
                ),
            ),
            rewrite(
                "filter-replacer-equals-negation",
                filter_replacer(
                    not_expr(binary_expr(
                        column_expr("?column"),
                        "=",
                        literal_expr("?literal"),
                    )),
                    "?cube",
                    "?members",
                    "?table_name",
                ),
                filter_replacer(
                    binary_expr(column_expr("?column"), "!=", literal_expr("?literal")),
                    "?cube",
                    "?members",
                    "?table_name",
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
                    "?cube",
                    "?members",
                    "?table_name",
                ),
                filter_replacer(
                    binary_expr(column_expr("?column"), "=", literal_expr("?literal")),
                    "?cube",
                    "?members",
                    "?table_name",
                ),
            ),
            rewrite(
                "filter-replacer-is-null-negation",
                filter_replacer(
                    not_expr(is_null_expr("?expr")),
                    "?cube",
                    "?members",
                    "?table_name",
                ),
                filter_replacer(
                    is_not_null_expr("?expr"),
                    "?cube",
                    "?members",
                    "?table_name",
                ),
            ),
            rewrite(
                "filter-replacer-is-not-null-negation",
                filter_replacer(
                    not_expr(is_not_null_expr("?expr")),
                    "?cube",
                    "?members",
                    "?table_name",
                ),
                filter_replacer(is_null_expr("?expr"), "?cube", "?members", "?table_name"),
            ),
            rewrite(
                "filter-replacer-double-negation",
                filter_replacer(
                    not_expr(not_expr("?expr")),
                    "?cube",
                    "?members",
                    "?table_name",
                ),
                filter_replacer("?expr", "?cube", "?members", "?table_name"),
            ),
            transforming_rewrite(
                "filter-replacer-between-dates",
                filter_replacer(
                    between_expr(column_expr("?column"), "?negated", "?low", "?high"),
                    "?cube",
                    "?members",
                    "?table_name",
                ),
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_between_dates(
                    "?column",
                    "?negated",
                    "?low",
                    "?high",
                    "?cube",
                    "?members",
                    "?table_name",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                ),
            ),
            transforming_rewrite(
                "filter-replacer-between-numbers",
                filter_replacer(
                    between_expr(column_expr("?column"), "?negated", "?low", "?high"),
                    "?cube",
                    "?members",
                    "?table_name",
                ),
                filter_replacer(
                    binary_expr(
                        binary_expr(column_expr("?column"), ">=", "?low"),
                        "AND",
                        binary_expr(column_expr("?column"), "<=", "?high"),
                    ),
                    "?cube",
                    "?members",
                    "?table_name",
                ),
                self.transform_between_numbers(
                    "?column",
                    "?negated",
                    "?cube",
                    "?members",
                    "?table_name",
                    false,
                ),
            ),
            transforming_rewrite(
                "filter-replacer-not-between-numbers",
                filter_replacer(
                    between_expr(column_expr("?column"), "?negated", "?low", "?high"),
                    "?cube",
                    "?members",
                    "?table_name",
                ),
                filter_replacer(
                    binary_expr(
                        binary_expr(column_expr("?column"), "<", "?low"),
                        "OR",
                        binary_expr(column_expr("?column"), ">", "?high"),
                    ),
                    "?cube",
                    "?members",
                    "?table_name",
                ),
                self.transform_between_numbers(
                    "?column",
                    "?negated",
                    "?cube",
                    "?members",
                    "?table_name",
                    true,
                ),
            ),
            rewrite(
                "filter-replacer-and",
                filter_replacer(
                    binary_expr("?left", "AND", "?right"),
                    "?cube",
                    "?members",
                    "?table_name",
                ),
                filter_op(
                    filter_op_filters(
                        filter_replacer("?left", "?cube", "?members", "?table_name"),
                        filter_replacer("?right", "?cube", "?members", "?table_name"),
                    ),
                    "and",
                ),
            ),
            rewrite(
                "filter-replacer-or",
                filter_replacer(
                    binary_expr("?left", "OR", "?right"),
                    "?cube",
                    "?members",
                    "?table_name",
                ),
                filter_op(
                    filter_op_filters(
                        filter_replacer("?left", "?cube", "?members", "?table_name"),
                        filter_replacer("?right", "?cube", "?members", "?table_name"),
                    ),
                    "or",
                ),
            ),
            rewrite(
                "filter-replacer-lower-str",
                filter_replacer(
                    binary_expr(fun_expr("Lower", vec!["?lower_param"]), "?op", "?right"),
                    "?cube",
                    "?members",
                    "?table_name",
                ),
                filter_replacer(
                    binary_expr("?lower_param", "?op", "?right"),
                    "?cube",
                    "?members",
                    "?table_name",
                ),
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
                    "?cube",
                    "?members",
                    "?table_name",
                ),
                filter_replacer(
                    binary_expr(column_expr("?column"), "LIKE", literal_expr("?value")),
                    "?cube",
                    "?members",
                    "?table_name",
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
                    "?cube",
                    "?members",
                    "?table_name",
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
                    "?cube",
                    "?members",
                    "?table_name",
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
                                    vec![(
                                        is_not_null_expr(column_expr("?column")),
                                        column_expr("?column"),
                                    )],
                                    literal_string(""),
                                ),
                                literal_expr("?value"),
                            ],
                        ),
                        ">",
                        literal_expr("?zero"),
                    ),
                    "?cube",
                    "?members",
                    "?table_name",
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
                    "?cube",
                    "?members",
                    "?table_name",
                ),
            ),
            rewrite(
                "between-move-interval-beyond-equal-sign",
                between_expr(
                    binary_expr(column_expr("?column"), "+", "?interval"),
                    "?negated",
                    "?low",
                    "?high",
                ),
                between_expr(
                    column_expr("?column"),
                    "?negated",
                    binary_expr("?low", "-", "?interval"),
                    binary_expr("?high", "-", "?interval"),
                ),
            ),
            rewrite(
                "not-expt-like-to-expr-not-like",
                not_expr(binary_expr("?left", "LIKE", "?right")),
                binary_expr("?left", "NOT_LIKE", "?right"),
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
                filter_cast_unwrap_replacer(between_expr(
                    column_expr("?column"),
                    "?negated",
                    "?low",
                    "?high",
                )),
                between_expr(
                    column_expr("?column"),
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
                    filter_op(filter_op_filters("?left", "?right"), "and"),
                    "?tail",
                ),
                cube_scan_filters(cube_scan_filters("?left", "?right"), "?tail"),
            ),
            rewrite(
                "filter-flatten-upper-and-right",
                cube_scan_filters(
                    "?tail",
                    filter_op(filter_op_filters("?left", "?right"), "and"),
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
            filter_flatten_rewrite_left("or"),
            filter_flatten_rewrite_right("or"),
            filter_flatten_rewrite_left("and"),
            filter_flatten_rewrite_right("and"),
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
                    "and",
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
                    "and",
                ),
                filter_op(
                    filter_op_filters(
                        filter_member("?member", "FilterMemberOp:afterDate", "?date_range_start"),
                        filter_member("?member", "FilterMemberOp:beforeDate", "?date_range_end"),
                    ),
                    "and",
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
                    "?aliases",
                    "?table_name",
                    "CubeScanSplit:false",
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
                    "?aliases",
                    "?table_name",
                    "CubeScanSplit:false",
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
        ]
    }
}

impl FilterRules {
    pub fn new(cube_context: Arc<CubeContext>) -> Self {
        Self { cube_context }
    }

    fn push_down_filter(
        &self,
        source_table_name_var: &'static str,
        table_name_var: &'static str,
        exp_var: &'static str,
        cube_var: &'static str,
        filter_table_name_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let source_table_name_var = var!(source_table_name_var);
        let table_name_var = var!(table_name_var);
        let exp_var = var!(exp_var);
        let cube_var = var!(cube_var);
        let filter_table_name_var = var!(filter_table_name_var);
        move |egraph, subst| {
            for cube in var_iter!(
                egraph[subst[source_table_name_var]],
                TableScanSourceTableName
            )
            .cloned()
            {
                for table_name in
                    var_iter!(egraph[subst[table_name_var]], CubeScanTableName).cloned()
                {
                    if let Some(_referenced_expr) =
                        &egraph.index(subst[exp_var]).data.referenced_expr
                    {
                        // TODO check referenced_expr
                        subst.insert(
                            cube_var,
                            egraph.add(LogicalPlanLanguage::FilterReplacerCube(
                                FilterReplacerCube(Some(cube.to_string())),
                            )),
                        );

                        subst.insert(
                            filter_table_name_var,
                            egraph.add(LogicalPlanLanguage::FilterReplacerTableName(
                                FilterReplacerTableName(table_name.to_string()),
                            )),
                        );
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
        new_limit_n_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let literal_var = var!(literal_var);
        let new_limit_var = var!(new_limit_var);
        let new_limit_n_var = var!(new_limit_n_var);
        move |egraph, subst| {
            for literal_value in var_iter!(egraph[subst[literal_var]], LiteralExprValue) {
                if let ScalarValue::Boolean(Some(false)) = literal_value {
                    subst.insert(
                        new_limit_var,
                        egraph.add(LogicalPlanLanguage::CubeScanLimit(CubeScanLimit(Some(1)))),
                    );
                    subst.insert(
                        new_limit_n_var,
                        egraph.add(LogicalPlanLanguage::LimitN(LimitN(0))),
                    );
                    return true;
                }
            }
            false
        }
    }

    fn transform_filter(
        &self,
        column_var: &'static str,
        op_var: &'static str,
        literal_var: &'static str,
        cube_var: &'static str,
        members_var: &'static str,
        table_name_var: &'static str,
        filter_member_var: &'static str,
        filter_op_var: &'static str,
        filter_values_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let column_var = column_var.parse().unwrap();
        let op_var = op_var.parse().unwrap();
        let literal_var = literal_var.parse().unwrap();
        let cube_var = cube_var.parse().unwrap();
        let members_var = var!(members_var);
        let table_name_var = var!(table_name_var);
        let filter_member_var = filter_member_var.parse().unwrap();
        let filter_op_var = filter_op_var.parse().unwrap();
        let filter_values_var = filter_values_var.parse().unwrap();
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for expr_op in var_iter!(egraph[subst[op_var]], BinaryExprOp) {
                for literal in var_iter!(egraph[subst[literal_var]], LiteralExprValue) {
                    if let Some((member_name, cube)) = Self::filter_member_name(
                        egraph,
                        subst,
                        &meta_context,
                        cube_var,
                        column_var,
                        members_var,
                        table_name_var,
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
                                        if op == "contains" || op == "notContains" {
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
                                                    .checked_sub_signed(Duration::milliseconds(1))
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
                                            log::trace!("Can't get timestamp for {:?}", literal);
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

            false
        }
    }

    fn transform_segment(
        &self,
        column_var: &'static str,
        op_var: &'static str,
        literal_var: &'static str,
        cube_var: &'static str,
        members_var: &'static str,
        table_name_var: &'static str,
        segment_member_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let column_var = column_var.parse().unwrap();
        let op_var = op_var.parse().unwrap();
        let literal_var = literal_var.parse().unwrap();
        let cube_var = cube_var.parse().unwrap();
        let members_var = var!(members_var);
        let table_name_var = var!(table_name_var);
        let segment_member_var = segment_member_var.parse().unwrap();
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for expr_op in var_iter!(egraph[subst[op_var]], BinaryExprOp) {
                for literal in var_iter!(egraph[subst[literal_var]], LiteralExprValue) {
                    if expr_op == &Operator::Eq {
                        if literal == &ScalarValue::Boolean(Some(true))
                            || literal == &ScalarValue::Utf8(Some("true".to_string()))
                        {
                            if let Some((member_name, cube)) = Self::filter_member_name(
                                egraph,
                                subst,
                                &meta_context,
                                cube_var,
                                column_var,
                                members_var,
                                table_name_var,
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

            false
        }
    }

    fn transform_change_user(
        &self,
        column_var: &'static str,
        op_var: &'static str,
        literal_var: &'static str,
        change_user_member_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let column_var = column_var.parse().unwrap();
        let op_var = op_var.parse().unwrap();
        let literal_var = literal_var.parse().unwrap();
        let change_user_member_var = change_user_member_var.parse().unwrap();

        move |egraph, subst| {
            for expr_op in var_iter!(egraph[subst[op_var]], BinaryExprOp) {
                for literal in var_iter!(egraph[subst[literal_var]], LiteralExprValue) {
                    if expr_op == &Operator::Eq {
                        if let ScalarValue::Utf8(Some(change_user)) = literal {
                            let specified_user = change_user.clone();

                            for column in
                                var_iter!(egraph[subst[column_var]], ColumnExprColumn).cloned()
                            {
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
                }
            }

            false
        }
    }

    fn transform_in_filter(
        &self,
        column_var: &'static str,
        list_var: &'static str,
        negated_var: &'static str,
        cube_var: &'static str,
        members_var: &'static str,
        table_name_var: &'static str,
        filter_member_var: &'static str,
        filter_op_var: &'static str,
        filter_values_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let column_var = var!(column_var);
        let list_var = var!(list_var);
        let negated_var = var!(negated_var);
        let cube_var = var!(cube_var);
        let members_var = var!(members_var);
        let table_name_var = var!(table_name_var);
        let filter_member_var = var!(filter_member_var);
        let filter_op_var = var!(filter_op_var);
        let filter_values_var = var!(filter_values_var);
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            if let Some(list) = &egraph[subst[list_var]].data.constant_in_list {
                let values = list
                    .into_iter()
                    .map(|literal| FilterRules::scalar_to_value(literal))
                    .collect::<Vec<_>>();

                if let Some((member_name, cube)) = Self::filter_member_name(
                    egraph,
                    subst,
                    &meta_context,
                    cube_var,
                    column_var,
                    members_var,
                    table_name_var,
                ) {
                    if cube.contains_member(&member_name) {
                        for negated in var_iter!(egraph[subst[negated_var]], InListExprNegated) {
                            let negated = *negated;
                            subst.insert(
                                filter_member_var,
                                egraph.add(LogicalPlanLanguage::FilterMemberMember(
                                    FilterMemberMember(member_name.to_string()),
                                )),
                            );

                            subst.insert(
                                filter_op_var,
                                egraph.add(LogicalPlanLanguage::FilterMemberOp(FilterMemberOp(
                                    if negated {
                                        "notEquals".to_string()
                                    } else {
                                        "equals".to_string()
                                    },
                                ))),
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
        cube_var: &'static str,
        members_var: &'static str,
        table_name_var: &'static str,
        filter_member_var: &'static str,
        filter_op_var: &'static str,
        filter_values_var: &'static str,
        is_null_op: bool,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let column_var = var!(column_var);
        let cube_var = var!(cube_var);
        let members_var = var!(members_var);
        let table_name_var = var!(table_name_var);
        let filter_member_var = var!(filter_member_var);
        let filter_op_var = var!(filter_op_var);
        let filter_values_var = var!(filter_values_var);
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            if let Some((member_name, cube)) = Self::filter_member_name(
                egraph,
                subst,
                &meta_context,
                cube_var,
                column_var,
                members_var,
                table_name_var,
            ) {
                if cube.contains_member(&member_name) {
                    subst.insert(
                        filter_member_var,
                        egraph.add(LogicalPlanLanguage::FilterMemberMember(FilterMemberMember(
                            member_name.to_string(),
                        ))),
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
                        egraph.add(LogicalPlanLanguage::FilterMemberValues(FilterMemberValues(
                            Vec::new(),
                        ))),
                    );

                    return true;
                }
            }

            false
        }
    }

    fn filter_member_name(
        egraph: &EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
        subst: &Subst,
        meta_context: &Arc<MetaContext>,
        cube_var: Var,
        column_var: Var,
        members_var: Var,
        table_name_var: Var,
    ) -> Option<(String, V1CubeMeta)> {
        for cube in var_iter!(egraph[subst[cube_var]], FilterReplacerCube) {
            if let Some(cube) = cube
                .as_ref()
                .and_then(|cube| meta_context.find_cube_with_name(cube))
            {
                for column in var_iter!(egraph[subst[column_var]], ColumnExprColumn).cloned() {
                    for table_name in
                        var_iter!(egraph[subst[table_name_var]], FilterReplacerTableName).cloned()
                    {
                        let alias_name = expr_column_name(
                            Expr::Column(column.clone()),
                            &Some(table_name.to_string()),
                        );
                        let member_name = member_name_by_alias(
                            egraph,
                            subst[members_var],
                            &alias_name,
                            table_name.to_string(),
                        )
                        .unwrap_or(format!("{}.{}", cube.name, column.name));

                        return Some((member_name, cube));
                    }
                }
            }
        }

        None
    }

    fn transform_between_dates(
        &self,
        column_var: &'static str,
        negated_var: &'static str,
        low_var: &'static str,
        high_var: &'static str,
        cube_var: &'static str,
        members_var: &'static str,
        table_name_var: &'static str,
        filter_member_var: &'static str,
        filter_op_var: &'static str,
        filter_values_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let column_var = var!(column_var);
        let negated_var = var!(negated_var);
        let low_var = var!(low_var);
        let high_var = var!(high_var);
        let cube_var = var!(cube_var);
        let members_var = var!(members_var);
        let table_name_var = var!(table_name_var);
        let filter_member_var = var!(filter_member_var);
        let filter_op_var = var!(filter_op_var);
        let filter_values_var = var!(filter_values_var);
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            if let Some((member_name, cube)) = Self::filter_member_name(
                egraph,
                subst,
                &meta_context,
                cube_var,
                column_var,
                members_var,
                table_name_var,
            ) {
                if let Some(_) = cube.lookup_dimension_by_member_name(&member_name) {
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

            false
        }
    }

    fn transform_between_numbers(
        &self,
        column_var: &'static str,
        negated_var: &'static str,
        cube_var: &'static str,
        members_var: &'static str,
        table_name_var: &'static str,
        is_negated: bool,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let column_var = var!(column_var);
        let negated_var = var!(negated_var);
        let cube_var = var!(cube_var);
        let members_var = var!(members_var);
        let table_name_var = var!(table_name_var);
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            if let Some((member_name, cube)) = Self::filter_member_name(
                egraph,
                subst,
                &meta_context,
                cube_var,
                column_var,
                members_var,
                table_name_var,
            ) {
                if let Some(_) = cube.lookup_dimension_by_member_name(&member_name) {
                    for negated in var_iter!(egraph[subst[negated_var]], BetweenExprNegated) {
                        match cube.member_type(&member_name) {
                            Some(MemberType::Number) if &is_negated == negated => return true,
                            _ => continue,
                        }
                    }
                }
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
                    if member_name_to_expr.iter().all(|(m, _)| m != &member) {
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
                    if member_name_to_expr.iter().any(|(m, _)| m == member) {
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
}

fn filter_flatten_rewrite_left(
    op: impl Display + Copy,
) -> Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis> {
    rewrite(
        &format!("filter-flatten-{}-left", op),
        filter_op(
            filter_op_filters(filter_op(filter_op_filters("?left", "?right"), op), "?tail"),
            op,
        ),
        filter_op(
            filter_op_filters(filter_op_filters("?left", "?right"), "?tail"),
            op,
        ),
    )
}

fn filter_flatten_rewrite_right(
    op: impl Display + Copy,
) -> Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis> {
    rewrite(
        &format!("filter-flatten-{}-right", op),
        filter_op(
            filter_op_filters("?tail", filter_op(filter_op_filters("?left", "?right"), op)),
            op,
        ),
        filter_op(
            filter_op_filters("?tail", filter_op_filters("?left", "?right")),
            op,
        ),
    )
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
