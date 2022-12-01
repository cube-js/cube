use super::utils;
use crate::{
    compile::{
        engine::provider::CubeContext,
        rewrite::{
            agg_fun_expr, aggr_aggr_expr, aggr_aggr_expr_empty_tail, aggr_group_expr,
            aggr_group_expr_empty_tail, aggregate, alias_expr, analysis::LogicalPlanAnalysis,
            binary_expr, cast_expr, cast_expr_explicit, column_expr, cube_scan, event_notification,
            fun_expr, group_aggregate_split_replacer, group_expr_split_replacer,
            inner_aggregate_split_replacer, is_not_null_expr, is_null_expr, literal_expr,
            literal_number, literal_string, original_expr_name, outer_aggregate_split_replacer,
            outer_projection_split_replacer, projection, projection_expr,
            projection_expr_empty_tail, rewrite, rewriter::RewriteRules,
            rules::members::MemberRules, transforming_chain_rewrite, transforming_rewrite,
            udf_expr, AggregateFunctionExprDistinct, AggregateFunctionExprFun, AliasExprAlias,
            BinaryExprOp, ColumnExprColumn, CubeScanAliasToCube, EventNotificationMeta,
            GroupAggregateSplitReplacerAliasToCube, GroupExprSplitReplacerAliasToCube,
            InnerAggregateSplitReplacerAliasToCube, LiteralExprValue, LogicalPlanLanguage,
            OuterAggregateSplitReplacerAliasToCube, OuterProjectionSplitReplacerAliasToCube,
            ProjectionAlias,
        },
    },
    transport::V1CubeMetaExt,
    var, var_iter, CubeError,
};
use datafusion::{
    arrow::datatypes::DataType as ArrowDataType,
    logical_plan::{Column, DFSchema, Operator},
    physical_plan::aggregates::AggregateFunction,
    scalar::ScalarValue,
};
use egg::{EGraph, Id, Rewrite, Subst, Var};
use std::{fmt::Display, ops::Index, sync::Arc};

pub struct SplitRules {
    cube_context: Arc<CubeContext>,
}

impl RewriteRules for SplitRules {
    fn rewrite_rules(&self) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
        let mut rules = vec![
            transforming_rewrite(
                "split-projection-aggregate",
                aggregate(
                    cube_scan(
                        "?alias_to_cube",
                        "?members",
                        "?filters",
                        "?orders",
                        "?limit",
                        "?offset",
                        "?aliases",
                        "CubeScanSplit:false",
                        "?can_pushdown_join",
                    ),
                    "?group_expr",
                    "?aggr_expr",
                ),
                projection(
                    projection_expr(
                        outer_projection_split_replacer("?group_expr", "?outer_projection_cube"),
                        outer_projection_split_replacer("?aggr_expr", "?outer_projection_cube"),
                    ),
                    aggregate(
                        cube_scan(
                            "?alias_to_cube",
                            "?members",
                            "?filters",
                            "?orders",
                            "?limit",
                            "?offset",
                            "?aliases",
                            "CubeScanSplit:true",
                            "?can_pushdown_join",
                        ),
                        inner_aggregate_split_replacer("?group_expr", "?inner_aggregate_cube"),
                        inner_aggregate_split_replacer("?aggr_expr", "?inner_aggregate_cube"),
                    ),
                    "?projection_alias",
                ),
                self.split_projection_aggregate(
                    "?alias_to_cube",
                    "?inner_aggregate_cube",
                    "?outer_projection_cube",
                    "?projection_alias",
                ),
            ),
            transforming_rewrite(
                "split-projection-projection",
                projection(
                    "?expr",
                    cube_scan(
                        "?alias_to_cube",
                        "?members",
                        "?filters",
                        "?orders",
                        "?limit",
                        "?offset",
                        "?aliases",
                        "CubeScanSplit:false",
                        "?can_pushdown_join",
                    ),
                    "?alias",
                ),
                projection(
                    outer_projection_split_replacer("?expr", "?outer_projection_cube"),
                    projection(
                        inner_aggregate_split_replacer("?expr", "?inner_aggregate_cube"),
                        cube_scan(
                            "?alias_to_cube",
                            "?members",
                            "?filters",
                            "?orders",
                            "?limit",
                            "?offset",
                            "?aliases",
                            "CubeScanSplit:true",
                            "?can_pushdown_join",
                        ),
                        "?projection_alias",
                    ),
                    "?alias",
                ),
                self.split_projection_aggregate(
                    "?alias_to_cube",
                    "?inner_aggregate_cube",
                    "?outer_projection_cube",
                    "?projection_alias",
                ),
            ),
            // TODO: reaggregate rule requires aliases for all exprs in projection
            transforming_rewrite(
                "split-reaggregate-projection",
                projection(
                    "?expr",
                    cube_scan(
                        "?alias_to_cube",
                        "?members",
                        "?filters",
                        "?orders",
                        "?limit",
                        "?offset",
                        "?aliases",
                        "CubeScanSplit:false",
                        "?can_pushdown_join",
                    ),
                    "?alias",
                ),
                projection(
                    "?outer_expr",
                    aggregate(
                        projection(
                            inner_aggregate_split_replacer("?expr", "?inner_aggregate_cube"),
                            cube_scan(
                                "?alias_to_cube",
                                "?members",
                                "?filters",
                                "?orders",
                                "?limit",
                                "?offset",
                                "?aliases",
                                "CubeScanSplit:true",
                                "?can_pushdown_join",
                            ),
                            "?inner_projection_alias",
                        ),
                        group_expr_split_replacer("?expr", "?group_expr_cube"),
                        group_aggregate_split_replacer("?expr", "?group_aggregate_cube"),
                    ),
                    "?alias",
                ),
                self.split_reaggregate_projection(
                    "?expr",
                    "?alias_to_cube",
                    "?inner_aggregate_cube",
                    "?group_expr_cube",
                    "?group_aggregate_cube",
                    "?outer_expr",
                    "?inner_projection_alias",
                ),
            ),
            transforming_rewrite(
                "split-aggregate-aggregate",
                aggregate(
                    cube_scan(
                        "?alias_to_cube",
                        "?members",
                        "?filters",
                        "?orders",
                        "?limit",
                        "?offset",
                        "?aliases",
                        "CubeScanSplit:false",
                        "?can_pushdown_join",
                    ),
                    "?group_expr",
                    "?aggr_expr",
                ),
                aggregate(
                    aggregate(
                        cube_scan(
                            "?alias_to_cube",
                            "?members",
                            "?filters",
                            "?orders",
                            "?limit",
                            "?offset",
                            "?aliases",
                            "CubeScanSplit:true",
                            "?can_pushdown_join",
                        ),
                        inner_aggregate_split_replacer("?group_expr", "?inner_aggregate_cube"),
                        inner_aggregate_split_replacer("?aggr_expr", "?inner_aggregate_cube"),
                    ),
                    outer_aggregate_split_replacer("?group_expr", "?outer_aggregate_cube"),
                    outer_aggregate_split_replacer("?aggr_expr", "?outer_aggregate_cube"),
                ),
                self.split_aggregate_aggregate(
                    "?alias_to_cube",
                    "?inner_aggregate_cube",
                    "?outer_aggregate_cube",
                ),
            ),
            // Inner aggregate replacers -- aggregation
            rewrite(
                "split-push-down-group-inner-replacer",
                inner_aggregate_split_replacer(aggr_group_expr("?left", "?right"), "?cube"),
                aggr_group_expr(
                    inner_aggregate_split_replacer("?left", "?cube"),
                    inner_aggregate_split_replacer("?right", "?cube"),
                ),
            ),
            rewrite(
                "split-push-down-aggr-inner-replacer",
                inner_aggregate_split_replacer(aggr_aggr_expr("?left", "?right"), "?cube"),
                aggr_aggr_expr(
                    inner_aggregate_split_replacer("?left", "?cube"),
                    inner_aggregate_split_replacer("?right", "?cube"),
                ),
            ),
            rewrite(
                "split-push-down-group-inner-replacer-tail",
                inner_aggregate_split_replacer(aggr_group_expr_empty_tail(), "?cube"),
                aggr_group_expr_empty_tail(),
            ),
            rewrite(
                "split-push-down-aggr-inner-replacer-tail",
                inner_aggregate_split_replacer(aggr_aggr_expr_empty_tail(), "?cube"),
                aggr_aggr_expr_empty_tail(),
            ),
            // Inner aggregate replacers -- projection
            rewrite(
                "split-push-down-projection-inner-replacer",
                inner_aggregate_split_replacer(projection_expr("?left", "?right"), "?cube"),
                projection_expr(
                    inner_aggregate_split_replacer("?left", "?cube"),
                    inner_aggregate_split_replacer("?right", "?cube"),
                ),
            ),
            rewrite(
                "split-push-down-projection-inner-replacer-tail",
                inner_aggregate_split_replacer(projection_expr_empty_tail(), "?cube"),
                projection_expr_empty_tail(),
            ),
            // Outer projection replacer
            rewrite(
                "split-push-down-group-outer-replacer",
                outer_projection_split_replacer(aggr_group_expr("?left", "?right"), "?cube"),
                projection_expr(
                    outer_projection_split_replacer("?left", "?cube"),
                    outer_projection_split_replacer("?right", "?cube"),
                ),
            ),
            rewrite(
                "split-push-down-aggr-outer-replacer",
                outer_projection_split_replacer(aggr_aggr_expr("?left", "?right"), "?cube"),
                projection_expr(
                    outer_projection_split_replacer("?left", "?cube"),
                    outer_projection_split_replacer("?right", "?cube"),
                ),
            ),
            rewrite(
                "split-push-down-group-outer-replacer-tail",
                outer_projection_split_replacer(aggr_group_expr_empty_tail(), "?cube"),
                projection_expr_empty_tail(),
            ),
            rewrite(
                "split-push-down-aggr-outer-replacer-tail",
                outer_projection_split_replacer(aggr_aggr_expr_empty_tail(), "?cube"),
                projection_expr_empty_tail(),
            ),
            // Outer projection replacer -- projection
            rewrite(
                "split-push-down-projection-outer-replacer",
                outer_projection_split_replacer(projection_expr("?left", "?right"), "?cube"),
                projection_expr(
                    outer_projection_split_replacer("?left", "?cube"),
                    outer_projection_split_replacer("?right", "?cube"),
                ),
            ),
            rewrite(
                "split-push-down-projection-outer-replacer-tail",
                outer_projection_split_replacer(projection_expr_empty_tail(), "?cube"),
                projection_expr_empty_tail(),
            ),
            // Outer aggregate replacer
            rewrite(
                "split-push-down-group-outer-aggr-replacer",
                outer_aggregate_split_replacer(aggr_group_expr("?left", "?right"), "?cube"),
                aggr_group_expr(
                    outer_aggregate_split_replacer("?left", "?cube"),
                    outer_aggregate_split_replacer("?right", "?cube"),
                ),
            ),
            rewrite(
                "split-push-down-aggr-outer-aggr-replacer",
                outer_aggregate_split_replacer(aggr_aggr_expr("?left", "?right"), "?cube"),
                aggr_aggr_expr(
                    outer_aggregate_split_replacer("?left", "?cube"),
                    outer_aggregate_split_replacer("?right", "?cube"),
                ),
            ),
            rewrite(
                "split-push-down-projection-outer-aggr-replacer",
                outer_aggregate_split_replacer(projection_expr("?left", "?right"), "?cube"),
                projection_expr(
                    outer_aggregate_split_replacer("?left", "?cube"),
                    outer_aggregate_split_replacer("?right", "?cube"),
                ),
            ),
            rewrite(
                "split-push-down-group-outer-aggr-replacer-tail",
                outer_aggregate_split_replacer(aggr_group_expr_empty_tail(), "?cube"),
                aggr_group_expr_empty_tail(),
            ),
            rewrite(
                "split-push-down-aggr-outer-aggr-replacer-tail",
                outer_aggregate_split_replacer(aggr_aggr_expr_empty_tail(), "?cube"),
                aggr_aggr_expr_empty_tail(),
            ),
            rewrite(
                "split-push-down-projection-outer-aggr-replacer-tail",
                outer_aggregate_split_replacer(projection_expr_empty_tail(), "?cube"),
                projection_expr_empty_tail(),
            ),
            // Reaggregate replacers -- group expr
            rewrite(
                "split-push-down-reaggregate-group-expr-replacer",
                group_expr_split_replacer(projection_expr("?left", "?right"), "?cube"),
                aggr_group_expr(
                    group_expr_split_replacer("?left", "?cube"),
                    group_expr_split_replacer("?right", "?cube"),
                ),
            ),
            rewrite(
                "split-push-down-reaggregate-group-expr-replacer-tail",
                group_expr_split_replacer(projection_expr_empty_tail(), "?cube"),
                aggr_group_expr_empty_tail(),
            ),
            // Reaggregate replacers -- group aggregate
            rewrite(
                "split-push-down-reaggregate-group-aggregate-replacer",
                group_aggregate_split_replacer(projection_expr("?left", "?right"), "?cube"),
                aggr_aggr_expr(
                    group_aggregate_split_replacer("?left", "?cube"),
                    group_aggregate_split_replacer("?right", "?cube"),
                ),
            ),
            rewrite(
                "split-push-down-reaggregate-group-aggregate-replacer-tail",
                group_aggregate_split_replacer(projection_expr_empty_tail(), "?cube"),
                aggr_aggr_expr_empty_tail(),
            ),
            // Members
            // Column rules
            rewrite(
                "split-push-down-column-inner-replacer",
                inner_aggregate_split_replacer(column_expr("?column"), "?cube"),
                column_expr("?column"),
            ),
            rewrite(
                "split-push-down-column-outer-replacer",
                outer_projection_split_replacer(column_expr("?column"), "?cube"),
                column_expr("?column"),
            ),
            rewrite(
                "split-push-down-column-outer-aggr-replacer",
                outer_aggregate_split_replacer(column_expr("?column"), "?cube"),
                column_expr("?column"),
            ),
            // Literal rules
            rewrite(
                "split-push-down-literal-inner-replacer",
                inner_aggregate_split_replacer(literal_expr("?expr"), "?cube"),
                literal_expr("?expr"),
            ),
            rewrite(
                "split-push-down-literal-outer-replacer",
                outer_projection_split_replacer(literal_expr("?expr"), "?cube"),
                literal_expr("?expr"),
            ),
            rewrite(
                "split-push-down-literal-outer-aggr-replacer",
                outer_aggregate_split_replacer(literal_expr("?expr"), "?cube"),
                literal_expr("?expr"),
            ),
            // AggFun with literal
            transforming_rewrite(
                "split-push-down-agg-fun-literal-inner-replacer",
                inner_aggregate_split_replacer(
                    agg_fun_expr(
                        "?fun",
                        vec![cast_expr(literal_expr("?expr"), "?data_type")],
                        "?distinct",
                    ),
                    "?cube",
                ),
                literal_expr("?expr"),
                self.transform_aggr_fun_with_literal("?fun", "?expr"),
            ),
            transforming_rewrite(
                "split-push-down-agg-fun-literal-outer-replacer",
                outer_projection_split_replacer(
                    agg_fun_expr(
                        "?fun",
                        vec![cast_expr(literal_expr("?expr"), "?data_type")],
                        "?distinct",
                    ),
                    "?cube",
                ),
                agg_fun_expr(
                    "?fun",
                    vec![cast_expr(literal_expr("?expr"), "?data_type")],
                    "?distinct",
                ),
                self.transform_aggr_fun_with_literal("?fun", "?expr"),
            ),
            transforming_rewrite(
                "split-push-down-agg-fun-literal-outer-aggr-replacer",
                outer_aggregate_split_replacer(
                    agg_fun_expr(
                        "?fun",
                        vec![cast_expr(literal_expr("?expr"), "?data_type")],
                        "?distinct",
                    ),
                    "?cube",
                ),
                agg_fun_expr(
                    "?fun",
                    vec![cast_expr(literal_expr("?expr"), "?data_type")],
                    "?distinct",
                ),
                self.transform_aggr_fun_with_literal("?fun", "?expr"),
            ),
            // Date trunc
            transforming_rewrite(
                "split-push-down-date-trunc-inner-replacer",
                inner_aggregate_split_replacer(
                    fun_expr(
                        "DateTrunc",
                        vec![literal_expr("?granularity"), column_expr("?column")],
                    ),
                    "?cube",
                ),
                fun_expr(
                    "DateTrunc",
                    vec![
                        literal_expr("?rewritten_granularity"),
                        column_expr("?column"),
                    ],
                ),
                // To validate & de-aliasing granularity
                self.split_date_trunc("?granularity", "?rewritten_granularity"),
            ),
            // Date part
            transforming_chain_rewrite(
                "split-push-down-date-part-inner-replacer",
                inner_aggregate_split_replacer(
                    fun_expr(
                        "DatePart",
                        vec![literal_expr("?granularity"), "?expr".to_string()],
                    ),
                    "?cube",
                ),
                vec![("?expr", column_expr("?column"))],
                alias_expr(
                    fun_expr(
                        "DateTrunc",
                        vec![
                            literal_expr("?date_trunc_granularity"),
                            column_expr("?column"),
                        ],
                    ),
                    "?alias",
                ),
                MemberRules::transform_original_expr_date_trunc(
                    "?expr",
                    "?granularity",
                    // It will returns new granularity for DateTrunc
                    "?date_trunc_granularity",
                    "?alias_column",
                    Some("?alias"),
                    true,
                ),
            ),
            // DatePart ("?expr", DateTrunc)
            transforming_chain_rewrite(
                "split-push-down-date-part-with-date-trunc-inner-replacer",
                inner_aggregate_split_replacer(
                    fun_expr(
                        "DatePart",
                        vec![literal_expr("?date_part_granularity"), "?expr".to_string()],
                    ),
                    "?cube",
                ),
                vec![(
                    "?expr",
                    fun_expr(
                        "DateTrunc",
                        vec![
                            literal_expr("?date_trunc_granularity"),
                            column_expr("?column"),
                        ],
                    ),
                )],
                alias_expr(
                    fun_expr(
                        "DateTrunc",
                        vec![
                            literal_expr("?rewritten_granularity"),
                            column_expr("?column"),
                        ],
                    ),
                    "?alias",
                ),
                MemberRules::transform_original_expr_nested_date_trunc(
                    "?expr",
                    "?date_part_granularity",
                    "?date_trunc_granularity",
                    "?rewritten_granularity",
                    "?alias_column",
                    Some("?alias"),
                    true,
                ),
            ),
            transforming_chain_rewrite(
                "split-push-down-date-part-with-date-trunc-outer-aggr-replacer",
                outer_aggregate_split_replacer(
                    fun_expr(
                        "DatePart",
                        vec![literal_expr("?date_part_granularity"), "?expr".to_string()],
                    ),
                    "?cube",
                ),
                vec![(
                    "?expr",
                    fun_expr(
                        "DateTrunc",
                        vec![
                            literal_expr("?date_trunc_granularity"),
                            column_expr("?column"),
                        ],
                    ),
                )],
                fun_expr(
                    "DatePart",
                    vec![
                        literal_expr("?date_part_granularity"),
                        alias_expr("?alias_column", "?alias"),
                    ],
                ),
                MemberRules::transform_original_expr_date_trunc(
                    "?expr",
                    "?date_part_granularity",
                    "?date_part_granularity",
                    "?alias_column",
                    Some("?alias"),
                    false,
                ),
            ),
            // TODO: refactor. rm this rule and add uncast rule + alias rewrite on top projection
            transforming_chain_rewrite(
                "split-push-down-date-part-with-date-trunc-and-cast-inner-replacer",
                inner_aggregate_split_replacer(
                    fun_expr(
                        "DatePart",
                        vec![literal_expr("?date_part_granularity"), "?expr".to_string()],
                    ),
                    "?cube",
                ),
                vec![(
                    "?expr",
                    cast_expr(
                        fun_expr(
                            "DateTrunc",
                            vec![
                                literal_expr("?date_trunc_granularity"),
                                column_expr("?column"),
                            ],
                        ),
                        "?data_type",
                    ),
                )],
                alias_expr(
                    fun_expr(
                        "DateTrunc",
                        vec![
                            literal_expr("?rewritten_granularity"),
                            column_expr("?column"),
                        ],
                    ),
                    "?alias",
                ),
                MemberRules::transform_original_expr_nested_date_trunc(
                    "?expr",
                    "?date_part_granularity",
                    "?date_trunc_granularity",
                    "?rewritten_granularity",
                    "?alias_column",
                    Some("?alias"),
                    true,
                ),
            ),
            // TODO: refactor. rm this rule and add uncast rule + alias rewrite on top projection
            transforming_chain_rewrite(
                "split-push-down-date-part-with-date-trunc-and-cast-outer-aggr-replacer",
                outer_aggregate_split_replacer(
                    fun_expr(
                        "DatePart",
                        vec![literal_expr("?date_part_granularity"), "?expr".to_string()],
                    ),
                    "?cube",
                ),
                vec![(
                    "?expr",
                    cast_expr(
                        fun_expr(
                            "DateTrunc",
                            vec![
                                literal_expr("?date_trunc_granularity"),
                                column_expr("?column"),
                            ],
                        ),
                        "?data_type",
                    ),
                )],
                fun_expr(
                    "DatePart",
                    vec![
                        literal_expr("?date_part_granularity"),
                        alias_expr("?alias_column", "?alias"),
                    ],
                ),
                MemberRules::transform_original_expr_date_trunc(
                    "?expr",
                    "?date_part_granularity",
                    "?date_part_granularity",
                    "?alias_column",
                    Some("?alias"),
                    false,
                ),
            ),
            // (DATEDIFF(day, DATE '1970-01-01', "ta_1"."createdAt") + 3) % 7) + 1)
            transforming_chain_rewrite(
                "split-push-down-dow-inner-replacer",
                inner_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    binary_expr(
                        binary_expr(
                            binary_expr(
                                udf_expr(
                                    "datediff",
                                    vec![
                                        literal_string("day"),
                                        cast_expr_explicit(
                                            literal_string("1970-01-01"),
                                            ArrowDataType::Date32,
                                        ),
                                        column_expr("?column"),
                                    ],
                                ),
                                "+",
                                literal_number(3),
                            ),
                            "%",
                            literal_number(7),
                        ),
                        "+",
                        literal_number(1),
                    ),
                )],
                alias_expr(
                    fun_expr(
                        "DateTrunc",
                        vec![literal_string("day"), column_expr("?column")],
                    ),
                    "?outer_alias",
                ),
                self.transform_original_expr_to_alias_and_column("?expr", "?outer_alias", None),
            ),
            // (DATEDIFF(day, DATE '1970-01-01', "ta_1"."createdAt") + 3) % 7) + 1)
            transforming_chain_rewrite(
                "split-push-down-dow-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    binary_expr(
                        binary_expr(
                            binary_expr(
                                udf_expr(
                                    "datediff",
                                    vec![
                                        literal_string("day"),
                                        cast_expr_explicit(
                                            literal_string("1970-01-01"),
                                            ArrowDataType::Date32,
                                        ),
                                        column_expr("?column"),
                                    ],
                                ),
                                "+",
                                literal_number(3),
                            ),
                            "%",
                            literal_number(7),
                        ),
                        "+",
                        literal_number(1),
                    ),
                )],
                alias_expr(
                    fun_expr(
                        "DatePart",
                        vec![literal_string("dow"), column_expr("?outer_column")],
                    ),
                    "?outer_alias",
                ),
                self.transform_original_expr_to_alias_and_column(
                    "?expr",
                    "?outer_alias",
                    Some("?outer_column"),
                ),
            ),
            // (DATEDIFF(day, DATEADD(month, CAST(((EXTRACT(MONTH FROM "ta_1"."createdAt") - 1) * -1) AS int), CAST(CAST(((((EXTRACT(YEAR FROM "ta_1"."createdAt") * 100) + EXTRACT(MONTH FROM "ta_1"."createdAt")) * 100) + 1) AS varchar) AS date)), "ta_1"."createdAt")
            transforming_chain_rewrite(
                "split-push-down-doy-inner-replacer",
                inner_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    binary_expr(
                        udf_expr(
                            "datediff",
                            vec![
                                literal_string("day"),
                                udf_expr(
                                    "dateadd",
                                    vec![
                                        literal_string("month"),
                                        cast_expr_explicit(
                                            binary_expr(
                                                binary_expr(
                                                    fun_expr(
                                                        "DatePart",
                                                        vec![
                                                            literal_string("MONTH"),
                                                            column_expr("?column"),
                                                        ],
                                                    ),
                                                    "-",
                                                    literal_number(1),
                                                ),
                                                "*",
                                                literal_number(-1),
                                            ),
                                            ArrowDataType::Int32,
                                        ),
                                        cast_expr_explicit(
                                            cast_expr_explicit(
                                                binary_expr(
                                                    binary_expr(
                                                        binary_expr(
                                                            binary_expr(
                                                                fun_expr(
                                                                    "DatePart",
                                                                    vec![
                                                                        literal_string("YEAR"),
                                                                        column_expr("?column"),
                                                                    ],
                                                                ),
                                                                "*",
                                                                literal_number(100),
                                                            ),
                                                            "+",
                                                            fun_expr(
                                                                "DatePart",
                                                                vec![
                                                                    literal_string("MONTH"),
                                                                    column_expr("?column"),
                                                                ],
                                                            ),
                                                        ),
                                                        "*",
                                                        literal_number(100),
                                                    ),
                                                    "+",
                                                    literal_number(1),
                                                ),
                                                ArrowDataType::Utf8,
                                            ),
                                            ArrowDataType::Date32,
                                        ),
                                    ],
                                ),
                                column_expr("?column"),
                            ],
                        ),
                        "+",
                        literal_number(1),
                    ),
                )],
                alias_expr(
                    fun_expr(
                        "DateTrunc",
                        vec![literal_string("day"), column_expr("?column")],
                    ),
                    "?outer_alias",
                ),
                self.transform_original_expr_to_alias_and_column("?expr", "?outer_alias", None),
            ),
            // (DATEDIFF(day, DATEADD(month, CAST(((EXTRACT(MONTH FROM "ta_1"."createdAt") - 1) * -1) AS int), CAST(CAST(((((EXTRACT(YEAR FROM "ta_1"."createdAt") * 100) + EXTRACT(MONTH FROM "ta_1"."createdAt")) * 100) + 1) AS varchar) AS date)), "ta_1"."createdAt")
            transforming_chain_rewrite(
                "split-push-down-doy-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    binary_expr(
                        udf_expr(
                            "datediff",
                            vec![
                                literal_string("day"),
                                udf_expr(
                                    "dateadd",
                                    vec![
                                        literal_string("month"),
                                        cast_expr_explicit(
                                            binary_expr(
                                                binary_expr(
                                                    fun_expr(
                                                        "DatePart",
                                                        vec![
                                                            literal_string("MONTH"),
                                                            column_expr("?column"),
                                                        ],
                                                    ),
                                                    "-",
                                                    literal_number(1),
                                                ),
                                                "*",
                                                literal_number(-1),
                                            ),
                                            ArrowDataType::Int32,
                                        ),
                                        cast_expr_explicit(
                                            cast_expr_explicit(
                                                binary_expr(
                                                    binary_expr(
                                                        binary_expr(
                                                            binary_expr(
                                                                fun_expr(
                                                                    "DatePart",
                                                                    vec![
                                                                        literal_string("YEAR"),
                                                                        column_expr("?column"),
                                                                    ],
                                                                ),
                                                                "*",
                                                                literal_number(100),
                                                            ),
                                                            "+",
                                                            fun_expr(
                                                                "DatePart",
                                                                vec![
                                                                    literal_string("MONTH"),
                                                                    column_expr("?column"),
                                                                ],
                                                            ),
                                                        ),
                                                        "*",
                                                        literal_number(100),
                                                    ),
                                                    "+",
                                                    literal_number(1),
                                                ),
                                                ArrowDataType::Utf8,
                                            ),
                                            ArrowDataType::Date32,
                                        ),
                                    ],
                                ),
                                column_expr("?column"),
                            ],
                        ),
                        "+",
                        literal_number(1),
                    ),
                )],
                alias_expr(
                    fun_expr(
                        "DatePart",
                        vec![literal_string("doy"), column_expr("?outer_column")],
                    ),
                    "?outer_alias",
                ),
                self.transform_original_expr_to_alias_and_column(
                    "?expr",
                    "?outer_alias",
                    Some("?outer_column"),
                ),
            ),
            // Skyvia Day (Date) to DateTrunc
            transforming_chain_rewrite(
                "split-push-down-skyvia-day-to-date-trunc-inner-replacer",
                inner_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    cast_expr_explicit(
                        cast_expr_explicit(
                            fun_expr(
                                "DateTrunc",
                                vec![literal_string("day"), column_expr("?column")],
                            ),
                            ArrowDataType::Date32,
                        ),
                        ArrowDataType::Utf8,
                    ),
                )],
                alias_expr(
                    fun_expr(
                        "DateTrunc",
                        vec![literal_string("day"), column_expr("?column")],
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column("?expr", "?alias", None),
            ),
            transforming_chain_rewrite(
                "split-push-down-skyvia-day-to-date-trunc-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    cast_expr_explicit(
                        cast_expr_explicit(
                            fun_expr(
                                "DateTrunc",
                                vec![literal_string("day"), column_expr("?column")],
                            ),
                            ArrowDataType::Date32,
                        ),
                        ArrowDataType::Utf8,
                    ),
                )],
                alias_expr(
                    cast_expr_explicit(
                        cast_expr_explicit(column_expr("?outer_column"), ArrowDataType::Date32),
                        ArrowDataType::Utf8,
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column(
                    "?expr",
                    "?alias",
                    Some("?outer_column"),
                ),
            ),
            transforming_chain_rewrite(
                "split-push-down-skyvia-day-to-date-trunc-outer-replacer",
                outer_projection_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    cast_expr_explicit(
                        cast_expr_explicit(
                            fun_expr(
                                "DateTrunc",
                                vec![literal_string("day"), column_expr("?column")],
                            ),
                            ArrowDataType::Date32,
                        ),
                        ArrowDataType::Utf8,
                    ),
                )],
                alias_expr(
                    cast_expr_explicit(
                        cast_expr_explicit(column_expr("?outer_column"), ArrowDataType::Date32),
                        ArrowDataType::Utf8,
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column(
                    "?expr",
                    "?alias",
                    Some("?outer_column"),
                ),
            ),
            // Skyvia Month to DateTrunc
            transforming_chain_rewrite(
                "split-push-down-skyvia-month-to-date-trunc-inner-replacer",
                inner_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    binary_expr(
                        binary_expr(
                            cast_expr_explicit(
                                fun_expr(
                                    "DatePart",
                                    vec![literal_string("YEAR"), column_expr("?column")],
                                ),
                                ArrowDataType::Utf8,
                            ),
                            "||",
                            literal_string(","),
                        ),
                        "||",
                        fun_expr(
                            "Lpad",
                            vec![
                                cast_expr_explicit(
                                    fun_expr(
                                        "DatePart",
                                        vec![literal_string("MONTH"), column_expr("?column")],
                                    ),
                                    ArrowDataType::Utf8,
                                ),
                                literal_number(2),
                                literal_string("0"),
                            ],
                        ),
                    ),
                )],
                alias_expr(
                    fun_expr(
                        "DateTrunc",
                        vec![literal_string("month"), column_expr("?column")],
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column("?expr", "?alias", None),
            ),
            transforming_chain_rewrite(
                "split-push-down-skyvia-month-to-date-trunc-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    binary_expr(
                        binary_expr(
                            cast_expr_explicit(
                                fun_expr(
                                    "DatePart",
                                    vec![literal_string("YEAR"), column_expr("?column")],
                                ),
                                ArrowDataType::Utf8,
                            ),
                            "||",
                            literal_string(","),
                        ),
                        "||",
                        fun_expr(
                            "Lpad",
                            vec![
                                cast_expr_explicit(
                                    fun_expr(
                                        "DatePart",
                                        vec![literal_string("MONTH"), column_expr("?column")],
                                    ),
                                    ArrowDataType::Utf8,
                                ),
                                literal_number(2),
                                literal_string("0"),
                            ],
                        ),
                    ),
                )],
                alias_expr(
                    udf_expr(
                        "to_char",
                        vec![column_expr("?outer_column"), literal_string("YYYY,MM")],
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column(
                    "?expr",
                    "?alias",
                    Some("?outer_column"),
                ),
            ),
            transforming_chain_rewrite(
                "split-push-down-skyvia-month-to-date-trunc-outer-replacer",
                outer_projection_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    binary_expr(
                        binary_expr(
                            cast_expr_explicit(
                                fun_expr(
                                    "DatePart",
                                    vec![literal_string("YEAR"), column_expr("?column")],
                                ),
                                ArrowDataType::Utf8,
                            ),
                            "||",
                            literal_string(","),
                        ),
                        "||",
                        fun_expr(
                            "Lpad",
                            vec![
                                cast_expr_explicit(
                                    fun_expr(
                                        "DatePart",
                                        vec![literal_string("MONTH"), column_expr("?column")],
                                    ),
                                    ArrowDataType::Utf8,
                                ),
                                literal_number(2),
                                literal_string("0"),
                            ],
                        ),
                    ),
                )],
                alias_expr(
                    udf_expr(
                        "to_char",
                        vec![column_expr("?outer_column"), literal_string("YYYY,MM")],
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column(
                    "?expr",
                    "?alias",
                    Some("?outer_column"),
                ),
            ),
            // Skyvia Quarter to DateTrunc
            transforming_chain_rewrite(
                "split-push-down-skyvia-quarter-to-date-trunc-inner-replacer",
                inner_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    binary_expr(
                        binary_expr(
                            cast_expr_explicit(
                                fun_expr(
                                    "DatePart",
                                    vec![literal_string("YEAR"), column_expr("?column")],
                                ),
                                ArrowDataType::Utf8,
                            ),
                            "||",
                            literal_string(","),
                        ),
                        "||",
                        cast_expr_explicit(
                            fun_expr(
                                "DatePart",
                                vec![literal_string("QUARTER"), column_expr("?column")],
                            ),
                            ArrowDataType::Utf8,
                        ),
                    ),
                )],
                alias_expr(
                    fun_expr(
                        "DateTrunc",
                        vec![literal_string("quarter"), column_expr("?column")],
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column("?expr", "?alias", None),
            ),
            transforming_chain_rewrite(
                "split-push-down-skyvia-quarter-to-date-trunc-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    binary_expr(
                        binary_expr(
                            cast_expr_explicit(
                                fun_expr(
                                    "DatePart",
                                    vec![literal_string("YEAR"), column_expr("?column")],
                                ),
                                ArrowDataType::Utf8,
                            ),
                            "||",
                            literal_string(","),
                        ),
                        "||",
                        cast_expr_explicit(
                            fun_expr(
                                "DatePart",
                                vec![literal_string("QUARTER"), column_expr("?column")],
                            ),
                            ArrowDataType::Utf8,
                        ),
                    ),
                )],
                alias_expr(
                    udf_expr(
                        "to_char",
                        vec![column_expr("?outer_column"), literal_string("YYYY,Q")],
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column(
                    "?expr",
                    "?alias",
                    Some("?outer_column"),
                ),
            ),
            transforming_chain_rewrite(
                "split-push-down-skyvia-quarter-to-date-trunc-outer-replacer",
                outer_projection_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    binary_expr(
                        binary_expr(
                            cast_expr_explicit(
                                fun_expr(
                                    "DatePart",
                                    vec![literal_string("YEAR"), column_expr("?column")],
                                ),
                                ArrowDataType::Utf8,
                            ),
                            "||",
                            literal_string(","),
                        ),
                        "||",
                        cast_expr_explicit(
                            fun_expr(
                                "DatePart",
                                vec![literal_string("QUARTER"), column_expr("?column")],
                            ),
                            ArrowDataType::Utf8,
                        ),
                    ),
                )],
                alias_expr(
                    udf_expr(
                        "to_char",
                        vec![column_expr("?outer_column"), literal_string("YYYY,Q")],
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column(
                    "?expr",
                    "?alias",
                    Some("?outer_column"),
                ),
            ),
            // Skyvia Year to DatePart
            transforming_chain_rewrite(
                "split-push-down-skyvia-year-to-date-trunc-inner-replacer",
                inner_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    cast_expr_explicit(
                        fun_expr(
                            "DatePart",
                            vec![literal_string("YEAR"), column_expr("?column")],
                        ),
                        ArrowDataType::Utf8,
                    ),
                )],
                alias_expr(
                    fun_expr(
                        "DateTrunc",
                        vec![literal_string("year"), column_expr("?column")],
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column("?expr", "?alias", None),
            ),
            transforming_chain_rewrite(
                "split-push-down-skyvia-year-to-date-trunc-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    cast_expr_explicit(
                        fun_expr(
                            "DatePart",
                            vec![literal_string("YEAR"), column_expr("?column")],
                        ),
                        ArrowDataType::Utf8,
                    ),
                )],
                alias_expr(
                    udf_expr(
                        "to_char",
                        vec![column_expr("?outer_column"), literal_string("YYYY")],
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column(
                    "?expr",
                    "?alias",
                    Some("?outer_column"),
                ),
            ),
            transforming_chain_rewrite(
                "split-push-down-skyvia-year-to-date-trunc-outer-replacer",
                outer_projection_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    cast_expr_explicit(
                        fun_expr(
                            "DatePart",
                            vec![literal_string("YEAR"), column_expr("?column")],
                        ),
                        ArrowDataType::Utf8,
                    ),
                )],
                alias_expr(
                    udf_expr(
                        "to_char",
                        vec![column_expr("?outer_column"), literal_string("YYYY")],
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column(
                    "?expr",
                    "?alias",
                    Some("?outer_column"),
                ),
            ),
            //
            rewrite(
                "split-push-down-to-char-date-trunc-literal-inner-replacer",
                inner_aggregate_split_replacer(
                    udf_expr(
                        "to_char",
                        vec![
                            cast_expr(
                                fun_expr(
                                    "DateTrunc",
                                    vec![
                                        literal_expr("?granularity"),
                                        cast_expr(
                                            cast_expr(literal_expr("?literal"), "?data_type_inner"),
                                            "?data_type_outer",
                                        ),
                                    ],
                                ),
                                "?date_trunc_data_type",
                            ),
                            literal_expr("?format"),
                        ],
                    ),
                    "?cube",
                ),
                literal_expr("?literal"),
            ),
            transforming_chain_rewrite(
                "split-push-down-to-char-date-trunc-literal-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?cube"),
                vec![(
                    "?expr",
                    udf_expr(
                        "to_char",
                        vec![
                            cast_expr(
                                fun_expr(
                                    "DateTrunc",
                                    vec![
                                        literal_expr("?granularity"),
                                        cast_expr(
                                            cast_expr(literal_expr("?literal"), "?data_type_inner"),
                                            "?data_type_outer",
                                        ),
                                    ],
                                ),
                                "?date_trunc_data_type",
                            ),
                            literal_expr("?format"),
                        ],
                    ),
                )],
                alias_expr(literal_expr("?literal"), "?alias"),
                self.make_alias_like_expression("?expr", "?alias"),
            ),
            transforming_chain_rewrite(
                "split-push-down-aggr-fun-with-date-trunc-inner-aggr-replacer",
                inner_aggregate_split_replacer(
                    agg_fun_expr("?fun", vec!["?expr".to_string()], "?distinct"),
                    "?cube",
                ),
                vec![(
                    "?expr",
                    fun_expr(
                        "DateTrunc",
                        vec![literal_expr("?granularity"), column_expr("?column")],
                    ),
                )],
                alias_expr(
                    fun_expr(
                        "DateTrunc",
                        vec![
                            literal_expr("?rewritten_granularity"),
                            column_expr("?column"),
                        ],
                    ),
                    "?alias",
                ),
                MemberRules::transform_original_expr_nested_date_trunc(
                    "?expr",
                    "?granularity",
                    "?granularity",
                    "?rewritten_granularity",
                    "?alias_column",
                    Some("?alias"),
                    true,
                ),
            ),
            transforming_chain_rewrite(
                "split-push-down-aggr-fun-with-date-trunc-outer-aggr-replacer",
                outer_aggregate_split_replacer(
                    agg_fun_expr("?fun", vec!["?expr".to_string()], "?distinct"),
                    "?cube",
                ),
                vec![(
                    "?expr",
                    fun_expr(
                        "DateTrunc",
                        vec![literal_expr("?granularity"), column_expr("?column")],
                    ),
                )],
                agg_fun_expr(
                    "?fun",
                    vec![alias_expr("?alias_column", "?alias")],
                    "?distinct",
                ),
                MemberRules::transform_original_expr_date_trunc(
                    "?expr",
                    "?granularity",
                    "?granularity",
                    "?alias_column",
                    Some("?alias"),
                    false,
                ),
            ),
            // Aggregate function
            transforming_rewrite(
                "split-push-down-aggr-fun-inner-replacer",
                inner_aggregate_split_replacer(
                    agg_fun_expr("?fun", vec![column_expr("?column")], "?distinct"),
                    "?cube",
                ),
                agg_fun_expr("?fun", vec![column_expr("?column")], "?distinct"),
                self.transform_inner_measure("?cube", Some("?column")),
            ),
            transforming_rewrite(
                "split-push-down-aggr-fun-inner-replacer-simple-count",
                inner_aggregate_split_replacer(
                    agg_fun_expr("?fun", vec![literal_expr("?literal")], "?distinct"),
                    "?cube",
                ),
                agg_fun_expr("?fun", vec![literal_expr("?literal")], "?distinct"),
                self.transform_inner_measure("?cube", None),
            ),
            transforming_rewrite(
                "split-push-down-aggr-fun-inner-replacer-missing-count",
                inner_aggregate_split_replacer(
                    agg_fun_expr("?fun", vec![literal_expr("?literal")], "?distinct"),
                    "?cube",
                ),
                aggr_aggr_expr_empty_tail(),
                self.transform_inner_measure_missing_count("?cube"),
            ),
            transforming_chain_rewrite(
                "split-push-down-aggr-fun-outer-replacer",
                outer_projection_split_replacer("?expr", "?cube"),
                vec![(
                    "?expr",
                    agg_fun_expr("?fun", vec![column_expr("?column")], "?distinct"),
                )],
                "?alias".to_string(),
                self.transform_outer_projection_aggr_fun("?cube", "?expr", "?column", "?alias"),
            ),
            transforming_chain_rewrite(
                "split-push-down-aggr-fun-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?cube"),
                vec![
                    ("?expr", agg_fun_expr("?fun", vec!["?arg"], "?distinct")),
                    ("?arg", column_expr("?column")),
                ],
                alias_expr(
                    agg_fun_expr("?output_fun", vec!["?alias".to_string()], "?distinct"),
                    "?outer_alias",
                ),
                self.transform_outer_aggr_fun(
                    "?cube",
                    "?expr",
                    "?fun",
                    "?arg",
                    Some("?column"),
                    "?alias",
                    "?outer_alias",
                    "?output_fun",
                    "?distinct",
                    false,
                    "?output_distinct",
                ),
            ),
            transforming_chain_rewrite(
                "split-push-down-aggr-fun-outer-aggr-replacer-simple-count",
                outer_aggregate_split_replacer("?expr", "?cube"),
                vec![
                    ("?expr", agg_fun_expr("?fun", vec!["?arg"], "?distinct")),
                    ("?arg", literal_expr("?literal")),
                ],
                alias_expr(
                    agg_fun_expr("?output_fun", vec!["?alias".to_string()], "?distinct"),
                    "?outer_alias",
                ),
                self.transform_outer_aggr_fun(
                    "?cube",
                    "?expr",
                    "?fun",
                    "?arg",
                    None,
                    "?alias",
                    "?outer_alias",
                    "?output_fun",
                    "?distinct",
                    false,
                    "?output_distinct",
                ),
            ),
            transforming_rewrite(
                "split-push-down-aggr-fun-outer-aggr-replacer-missing-count",
                outer_aggregate_split_replacer(
                    agg_fun_expr("?fun", vec![literal_expr("?literal")], "?distinct"),
                    "?cube",
                ),
                agg_fun_expr("?fun", vec![literal_expr("?literal")], "?distinct"),
                self.transform_outer_aggr_fun_missing_count("?cube", "?fun"),
            ),
            // TODO It replaces aggregate function with scalar one. This breaks Aggregate consistency.
            // Works because push down aggregate rule doesn't care about if it's in group by or aggregate.
            // Member types detected by column names.
            transforming_chain_rewrite(
                "split-push-down-aggr-min-max-date-trunc-fun-inner-replacer",
                inner_aggregate_split_replacer(
                    agg_fun_expr("?fun", vec!["?arg"], "?distinct"),
                    "?cube",
                ),
                vec![("?arg", column_expr("?column"))],
                alias_expr(
                    fun_expr(
                        "DateTrunc",
                        vec![literal_string("month"), column_expr("?column")],
                    ),
                    "?alias",
                ),
                self.transform_min_max_dimension(
                    "?cube", "?fun", "?arg", "?column", "?alias", true,
                ),
            ),
            transforming_chain_rewrite(
                "split-push-down-aggr-min-max-dimension-fun-inner-replacer",
                inner_aggregate_split_replacer(
                    agg_fun_expr("?fun", vec!["?arg"], "?distinct"),
                    "?cube",
                ),
                vec![("?arg", column_expr("?column"))],
                alias_expr("?arg", "?alias"),
                self.transform_min_max_dimension(
                    "?cube", "?fun", "?arg", "?column", "?alias", false,
                ),
            ),
            transforming_rewrite(
                "split-push-down-aggr-approx-distinct-dimension-fun-inner-replacer",
                inner_aggregate_split_replacer(
                    agg_fun_expr(
                        "ApproxDistinct",
                        vec![column_expr("?column")],
                        "AggregateFunctionExprDistinct:false",
                    ),
                    "?alias_to_cube",
                ),
                inner_aggregate_split_replacer(column_expr("?column"), "?alias_to_cube"),
                self.transform_inner_dimension("?alias_to_cube", "?column"),
            ),
            transforming_rewrite(
                "split-push-down-aggr-approx-distinct-dimension-fun-outer-aggr-replacer",
                outer_aggregate_split_replacer(
                    agg_fun_expr(
                        "ApproxDistinct",
                        vec![column_expr("?column")],
                        "AggregateFunctionExprDistinct:false",
                    ),
                    "?alias_to_cube",
                ),
                agg_fun_expr(
                    "ApproxDistinct",
                    vec![outer_aggregate_split_replacer(
                        column_expr("?column"),
                        "?alias_to_cube",
                    )],
                    "AggregateFunctionExprDistinct:false",
                ),
                self.transform_outer_aggr_dimension("?alias_to_cube", "?column"),
            ),
            // ?expr ?op literal_expr("?right")
            transforming_rewrite(
                "split-push-down-binary-inner-replacer",
                inner_aggregate_split_replacer(
                    binary_expr("?expr", "?original_op", literal_expr("?original_literal")),
                    "?cube",
                ),
                inner_aggregate_split_replacer("?expr", "?cube"),
                self.split_binary("?original_op", "?original_literal", None),
            ),
            transforming_rewrite(
                "split-push-down-binary-outer-replacer",
                outer_projection_split_replacer(
                    binary_expr("?expr", "?original_op", literal_expr("?original_literal")),
                    "?cube",
                ),
                binary_expr(
                    outer_projection_split_replacer("?expr", "?cube"),
                    "?op",
                    literal_expr("?literal"),
                ),
                self.split_binary(
                    "?original_op",
                    "?original_literal",
                    Some(("?op", "?literal")),
                ),
            ),
            transforming_rewrite(
                "split-push-down-binary-outer-aggr-replacer",
                outer_aggregate_split_replacer(
                    binary_expr("?expr", "?original_op", literal_expr("?original_literal")),
                    "?cube",
                ),
                binary_expr(
                    outer_aggregate_split_replacer("?expr", "?cube"),
                    "?op",
                    literal_expr("?literal"),
                ),
                self.split_binary(
                    "?original_op",
                    "?original_literal",
                    Some(("?op", "?literal")),
                ),
            ),
            // Floor
            rewrite(
                "split-push-down-floor-inner-aggr-replacer",
                inner_aggregate_split_replacer(fun_expr("Floor", vec!["?expr"]), "?cube"),
                inner_aggregate_split_replacer("?expr", "?cube"),
            ),
            rewrite(
                "split-push-down-floor-outer-aggr-replacer",
                outer_aggregate_split_replacer(fun_expr("Floor", vec!["?expr"]), "?cube"),
                fun_expr(
                    "Floor",
                    vec![outer_aggregate_split_replacer("?expr", "?cube")],
                ),
            ),
            // Cast
            rewrite(
                "split-push-down-cast-inner-replacer",
                inner_aggregate_split_replacer(cast_expr("?expr", "?data_type"), "?cube"),
                inner_aggregate_split_replacer("?expr", "?cube"),
            ),
            rewrite(
                "split-push-down-cast-outer-replacer",
                outer_projection_split_replacer(cast_expr("?expr", "?data_type"), "?cube"),
                cast_expr(
                    outer_projection_split_replacer("?expr", "?cube"),
                    "?data_type",
                ),
            ),
            rewrite(
                "split-push-down-cast-outer-aggr-replacer",
                outer_aggregate_split_replacer(cast_expr("?expr", "?data_type"), "?cube"),
                cast_expr(
                    outer_aggregate_split_replacer("?expr", "?cube"),
                    "?data_type",
                ),
            ),
            // Substring
            rewrite(
                "split-push-down-substr-inner-replacer",
                inner_aggregate_split_replacer(
                    fun_expr("Substr", vec!["?expr", "?from", "?to"]),
                    "?cube",
                ),
                inner_aggregate_split_replacer("?expr", "?cube"),
            ),
            // Alias
            rewrite(
                "split-push-down-alias-inner-replacer",
                inner_aggregate_split_replacer(alias_expr("?expr", "?alias"), "?cube"),
                inner_aggregate_split_replacer("?expr", "?cube"),
            ),
            rewrite(
                "split-push-down-alias-outer-replacer",
                outer_projection_split_replacer(alias_expr("?expr", "?alias"), "?cube"),
                alias_expr(outer_projection_split_replacer("?expr", "?cube"), "?alias"),
            ),
            rewrite(
                "split-push-down-alias-outer-aggr-replacer",
                outer_aggregate_split_replacer(alias_expr("?expr", "?alias"), "?cube"),
                alias_expr(outer_aggregate_split_replacer("?expr", "?cube"), "?alias"),
            ),
            rewrite(
                "split-push-down-alias-group-expr-replacer",
                group_expr_split_replacer(alias_expr("?expr", "?alias"), "?cube"),
                alias_expr(group_expr_split_replacer("?expr", "?cube"), "?alias"),
            ),
            rewrite(
                "split-push-down-alias-group-aggregate-replacer",
                group_aggregate_split_replacer(alias_expr("?expr", "?alias"), "?cube"),
                alias_expr(group_aggregate_split_replacer("?expr", "?cube"), "?alias"),
            ),
            rewrite(
                "split-push-down-alias-aggr-group-tail-replacer",
                alias_expr(aggr_group_expr_empty_tail(), "?alias"),
                aggr_group_expr_empty_tail(),
            ),
            rewrite(
                "split-push-down-alias-aggr-aggr-tail-replacer",
                alias_expr(aggr_aggr_expr_empty_tail(), "?alias"),
                aggr_aggr_expr_empty_tail(),
            ),
            // Trunc
            rewrite(
                "split-push-down-trunc-inner-replacer",
                inner_aggregate_split_replacer(fun_expr("Trunc", vec!["?expr"]), "?cube"),
                inner_aggregate_split_replacer("?expr", "?cube"),
            ),
            rewrite(
                "split-push-down-trunc-outer-aggr-replacer",
                outer_aggregate_split_replacer(fun_expr("Trunc", vec!["?expr"]), "?cube"),
                fun_expr(
                    "Trunc",
                    vec![outer_aggregate_split_replacer("?expr", "?cube")],
                ),
            ),
            // Ceil
            rewrite(
                "split-push-down-ceil-inner-replacer",
                inner_aggregate_split_replacer(fun_expr("Ceil", vec!["?expr"]), "?cube"),
                inner_aggregate_split_replacer("?expr", "?cube"),
            ),
            rewrite(
                "split-push-down-ceil-outer-aggr-replacer",
                outer_aggregate_split_replacer(fun_expr("Ceil", vec!["?expr"]), "?cube"),
                fun_expr(
                    "Ceil",
                    vec![outer_aggregate_split_replacer("?expr", "?cube")],
                ),
            ),
            // ToChar
            rewrite(
                "split-push-down-to-char-inner-replacer",
                inner_aggregate_split_replacer(
                    udf_expr(
                        "to_char",
                        vec!["?expr".to_string(), literal_expr("?format")],
                    ),
                    "?cube",
                ),
                inner_aggregate_split_replacer("?expr", "?cube"),
            ),
            // CharacterLength
            rewrite(
                "split-push-down-char-length-inner-replacer",
                inner_aggregate_split_replacer(fun_expr("CharacterLength", vec!["?expr"]), "?cube"),
                inner_aggregate_split_replacer("?expr", "?cube"),
            ),
            // IS NULL, IS NOT NULL
            rewrite(
                "split-push-down-is-null-inner-replacer",
                inner_aggregate_split_replacer(is_null_expr("?expr"), "?cube"),
                inner_aggregate_split_replacer("?expr", "?cube"),
            ),
            rewrite(
                "split-push-down-is-not-null-inner-replacer",
                inner_aggregate_split_replacer(is_not_null_expr("?expr"), "?cube"),
                inner_aggregate_split_replacer("?expr", "?cube"),
            ),
            // push up event_notification
            rewrite(
                "split-push-up-group-event-notification-left",
                aggr_group_expr(event_notification("?name", "?left", "?meta"), "?right"),
                event_notification("?name", aggr_group_expr("?left", "?right"), "?meta"),
            ),
            rewrite(
                "split-push-up-group-event-notification-right",
                aggr_group_expr("?left", event_notification("?name", "?right", "?meta")),
                event_notification("?name", aggr_group_expr("?left", "?right"), "?meta"),
            ),
            rewrite(
                "split-push-up-aggr-event-notification-left",
                aggr_aggr_expr(event_notification("?name", "?left", "?meta"), "?right"),
                event_notification("?name", aggr_aggr_expr("?left", "?right"), "?meta"),
            ),
            rewrite(
                "split-push-up-aggr-event-notification-right",
                aggr_aggr_expr("?left", event_notification("?name", "?right", "?meta")),
                event_notification("?name", aggr_aggr_expr("?left", "?right"), "?meta"),
            ),
            rewrite(
                "split-push-up-cast-event-notification",
                cast_expr(event_notification("?name", "?expr", "?meta"), "?data_type"),
                event_notification("?name", cast_expr("?expr", "?data_type"), "?meta"),
            ),
            rewrite(
                "split-push-up-binary-expr-event-notification-left",
                binary_expr(
                    event_notification("?name", "?left", "?meta"),
                    "?op",
                    "?right",
                ),
                event_notification("?name", binary_expr("?left", "?op", "?right"), "?meta"),
            ),
            rewrite(
                "split-push-up-binary-expr-event-notification-right",
                binary_expr(
                    "?left",
                    "?op",
                    event_notification("?name", "?right", "?meta"),
                ),
                event_notification("?name", binary_expr("?left", "?op", "?right"), "?meta"),
            ),
            rewrite(
                "split-push-up-datetrunc-event-notification-right",
                fun_expr(
                    "DateTrunc",
                    vec![
                        literal_expr("?granularity"),
                        event_notification("?name", "?expr", "?meta"),
                    ],
                ),
                event_notification(
                    "?name",
                    fun_expr(
                        "DateTrunc",
                        vec![literal_expr("?granularity"), "?expr".to_string()],
                    ),
                    "?meta",
                ),
            ),
            // split countDistinct rules
            transforming_rewrite(
                "split-date-part-year-notification",
                outer_aggregate_split_replacer(
                    fun_expr(
                        "DatePart",
                        vec![literal_string("YEAR"), column_expr("?column")],
                    ),
                    "?cube",
                ),
                event_notification(
                    literal_string("split-date-part-or-trunc-year-notification"),
                    outer_aggregate_split_replacer(
                        fun_expr(
                            "DatePart",
                            vec![literal_string("YEAR"), column_expr("?column")],
                        ),
                        "?cube",
                    ),
                    "?meta",
                ),
                self.meta_from_column("?column", "?meta"),
            ),
            transforming_rewrite(
                "split-date-part-month-notification",
                outer_aggregate_split_replacer(
                    fun_expr(
                        "DatePart",
                        vec![literal_string("MONTH"), column_expr("?column")],
                    ),
                    "?cube",
                ),
                event_notification(
                    literal_string("split-date-part-or-trunc-month-notification"),
                    outer_aggregate_split_replacer(
                        fun_expr(
                            "DatePart",
                            vec![literal_string("MONTH"), column_expr("?column")],
                        ),
                        "?cube",
                    ),
                    "?meta",
                ),
                self.meta_from_column("?column", "?meta"),
            ),
            transforming_rewrite(
                "split-datetrunc-year-notification",
                outer_aggregate_split_replacer(
                    fun_expr(
                        "DateTrunc",
                        vec![literal_string("year"), column_expr("?column")],
                    ),
                    "?cube",
                ),
                event_notification(
                    literal_string("split-date-part-or-trunc-year-notification"),
                    outer_aggregate_split_replacer(
                        fun_expr(
                            "DateTrunc",
                            vec![literal_string("year"), column_expr("?column")],
                        ),
                        "?cube",
                    ),
                    "?meta",
                ),
                self.meta_from_column("?column", "?meta"),
            ),
            transforming_rewrite(
                "split-datetrunc-month-notification",
                outer_aggregate_split_replacer(
                    fun_expr(
                        "DateTrunc",
                        vec![literal_string("month"), column_expr("?column")],
                    ),
                    "?cube",
                ),
                event_notification(
                    literal_string("split-date-part-or-trunc-month-notification"),
                    outer_aggregate_split_replacer(
                        fun_expr(
                            "DateTrunc",
                            vec![literal_string("month"), column_expr("?column")],
                        ),
                        "?cube",
                    ),
                    "?meta",
                ),
                self.meta_from_column("?column", "?meta"),
            ),
            transforming_chain_rewrite(
                "split-count-distinct-to-sum-notification",
                outer_aggregate_split_replacer("?agg_fun", "?cube"),
                vec![
                    ("?agg_fun", agg_fun_expr("?fun", vec!["?arg"], "?distinct")),
                    ("?arg", column_expr("?column")),
                    ("?fun", "AggregateFunctionExprFun:Count".to_string()),
                    (
                        "?distinct",
                        "AggregateFunctionExprDistinct:true".to_string(),
                    ),
                ],
                event_notification(
                    literal_string("split-count-distinct-to-sum-notification"),
                    alias_expr(
                        agg_fun_expr(
                            "Sum",
                            vec!["?alias".to_string()],
                            "AggregateFunctionExprDistinct:false".to_string(),
                        ),
                        "?outer_alias",
                    ),
                    "?meta",
                ),
                self.transform_count_distinct(
                    "?cube",
                    "?agg_fun",
                    "?arg",
                    Some("?column"),
                    "?alias",
                    "?outer_alias",
                    "?meta",
                ),
            ),
            transforming_rewrite(
                "split-count-distinct-with-year-and-month-notification-handler",
                aggregate(
                    "?cube_scan",
                    aggr_group_expr(
                        event_notification(
                            literal_expr("?first_notification_name"),
                            "?left",
                            "?first_meta",
                        ),
                        event_notification(
                            literal_expr("?second_notification_name"),
                            "?right",
                            "?second_meta",
                        ),
                    ),
                    event_notification(
                        literal_string("split-count-distinct-to-sum-notification"),
                        "?aggr_aggr_expr",
                        "?aggr_meta",
                    ),
                ),
                aggregate(
                    "?cube_scan",
                    aggr_group_expr("?left", "?right"),
                    "?aggr_aggr_expr",
                ),
                self.count_distinct_with_year_and_month_notification_handler(
                    vec![
                        "split-date-part-or-trunc-month-notification",
                        "split-date-part-or-trunc-year-notification",
                    ],
                    vec!["?first_notification_name", "?second_notification_name"],
                    vec!["?first_meta", "?second_meta"],
                    vec!["column_name"],
                ),
            ),
        ];

        // Combinator rules
        // Column
        rules.extend(
            self.outer_aggr_group_expr_aggr_combinator_rewrite(
                "split-push-down-column-replacer",
                |split_replacer| split_replacer(column_expr("?column"), "?cube"),
                |_| vec![],
                |_| column_expr("?column"),
                |_, _| true,
                false,
                true,
                true,
                None,
            )
            .into_iter(),
        );
        // DateTrunc
        rules.extend(
            self.outer_aggr_group_expr_aggr_combinator_rewrite(
                "split-push-down-date-trunc-replacer",
                |split_replacer| split_replacer("?expr", "?cube"),
                |_| {
                    vec![(
                        "?expr",
                        fun_expr(
                            "DateTrunc",
                            vec![literal_expr("?granularity"), column_expr("?column")],
                        ),
                    )]
                },
                |_| "?alias".to_string(),
                self.transform_original_expr_alias(
                    |egraph, id| {
                        var_iter!(egraph[id], OuterAggregateSplitReplacerAliasToCube)
                            .cloned()
                            .chain(
                                var_iter!(egraph[id], OuterProjectionSplitReplacerAliasToCube)
                                    .cloned(),
                            )
                            .chain(
                                var_iter!(egraph[id], GroupExprSplitReplacerAliasToCube).cloned(),
                            )
                            .chain(
                                var_iter!(egraph[id], GroupAggregateSplitReplacerAliasToCube)
                                    .cloned(),
                            )
                            .collect()
                    },
                    "?expr",
                    "?column",
                    "?cube",
                    "?alias",
                ),
                true,
                true,
                true,
                None,
            )
            .into_iter(),
        );
        // DatePart
        rules.extend(
            self.outer_aggr_group_expr_aggr_combinator_rewrite(
                "split-push-down-date-part-replacer",
                |split_replacer| {
                    split_replacer(
                        fun_expr(
                            "DatePart",
                            vec![literal_expr("?granularity"), "?expr".to_string()],
                        ),
                        "?cube",
                    )
                },
                |_| vec![("?expr", column_expr("?column"))],
                |_| {
                    fun_expr(
                        "DatePart",
                        vec![
                            literal_expr("?granularity"),
                            alias_expr("?alias_column", "?alias"),
                        ],
                    )
                },
                MemberRules::transform_original_expr_date_trunc(
                    "?expr",
                    "?granularity",
                    "?granularity",
                    "?alias_column",
                    Some("?alias"),
                    false,
                ),
                false,
                true,
                true,
                None,
            )
            .into_iter(),
        );
        // Substr
        rules.extend(
            self.outer_aggr_group_expr_aggr_combinator_rewrite(
                "split-push-down-substr-replacer",
                |split_replacer| {
                    split_replacer(fun_expr("Substr", vec!["?expr", "?from", "?to"]), "?cube")
                },
                |_| vec![],
                |split_replacer| {
                    fun_expr(
                        "Substr",
                        vec![
                            split_replacer("?expr".to_string(), "?cube"),
                            "?from".to_string(),
                            "?to".to_string(),
                        ],
                    )
                },
                |_, _| true,
                false,
                false,
                true,
                Some(vec![("?expr", column_expr("?column"))]),
            )
            .into_iter(),
        );
        // CharacterLength
        rules.extend(
            self.outer_aggr_group_expr_aggr_combinator_rewrite(
                "split-push-down-char-length-replacer",
                |split_replacer| {
                    split_replacer(fun_expr("CharacterLength", vec!["?expr"]), "?cube")
                },
                |_| vec![],
                |split_replacer| {
                    fun_expr(
                        "CharacterLength",
                        vec![split_replacer("?expr".to_string(), "?cube")],
                    )
                },
                |_, _| true,
                false,
                false,
                true,
                Some(vec![("?expr", column_expr("?column"))]),
            )
            .into_iter(),
        );
        // to_char
        rules.extend(
            self.outer_aggr_group_expr_aggr_combinator_rewrite(
                "split-push-down-to-char-replacer",
                |split_replacer| {
                    split_replacer(
                        udf_expr(
                            "to_char",
                            vec!["?expr".to_string(), literal_expr("?format")],
                        ),
                        "?cube",
                    )
                },
                |_| vec![],
                |split_replacer| {
                    udf_expr(
                        "to_char",
                        vec![
                            split_replacer("?expr".to_string(), "?cube"),
                            literal_expr("?format"),
                        ],
                    )
                },
                |_, _| true,
                false,
                false,
                true,
                Some(vec![("?expr", column_expr("?column"))]),
            )
            .into_iter(),
        );
        // IS NULL
        rules.extend(
            self.outer_aggr_group_expr_aggr_combinator_rewrite(
                "split-push-down-is-null-replacer",
                |split_replacer| split_replacer(is_null_expr("?expr"), "?cube"),
                |_| vec![],
                |split_replacer| is_null_expr(split_replacer("?expr".to_string(), "?cube")),
                |_, _| true,
                false,
                true,
                true,
                Some(vec![("?expr", column_expr("?column"))]),
            )
            .into_iter(),
        );
        // IS NOT NULL
        rules.extend(
            self.outer_aggr_group_expr_aggr_combinator_rewrite(
                "split-push-down-is-not-null-replacer",
                |split_replacer| split_replacer(is_not_null_expr("?expr"), "?cube"),
                |_| vec![],
                |split_replacer| is_not_null_expr(split_replacer("?expr".to_string(), "?cube")),
                |_, _| true,
                false,
                true,
                true,
                Some(vec![("?expr", column_expr("?column"))]),
            )
            .into_iter(),
        );

        rules
    }
}

impl SplitRules {
    pub fn new(cube_context: Arc<CubeContext>) -> Self {
        Self {
            cube_context: cube_context,
        }
    }

    /// Returns a vec with rules for outer aggregate, group expr and group aggregate split replacers.
    ///
    /// Subst prerequisites:
    /// - `?cube` is a required subst for split replacer `alias_to_cube` parameter;
    /// - `?column` subst must appear in either searcher or chain if `unwrap_agg_chain` is none,
    ///   and in `unwrap_agg_chain` itself otherwise;
    /// - `?output_fun` and `?distinct` are reserved, do not use in any of the parameters.
    ///
    /// For searcher, chain, and applier, the only parameter for closure is a split replacer generator function.
    /// For instance, in `|split_replacer| split_replacer("?expr", "?cube")` closure,
    /// `split_replacer` will be replaced with the correct replacer for the respective rule.
    ///
    /// The first boolean parameter, `outer_projection`, determines whether an `outer-projection` rewrite
    /// should be generated in addition to `outer-aggr`.
    ///
    /// The two parameters `is_measure` and `is_dimension` are two flags to enable
    /// the generation of measure-related and dimension-related rules respectively.
    ///
    /// The last parameter `unwrap_agg_chain` is used to find a column in an expresssion
    /// in order for that column to be extracted and used with `agg_fun_expr`. This is only applicable
    /// if `is_measure` is true.
    pub fn outer_aggr_group_expr_aggr_combinator_rewrite<'a, M, C, A, T, D: Display, DD: Display>(
        &self,
        base_name: &str,
        main_searcher: M,
        chain: C,
        applier: A,
        transform_fn: T,
        outer_projection: bool,
        is_measure: bool,
        is_dimension: bool,
        unwrap_agg_chain: Option<Vec<(&'a str, String)>>,
    ) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>
    where
        M: Fn(fn(D, DD) -> String) -> String,
        C: Fn(fn(D, DD) -> String) -> Vec<(&'a str, String)>,
        A: Fn(fn(D, DD) -> String) -> String,
        T: Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool
            + Sync
            + Send
            + Clone
            + 'static,
    {
        let mut rules = vec![transforming_chain_rewrite(
            &format!("{}-outer-aggr", base_name),
            main_searcher(outer_aggregate_split_replacer),
            chain(outer_aggregate_split_replacer),
            applier(outer_aggregate_split_replacer),
            transform_fn.clone(),
        )];

        if outer_projection {
            rules.extend(
                vec![transforming_chain_rewrite(
                    &format!("{}-outer-projection", base_name),
                    main_searcher(outer_projection_split_replacer),
                    chain(outer_projection_split_replacer),
                    applier(outer_projection_split_replacer),
                    transform_fn.clone(),
                )]
                .into_iter(),
            );
        }

        let unwrap_expr = unwrap_agg_chain.is_some();
        let column_subst = if unwrap_expr { None } else { Some("?column") };

        if is_measure {
            rules.extend(
                vec![
                    // Group expr -- skip measures
                    transforming_chain_rewrite(
                        &format!("{}-group-expr-measure", base_name),
                        main_searcher(group_expr_split_replacer),
                        chain(group_expr_split_replacer),
                        if unwrap_expr {
                            applier(group_expr_split_replacer)
                        } else {
                            aggr_group_expr_empty_tail()
                        },
                        self.transform_group_expr_measure(
                            "?cube",
                            column_subst,
                            transform_fn.clone(),
                        ),
                    ),
                    // Group aggr -- keep & wrap measures
                    transforming_chain_rewrite(
                        &format!("{}-group-aggr-measure", base_name),
                        main_searcher(group_aggregate_split_replacer),
                        chain(group_aggregate_split_replacer),
                        if unwrap_expr {
                            applier(group_aggregate_split_replacer)
                        } else {
                            agg_fun_expr(
                                "?output_fun",
                                vec![applier(group_aggregate_split_replacer)],
                                "?distinct",
                            )
                        },
                        self.transform_group_aggregate_measure(
                            "?cube",
                            column_subst,
                            if unwrap_expr {
                                None
                            } else {
                                Some("?output_fun")
                            },
                            if unwrap_expr { None } else { Some("?distinct") },
                            transform_fn.clone(),
                        ),
                    ),
                ]
                .into_iter(),
            );

            if let Some(unwrap_agg_chain) = unwrap_agg_chain {
                rules.extend(
                    vec![
                        rewrite(
                            &format!("{}-unwrap-group-expr-empty-tail", base_name),
                            applier(|_, _| aggr_group_expr_empty_tail()),
                            aggr_group_expr_empty_tail(),
                        ),
                        transforming_chain_rewrite(
                            &format!("{}-unwrap-group-aggr-agg-fun", base_name),
                            applier(|expr, _| agg_fun_expr("?output_fun", vec![expr], "?distinct")),
                            unwrap_agg_chain,
                            agg_fun_expr("?output_fun", vec![column_expr("?column")], "?distinct"),
                            |_, _| true,
                        ),
                    ]
                    .into_iter(),
                );
            }
        }

        if is_dimension {
            rules.extend(
                vec![
                    // Group expr -- keep dimensions
                    transforming_chain_rewrite(
                        &format!("{}-group-expr-dimension", base_name),
                        main_searcher(group_expr_split_replacer),
                        chain(group_expr_split_replacer),
                        applier(group_expr_split_replacer),
                        self.transform_group_expr_dimension(
                            "?cube",
                            column_subst,
                            transform_fn.clone(),
                        ),
                    ),
                    // Group aggr -- skip dimensions
                    transforming_chain_rewrite(
                        &format!("{}-group-aggr-dimension", base_name),
                        main_searcher(group_aggregate_split_replacer),
                        chain(group_aggregate_split_replacer),
                        if unwrap_expr {
                            applier(group_aggregate_split_replacer)
                        } else {
                            aggr_aggr_expr_empty_tail()
                        },
                        self.transform_group_aggregate_dimension(
                            "?cube",
                            column_subst,
                            transform_fn,
                        ),
                    ),
                ]
                .into_iter(),
            );

            if unwrap_expr {
                rules.extend(
                    vec![rewrite(
                        &format!("{}-unwrap-group-aggr-empty-tail", base_name),
                        applier(|_, _| aggr_aggr_expr_empty_tail()),
                        aggr_aggr_expr_empty_tail(),
                    )]
                    .into_iter(),
                );
            }
        }

        rules
    }

    pub fn transform_original_expr_to_alias_and_column(
        &self,
        original_expr_var: &'static str,
        out_alias_expr_var: &'static str,
        out_column_expr_var: Option<&'static str>,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let original_expr_var = var!(original_expr_var);
        let out_alias_expr_var = var!(out_alias_expr_var);
        let out_column_expr_var =
            out_column_expr_var.map(|out_column_expr_var| var!(out_column_expr_var));

        move |egraph, subst| {
            let original_expr_id = subst[original_expr_var];
            let res =
                egraph[original_expr_id]
                    .data
                    .original_expr
                    .as_ref()
                    .ok_or(CubeError::internal(format!(
                        "Original expr wasn't prepared for {:?}",
                        original_expr_id
                    )));

            if let Ok(expr) = res {
                // TODO unwrap
                let name = expr.name(&DFSchema::empty()).unwrap();
                let column = Column::from_name(name.to_string());

                subst.insert(
                    out_alias_expr_var,
                    egraph.add(LogicalPlanLanguage::AliasExprAlias(AliasExprAlias(name))),
                );

                if let Some(out_column_expr_var) = out_column_expr_var {
                    subst.insert(
                        out_column_expr_var,
                        egraph.add(LogicalPlanLanguage::ColumnExprColumn(ColumnExprColumn(
                            column,
                        ))),
                    );
                }

                return true;
            }

            false
        }
    }

    pub fn transform_original_expr_alias(
        &self,
        alias_to_cube_fn: fn(
            &EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
            Id,
        ) -> Vec<Vec<(String, String)>>,
        original_expr_var: &'static str,
        column_var: &'static str,
        alias_to_cube_var: &'static str,
        alias_expr_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool + Clone
    {
        let original_expr_var = var!(original_expr_var);
        let column_var = var!(column_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let alias_expr_var = var!(alias_expr_var);
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            let original_expr_id = subst[original_expr_var];
            let res =
                egraph[original_expr_id]
                    .data
                    .original_expr
                    .as_ref()
                    .ok_or(CubeError::internal(format!(
                        "Original expr wasn't prepared for {:?}",
                        original_expr_id
                    )));
            if let Ok(expr) = res {
                for alias_to_cube in alias_to_cube_fn(egraph, subst[alias_to_cube_var]) {
                    for column in var_iter!(egraph[subst[column_var]], ColumnExprColumn).cloned() {
                        if let Some((alias, _)) =
                            meta_context.find_cube_by_column(&alias_to_cube, &column)
                        {
                            // TODO unwrap
                            let name = expr.name(&DFSchema::empty()).unwrap();
                            let column1 = Column {
                                relation: Some(alias),
                                name: name.to_string(),
                            };
                            let alias = egraph.add(LogicalPlanLanguage::ColumnExprColumn(
                                ColumnExprColumn(column1),
                            ));
                            let alias_name = egraph.add(LogicalPlanLanguage::AliasExprAlias(
                                AliasExprAlias(name.to_string()),
                            ));
                            let column = egraph.add(LogicalPlanLanguage::ColumnExpr([alias]));
                            // TODO re-aliasing underlying column as it'll be fully qualified which will break outer alias in case date_trunc is wrapped in some other function
                            // TODO alias in plans should be generally no-op however there's no place in datafusion where it's used like that
                            let alias =
                                egraph.add(LogicalPlanLanguage::AliasExpr([column, alias_name]));
                            subst.insert(alias_expr_var, alias);
                            return true;
                        }
                    }
                }
            }
            false
        }
    }

    fn transform_min_max_dimension(
        &self,
        cube_expr_var: &'static str,
        fun_expr_var: &'static str,
        arg_expr_var: &'static str,
        column_var: &'static str,
        alias_var: &'static str,
        is_time_dimension: bool,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_expr_var = var!(cube_expr_var);
        let fun_expr_var = var!(fun_expr_var);
        let arg_expr_var = var!(arg_expr_var);
        let column_var = var!(column_var);
        let alias_var = var!(alias_var);
        let meta = self.cube_context.meta.clone();
        move |egraph, subst| {
            for alias_to_cube in var_iter!(
                egraph[subst[cube_expr_var]],
                InnerAggregateSplitReplacerAliasToCube
            )
            .cloned()
            {
                for fun in var_iter!(egraph[subst[fun_expr_var]], AggregateFunctionExprFun).cloned()
                {
                    if fun == AggregateFunction::Min || fun == AggregateFunction::Max {
                        for column in var_iter!(egraph[subst[column_var]], ColumnExprColumn) {
                            if let Some((_, cube)) =
                                meta.find_cube_by_column(&alias_to_cube, &column)
                            {
                                if let Some(dimension) = cube.lookup_dimension(&column.name) {
                                    if is_time_dimension && dimension._type == "time"
                                        || !is_time_dimension && dimension._type != "time"
                                    {
                                        if let Some(expr_name) =
                                            original_expr_name(egraph, subst[arg_expr_var])
                                        {
                                            subst.insert(
                                                alias_var,
                                                egraph.add(LogicalPlanLanguage::AliasExprAlias(
                                                    AliasExprAlias(expr_name),
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
            }
            false
        }
    }

    fn transform_inner_measure(
        &self,
        cube_expr_var: &'static str,
        column_var: Option<&'static str>,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_expr_var = var!(cube_expr_var);
        let column_var = column_var.map(|column_var| var!(column_var));
        let meta = self.cube_context.meta.clone();
        move |egraph, subst| {
            for alias_to_cube in var_iter!(
                egraph[subst[cube_expr_var]],
                InnerAggregateSplitReplacerAliasToCube
            )
            .cloned()
            {
                for column in column_var
                    .map(|column_var| {
                        var_iter!(egraph[subst[column_var]], ColumnExprColumn)
                            .map(|c| c.clone())
                            .collect()
                    })
                    .unwrap_or(vec![Column::from_name(
                        MemberRules::default_count_measure_name(),
                    )])
                {
                    if let Some((_, cube)) = meta.find_cube_by_column(&alias_to_cube, &column) {
                        if cube.lookup_measure(&column.name).is_some() {
                            return true;
                        }
                    }
                }
            }
            false
        }
    }

    fn transform_inner_dimension(
        &self,
        alias_to_cube_var: &'static str,
        column_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let column_var = var!(column_var);
        let meta = self.cube_context.meta.clone();
        move |egraph, subst| {
            for alias_to_cube in var_iter!(
                egraph[subst[alias_to_cube_var]],
                InnerAggregateSplitReplacerAliasToCube
            ) {
                for column in var_iter!(egraph[subst[column_var]], ColumnExprColumn) {
                    if let Some((_, cube)) = meta.find_cube_by_column(&alias_to_cube, &column) {
                        if cube.lookup_dimension(&column.name).is_some() {
                            return true;
                        }
                    }
                }
            }
            false
        }
    }

    fn transform_inner_measure_missing_count(
        &self,
        cube_expr_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_expr_var = var!(cube_expr_var);
        let meta = self.cube_context.meta.clone();
        move |egraph, subst| {
            for alias_to_cube in var_iter!(
                egraph[subst[cube_expr_var]],
                InnerAggregateSplitReplacerAliasToCube
            )
            .cloned()
            {
                let default_count_measure_name = MemberRules::default_count_measure_name();
                if let Some((_, cube)) = meta.find_cube_by_column(
                    &alias_to_cube,
                    &Column::from_name(default_count_measure_name.to_string()),
                ) {
                    if cube.lookup_measure(&default_count_measure_name).is_none() {
                        return true;
                    }
                } else {
                    return true;
                }
            }
            false
        }
    }

    fn transform_group_expr_measure<T>(
        &self,
        alias_to_cube_var: &'static str,
        column_var: Option<&'static str>,
        original_transform_fn: T,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool
    where
        T: Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool,
    {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let column_var = column_var.map(|column_var| var!(column_var));
        let meta = self.cube_context.meta.clone();
        move |egraph, subst| {
            if let Some(column_var) = column_var {
                for alias_to_cube in var_iter!(
                    egraph[subst[alias_to_cube_var]],
                    GroupExprSplitReplacerAliasToCube
                )
                .cloned()
                {
                    for column in var_iter!(egraph[subst[column_var]], ColumnExprColumn).cloned() {
                        if let Some((_, cube)) = meta.find_cube_by_column(&alias_to_cube, &column) {
                            if cube.lookup_measure(&column.name).is_some() {
                                return true;
                            }
                        }
                    }
                }

                return false;
            }

            original_transform_fn(egraph, subst)
        }
    }

    fn transform_group_expr_dimension<T>(
        &self,
        alias_to_cube_var: &'static str,
        column_var: Option<&'static str>,
        original_transform_fn: T,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool
    where
        T: Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool,
    {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let column_var = column_var.map(|column_var| var!(column_var));
        let meta = self.cube_context.meta.clone();
        move |egraph, subst| {
            if !original_transform_fn(egraph, subst) {
                return false;
            }

            if let Some(column_var) = column_var {
                for alias_to_cube in var_iter!(
                    egraph[subst[alias_to_cube_var]],
                    GroupExprSplitReplacerAliasToCube
                )
                .cloned()
                {
                    for column in var_iter!(egraph[subst[column_var]], ColumnExprColumn).cloned() {
                        if let Some((_, cube)) = meta.find_cube_by_column(&alias_to_cube, &column) {
                            if cube.lookup_dimension(&column.name).is_some()
                                || column.name == "__user"
                                || column.name == "__cubeJoinField"
                            {
                                return true;
                            }
                        }
                    }
                }

                return false;
            }

            true
        }
    }

    fn transform_group_aggregate_measure<T>(
        &self,
        alias_to_cube_var: &'static str,
        column_var: Option<&'static str>,
        output_fun_var: Option<&'static str>,
        distinct_var: Option<&'static str>,
        original_transform_fn: T,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool
    where
        T: Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool,
    {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let column_var = column_var.map(|column_var| var!(column_var));
        let output_fun_var = output_fun_var.map(|output_fun_var| var!(output_fun_var));
        let distinct_var = distinct_var.map(|distinct_var| var!(distinct_var));
        let meta = self.cube_context.meta.clone();
        move |egraph, subst| {
            if !original_transform_fn(egraph, subst) {
                return false;
            }

            if let (Some(column_var), Some(output_fun_var), Some(distinct_var)) =
                (column_var, output_fun_var, distinct_var)
            {
                for alias_to_cube in var_iter!(
                    egraph[subst[alias_to_cube_var]],
                    GroupAggregateSplitReplacerAliasToCube
                )
                .cloned()
                {
                    for column in var_iter!(egraph[subst[column_var]], ColumnExprColumn).cloned() {
                        if let Some((_, cube)) = meta.find_cube_by_column(&alias_to_cube, &column) {
                            if let Some(measure) = cube.lookup_measure(&column.name) {
                                if measure.agg_type.is_none() {
                                    continue;
                                }

                                let output_fun = match measure.agg_type.as_ref().unwrap().as_str() {
                                    "count" => AggregateFunction::Sum,
                                    "sum" => AggregateFunction::Sum,
                                    "min" => AggregateFunction::Min,
                                    "max" => AggregateFunction::Max,
                                    _ => continue,
                                };
                                subst.insert(
                                    output_fun_var,
                                    egraph.add(LogicalPlanLanguage::AggregateFunctionExprFun(
                                        AggregateFunctionExprFun(output_fun),
                                    )),
                                );
                                subst.insert(
                                    distinct_var,
                                    egraph.add(LogicalPlanLanguage::AggregateFunctionExprDistinct(
                                        AggregateFunctionExprDistinct(false),
                                    )),
                                );
                                return true;
                            }
                        }
                    }
                }

                return false;
            }

            true
        }
    }

    fn transform_group_aggregate_dimension<T>(
        &self,
        alias_to_cube_var: &'static str,
        column_var: Option<&'static str>,
        original_transform_fn: T,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool
    where
        T: Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool,
    {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let column_var = column_var.map(|column_var| var!(column_var));
        let meta = self.cube_context.meta.clone();
        move |egraph, subst| {
            if let Some(column_var) = column_var {
                for alias_to_cube in var_iter!(
                    egraph[subst[alias_to_cube_var]],
                    GroupAggregateSplitReplacerAliasToCube
                )
                .cloned()
                {
                    for column in var_iter!(egraph[subst[column_var]], ColumnExprColumn).cloned() {
                        if let Some((_, cube)) = meta.find_cube_by_column(&alias_to_cube, &column) {
                            if cube.lookup_dimension(&column.name).is_some()
                                || column.name == "__user"
                                || column.name == "__cubeJoinField"
                            {
                                return true;
                            }
                        }
                    }
                }

                return false;
            }

            original_transform_fn(egraph, subst)
        }
    }

    fn split_reaggregate_projection(
        &self,
        projection_expr_var: &'static str,
        alias_to_cube_var: &'static str,
        inner_aggregate_cube_var: &'static str,
        group_expr_cube_var: &'static str,
        group_aggregate_cube_var: &'static str,
        new_expr_var: &'static str,
        inner_projection_alias_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let projection_expr_var = var!(projection_expr_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let inner_aggregate_cube_var = var!(inner_aggregate_cube_var);
        let group_expr_cube_var = var!(group_expr_cube_var);
        let group_aggregate_cube_var = var!(group_aggregate_cube_var);
        let new_expr_var = var!(new_expr_var);
        let inner_projection_alias_var = var!(inner_projection_alias_var);
        move |egraph, subst| {
            if let Some(expr_to_alias) =
                &egraph.index(subst[projection_expr_var]).data.expr_to_alias
            {
                for alias_to_cube in
                    var_iter!(egraph[subst[alias_to_cube_var]], CubeScanAliasToCube).cloned()
                {
                    // Replace outer projection columns with unqualified variants
                    let expr = expr_to_alias
                        .clone()
                        .into_iter()
                        .map(|(_, a)| {
                            let column = Column::from_name(a);
                            let column_expr_column = egraph.add(
                                LogicalPlanLanguage::ColumnExprColumn(ColumnExprColumn(column)),
                            );
                            egraph.add(LogicalPlanLanguage::ColumnExpr([column_expr_column]))
                        })
                        .collect::<Vec<_>>();
                    let mut projection_expr =
                        egraph.add(LogicalPlanLanguage::ProjectionExpr(vec![]));
                    for i in expr.into_iter().rev() {
                        projection_expr = egraph.add(LogicalPlanLanguage::ProjectionExpr(vec![
                            i,
                            projection_expr,
                        ]));
                    }
                    subst.insert(new_expr_var, projection_expr);

                    subst.insert(
                        inner_projection_alias_var,
                        // Do not put alias on inner projection so table name from cube scan can be reused
                        egraph.add(LogicalPlanLanguage::ProjectionAlias(ProjectionAlias(None))),
                    );

                    subst.insert(
                        inner_aggregate_cube_var,
                        egraph.add(LogicalPlanLanguage::InnerAggregateSplitReplacerAliasToCube(
                            InnerAggregateSplitReplacerAliasToCube(alias_to_cube.clone()),
                        )),
                    );

                    subst.insert(
                        group_expr_cube_var,
                        egraph.add(LogicalPlanLanguage::GroupExprSplitReplacerAliasToCube(
                            GroupExprSplitReplacerAliasToCube(alias_to_cube.clone()),
                        )),
                    );

                    subst.insert(
                        group_aggregate_cube_var,
                        egraph.add(LogicalPlanLanguage::GroupAggregateSplitReplacerAliasToCube(
                            GroupAggregateSplitReplacerAliasToCube(alias_to_cube.clone()),
                        )),
                    );

                    return true;
                }
            }
            false
        }
    }

    fn split_projection_aggregate(
        &self,
        alias_to_cube_var: &'static str,
        inner_aggregate_cube_expr_var: &'static str,
        outer_projection_cube_expr_var: &'static str,
        projection_alias_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let inner_aggregate_cube_expr_var = var!(inner_aggregate_cube_expr_var);
        let outer_projection_cube_expr_var = var!(outer_projection_cube_expr_var);
        let projection_alias_var = var!(projection_alias_var);
        move |egraph, subst| {
            for alias_to_cube in
                var_iter!(egraph[subst[alias_to_cube_var]], CubeScanAliasToCube).cloned()
            {
                subst.insert(
                    projection_alias_var,
                    // Do not put alias on inner projection so table name from cube scan can be reused
                    egraph.add(LogicalPlanLanguage::ProjectionAlias(ProjectionAlias(None))),
                );

                subst.insert(
                    inner_aggregate_cube_expr_var,
                    egraph.add(LogicalPlanLanguage::InnerAggregateSplitReplacerAliasToCube(
                        InnerAggregateSplitReplacerAliasToCube(alias_to_cube.clone()),
                    )),
                );

                subst.insert(
                    outer_projection_cube_expr_var,
                    egraph.add(
                        LogicalPlanLanguage::OuterProjectionSplitReplacerAliasToCube(
                            OuterProjectionSplitReplacerAliasToCube(alias_to_cube.clone()),
                        ),
                    ),
                );
                return true;
            }
            false
        }
    }

    fn split_date_trunc(
        &self,
        granularity_var: &'static str,
        out_granularity_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let granularity_var = var!(granularity_var);
        let out_granularity_var = var!(out_granularity_var);

        move |egraph, subst| {
            for granularity in var_iter!(egraph[subst[granularity_var]], LiteralExprValue) {
                let output_granularity = match utils::parse_granularity(granularity, false) {
                    Some(g) => g,
                    None => continue,
                };

                subst.insert(
                    out_granularity_var,
                    egraph.add(LogicalPlanLanguage::LiteralExprValue(LiteralExprValue(
                        ScalarValue::Utf8(Some(output_granularity)),
                    ))),
                );

                return true;
            }

            return false;
        }
    }

    fn split_binary(
        &self,
        binary_op_var: &'static str,
        literal_expr_var: &'static str,
        return_vars: Option<(&'static str, &'static str)>,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let binary_op_var = var!(binary_op_var);
        let literal_expr_var = var!(literal_expr_var);

        move |egraph, subst| {
            for operator in var_iter!(egraph[subst[binary_op_var]], BinaryExprOp).cloned() {
                let check_is_zero = match operator {
                    Operator::Plus | Operator::Minus | Operator::Divide => false,
                    Operator::Multiply => true,
                    _ => continue,
                };

                for scalar in var_iter!(egraph[subst[literal_expr_var]], LiteralExprValue).cloned()
                {
                    // This match is re-used to verify literal_expr type
                    let is_zero = match scalar {
                        ScalarValue::UInt64(Some(v)) => v == 0,
                        ScalarValue::UInt32(Some(v)) => v == 0,
                        ScalarValue::UInt16(Some(v)) => v == 0,
                        ScalarValue::UInt8(Some(v)) => v == 0,
                        ScalarValue::Int64(Some(v)) => v == 0,
                        ScalarValue::Int32(Some(v)) => v == 0,
                        ScalarValue::Int16(Some(v)) => v == 0,
                        ScalarValue::Int8(Some(v)) => v == 0,
                        ScalarValue::Float32(Some(v)) => v == 0.0,
                        ScalarValue::Float64(Some(v)) => v == 0.0,
                        _ => continue,
                    };

                    if check_is_zero && is_zero {
                        continue;
                    }

                    if let Some((return_binary_op_var, return_literal_expr_var)) = return_vars {
                        let return_binary_op_var = var!(return_binary_op_var);
                        let return_literal_expr_var = var!(return_literal_expr_var);

                        subst.insert(
                            return_binary_op_var,
                            egraph.add(LogicalPlanLanguage::BinaryExprOp(BinaryExprOp(operator))),
                        );

                        subst.insert(
                            return_literal_expr_var,
                            egraph.add(LogicalPlanLanguage::LiteralExprValue(LiteralExprValue(
                                scalar,
                            ))),
                        );
                    }

                    return true;
                }
            }

            false
        }
    }

    fn split_aggregate_aggregate(
        &self,
        alias_to_cube_var: &'static str,
        inner_aggregate_cube_expr_var: &'static str,
        outer_aggregate_cube_expr_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let inner_aggregate_cube_expr_var = var!(inner_aggregate_cube_expr_var);
        let outer_aggregate_cube_expr_var = var!(outer_aggregate_cube_expr_var);
        move |egraph, subst| {
            for alias_to_cube in
                var_iter!(egraph[subst[alias_to_cube_var]], CubeScanAliasToCube).cloned()
            {
                subst.insert(
                    inner_aggregate_cube_expr_var,
                    egraph.add(LogicalPlanLanguage::InnerAggregateSplitReplacerAliasToCube(
                        InnerAggregateSplitReplacerAliasToCube(alias_to_cube.clone()),
                    )),
                );

                subst.insert(
                    outer_aggregate_cube_expr_var,
                    egraph.add(LogicalPlanLanguage::OuterAggregateSplitReplacerAliasToCube(
                        OuterAggregateSplitReplacerAliasToCube(alias_to_cube.clone()),
                    )),
                );

                return true;
            }
            false
        }
    }

    fn transform_outer_aggr_dimension(
        &self,
        alias_to_cube_var: &'static str,
        column_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let column_var = var!(column_var);
        let meta = self.cube_context.meta.clone();
        move |egraph, subst| {
            for alias_to_cube in var_iter!(
                egraph[subst[alias_to_cube_var]],
                OuterAggregateSplitReplacerAliasToCube
            ) {
                for column in var_iter!(egraph[subst[column_var]], ColumnExprColumn) {
                    if let Some((_, cube)) = meta.find_cube_by_column(&alias_to_cube, &column) {
                        if cube.lookup_dimension(&column.name).is_some() {
                            return true;
                        }
                    }
                }
            }
            false
        }
    }

    pub fn transform_outer_aggr_fun(
        &self,
        cube_var: &'static str,
        original_expr_var: &'static str,
        fun_expr_var: &'static str,
        arg_var: &'static str,
        column_var: Option<&'static str>,
        alias_expr_var: &'static str,
        outer_alias_expr_var: &'static str,
        output_fun_var: &'static str,
        distinct_var: &'static str,
        allow_count_distinct: bool,
        output_distinct_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_var = var!(cube_var);
        let original_expr_var = var!(original_expr_var);
        let fun_expr_var = var!(fun_expr_var);
        let arg_var = var!(arg_var);
        let column_var = column_var.map(|column_var| var!(column_var));
        let alias_expr_var = var!(alias_expr_var);
        let outer_alias_expr_var = var!(outer_alias_expr_var);
        let output_fun_var = var!(output_fun_var);
        let distinct_var = var!(distinct_var);
        let output_distinct_var = var!(output_distinct_var);
        let meta = self.cube_context.meta.clone();
        move |egraph, subst| {
            for fun in var_iter!(egraph[subst[fun_expr_var]], AggregateFunctionExprFun) {
                for distinct in
                    var_iter!(egraph[subst[distinct_var]], AggregateFunctionExprDistinct)
                {
                    let output_fun = match fun {
                        AggregateFunction::Count if *distinct && !allow_count_distinct => continue,
                        AggregateFunction::Count => AggregateFunction::Sum,
                        AggregateFunction::Sum => AggregateFunction::Sum,
                        AggregateFunction::Min => AggregateFunction::Min,
                        AggregateFunction::Max => AggregateFunction::Max,
                        _ => continue,
                    };

                    for alias_to_cube in var_iter!(
                        egraph[subst[cube_var]],
                        OuterAggregateSplitReplacerAliasToCube
                    )
                    .cloned()
                    {
                        for column in column_var
                            .map(|column_var| {
                                var_iter!(egraph[subst[column_var]], ColumnExprColumn)
                                    .cloned()
                                    .collect()
                            })
                            .unwrap_or(vec![Column::from_name(
                                MemberRules::default_count_measure_name(),
                            )])
                        {
                            let (name, cube) = match (
                                original_expr_name(egraph, subst[original_expr_var]),
                                meta.find_cube_by_column(&alias_to_cube, &column),
                            ) {
                                (Some(name), Some((_, cube))) => (name, cube),
                                _ => continue,
                            };

                            let inner_and_outer_alias: Option<(String, String)> =
                                if cube.lookup_measure(&column.name).is_some() {
                                    Some((name.to_string(), name.to_string()))
                                } else if cube.lookup_dimension(&column.name).is_some() {
                                    original_expr_name(egraph, subst[arg_var])
                                        .map(|inner| (inner, name.to_string()))
                                } else {
                                    None
                                };

                            if let Some((inner_alias, outer_alias)) = inner_and_outer_alias {
                                let alias = egraph.add(LogicalPlanLanguage::ColumnExprColumn(
                                    ColumnExprColumn(Column::from_name(inner_alias.to_string())),
                                ));
                                subst.insert(
                                    alias_expr_var,
                                    egraph.add(LogicalPlanLanguage::ColumnExpr([alias])),
                                );
                                subst.insert(
                                    outer_alias_expr_var,
                                    egraph.add(LogicalPlanLanguage::AliasExprAlias(
                                        AliasExprAlias(outer_alias.to_string()),
                                    )),
                                );
                                subst.insert(
                                    output_fun_var,
                                    egraph.add(LogicalPlanLanguage::AggregateFunctionExprFun(
                                        AggregateFunctionExprFun(output_fun),
                                    )),
                                );

                                if allow_count_distinct {
                                    subst.insert(
                                        output_distinct_var,
                                        egraph.add(
                                            LogicalPlanLanguage::AggregateFunctionExprDistinct(
                                                AggregateFunctionExprDistinct(false),
                                            ),
                                        ),
                                    );
                                }

                                return true;
                            }
                        }
                    }
                }
            }

            false
        }
    }

    pub fn transform_count_distinct(
        &self,
        cube_var: &'static str,
        original_expr_var: &'static str,
        arg_var: &'static str,
        column_var: Option<&'static str>,
        alias_expr_var: &'static str,
        outer_alias_expr_var: &'static str,
        meta_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_var = var!(cube_var);
        let original_expr_var = var!(original_expr_var);
        let arg_var = var!(arg_var);
        let column_var = column_var.map(|column_var| var!(column_var));
        let alias_expr_var = var!(alias_expr_var);
        let outer_alias_expr_var = var!(outer_alias_expr_var);
        let meta_var = var!(meta_var);
        let meta = self.cube_context.meta.clone();
        move |egraph, subst| {
            for alias_to_cube in var_iter!(
                egraph[subst[cube_var]],
                OuterAggregateSplitReplacerAliasToCube
            )
            .cloned()
            {
                for column in column_var
                    .map(|column_var| {
                        var_iter!(egraph[subst[column_var]], ColumnExprColumn)
                            .cloned()
                            .collect()
                    })
                    .unwrap_or(vec![Column::from_name(
                        MemberRules::default_count_measure_name(),
                    )])
                {
                    let (name, cube) = match (
                        original_expr_name(egraph, subst[original_expr_var]),
                        meta.find_cube_by_column(&alias_to_cube, &column),
                    ) {
                        (Some(name), Some((_, cube))) => (name, cube),
                        _ => continue,
                    };

                    let inner_and_outer_alias: Option<(String, String)> =
                        if cube.lookup_measure(&column.name).is_some() {
                            Some((name.to_string(), name.to_string()))
                        } else if cube.lookup_dimension(&column.name).is_some() {
                            original_expr_name(egraph, subst[arg_var])
                                .map(|inner| (inner, name.to_string()))
                        } else {
                            None
                        };

                    if let Some((inner_alias, outer_alias)) = inner_and_outer_alias {
                        let alias = egraph.add(LogicalPlanLanguage::ColumnExprColumn(
                            ColumnExprColumn(Column::from_name(inner_alias.to_string())),
                        ));
                        subst.insert(
                            alias_expr_var,
                            egraph.add(LogicalPlanLanguage::ColumnExpr([alias])),
                        );
                        subst.insert(
                            outer_alias_expr_var,
                            egraph.add(LogicalPlanLanguage::AliasExprAlias(AliasExprAlias(
                                outer_alias.to_string(),
                            ))),
                        );

                        subst.insert(
                            meta_var,
                            egraph.add(LogicalPlanLanguage::EventNotificationMeta(
                                EventNotificationMeta(None),
                            )),
                        );

                        return true;
                    }
                }
            }

            false
        }
    }

    pub fn transform_outer_aggr_fun_missing_count(
        &self,
        cube_var: &'static str,
        fun_expr_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_var = var!(cube_var);
        let fun_expr_var = var!(fun_expr_var);
        let meta = self.cube_context.meta.clone();
        move |egraph, subst| {
            for fun in var_iter!(egraph[subst[fun_expr_var]], AggregateFunctionExprFun) {
                if fun == &AggregateFunction::Count || fun == &AggregateFunction::Sum {
                    for alias_to_cube in var_iter!(
                        egraph[subst[cube_var]],
                        OuterAggregateSplitReplacerAliasToCube
                    )
                    .cloned()
                    {
                        let default_count_measure_name = MemberRules::default_count_measure_name();
                        if let Some((_, cube)) = meta.find_cube_by_column(
                            &alias_to_cube,
                            &Column::from_name(default_count_measure_name.to_string()),
                        ) {
                            if cube.lookup_measure(&default_count_measure_name).is_none() {
                                return true;
                            }
                        } else {
                            return true;
                        }
                    }
                }
            }
            false
        }
    }

    pub fn transform_outer_projection_aggr_fun(
        &self,
        cube_var: &'static str,
        original_expr_var: &'static str,
        column_var: &'static str,
        alias_expr_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_var = var!(cube_var);
        let original_expr_var = var!(original_expr_var);
        let column_var = var!(column_var);
        let alias_expr_var = var!(alias_expr_var);
        let meta = self.cube_context.meta.clone();
        move |egraph, subst| {
            for alias_to_cube in var_iter!(
                egraph[subst[cube_var]],
                OuterAggregateSplitReplacerAliasToCube
            )
            .cloned()
            {
                if let Some(name) = original_expr_name(egraph, subst[original_expr_var]) {
                    for column in var_iter!(egraph[subst[column_var]], ColumnExprColumn).cloned() {
                        if let Some((_, cube)) = meta.find_cube_by_column(&alias_to_cube, &column) {
                            if cube.lookup_measure(&column.name).is_some() {
                                let alias = egraph.add(LogicalPlanLanguage::ColumnExprColumn(
                                    ColumnExprColumn(Column::from_name(name.to_string())),
                                ));
                                subst.insert(
                                    alias_expr_var,
                                    egraph.add(LogicalPlanLanguage::ColumnExpr([alias])),
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

    fn transform_aggr_fun_with_literal(
        &self,
        fun_expr_var: &'static str,
        expr_val: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let fun_expr_var = var!(fun_expr_var);
        let expr_val = var!(expr_val);
        move |egraph, subst| {
            for fun in var_iter!(egraph[subst[fun_expr_var]], AggregateFunctionExprFun) {
                match fun {
                    AggregateFunction::Count | AggregateFunction::Sum => (),
                    _ => return true,
                };

                for expr in var_iter!(egraph[subst[expr_val]], LiteralExprValue) {
                    match expr {
                        ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) => return true,
                        _ => (),
                    }
                }
            }

            false
        }
    }

    fn make_alias_like_expression(
        &self,
        expr_val: &'static str,
        alias_val: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let alias_val = var!(alias_val);
        let expr_val = var!(expr_val);
        move |egraph, subst| {
            let original_expr_id = subst[expr_val];
            let res =
                egraph[original_expr_id]
                    .data
                    .original_expr
                    .as_ref()
                    .ok_or(CubeError::internal(format!(
                        "Original expr wasn't prepared for {:?}",
                        original_expr_id
                    )));
            if let Ok(expr) = res {
                let name = expr.name(&DFSchema::empty()).unwrap();
                subst.insert(
                    alias_val,
                    egraph.add(LogicalPlanLanguage::AliasExprAlias(AliasExprAlias(name))),
                );
                return true;
            }

            false
        }
    }

    fn count_distinct_with_year_and_month_notification_handler(
        &self,
        vector: Vec<&'static str>,
        literal_strings: Vec<&'static str>,
        metas: Vec<&'static str>,
        keys: Vec<&'static str>,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let literal_strings = literal_strings
            .into_iter()
            .map(|l| var!(l))
            .collect::<Vec<Var>>();
        let metas = metas.into_iter().map(|m| var!(m)).collect::<Vec<Var>>();
        move |egraph, subst| {
            for literal_var in literal_strings.clone() {
                let mut includes = false;
                for literal_value in var_iter!(egraph[subst[literal_var]], LiteralExprValue) {
                    match literal_value {
                        ScalarValue::Utf8(Some(str)) | ScalarValue::LargeUtf8(Some(str))
                            if vector.contains(&str.as_str()) =>
                        {
                            includes = true;
                            break;
                        }
                        _ => (),
                    }
                }

                if !includes {
                    return false;
                }
            }

            let mut metas = metas.iter().cloned();
            for key in keys.clone() {
                let first_meta = match metas.next() {
                    Some(val) => val,
                    None => continue,
                };

                for first_meta in var_iter!(egraph[subst[first_meta]], EventNotificationMeta) {
                    let first_meta = match first_meta {
                        Some(val) => val,
                        None => continue,
                    };
                    let first_value = match first_meta.iter().find(|(k, _)| k == key) {
                        Some((_, val)) => val,
                        _ => continue,
                    };

                    for meta in metas.clone() {
                        let mut found = false;
                        for meta in var_iter!(egraph[subst[meta]], EventNotificationMeta) {
                            let meta = match meta {
                                Some(val) => val,
                                None => continue,
                            };

                            match meta.iter().find(|(k, _)| k == key) {
                                Some((_, val)) if val == first_value => {
                                    found = true;
                                    break;
                                }
                                _ => continue,
                            };
                        }

                        if !found {
                            return false;
                        }
                    }
                }
            }

            return true;
        }
    }

    fn meta_from_column(
        &self,
        column_var: &'static str,
        meta_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let column_var = var!(column_var);
        let meta_var = var!(meta_var);
        move |egraph, subst| {
            for column in var_iter!(egraph[subst[column_var]], ColumnExprColumn).cloned() {
                subst.insert(
                    meta_var,
                    egraph.add(LogicalPlanLanguage::EventNotificationMeta(
                        EventNotificationMeta(Some(vec![(
                            "column_name".to_string(),
                            column.flat_name(),
                        )])),
                    )),
                );

                return true;
            }

            return false;
        }
    }
}
