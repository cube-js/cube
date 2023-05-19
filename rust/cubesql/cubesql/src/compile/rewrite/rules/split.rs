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
            literal_float, literal_int, literal_string, original_expr_name,
            outer_aggregate_split_replacer, outer_projection_split_replacer, projection,
            projection_expr, projection_expr_empty_tail, rewrite, rewriter::RewriteRules,
            rules::members::MemberRules, transforming_chain_rewrite, transforming_rewrite,
            udf_expr, AggregateFunctionExprDistinct, AggregateFunctionExprFun, AliasExprAlias,
            BinaryExprOp, CastExprDataType, ColumnExprColumn, CubeScanAliasToCube,
            EventNotificationMeta, GroupAggregateSplitReplacerAliasToCube,
            GroupExprSplitReplacerAliasToCube, InnerAggregateSplitReplacerAliasToCube,
            LiteralExprValue, LogicalPlanLanguage, OuterAggregateSplitReplacerAliasToCube,
            OuterProjectionSplitReplacerAliasToCube, ProjectionAlias, ScalarFunctionExprFun,
        },
    },
    transport::{V1CubeMetaExt, V1CubeMetaMeasureExt},
    var, var_iter, CubeError,
};
use datafusion::{
    arrow::datatypes::DataType as ArrowDataType,
    logical_plan::{Column, DFSchema, Expr, Operator},
    physical_plan::{aggregates::AggregateFunction, functions::BuiltinScalarFunction},
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
                        "CubeScanWrapped:false",
                    ),
                    "?group_expr",
                    "?aggr_expr",
                    "AggregateSplit:false",
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
                            "CubeScanWrapped:false",
                        ),
                        inner_aggregate_split_replacer("?group_expr", "?inner_aggregate_cube"),
                        inner_aggregate_split_replacer("?aggr_expr", "?inner_aggregate_cube"),
                        "AggregateSplit:true",
                    ),
                    "?projection_alias",
                    "ProjectionSplit:true",
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
                        "CubeScanWrapped:false",
                    ),
                    "?alias",
                    "ProjectionSplit:false",
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
                            "CubeScanWrapped:false",
                        ),
                        "?projection_alias",
                        "ProjectionSplit:true",
                    ),
                    "?alias",
                    "ProjectionSplit:true",
                ),
                self.split_projection_aggregate(
                    "?alias_to_cube",
                    "?inner_aggregate_cube",
                    "?outer_projection_cube",
                    "?projection_alias",
                ),
            ),
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
                        "CubeScanWrapped:false",
                    ),
                    "?alias",
                    "ProjectionSplit:false",
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
                                "CubeScanWrapped:false",
                            ),
                            "?inner_projection_alias",
                            "ProjectionSplit:true",
                        ),
                        group_expr_split_replacer("?expr", "?group_expr_cube"),
                        group_aggregate_split_replacer("?expr", "?group_aggregate_cube"),
                        "AggregateSplit:true",
                    ),
                    "?alias",
                    "ProjectionSplit:true",
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
                        "CubeScanWrapped:false",
                    ),
                    "?group_expr",
                    "?aggr_expr",
                    "AggregateSplit:false",
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
                            "CubeScanWrapped:false",
                        ),
                        inner_aggregate_split_replacer("?group_expr", "?inner_aggregate_cube"),
                        inner_aggregate_split_replacer("?aggr_expr", "?inner_aggregate_cube"),
                        "AggregateSplit:true",
                    ),
                    outer_aggregate_split_replacer("?group_expr", "?outer_aggregate_cube"),
                    outer_aggregate_split_replacer("?aggr_expr", "?outer_aggregate_cube"),
                    "AggregateSplit:true",
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
            transforming_chain_rewrite(
                "split-push-down-date-trunc-inner-replacer",
                inner_aggregate_split_replacer("?expr", "?alias_to_cube"),
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
                // To validate & de-aliasing granularity
                self.transform_original_expr_to_alias_and_column_with_chain_transform(
                    "?expr",
                    "?alias",
                    None,
                    self.split_date_trunc("?granularity", "?rewritten_granularity"),
                ),
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
                                literal_int(3),
                            ),
                            "%",
                            literal_int(7),
                        ),
                        "+",
                        literal_int(1),
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
                                literal_int(3),
                            ),
                            "%",
                            literal_int(7),
                        ),
                        "+",
                        literal_int(1),
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
                                                    literal_int(1),
                                                ),
                                                "*",
                                                literal_int(-1),
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
                                                                literal_int(100),
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
                                                        literal_int(100),
                                                    ),
                                                    "+",
                                                    literal_int(1),
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
                        literal_int(1),
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
                                                    literal_int(1),
                                                ),
                                                "*",
                                                literal_int(-1),
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
                                                                literal_int(100),
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
                                                        literal_int(100),
                                                    ),
                                                    "+",
                                                    literal_int(1),
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
                        literal_int(1),
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
            // (EXTRACT(DAY FROM "ta_1"."LO_ORDERDATE") = 15.0)
            transforming_chain_rewrite(
                "split-push-down-datepart-equals-literal-inner-replacer",
                inner_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    binary_expr(
                        fun_expr(
                            "DatePart",
                            vec![literal_expr("?granularity"), column_expr("?column")],
                        ),
                        "=",
                        literal_expr("?literal"),
                    ),
                )],
                alias_expr(
                    fun_expr(
                        "DateTrunc",
                        vec![literal_expr("?granularity"), column_expr("?column")],
                    ),
                    "?outer_alias",
                ),
                self.transform_original_expr_to_alias_and_column("?expr", "?outer_alias", None),
            ),
            transforming_chain_rewrite(
                "split-push-down-datepart-equals-literal-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    binary_expr(
                        fun_expr(
                            "DatePart",
                            vec![literal_expr("?granularity"), column_expr("?column")],
                        ),
                        "=",
                        literal_expr("?literal"),
                    ),
                )],
                alias_expr(
                    binary_expr(
                        fun_expr(
                            "DatePart",
                            vec![literal_expr("?granularity"), column_expr("?outer_column")],
                        ),
                        "=",
                        literal_expr("?literal"),
                    ),
                    "?outer_alias",
                ),
                self.transform_original_expr_to_alias_and_column(
                    "?expr",
                    "?outer_alias",
                    Some("?outer_column"),
                ),
            ),
            // (((EXTRACT(MONTH FROM "ta_1"."LO_COMMITDATE") - 1) % 3) + 1)
            transforming_chain_rewrite(
                "split-push-down-datepart-month-in-quarter-inner-replacer",
                inner_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    binary_expr(
                        binary_expr(
                            binary_expr(
                                fun_expr(
                                    "DatePart",
                                    vec![literal_string("MONTH"), column_expr("?column")],
                                ),
                                "-",
                                literal_int(1),
                            ),
                            "%",
                            literal_int(3),
                        ),
                        "+",
                        literal_int(1),
                    ),
                )],
                alias_expr(
                    fun_expr(
                        "DateTrunc",
                        vec![literal_string("MONTH"), column_expr("?column")],
                    ),
                    "?outer_alias",
                ),
                self.transform_original_expr_to_alias_and_column("?expr", "?outer_alias", None),
            ),
            transforming_chain_rewrite(
                "split-push-down-datepart-month-in-quarter-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    binary_expr(
                        binary_expr(
                            binary_expr(
                                fun_expr(
                                    "DatePart",
                                    vec![literal_string("MONTH"), column_expr("?column")],
                                ),
                                "-",
                                literal_int(1),
                            ),
                            "%",
                            literal_int(3),
                        ),
                        "+",
                        literal_int(1),
                    ),
                )],
                alias_expr(
                    binary_expr(
                        binary_expr(
                            binary_expr(
                                fun_expr(
                                    "DatePart",
                                    vec![literal_string("MONTH"), column_expr("?outer_column")],
                                ),
                                "-",
                                literal_int(1),
                            ),
                            "%",
                            literal_int(3),
                        ),
                        "+",
                        literal_int(1),
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
                                literal_int(2),
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
                                literal_int(2),
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
                                literal_int(2),
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
            // Redshift CHARINDEX to STRPOS
            rewrite(
                "redshift-charindex-to-strpos",
                udf_expr(
                    "charindex",
                    vec!["?substring", "?string"],
                ),
                fun_expr(
                    "Strpos",
                    vec!["?string", "?substring"],
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
            transforming_chain_rewrite(
                "split-push-down-aggr-fun-inner-replacer",
                inner_aggregate_split_replacer(
                    "?aggr_expr",
                    "?cube",
                ),
                vec![("?aggr_expr", agg_fun_expr("?fun", vec![column_expr("?column")], "?distinct"))],
                "?out_expr".to_string(),
                self.transform_inner_measure("?cube", Some("?column"), Some("?aggr_expr"), Some("?fun"), Some("?distinct"), Some("?out_expr")),
            ),
            transforming_rewrite(
                "split-push-down-aggr-fun-inner-replacer-simple-count",
                inner_aggregate_split_replacer(
                    agg_fun_expr("?fun", vec![literal_expr("?literal")], "?distinct"),
                    "?cube",
                ),
                agg_fun_expr("?fun", vec![literal_expr("?literal")], "?distinct"),
                self.transform_inner_measure("?cube", None, None, None, None, None),
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
                self.transform_outer_projection_aggr_fun("?cube", "?expr", Some("?column"), "?alias"),
            ),
            transforming_chain_rewrite(
                "split-push-down-aggr-fun-outer-replacer-simple-count",
                outer_projection_split_replacer("?expr", "?cube"),
                vec![(
                    "?expr",
                    agg_fun_expr("?fun", vec![literal_expr("?literal")], "?distinct"),
                )],
                "?alias".to_string(),
                self.transform_outer_projection_aggr_fun("?cube", "?expr", None, "?alias"),
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
            transforming_chain_rewrite(
                "split-push-down-aggr-fun-dateadd-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?cube"),
                vec![
                    ("?expr", agg_fun_expr("?fun", vec!["?arg"], "?distinct")),
                    (
                        "?arg",
                        udf_expr(
                            "dateadd",
                            vec![
                                literal_expr("?datepart"),
                                cast_expr_explicit(
                                    literal_expr("?interval_int"),
                                    ArrowDataType::Int32,
                                ),
                                column_expr("?column"),
                            ],
                        ),
                    ),
                ],
                alias_expr(
                    agg_fun_expr(
                        "?output_fun",
                        vec![udf_expr(
                            "dateadd",
                            vec![
                                literal_expr("?datepart"),
                                cast_expr_explicit(
                                    literal_expr("?interval_int"),
                                    ArrowDataType::Int32,
                                ),
                                "?alias".to_string(),
                            ],
                        )],
                        "?distinct",
                    ),
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
            // Aggregate function with cast argument
            rewrite(
                "split-push-down-aggr-fun-cast-arg-float64-inner-replacer",
                inner_aggregate_split_replacer(
                    agg_fun_expr(
                        "?fun",
                        vec![cast_expr_explicit(
                            column_expr("?column"),
                            ArrowDataType::Float64,
                        )],
                        "?distinct",
                    ),
                    "?alias_to_cube",
                ),
                inner_aggregate_split_replacer(
                    agg_fun_expr("?fun", vec![column_expr("?column")], "?distinct"),
                    "?alias_to_cube",
                ),
            ),
            transforming_chain_rewrite(
                "split-push-down-aggr-fun-cast-arg-float64-outer-replacer",
                outer_projection_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    agg_fun_expr(
                        "?fun",
                        vec![cast_expr_explicit(
                            column_expr("?column"),
                            ArrowDataType::Float64,
                        )],
                        "?distinct",
                    ),
                )],
                alias_expr(
                    cast_expr_explicit(
                        outer_projection_split_replacer(
                            agg_fun_expr("?fun", vec![column_expr("?column")], "?distinct"),
                            "?alias_to_cube",
                        ),
                        ArrowDataType::Float64,
                    ),
                    "?alias",
                ),
                self.transform_outer_aggr_agg_fun_cast_arg("?expr", "?alias"),
            ),
            transforming_chain_rewrite(
                "split-push-down-aggr-fun-cast-arg-float64-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    agg_fun_expr(
                        "?fun",
                        vec![cast_expr_explicit(
                            column_expr("?column"),
                            ArrowDataType::Float64,
                        )],
                        "?distinct",
                    ),
                )],
                alias_expr(
                    cast_expr_explicit(
                        outer_aggregate_split_replacer(
                            agg_fun_expr("?fun", vec![column_expr("?column")], "?distinct"),
                            "?alias_to_cube",
                        ),
                        ArrowDataType::Float64,
                    ),
                    "?alias",
                ),
                self.transform_outer_aggr_agg_fun_cast_arg("?expr", "?alias"),
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
                        vec![literal_string("day"), column_expr("?column")],
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
            transforming_chain_rewrite(
                "split-push-down-aggr-min-max-dimension-fun-dateadd-inner-replacer",
                inner_aggregate_split_replacer(
                    agg_fun_expr("?fun", vec!["?arg"], "?distinct"),
                    "?cube",
                ),
                vec![(
                    "?arg",
                    udf_expr(
                        "dateadd",
                        vec![
                            literal_expr("?datepart"),
                            cast_expr_explicit(literal_expr("?interval_int"), ArrowDataType::Int32),
                            column_expr("?column"),
                        ],
                    ),
                )],
                alias_expr(column_expr("?column"), "?alias"),
                self.transform_min_max_dimension(
                    "?cube", "?fun", "?arg", "?column", "?alias", true,
                ),
            ),
            // Thoughtspot [APPROXIMATE] COUNT(DISTINCT column) for search suggestions
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
            transforming_rewrite(
                "split-push-down-aggr-count-distinct-dimension-fun-inner-replacer",
                inner_aggregate_split_replacer(
                    agg_fun_expr(
                        "Count",
                        vec![column_expr("?column")],
                        "AggregateFunctionExprDistinct:true",
                    ),
                    "?alias_to_cube",
                ),
                inner_aggregate_split_replacer(column_expr("?column"), "?alias_to_cube"),
                self.transform_inner_dimension("?alias_to_cube", "?column"),
            ),
            transforming_rewrite(
                "split-push-down-aggr-count-distinct-dimension-fun-outer-aggr-replacer",
                outer_aggregate_split_replacer(
                    agg_fun_expr(
                        "Count",
                        vec![column_expr("?column")],
                        "AggregateFunctionExprDistinct:true",
                    ),
                    "?alias_to_cube",
                ),
                agg_fun_expr(
                    "Count",
                    vec![outer_aggregate_split_replacer(
                        column_expr("?column"),
                        "?alias_to_cube",
                    )],
                    "AggregateFunctionExprDistinct:true",
                ),
                self.transform_outer_aggr_dimension("?alias_to_cube", "?column"),
            ),
            // ?expr ?op literal_expr("?right")
            transforming_rewrite(
                "split-push-down-binary-inner-replacer",
                inner_aggregate_split_replacer(
                    binary_expr("?expr", "?op", literal_expr("?literal")),
                    "?cube",
                ),
                inner_aggregate_split_replacer("?expr", "?cube"),
                self.split_binary("?op", "?literal", false),
            ),
            transforming_rewrite(
                "split-push-down-binary-outer-replacer",
                outer_projection_split_replacer(
                    binary_expr("?expr", "?op", literal_expr("?literal")),
                    "?cube",
                ),
                binary_expr(
                    outer_projection_split_replacer("?expr", "?cube"),
                    "?op",
                    literal_expr("?literal"),
                ),
                self.split_binary("?op", "?literal", true),
            ),
            transforming_rewrite(
                "split-push-down-binary-outer-aggr-replacer",
                outer_aggregate_split_replacer(
                    binary_expr("?expr", "?op", literal_expr("?literal")),
                    "?cube",
                ),
                binary_expr(
                    outer_aggregate_split_replacer("?expr", "?cube"),
                    "?op",
                    literal_expr("?literal"),
                ),
                self.split_binary("?op", "?literal", false),
            ),
            // ?expr ?op cast(literal_expr("?right") as ?data_type)
            transforming_rewrite(
                "split-push-down-binary-with-cast-inner-replacer",
                inner_aggregate_split_replacer(
                    binary_expr(
                        "?expr",
                        "?op",
                        cast_expr(literal_expr("?literal"), "?data_type"),
                    ),
                    "?cube",
                ),
                inner_aggregate_split_replacer("?expr", "?cube"),
                self.split_binary("?op", "?literal", false),
            ),
            transforming_rewrite(
                "split-push-down-binary-with-cast-outer-aggr-replacer",
                outer_aggregate_split_replacer(
                    binary_expr(
                        "?expr",
                        "?op",
                        cast_expr(literal_expr("?literal"), "?data_type"),
                    ),
                    "?cube",
                ),
                binary_expr(
                    outer_aggregate_split_replacer("?expr", "?cube"),
                    "?op",
                    cast_expr(literal_expr("?literal"), "?data_type"),
                ),
                self.split_binary("?op", "?literal", false),
            ),
            // ?left_column ?op ?right_column
            transforming_rewrite(
                "split-push-down-binary-columns-inner-replacer",
                inner_aggregate_split_replacer(
                    aggr_group_expr(
                        binary_expr(
                            column_expr("?left_column"),
                            "?op",
                            column_expr("?right_column"),
                        ),
                        "?tail",
                    ),
                    "?alias_to_cube",
                ),
                aggr_group_expr(
                    inner_aggregate_split_replacer(column_expr("?left_column"), "?alias_to_cube"),
                    aggr_group_expr(
                        inner_aggregate_split_replacer(column_expr("?right_column"), "?alias_to_cube"),
                        inner_aggregate_split_replacer("?tail", "?alias_to_cube"),
                    ),
                ),
                self.transform_binary_columns_dimensions("?left_column", "?right_column", "?alias_to_cube"),
            ),
            transforming_chain_rewrite(
                "split-push-down-binary-columns-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    binary_expr(
                        column_expr("?left_column"),
                        "?op",
                        column_expr("?right_column"),
                    ),
                )],
                alias_expr(
                    binary_expr(
                        outer_aggregate_split_replacer(column_expr("?left_column"), "?alias_to_cube"),
                        "?op",
                        outer_aggregate_split_replacer(column_expr("?right_column"), "?alias_to_cube"),
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column_with_chain_transform(
                    "?expr",
                    "?alias",
                    None,
                    self.transform_binary_columns_dimensions("?left_column", "?right_column", "?alias_to_cube"),
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
            transforming_chain_rewrite(
                "split-push-down-cast-inner-replacer",
                inner_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    cast_expr("?inner_expr", "?data_type"),
                )],
                "?new_expr".to_string(),
                self.transform_cast_inner("?expr", "?alias_to_cube", "?inner_expr", "?data_type", "?new_expr"),
            ),
            transforming_chain_rewrite(
                "split-push-down-cast-outer-replacer",
                outer_projection_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    cast_expr("?inner_expr", "?data_type"),
                )],
                "?new_expr".to_string(),
                self.transform_cast_outer("?expr", "?alias_to_cube", "?inner_expr", "?data_type", "?new_expr", true),
            ),
            transforming_chain_rewrite(
                "split-push-down-cast-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    cast_expr("?inner_expr", "?data_type"),
                )],
                "?new_expr".to_string(),
                self.transform_cast_outer("?expr", "?alias_to_cube", "?inner_expr", "?data_type", "?new_expr", false),
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
            rewrite(
                "split-push-down-substr-outer-replacer-metabase",
                // Reaggregation may not be possible in all cases and won't change the final result
                // for SUBSTRING(column, 1, 1234) issued by Metabase
                outer_projection_split_replacer(
                    fun_expr("Substr", vec![
                        column_expr("?column"),
                        literal_int(1),
                        literal_int(1234),
                    ]),
                    "?alias_to_cube",
                ),
                fun_expr(
                    "Substr",
                    vec![
                        outer_projection_split_replacer(column_expr("?column"), "?alias_to_cube"),
                        literal_int(1),
                        literal_int(1234),
                    ],
                ),
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
            // Left
            rewrite(
                "split-push-down-left-inner-replacer",
                inner_aggregate_split_replacer(
                    fun_expr(
                        "Left",
                        vec!["?expr".to_string(), literal_expr("?length")],
                    ),
                    "?cube",
                ),
                inner_aggregate_split_replacer("?expr", "?cube"),
            ),
            // Right
            rewrite(
                "split-push-down-right-inner-replacer",
                inner_aggregate_split_replacer(
                    fun_expr(
                        "Right",
                        vec!["?expr".to_string(), literal_expr("?length")],
                    ),
                    "?cube",
                ),
                inner_aggregate_split_replacer("?expr", "?cube"),
            ),
            // NullIf
            rewrite(
                "split-push-down-nullif-inner-replacer",
                inner_aggregate_split_replacer(
                    fun_expr(
                        "NullIf",
                        vec!["?expr".to_string(), literal_expr("?literal")],
                    ),
                    "?cube",
                ),
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
                    "AggregateSplit:true",
                ),
                aggregate(
                    "?cube_scan",
                    aggr_group_expr("?left", "?right"),
                    "?aggr_aggr_expr",
                    "AggregateSplit:true",
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
            // (EXTRACT(MONTH FROM "ta_1"."LO_ORDERDATE") < (EXTRACT(MONTH FROM "ta_1"."LO_COMMITDATE") + 1.0))
            rewrite(
                "split-thoughtspot-extract-less-than-extract-inner-replacer",
                inner_aggregate_split_replacer(
                    aggr_group_expr(
                        binary_expr(
                            fun_expr(
                                "DatePart",
                                vec![literal_expr("?granularity"), column_expr("?left_column")],
                            ),
                            "<",
                            binary_expr(
                                fun_expr(
                                    "DatePart",
                                    vec![
                                        literal_expr("?granularity"),
                                        column_expr("?right_column"),
                                    ],
                                ),
                                "+",
                                literal_expr("?literal"),
                            ),
                        ),
                        "?tail",
                    ),
                    "?alias_to_cube",
                ),
                inner_aggregate_split_replacer(
                    aggr_group_expr(
                        fun_expr(
                            "DatePart",
                            vec![literal_expr("?granularity"), column_expr("?left_column")],
                        ),
                        aggr_group_expr(
                            fun_expr(
                                "DatePart",
                                vec![literal_expr("?granularity"), column_expr("?right_column")],
                            ),
                            "?tail",
                        ),
                    ),
                    "?alias_to_cube",
                ),
            ),
            rewrite(
                "split-thoughtspot-extract-less-than-extract-outer-aggr-replacer",
                outer_aggregate_split_replacer(
                    aggr_group_expr(
                        binary_expr(
                            fun_expr(
                                "DatePart",
                                vec![literal_expr("?granularity"), column_expr("?left_column")],
                            ),
                            "<",
                            binary_expr(
                                fun_expr(
                                    "DatePart",
                                    vec![
                                        literal_expr("?granularity"),
                                        column_expr("?right_column"),
                                    ],
                                ),
                                "+",
                                literal_expr("?literal"),
                            ),
                        ),
                        "?tail",
                    ),
                    "?alias_to_cube",
                ),
                aggr_group_expr(
                    binary_expr(
                        outer_aggregate_split_replacer(
                            fun_expr(
                                "DatePart",
                                vec![literal_expr("?granularity"), column_expr("?left_column")],
                            ),
                            "?alias_to_cube",
                        ),
                        "<",
                        binary_expr(
                            outer_aggregate_split_replacer(
                                fun_expr(
                                    "DatePart",
                                    vec![
                                        literal_expr("?granularity"),
                                        column_expr("?right_column"),
                                    ],
                                ),
                                "?alias_to_cube",
                            ),
                            "+",
                            literal_expr("?literal"),
                        ),
                    ),
                    "?tail",
                ),
            ),
            // (LOWER("ta_1"."C_REGION") = 'africa' OR LOWER("ta_1"."C_MKTSEGMENT") = 'automobile')
            rewrite(
                "split-thoughtspot-string-equal-or-inner-replacer",
                inner_aggregate_split_replacer(
                    aggr_group_expr(
                        binary_expr(
                            binary_expr(
                                fun_expr("Lower", vec![column_expr("?left_column")]),
                                "=",
                                literal_expr("?left_literal"),
                            ),
                            "OR",
                            binary_expr(
                                fun_expr("Lower", vec![column_expr("?right_column")]),
                                "=",
                                literal_expr("?right_literal"),
                            ),
                        ),
                        "?tail",
                    ),
                    "?alias_to_cube",
                ),
                aggr_group_expr(
                    column_expr("?left_column"),
                    aggr_group_expr(
                        column_expr("?right_column"),
                        inner_aggregate_split_replacer("?tail", "?alias_to_cube"),
                    ),
                ),
            ),
            rewrite(
                "split-thoughtspot-string-equal-or-outer-aggr-replacer",
                outer_aggregate_split_replacer(
                    binary_expr(
                        binary_expr(
                            fun_expr("Lower", vec![column_expr("?left_column")]),
                            "=",
                            literal_expr("?left_literal"),
                        ),
                        "OR",
                        binary_expr(
                            fun_expr("Lower", vec![column_expr("?right_column")]),
                            "=",
                            literal_expr("?right_literal"),
                        ),
                    ),
                    "?alias_to_cube",
                ),
                binary_expr(
                    binary_expr(
                        fun_expr(
                            "Lower",
                            vec![outer_aggregate_split_replacer(
                                column_expr("?left_column"),
                                "?alias_to_cube",
                            )],
                        ),
                        "=",
                        literal_expr("?left_literal"),
                    ),
                    "OR",
                    binary_expr(
                        fun_expr(
                            "Lower",
                            vec![outer_aggregate_split_replacer(
                                column_expr("?right_column"),
                                "?alias_to_cube",
                            )],
                        ),
                        "=",
                        literal_expr("?right_literal"),
                    ),
                ),
            ),
            // ("ta_1"."LO_COMMITDATE" = DATE '1994-05-01' OR "ta_1"."LO_COMMITDATE" = DATE '1996-05-03')
            rewrite(
                "split-thoughtspot-date-equal-or-inner-replacer",
                inner_aggregate_split_replacer(
                    aggr_group_expr(
                        binary_expr(
                            binary_expr(
                                column_expr("?left_column"),
                                "=",
                                cast_expr(literal_expr("?left_literal"), "?left_data_type"),
                            ),
                            "OR",
                            binary_expr(
                                column_expr("?right_column"),
                                "=",
                                cast_expr(literal_expr("?right_literal"), "?right_data_type"),
                            ),
                        ),
                        "?tail",
                    ),
                    "?alias_to_cube",
                ),
                aggr_group_expr(
                    column_expr("?left_column"),
                    aggr_group_expr(
                        column_expr("?right_column"),
                        inner_aggregate_split_replacer("?tail", "?alias_to_cube"),
                    ),
                ),
            ),
            rewrite(
                "split-thoughtspot-date-equal-or-outer-aggr-replacer",
                outer_aggregate_split_replacer(
                    binary_expr(
                        binary_expr(
                            column_expr("?left_column"),
                            "=",
                            cast_expr(literal_expr("?left_literal"), "?left_data_type"),
                        ),
                        "OR",
                        binary_expr(
                            column_expr("?right_column"),
                            "=",
                            cast_expr(literal_expr("?right_literal"), "?right_data_type"),
                        ),
                    ),
                    "?alias_to_cube",
                ),
                binary_expr(
                    binary_expr(
                        outer_aggregate_split_replacer(
                            column_expr("?left_column"),
                            "?alias_to_cube",
                        ),
                        "=",
                        cast_expr(literal_expr("?left_literal"), "?left_data_type"),
                    ),
                    "OR",
                    binary_expr(
                        outer_aggregate_split_replacer(
                            column_expr("?right_column"),
                            "?alias_to_cube",
                        ),
                        "=",
                        cast_expr(literal_expr("?right_literal"), "?right_data_type"),
                    ),
                ),
            ),
            // DATE_TRUNC('month', DATE_TRUNC('month', "ta_1"."createdAt"))
            transforming_chain_rewrite(
                "split-thoughtspot-double-date-trunc-inner-replacer",
                inner_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    fun_expr(
                        "DateTrunc",
                        vec![
                            literal_expr("?granularity"),
                            fun_expr(
                                "DateTrunc",
                                vec![literal_expr("?granularity"), "?column".to_string()],
                            ),
                        ],
                    ),
                )],
                alias_expr(
                    fun_expr(
                        "DateTrunc",
                        vec![literal_expr("?granularity"), "?column".to_string()],
                    ),
                    "?outer_alias",
                ),
                self.transform_original_expr_to_alias_and_column("?expr", "?outer_alias", None),
            ),
            transforming_chain_rewrite(
                "split-thoughtspot-double-date-trunc-outer-replacer",
                outer_projection_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    fun_expr(
                        "DateTrunc",
                        vec![
                            literal_expr("?granularity"),
                            fun_expr(
                                "DateTrunc",
                                vec![literal_expr("?granularity"), "?column".to_string()],
                            ),
                        ],
                    ),
                )],
                alias_expr(column_expr("?outer_column"), "?outer_alias"),
                self.transform_original_expr_to_alias_and_column(
                    "?expr",
                    "?outer_alias",
                    Some("?outer_column"),
                ),
            ),
            transforming_chain_rewrite(
                "split-thoughtspot-double-date-trunc-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    fun_expr(
                        "DateTrunc",
                        vec![
                            literal_expr("?granularity"),
                            fun_expr(
                                "DateTrunc",
                                vec![literal_expr("?granularity"), "?column".to_string()],
                            ),
                        ],
                    ),
                )],
                alias_expr(column_expr("?outer_column"), "?outer_alias"),
                self.transform_original_expr_to_alias_and_column(
                    "?expr",
                    "?outer_alias",
                    Some("?outer_column"),
                ),
            ),
            // DATE_TRUNC('month', CAST(CAST(((((EXTRACT(YEAR FROM "ta_1"."createdAt") * 100) + 1) * 100) + 1) AS CHARACTER VARYING) AS timestamp))
            transforming_chain_rewrite(
                "split-thoughtspot-trunc-extract-year-inner-replacer",
                inner_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    fun_expr(
                        "DateTrunc",
                        vec![
                            // Any granularity will do as long as our max is YEAR.
                            // If maximum raises to DECADE, ?granularity needs to be checked
                            literal_expr("?granularity"),
                            cast_expr(
                                cast_expr_explicit(
                                    binary_expr(
                                        binary_expr(
                                            binary_expr(
                                                binary_expr(
                                                    fun_expr(
                                                        "DatePart",
                                                        vec![
                                                            literal_string("YEAR"),
                                                            "?column".to_string(),
                                                        ],
                                                    ),
                                                    "*",
                                                    literal_int(100),
                                                ),
                                                "+",
                                                literal_int(1),
                                            ),
                                            "*",
                                            literal_int(100),
                                        ),
                                        "+",
                                        literal_int(1),
                                    ),
                                    ArrowDataType::Utf8,
                                ),
                                "?timestamp_type",
                            ),
                        ],
                    ),
                )],
                alias_expr(
                    fun_expr(
                        "DateTrunc",
                        vec![literal_string("year"), "?column".to_string()],
                    ),
                    "?outer_alias",
                ),
                self.transform_original_expr_to_alias_and_column("?expr", "?outer_alias", None),
            ),
            transforming_chain_rewrite(
                "split-thoughtspot-trunc-extract-year-outer-replacer",
                outer_projection_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    fun_expr(
                        "DateTrunc",
                        vec![
                            // Any granularity will do as long as our max is YEAR.
                            // If maximum raises to DECADE, ?granularity needs to be checked
                            literal_expr("?granularity"),
                            cast_expr(
                                cast_expr_explicit(
                                    binary_expr(
                                        binary_expr(
                                            binary_expr(
                                                binary_expr(
                                                    fun_expr(
                                                        "DatePart",
                                                        vec![
                                                            literal_string("YEAR"),
                                                            "?column".to_string(),
                                                        ],
                                                    ),
                                                    "*",
                                                    literal_int(100),
                                                ),
                                                "+",
                                                literal_int(1),
                                            ),
                                            "*",
                                            literal_int(100),
                                        ),
                                        "+",
                                        literal_int(1),
                                    ),
                                    ArrowDataType::Utf8,
                                ),
                                "?timestamp_type",
                            ),
                        ],
                    ),
                )],
                alias_expr(column_expr("?outer_column"), "?outer_alias"),
                self.transform_original_expr_to_alias_and_column(
                    "?expr",
                    "?outer_alias",
                    Some("?outer_column"),
                ),
            ),
            transforming_chain_rewrite(
                "split-thoughtspot-trunc-extract-year-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    fun_expr(
                        "DateTrunc",
                        vec![
                            // Any granularity will do as long as our max is YEAR.
                            // If maximum raises to DECADE, ?granularity needs to be checked
                            literal_expr("?granularity"),
                            cast_expr(
                                cast_expr_explicit(
                                    binary_expr(
                                        binary_expr(
                                            binary_expr(
                                                binary_expr(
                                                    fun_expr(
                                                        "DatePart",
                                                        vec![
                                                            literal_string("YEAR"),
                                                            "?column".to_string(),
                                                        ],
                                                    ),
                                                    "*",
                                                    literal_int(100),
                                                ),
                                                "+",
                                                literal_int(1),
                                            ),
                                            "*",
                                            literal_int(100),
                                        ),
                                        "+",
                                        literal_int(1),
                                    ),
                                    ArrowDataType::Utf8,
                                ),
                                "?timestamp_type",
                            ),
                        ],
                    ),
                )],
                alias_expr(column_expr("?outer_column"), "?outer_alias"),
                self.transform_original_expr_to_alias_and_column(
                    "?expr",
                    "?outer_alias",
                    Some("?outer_column"),
                ),
            ),
            // (DATEDIFF(day, DATEADD(month, CAST((((((EXTRACT(MONTH FROM "ta_1"."LO_COMMITDATE") - 1) % 3) + 1) - 1) * -1) AS int), CAST(CAST(((((EXTRACT(YEAR FROM "ta_1"."LO_COMMITDATE") * 100) + EXTRACT(MONTH FROM "ta_1"."LO_COMMITDATE")) * 100) + 1) AS varchar) AS date)), "ta_1"."LO_COMMITDATE") + 1)
            transforming_chain_rewrite(
                "split-thoughtspot-day-in-quarter-inner-replacer",
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
                                                    binary_expr(
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
                                                                literal_int(1),
                                                            ),
                                                            "%",
                                                            literal_int(3),
                                                        ),
                                                        "+",
                                                        literal_int(1),
                                                    ),
                                                    "-",
                                                    literal_int(1),
                                                ),
                                                "*",
                                                literal_int(-1),
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
                                                                literal_int(100),
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
                                                        literal_int(100),
                                                    ),
                                                    "+",
                                                    literal_int(1),
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
                        literal_int(1),
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
                "split-thoughtspot-day-in-quarter-outer-aggr-replacer",
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
                                                    binary_expr(
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
                                                                literal_int(1),
                                                            ),
                                                            "%",
                                                            literal_int(3),
                                                        ),
                                                        "+",
                                                        literal_int(1),
                                                    ),
                                                    "-",
                                                    literal_int(1),
                                                ),
                                                "*",
                                                literal_int(-1),
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
                                                                literal_int(100),
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
                                                        literal_int(100),
                                                    ),
                                                    "+",
                                                    literal_int(1),
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
                        literal_int(1),
                    ),
                )],
                alias_expr(
                    binary_expr(
                        udf_expr(
                            "datediff",
                            vec![
                                literal_string("day"),
                                fun_expr(
                                    "DateTrunc",
                                    vec![literal_string("quarter"), column_expr("?outer_column")],
                                ),
                                column_expr("?outer_column"),
                            ],
                        ),
                        "+",
                        literal_int(1),
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column(
                    "?expr",
                    "?alias",
                    Some("?outer_column"),
                ),
            ),
            // CAST(CAST(((((EXTRACT(YEAR FROM "ta_1"."LO_COMMITDATE") * 100) + 1) * 100) + 1) AS varchar) AS date)
            transforming_chain_rewrite(
                "split-thoughtspot-extract-year-to-date-trunc-inner-replacer",
                inner_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
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
                                            literal_int(100),
                                        ),
                                        "+",
                                        literal_int(1),
                                    ),
                                    "*",
                                    literal_int(100),
                                ),
                                "+",
                                literal_int(1),
                            ),
                            ArrowDataType::Utf8,
                        ),
                        ArrowDataType::Date32,
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
                "split-thoughtspot-extract-year-to-date-trunc-outer-replacer",
                outer_projection_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
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
                                            literal_int(100),
                                        ),
                                        "+",
                                        literal_int(1),
                                    ),
                                    "*",
                                    literal_int(100),
                                ),
                                "+",
                                literal_int(1),
                            ),
                            ArrowDataType::Utf8,
                        ),
                        ArrowDataType::Date32,
                    ),
                )],
                alias_expr(
                    cast_expr_explicit(
                        column_expr("?outer_column"),
                        ArrowDataType::Date32,
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
                "split-thoughtspot-extract-year-to-date-trunc-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
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
                                            literal_int(100),
                                        ),
                                        "+",
                                        literal_int(1),
                                    ),
                                    "*",
                                    literal_int(100),
                                ),
                                "+",
                                literal_int(1),
                            ),
                            ArrowDataType::Utf8,
                        ),
                        ArrowDataType::Date32,
                    ),
                )],
                alias_expr(
                    cast_expr_explicit(
                        column_expr("?outer_column"),
                        ArrowDataType::Date32,
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column(
                    "?expr",
                    "?alias",
                    Some("?outer_column"),
                ),
            ),
            // CAST(EXTRACT(YEAR FROM "ta_1"."orderDt") || '-' || 1 || '-01' AS DATE)
            transforming_chain_rewrite(
                "split-thoughtspot-pg-date-trunc-year-inner-replacer",
                inner_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    cast_expr_explicit(
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
                                    "||",
                                    literal_string("-"),
                                ),
                                "||",
                                literal_int(1),
                            ),
                            "||",
                            literal_string("-01"),
                        ),
                        ArrowDataType::Date32,
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
                "split-thoughtspot-pg-date-trunc-year-outer-replacer",
                outer_projection_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    cast_expr_explicit(
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
                                    "||",
                                    literal_string("-"),
                                ),
                                "||",
                                literal_int(1),
                            ),
                            "||",
                            literal_string("-01"),
                        ),
                        ArrowDataType::Date32,
                    ),
                )],
                alias_expr(
                    cast_expr_explicit(
                        column_expr("?outer_column"),
                        ArrowDataType::Date32,
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
                "split-thoughtspot-pg-date-trunc-year-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    cast_expr_explicit(
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
                                    "||",
                                    literal_string("-"),
                                ),
                                "||",
                                literal_int(1),
                            ),
                            "||",
                            literal_string("-01"),
                        ),
                        ArrowDataType::Date32,
                    ),
                )],
                alias_expr(
                    cast_expr_explicit(
                        column_expr("?outer_column"),
                        ArrowDataType::Date32,
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column(
                    "?expr",
                    "?alias",
                    Some("?outer_column"),
                ),
            ),
            // CAST(EXTRACT(YEAR FROM "ta_1"."completedAt") || '-' || ((FLOOR(((EXTRACT(MONTH FROM "ta_1"."completedAt") - 1) / NULLIF(3,0))) * 3) + 1) || '-01' AS DATE) AS "ca_2"
            transforming_chain_rewrite(
                "split-thoughtspot-pg-date-trunc-quarter-inner-replacer",
                inner_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    cast_expr_explicit(
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
                                    "||",
                                    literal_string("-"),
                                ),
                                "||",
                                binary_expr(
                                    binary_expr(
                                        fun_expr(
                                            "Floor",
                                            vec![
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
                                                        literal_int(1),
                                                    ),
                                                    "/",
                                                    fun_expr(
                                                        "NullIf",
                                                        vec![
                                                            literal_int(3),
                                                            literal_int(0),
                                                        ],
                                                    ),
                                                )
                                            ],
                                        ),
                                        "*",
                                        literal_int(3),
                                    ),
                                    "+",
                                    literal_int(1),
                                ),
                            ),
                            "||",
                            literal_string("-01"),
                        ),
                        ArrowDataType::Date32,
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
                "split-thoughtspot-pg-date-trunc-quarter-outer-replacer",
                outer_projection_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    cast_expr_explicit(
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
                                    "||",
                                    literal_string("-"),
                                ),
                                "||",
                                binary_expr(
                                    binary_expr(
                                        fun_expr(
                                            "Floor",
                                            vec![
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
                                                        literal_int(1),
                                                    ),
                                                    "/",
                                                    fun_expr(
                                                        "NullIf",
                                                        vec![
                                                            literal_int(3),
                                                            literal_int(0),
                                                        ],
                                                    ),
                                                )
                                            ],
                                        ),
                                        "*",
                                        literal_int(3),
                                    ),
                                    "+",
                                    literal_int(1),
                                ),
                            ),
                            "||",
                            literal_string("-01"),
                        ),
                        ArrowDataType::Date32,
                    ),
                )],
                alias_expr(
                    cast_expr_explicit(
                        column_expr("?outer_column"),
                        ArrowDataType::Date32,
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
                "split-thoughtspot-pg-date-trunc-quarter-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    cast_expr_explicit(
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
                                    "||",
                                    literal_string("-"),
                                ),
                                "||",
                                binary_expr(
                                    binary_expr(
                                        fun_expr(
                                            "Floor",
                                            vec![
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
                                                        literal_int(1),
                                                    ),
                                                    "/",
                                                    fun_expr(
                                                        "NullIf",
                                                        vec![
                                                            literal_int(3),
                                                            literal_int(0),
                                                        ],
                                                    ),
                                                )
                                            ],
                                        ),
                                        "*",
                                        literal_int(3),
                                    ),
                                    "+",
                                    literal_int(1),
                                ),
                            ),
                            "||",
                            literal_string("-01"),
                        ),
                        ArrowDataType::Date32,
                    ),
                )],
                alias_expr(
                    cast_expr_explicit(
                        column_expr("?outer_column"),
                        ArrowDataType::Date32,
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column(
                    "?expr",
                    "?alias",
                    Some("?outer_column"),
                ),
            ),
            // (MOD(CAST((EXTRACT(MONTH FROM "ta_1"."completedAt") - 1) AS numeric), 3) + 1)
            transforming_chain_rewrite(
                "split-thoughtspot-pg-extract-month-of-quarter-inner-replacer",
                inner_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    binary_expr(
                        binary_expr(
                            cast_expr(
                                binary_expr(
                                    fun_expr(
                                        "DatePart",
                                        vec![
                                            literal_string("MONTH"),
                                            column_expr("?column"),
                                        ],
                                    ),
                                    "-",
                                    literal_int(1),
                                ),
                                // TODO: explicitly test Decimal(38, 10)
                                "?numeric",
                            ),
                            "%",
                            literal_int(3),
                        ),
                        "+",
                        literal_int(1),
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
                "split-thoughtspot-pg-extract-month-of-quarter-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    binary_expr(
                        binary_expr(
                            cast_expr(
                                binary_expr(
                                    fun_expr(
                                        "DatePart",
                                        vec![
                                            literal_string("MONTH"),
                                            column_expr("?column"),
                                        ],
                                    ),
                                    "-",
                                    literal_int(1),
                                ),
                                // TODO: explicitly test Decimal(38, 10)
                                "?numeric",
                            ),
                            "%",
                            literal_int(3),
                        ),
                        "+",
                        literal_int(1),
                    ),
                )],
                alias_expr(
                    binary_expr(
                        binary_expr(
                            binary_expr(
                                fun_expr(
                                    "DatePart",
                                    vec![literal_string("month"), column_expr("?outer_column")],
                                ),
                                "-",
                                literal_int(1),
                            ),
                            "%",
                            literal_int(3),
                        ),
                        "+",
                        literal_int(1),
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column(
                    "?expr",
                    "?alias",
                    Some("?outer_column"),
                ),
            ),
            // (CAST("ta_1"."completedAt" AS date) - CAST((CAST(EXTRACT(YEAR FROM "ta_1"."completedAt") || '-' || EXTRACT(MONTH FROM "ta_1"."completedAt") || '-01' AS DATE) + ((EXTRACT(MONTH FROM "ta_1"."completedAt") - 1) * -1) * INTERVAL '1 month') AS date) + 1)
            transforming_chain_rewrite(
                "split-thoughtspot-pg-extract-day-of-year-inner-replacer",
                inner_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    binary_expr(
                        binary_expr(
                            cast_expr_explicit(
                                column_expr("?column"),
                                ArrowDataType::Date32,
                            ),
                            "-",
                            cast_expr_explicit(
                                binary_expr(
                                    cast_expr_explicit(
                                        binary_expr(
                                            binary_expr(
                                                binary_expr(
                                                    fun_expr(
                                                        "DatePart",
                                                        vec![literal_string("YEAR"), column_expr("?column")],
                                                    ),
                                                    "||",
                                                    literal_string("-"),
                                                ),
                                                "||",
                                                fun_expr(
                                                    "DatePart",
                                                    vec![literal_string("MONTH"), column_expr("?column")],
                                                ),
                                            ),
                                            "||",
                                            literal_string("-01"),
                                        ),
                                        ArrowDataType::Date32,
                                    ),
                                    "+",
                                    binary_expr(
                                        binary_expr(
                                            binary_expr(
                                                fun_expr(
                                                    "DatePart",
                                                    vec![literal_string("MONTH"), column_expr("?column")],
                                                ),
                                                "-",
                                                literal_int(1),
                                            ),
                                            "*",
                                            literal_int(-1),
                                        ),
                                        "*",
                                        literal_expr("?interval"),
                                    ),
                                ),
                                ArrowDataType::Date32,
                            ),
                        ),
                        "+",
                        literal_int(1),
                    ),
                )],
                alias_expr(
                    fun_expr(
                        "DateTrunc",
                        vec![literal_string("day"), column_expr("?column")],
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column_with_chain_transform(
                    "?expr",
                    "?alias",
                    None,
                    self.transform_is_interval_of_granularity("?interval", "month"),
                ),
            ),
            transforming_chain_rewrite(
                "split-thoughtspot-pg-extract-day-of-year-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    binary_expr(
                        binary_expr(
                            cast_expr_explicit(
                                column_expr("?column"),
                                ArrowDataType::Date32,
                            ),
                            "-",
                            cast_expr_explicit(
                                binary_expr(
                                    cast_expr_explicit(
                                        binary_expr(
                                            binary_expr(
                                                binary_expr(
                                                    fun_expr(
                                                        "DatePart",
                                                        vec![literal_string("YEAR"), column_expr("?column")],
                                                    ),
                                                    "||",
                                                    literal_string("-"),
                                                ),
                                                "||",
                                                fun_expr(
                                                    "DatePart",
                                                    vec![literal_string("MONTH"), column_expr("?column")],
                                                ),
                                            ),
                                            "||",
                                            literal_string("-01"),
                                        ),
                                        ArrowDataType::Date32,
                                    ),
                                    "+",
                                    binary_expr(
                                        binary_expr(
                                            binary_expr(
                                                fun_expr(
                                                    "DatePart",
                                                    vec![literal_string("MONTH"), column_expr("?column")],
                                                ),
                                                "-",
                                                literal_int(1),
                                            ),
                                            "*",
                                            literal_int(-1),
                                        ),
                                        "*",
                                        literal_expr("?interval"),
                                    ),
                                ),
                                ArrowDataType::Date32,
                            ),
                        ),
                        "+",
                        literal_int(1),
                    ),
                )],
                alias_expr(
                    binary_expr(
                        binary_expr(
                            cast_expr_explicit(
                                column_expr("?outer_column"),
                                ArrowDataType::Date32,
                            ),
                            "-",
                            cast_expr_explicit(
                                fun_expr(
                                    "DateTrunc",
                                    vec![literal_string("year"), column_expr("?outer_column")],
                                ),
                                ArrowDataType::Date32,
                            ),
                        ),
                        "+",
                        literal_int(1),
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column_with_chain_transform(
                    "?expr",
                    "?alias",
                    Some("?outer_column"),
                    self.transform_is_interval_of_granularity("?interval", "month"),
                ),
            ),
            // (CAST("ta_1"."completedAt" AS date) - CAST((CAST(EXTRACT(YEAR FROM "ta_1"."completedAt") || '-' || EXTRACT(MONTH FROM "ta_1"."completedAt") || '-01' AS DATE) + (((MOD(CAST((EXTRACT(MONTH FROM "ta_1"."completedAt") - 1) AS numeric), 3) + 1) - 1) * -1) * INTERVAL '1 month') AS date) + 1)
            transforming_chain_rewrite(
                "split-thoughtspot-pg-extract-day-of-quarter-inner-replacer",
                inner_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    binary_expr(
                        binary_expr(
                            cast_expr_explicit(
                                column_expr("?column"),
                                ArrowDataType::Date32,
                            ),
                            "-",
                            cast_expr_explicit(
                                binary_expr(
                                    cast_expr_explicit(
                                        binary_expr(
                                            binary_expr(
                                                binary_expr(
                                                    fun_expr(
                                                        "DatePart",
                                                        vec![literal_string("YEAR"), column_expr("?column")],
                                                    ),
                                                    "||",
                                                    literal_string("-"),
                                                ),
                                                "||",
                                                fun_expr(
                                                    "DatePart",
                                                    vec![literal_string("MONTH"), column_expr("?column")],
                                                ),
                                            ),
                                            "||",
                                            literal_string("-01"),
                                        ),
                                        ArrowDataType::Date32,
                                    ),
                                    "+",
                                    binary_expr(
                                        binary_expr(
                                            binary_expr(
                                                binary_expr(
                                                    binary_expr(
                                                        cast_expr(
                                                            binary_expr(
                                                                fun_expr(
                                                                    "DatePart",
                                                                    vec![literal_string("MONTH"), column_expr("?column")],
                                                                ),
                                                                "-",
                                                                literal_int(1),
                                                            ),
                                                            // TODO: explicitly test Decimal(38, 10)
                                                            "?numeric",
                                                        ),
                                                        "%",
                                                        literal_int(3),
                                                    ),
                                                    "+",
                                                    literal_int(1),
                                                ),
                                                "-",
                                                literal_int(1),
                                            ),
                                            "*",
                                            literal_int(-1),
                                        ),
                                        "*",
                                        literal_expr("?interval"),
                                    ),
                                ),
                                ArrowDataType::Date32,
                            ),
                        ),
                        "+",
                        literal_int(1),
                    ),
                )],
                alias_expr(
                    fun_expr(
                        "DateTrunc",
                        vec![literal_string("day"), column_expr("?column")],
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column_with_chain_transform(
                    "?expr",
                    "?alias",
                    None,
                    self.transform_is_interval_of_granularity("?interval", "month"),
                ),
            ),
            transforming_chain_rewrite(
                "split-thoughtspot-pg-extract-day-of-quarter-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    binary_expr(
                        binary_expr(
                            cast_expr_explicit(
                                column_expr("?column"),
                                ArrowDataType::Date32,
                            ),
                            "-",
                            cast_expr_explicit(
                                binary_expr(
                                    cast_expr_explicit(
                                        binary_expr(
                                            binary_expr(
                                                binary_expr(
                                                    fun_expr(
                                                        "DatePart",
                                                        vec![literal_string("YEAR"), column_expr("?column")],
                                                    ),
                                                    "||",
                                                    literal_string("-"),
                                                ),
                                                "||",
                                                fun_expr(
                                                    "DatePart",
                                                    vec![literal_string("MONTH"), column_expr("?column")],
                                                ),
                                            ),
                                            "||",
                                            literal_string("-01"),
                                        ),
                                        ArrowDataType::Date32,
                                    ),
                                    "+",
                                    binary_expr(
                                        binary_expr(
                                            binary_expr(
                                                binary_expr(
                                                    binary_expr(
                                                        cast_expr(
                                                            binary_expr(
                                                                fun_expr(
                                                                    "DatePart",
                                                                    vec![literal_string("MONTH"), column_expr("?column")],
                                                                ),
                                                                "-",
                                                                literal_int(1),
                                                            ),
                                                            // TODO: explicitly test Decimal(38, 10)
                                                            "?numeric",
                                                        ),
                                                        "%",
                                                        literal_int(3),
                                                    ),
                                                    "+",
                                                    literal_int(1),
                                                ),
                                                "-",
                                                literal_int(1),
                                            ),
                                            "*",
                                            literal_int(-1),
                                        ),
                                        "*",
                                        literal_expr("?interval"),
                                    ),
                                ),
                                ArrowDataType::Date32,
                            ),
                        ),
                        "+",
                        literal_int(1),
                    ),
                )],
                alias_expr(
                    binary_expr(
                        binary_expr(
                            cast_expr_explicit(
                                column_expr("?outer_column"),
                                ArrowDataType::Date32,
                            ),
                            "-",
                            cast_expr_explicit(
                                fun_expr(
                                    "DateTrunc",
                                    vec![literal_string("quarter"), column_expr("?outer_column")],
                                ),
                                ArrowDataType::Date32,
                            ),
                        ),
                        "+",
                        literal_int(1),
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column_with_chain_transform(
                    "?expr",
                    "?alias",
                    Some("?outer_column"),
                    self.transform_is_interval_of_granularity("?interval", "month"),
                ),
            ),
            // (MOD(CAST((CAST("ta_1"."completedAt" AS date) - CAST(DATE '1970-01-01' AS date) + 3) AS numeric), 7) + 1)
            transforming_chain_rewrite(
                "split-thoughtspot-pg-extract-day-of-week-inner-replacer",
                inner_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    binary_expr(
                        binary_expr(
                            cast_expr(
                                binary_expr(
                                    binary_expr(
                                        cast_expr_explicit(
                                            column_expr("?column"),
                                            ArrowDataType::Date32,
                                        ),
                                        "-",
                                        cast_expr_explicit(
                                            cast_expr_explicit(
                                                literal_string("1970-01-01"),
                                                ArrowDataType::Date32,
                                            ),
                                            ArrowDataType::Date32,
                                        ),
                                    ),
                                    "+",
                                    literal_int(3),
                                ),
                                // TODO: explicitly test Decimal(38, 10)
                                "?numeric",
                            ),
                            "%",
                            literal_int(7),
                        ),
                        "+",
                        literal_int(1),
                    ),
                )],
                alias_expr(
                    fun_expr(
                        "DateTrunc",
                        vec![literal_string("day"), column_expr("?column")],
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column(
                    "?expr",
                    "?alias",
                    None,
                ),
            ),
            transforming_chain_rewrite(
                "split-thoughtspot-pg-extract-day-of-week-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    binary_expr(
                        binary_expr(
                            cast_expr(
                                binary_expr(
                                    binary_expr(
                                        cast_expr_explicit(
                                            column_expr("?column"),
                                            ArrowDataType::Date32,
                                        ),
                                        "-",
                                        cast_expr_explicit(
                                            cast_expr_explicit(
                                                literal_string("1970-01-01"),
                                                ArrowDataType::Date32,
                                            ),
                                            ArrowDataType::Date32,
                                        ),
                                    ),
                                    "+",
                                    literal_int(3),
                                ),
                                // TODO: explicitly test Decimal(38, 10)
                                "?numeric",
                            ),
                            "%",
                            literal_int(7),
                        ),
                        "+",
                        literal_int(1),
                    ),
                )],
                alias_expr(
                    binary_expr(
                        binary_expr(
                            cast_expr_explicit(
                                column_expr("?outer_column"),
                                ArrowDataType::Date32,
                            ),
                            "-",
                            cast_expr_explicit(
                                fun_expr(
                                    "DateTrunc",
                                    vec![literal_string("week"), column_expr("?outer_column")],
                                ),
                                ArrowDataType::Date32,
                            ),
                        ),
                        "+",
                        literal_int(1),
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column(
                    "?expr",
                    "?alias",
                    Some("?outer_column"),
                ),
            ),
            // DATE_TRUNC('qtr', DATEADD(day, CAST(2 AS int), "ta_1"."LO_COMMITDATE"))
            transforming_chain_rewrite(
                "split-thoughtspot-date-trunc-offset-inner-replacer",
                inner_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    fun_expr(
                        "DateTrunc",
                        vec![
                            literal_expr("?granularity"),
                            udf_expr(
                                "dateadd",
                                vec![
                                    literal_expr("?datepart"),
                                    cast_expr_explicit(
                                        literal_expr("?interval"),
                                        ArrowDataType::Int32,
                                    ),
                                    column_expr("?column"),
                                ],
                            ),
                        ],
                    ),
                )],
                alias_expr(
                    fun_expr(
                        "DateTrunc",
                        vec![literal_expr("?min_granularity"), column_expr("?column")],
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column_with_chain_transform(
                    "?expr",
                    "?alias",
                    None,
                    self.transform_min_max_granularity(
                        "?granularity",
                        "?datepart",
                        "?min_granularity",
                        false,
                    ),
                ),
            ),
            transforming_chain_rewrite(
                "split-thoughtspot-date-trunc-offset-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    fun_expr(
                        "DateTrunc",
                        vec![
                            literal_expr("?granularity"),
                            udf_expr(
                                "dateadd",
                                vec![
                                    literal_expr("?datepart"),
                                    cast_expr_explicit(
                                        literal_expr("?interval"),
                                        ArrowDataType::Int32,
                                    ),
                                    column_expr("?column"),
                                ],
                            ),
                        ],
                    ),
                )],
                alias_expr(
                    fun_expr(
                        "DateTrunc",
                        vec![
                            literal_expr("?out_granularity"),
                            udf_expr(
                                "dateadd",
                                vec![
                                    literal_expr("?datepart"),
                                    literal_expr("?interval"),
                                    column_expr("?outer_column"),
                                ],
                            ),
                        ],
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column_with_chain_transform(
                    "?expr",
                    "?alias",
                    Some("?outer_column"),
                    self.split_date_trunc("?granularity", "?out_granularity"),
                ),
            ),
            // CAST(DATEADD(day, CAST(2 AS int), "ta_1"."Created") AS date)
            rewrite(
                "split-thoughtspot-date-offset-inner-replacer",
                inner_aggregate_split_replacer(
                    udf_expr(
                        "dateadd",
                        vec![
                            literal_expr("?datepart"),
                            cast_expr(literal_expr("?interval_int"), "?data_type"),
                            column_expr("?column"),
                        ],
                    ),
                    "?alias_to_cube",
                ),
                inner_aggregate_split_replacer(column_expr("?column"), "?alias_to_cube"),
            ),
            transforming_chain_rewrite(
                "split-thoughtspot-date-offset-outer-replacer",
                outer_projection_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    udf_expr(
                        "dateadd",
                        vec![
                            literal_expr("?datepart"),
                            cast_expr(literal_expr("?interval_int"), "?data_type"),
                            column_expr("?column"),
                        ],
                    ),
                )],
                alias_expr(
                    udf_expr(
                        "dateadd",
                        vec![
                            literal_expr("?datepart"),
                            cast_expr(literal_expr("?interval_int"), "?data_type"),
                            outer_aggregate_split_replacer(
                                column_expr("?column"),
                                "?alias_to_cube",
                            ),
                        ],
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column("?expr", "?alias", None),
            ),
            transforming_chain_rewrite(
                "split-thoughtspot-date-offset-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    udf_expr(
                        "dateadd",
                        vec![
                            literal_expr("?datepart"),
                            cast_expr(literal_expr("?interval_int"), "?data_type"),
                            column_expr("?column"),
                        ],
                    ),
                )],
                alias_expr(
                    udf_expr(
                        "dateadd",
                        vec![
                            literal_expr("?datepart"),
                            cast_expr(literal_expr("?interval_int"), "?data_type"),
                            outer_aggregate_split_replacer(
                                column_expr("?column"),
                                "?alias_to_cube",
                            ),
                        ],
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column("?expr", "?alias", None),
            ),
            transforming_chain_rewrite(
                "split-thoughtspot-date-offset-cast-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    cast_expr_explicit(
                        udf_expr(
                            "dateadd",
                            vec![
                                literal_expr("?datepart"),
                                cast_expr(literal_expr("?interval_int"), "?data_type"),
                                column_expr("?column"),
                            ],
                        ),
                        ArrowDataType::Date32,
                    ),
                )],
                alias_expr(
                    cast_expr_explicit(
                        udf_expr(
                            "dateadd",
                            vec![
                                literal_expr("?datepart"),
                                cast_expr(literal_expr("?interval_int"), "?data_type"),
                                outer_aggregate_split_replacer(
                                    column_expr("?column"),
                                    "?alias_to_cube",
                                ),
                            ],
                        ),
                        ArrowDataType::Date32,
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column("?expr", "?alias", None),
            ),
            // FLOOR(((EXTRACT(DAY FROM DATEADD(day, CAST((4 - (((DATEDIFF(day, DATE '1970-01-01', "ta_1"."LO_COMMITDATE") + 3) % 7) + 1)) AS int), "ta_1"."LO_COMMITDATE")) + 6) / NULLIF(CAST(7 AS FLOAT8),0.0)))
            transforming_chain_rewrite(
                "split-thoughtspot-week-num-in-month-inner-replacer",
                inner_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    fun_expr(
                        "Floor",
                        vec![
                            binary_expr(
                                binary_expr(
                                    fun_expr(
                                        "DatePart",
                                        vec![
                                            literal_string("DAY"),
                                            udf_expr(
                                                "dateadd",
                                                vec![
                                                    literal_string("day"),
                                                    cast_expr_explicit(
                                                        binary_expr(
                                                            literal_int(4),
                                                            "-",
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
                                                                        literal_int(3),
                                                                    ),
                                                                    "%",
                                                                    literal_int(7),
                                                                ),
                                                                "+",
                                                                literal_int(1),
                                                            ),
                                                        ),
                                                        ArrowDataType::Int32,
                                                    ),
                                                    column_expr("?column"),
                                                ],
                                            ),
                                        ],
                                    ),
                                    "+",
                                    literal_int(6),
                                ),
                                "/",
                                fun_expr(
                                    "NullIf",
                                    vec![
                                        cast_expr_explicit(
                                            literal_int(7),
                                            ArrowDataType::Float64,
                                        ),
                                        literal_float(0.0),
                                    ],
                                ),
                            ),
                        ],
                    ),
                )],
                alias_expr(
                    fun_expr(
                        "DateTrunc",
                        vec![literal_string("week"), column_expr("?column")],
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column(
                    "?expr",
                    "?alias",
                    None,
                ),
            ),
            transforming_chain_rewrite(
                "split-thoughtspot-week-num-in-month-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    fun_expr(
                        "Floor",
                        vec![
                            binary_expr(
                                binary_expr(
                                    fun_expr(
                                        "DatePart",
                                        vec![
                                            literal_string("DAY"),
                                            udf_expr(
                                                "dateadd",
                                                vec![
                                                    literal_string("day"),
                                                    cast_expr_explicit(
                                                        binary_expr(
                                                            literal_int(4),
                                                            "-",
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
                                                                        literal_int(3),
                                                                    ),
                                                                    "%",
                                                                    literal_int(7),
                                                                ),
                                                                "+",
                                                                literal_int(1),
                                                            ),
                                                        ),
                                                        ArrowDataType::Int32,
                                                    ),
                                                    column_expr("?column"),
                                                ],
                                            ),
                                        ],
                                    ),
                                    "+",
                                    literal_int(6),
                                ),
                                "/",
                                fun_expr(
                                    "NullIf",
                                    vec![
                                        cast_expr_explicit(
                                            literal_int(7),
                                            ArrowDataType::Float64,
                                        ),
                                        literal_float(0.0),
                                    ],
                                ),
                            ),
                        ],
                    ),
                )],
                alias_expr(
                    fun_expr(
                        "Ceil",
                        vec![
                            binary_expr(
                                fun_expr(
                                    "DatePart",
                                    vec![
                                        literal_string("day"),
                                        udf_expr(
                                            "dateadd",
                                            vec![
                                                literal_string("day"),
                                                literal_int(3),
                                                column_expr("?outer_column"),
                                            ],
                                        ),
                                    ],
                                ),
                                "/",
                                literal_float(7.0),
                            ),
                        ],
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column(
                    "?expr",
                    "?alias",
                    Some("?outer_column"),
                ),
            ),
            // CEIL((EXTRACT(MONTH FROM "ta_1"."LO_COMMITDATE") / NULLIF(3.0,0.0)))
            transforming_chain_rewrite(
                "split-thoughtspot-extract-quarter-inner-replacer",
                inner_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    fun_expr(
                        "Ceil",
                        vec![
                            binary_expr(
                                fun_expr(
                                    "DatePart",
                                    vec![
                                        literal_string("MONTH"),
                                        column_expr("?column"),
                                    ],
                                ),
                                "/",
                                fun_expr(
                                    "NullIf",
                                    vec![
                                        literal_float(3.0),
                                        literal_float(0.0),
                                    ],
                                ),
                            ),
                        ],
                    ),
                )],
                alias_expr(
                    fun_expr(
                        "DateTrunc",
                        vec![
                            literal_string("quarter"),
                            inner_aggregate_split_replacer(column_expr("?column"), "?alias_to_cube"),
                        ],
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column(
                    "?expr",
                    "?alias",
                    None,
                ),
            ),
            transforming_chain_rewrite(
                "split-thoughtspot-extract-quarter-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?alias_to_cube"),
                vec![(
                    "?expr",
                    fun_expr(
                        "Ceil",
                        vec![
                            binary_expr(
                                fun_expr(
                                    "DatePart",
                                    vec![
                                        literal_string("MONTH"),
                                        column_expr("?column"),
                                    ],
                                ),
                                "/",
                                fun_expr(
                                    "NullIf",
                                    vec![
                                        literal_float(3.0),
                                        literal_float(0.0),
                                    ],
                                ),
                            ),
                        ],
                    ),
                )],
                alias_expr(
                    fun_expr(
                        "DatePart",
                        vec![literal_string("quarter"), column_expr("?outer_column")],
                    ),
                    "?alias",
                ),
                self.transform_original_expr_to_alias_and_column(
                    "?expr",
                    "?alias",
                    Some("?outer_column"),
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
        // Left
        rules.extend(
            self.outer_aggr_group_expr_aggr_combinator_rewrite(
                "split-push-down-left-replacer",
                |split_replacer| {
                    split_replacer(
                        fun_expr("Left", vec!["?expr".to_string(), literal_expr("?length")]),
                        "?cube",
                    )
                },
                |_| vec![],
                |split_replacer| {
                    fun_expr(
                        "Left",
                        vec![
                            split_replacer("?expr".to_string(), "?cube"),
                            literal_expr("?length"),
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
        // Right
        rules.extend(
            self.outer_aggr_group_expr_aggr_combinator_rewrite(
                "split-push-down-right-replacer",
                |split_replacer| {
                    split_replacer(
                        fun_expr("Right", vec!["?expr".to_string(), literal_expr("?length")]),
                        "?cube",
                    )
                },
                |_| vec![],
                |split_replacer| {
                    fun_expr(
                        "Right",
                        vec![
                            split_replacer("?expr".to_string(), "?cube"),
                            literal_expr("?length"),
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
        // NullIf
        rules.extend(
            self.outer_aggr_group_expr_aggr_combinator_rewrite(
                "split-push-down-nullif-replacer",
                |split_replacer| {
                    split_replacer(
                        fun_expr("NullIf", vec!["?expr".to_string(), literal_expr("?else")]),
                        "?cube",
                    )
                },
                |_| vec![],
                |split_replacer| {
                    fun_expr(
                        "NullIf",
                        vec![
                            split_replacer("?expr".to_string(), "?cube"),
                            literal_expr("?else"),
                        ],
                    )
                },
                |_, _| true,
                false,
                true,
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
        self.transform_original_expr_to_alias_and_column_with_chain_transform(
            original_expr_var,
            out_alias_expr_var,
            out_column_expr_var,
            |_, _| true,
        )
    }

    pub fn transform_original_expr_to_alias_and_column_with_chain_transform<T>(
        &self,
        original_expr_var: &'static str,
        out_alias_expr_var: &'static str,
        out_column_expr_var: Option<&'static str>,
        chain_transform_fn: T,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool
    where
        T: Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool,
    {
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

                if !chain_transform_fn(egraph, subst) {
                    return false;
                }

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

    fn transform_binary_columns_dimensions(
        &self,
        left_column_var: &'static str,
        right_column_var: &'static str,
        alias_to_cube_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let left_column_var = var!(left_column_var);
        let right_column_var = var!(right_column_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for alias_to_cube in var_iter!(
                egraph[subst[alias_to_cube_var]],
                InnerAggregateSplitReplacerAliasToCube
            )
            .chain(var_iter!(
                egraph[subst[alias_to_cube_var]],
                OuterAggregateSplitReplacerAliasToCube
            )) {
                for left_column in var_iter!(egraph[subst[left_column_var]], ColumnExprColumn) {
                    if let Some((_, left_cube)) =
                        meta_context.find_cube_by_column(&alias_to_cube, &left_column)
                    {
                        if left_cube.lookup_dimension(&left_column.name).is_none() {
                            continue;
                        }

                        for right_column in
                            var_iter!(egraph[subst[right_column_var]], ColumnExprColumn)
                        {
                            if let Some((_, right_cube)) =
                                meta_context.find_cube_by_column(&alias_to_cube, &right_column)
                            {
                                if right_cube.lookup_dimension(&right_column.name).is_some() {
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

    fn transform_min_max_granularity(
        &self,
        left_granularity_var: &'static str,
        right_granularity_var: &'static str,
        out_granularity_var: &'static str,
        is_max: bool,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let left_granularity_var = var!(left_granularity_var);
        let right_granularity_var = var!(right_granularity_var);
        let out_granularity_var = var!(out_granularity_var);
        move |egraph, subst| {
            for left_granularity in var_iter!(egraph[subst[left_granularity_var]], LiteralExprValue)
            {
                for right_granularity in
                    var_iter!(egraph[subst[right_granularity_var]], LiteralExprValue)
                {
                    if let Some(out_granularity) = utils::min_max_granularity(
                        &left_granularity,
                        &right_granularity,
                        is_max,
                        Some(true),
                    ) {
                        subst.insert(
                            out_granularity_var,
                            egraph.add(LogicalPlanLanguage::LiteralExprValue(LiteralExprValue(
                                ScalarValue::Utf8(Some(out_granularity)),
                            ))),
                        );
                        return true;
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
        aggr_expr_var: Option<&'static str>,
        fun_var: Option<&'static str>,
        distinct_var: Option<&'static str>,
        out_expr_var: Option<&'static str>,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_expr_var = var!(cube_expr_var);
        let column_var = column_var.map(|column_var| var!(column_var));
        let aggr_expr_var = aggr_expr_var.map(|v| var!(v));
        let fun_var = fun_var.map(|v| var!(v));
        let distinct_var = distinct_var.map(|v| var!(v));
        let out_expr_var = out_expr_var.map(|v| var!(v));
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
                        if let Some(measure) = cube.lookup_measure(&column.name) {
                            if let Some((((fun_var, distinct_var), out_expr_var), aggr_expr_var)) =
                                fun_var
                                    .zip(distinct_var)
                                    .zip(out_expr_var)
                                    .zip(aggr_expr_var)
                            {
                                for fun in
                                    var_iter!(egraph[subst[fun_var]], AggregateFunctionExprFun)
                                {
                                    for distinct in var_iter!(
                                        egraph[subst[distinct_var]],
                                        AggregateFunctionExprDistinct
                                    ) {
                                        // If count is wrapping non count measure let's allow split and just count rows
                                        // TODO it might worth allowing all other non matching aggregate functions
                                        if let AggregateFunction::Count = fun {
                                            if !distinct {
                                                let agg_type = MemberRules::get_agg_type(
                                                    Some(&fun),
                                                    *distinct,
                                                );
                                                if !measure.is_same_agg_type(&agg_type.unwrap()) {
                                                    if let Some(expr_name) = original_expr_name(
                                                        egraph,
                                                        subst[aggr_expr_var],
                                                    ) {
                                                        let measure_fun = egraph.add(
                                                            LogicalPlanLanguage::AggregateFunctionExprFun(
                                                                AggregateFunctionExprFun(AggregateFunction::Count)
                                                            ),
                                                        );

                                                        let measure_distinct = egraph.add(
                                                            LogicalPlanLanguage::AggregateFunctionExprDistinct(
                                                                AggregateFunctionExprDistinct(false)
                                                            ),
                                                        );
                                                        let tail = egraph.add(
                                                            LogicalPlanLanguage::AggregateFunctionExprArgs(
                                                                vec![],
                                                            ),
                                                        );

                                                        let literal_expr_value = egraph.add(
                                                            LogicalPlanLanguage::LiteralExprValue(
                                                                LiteralExprValue(
                                                                    ScalarValue::Int64(None),
                                                                ),
                                                            ),
                                                        );

                                                        let column_expr = egraph.add(
                                                            LogicalPlanLanguage::LiteralExpr([
                                                                literal_expr_value,
                                                            ]),
                                                        );
                                                        let args = egraph.add(
                                                            LogicalPlanLanguage::AggregateFunctionExprArgs(
                                                                vec![column_expr, tail],
                                                            ),
                                                        );
                                                        let aggr_expr = egraph.add(
                                                            LogicalPlanLanguage::AggregateFunctionExpr(
                                                                [measure_fun, args, measure_distinct],
                                                            ),
                                                        );
                                                        let alias = egraph.add(
                                                            LogicalPlanLanguage::AliasExprAlias(
                                                                AliasExprAlias(expr_name),
                                                            ),
                                                        );

                                                        let alias_expr = egraph.add(
                                                            LogicalPlanLanguage::AliasExpr([
                                                                aggr_expr, alias,
                                                            ]),
                                                        );
                                                        subst.insert(out_expr_var, alias_expr);
                                                        return true;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            if let Some((aggr_expr_var, out_expr_var)) =
                                aggr_expr_var.zip(out_expr_var)
                            {
                                subst.insert(out_expr_var, subst[aggr_expr_var]);
                            }
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
                                || cube.lookup_segment(&column.name).is_some()
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
                                if let Some(agg_type) = &measure.agg_type {
                                    if let Some(output_fun) = utils::reaggragate_fun(agg_type) {
                                        subst.insert(
                                            output_fun_var,
                                            egraph.add(
                                                LogicalPlanLanguage::AggregateFunctionExprFun(
                                                    AggregateFunctionExprFun(output_fun),
                                                ),
                                            ),
                                        );
                                        subst.insert(
                                            distinct_var,
                                            egraph.add(
                                                LogicalPlanLanguage::AggregateFunctionExprDistinct(
                                                    AggregateFunctionExprDistinct(false),
                                                ),
                                            ),
                                        );
                                        return true;
                                    }
                                }
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
                                || cube.lookup_segment(&column.name).is_some()
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
        let meta = self.cube_context.meta.clone();
        move |egraph, subst| {
            if let Some(expr_to_alias) =
                &egraph.index(subst[projection_expr_var]).data.expr_to_alias
            {
                for alias_to_cube in
                    var_iter!(egraph[subst[alias_to_cube_var]], CubeScanAliasToCube).cloned()
                {
                    // Replace outer projection columns with unqualified variants
                    if let Some(expr_name_to_alias) = expr_to_alias
                        .clone()
                        .into_iter()
                        .map(|(expr, alias, explicit)| {
                            let default_alias = Some((alias.clone(), None));
                            if explicit == Some(true) {
                                return default_alias;
                            }
                            if let Expr::Column(column) = &expr {
                                if let Some((_, cube)) =
                                    meta.find_cube_by_column(&alias_to_cube, column)
                                {
                                    if let Some(measure) = cube.lookup_measure(&column.name) {
                                        if let Some(agg_type) = &measure.agg_type {
                                            let aggr_expr = Expr::AggregateFunction {
                                                fun: utils::reaggragate_fun(&agg_type)?,
                                                args: vec![expr],
                                                distinct: false,
                                            };
                                            let expr_name =
                                                aggr_expr.name(&DFSchema::empty()).ok()?;
                                            return Some((expr_name, Some(alias)));
                                        }
                                    }
                                }
                            }
                            default_alias
                        })
                        .collect::<Option<Vec<_>>>()
                    {
                        let expr = expr_name_to_alias
                            .into_iter()
                            .map(|(name, alias)| {
                                let column = Column::from_name(name);
                                let column_expr_column = egraph.add(
                                    LogicalPlanLanguage::ColumnExprColumn(ColumnExprColumn(column)),
                                );
                                let column_expr = egraph
                                    .add(LogicalPlanLanguage::ColumnExpr([column_expr_column]));
                                if let Some(alias) = alias {
                                    let alias_expr_alias = egraph.add(
                                        LogicalPlanLanguage::AliasExprAlias(AliasExprAlias(alias)),
                                    );
                                    return egraph.add(LogicalPlanLanguage::AliasExpr([
                                        column_expr,
                                        alias_expr_alias,
                                    ]));
                                }
                                column_expr
                            })
                            .collect::<Vec<_>>();

                        let mut projection_expr =
                            egraph.add(LogicalPlanLanguage::ProjectionExpr(vec![]));
                        for i in expr.into_iter().rev() {
                            projection_expr =
                                egraph.add(LogicalPlanLanguage::ProjectionExpr(vec![
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
                            egraph.add(
                                LogicalPlanLanguage::InnerAggregateSplitReplacerAliasToCube(
                                    InnerAggregateSplitReplacerAliasToCube(alias_to_cube.clone()),
                                ),
                            ),
                        );

                        subst.insert(
                            group_expr_cube_var,
                            egraph.add(LogicalPlanLanguage::GroupExprSplitReplacerAliasToCube(
                                GroupExprSplitReplacerAliasToCube(alias_to_cube.clone()),
                            )),
                        );

                        subst.insert(
                            group_aggregate_cube_var,
                            egraph.add(
                                LogicalPlanLanguage::GroupAggregateSplitReplacerAliasToCube(
                                    GroupAggregateSplitReplacerAliasToCube(alias_to_cube.clone()),
                                ),
                            ),
                        );

                        return true;
                    }
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
        is_outer_projection: bool,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let binary_op_var = var!(binary_op_var);
        let literal_expr_var = var!(literal_expr_var);

        move |egraph, subst| {
            for operator in var_iter!(egraph[subst[binary_op_var]], BinaryExprOp) {
                match operator {
                    Operator::Plus | Operator::Minus | Operator::Multiply | Operator::Divide => {
                        let check_is_zero = match operator {
                            Operator::Plus | Operator::Minus | Operator::Divide => false,
                            Operator::Multiply => true,
                            _ => continue,
                        };

                        for scalar in
                            var_iter!(egraph[subst[literal_expr_var]], LiteralExprValue).cloned()
                        {
                            if !is_outer_projection {
                                return true;
                            }

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

                            return true;
                        }
                    }
                    Operator::StringConcat => {
                        for scalar in var_iter!(egraph[subst[literal_expr_var]], LiteralExprValue) {
                            match scalar {
                                ScalarValue::Utf8(Some(_)) | ScalarValue::LargeUtf8(Some(_)) => (),
                                _ => continue,
                            };

                            return true;
                        }
                    }
                    Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
                    | Operator::Eq
                    | Operator::NotEq => {
                        if is_outer_projection {
                            continue;
                        }

                        for _ in var_iter!(egraph[subst[literal_expr_var]], LiteralExprValue) {
                            return true;
                        }
                    }
                    _ => continue,
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

    pub fn transform_outer_aggr_agg_fun_cast_arg(
        &self,
        expr_var: &'static str,
        alias_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let expr_var = var!(expr_var);
        let alias_var = var!(alias_var);
        move |egraph, subst| {
            if let Some(name) = original_expr_name(egraph, subst[expr_var]) {
                subst.insert(
                    alias_var,
                    egraph.add(LogicalPlanLanguage::AliasExprAlias(AliasExprAlias(name))),
                );
                return true;
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
        column_var: Option<&'static str>,
        alias_expr_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_var = var!(cube_var);
        let original_expr_var = var!(original_expr_var);
        let column_var = column_var.map(|v| var!(v));
        let alias_expr_var = var!(alias_expr_var);
        let meta = self.cube_context.meta.clone();
        move |egraph, subst| {
            for alias_to_cube in var_iter!(
                egraph[subst[cube_var]],
                OuterProjectionSplitReplacerAliasToCube
            )
            .cloned()
            {
                if let Some(name) = original_expr_name(egraph, subst[original_expr_var]) {
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

    fn transform_cast_inner(
        &self,
        expr_var: &'static str,
        alias_to_cube_var: &'static str,
        inner_expr_var: &'static str,
        data_type_var: &'static str,
        new_expr_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let expr_var = var!(expr_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let inner_expr_var = var!(inner_expr_var);
        let data_type_var = var!(data_type_var);
        let new_expr_var = var!(new_expr_var);
        move |egraph, subst| {
            let expr_id = subst[expr_var];
            let res = egraph[expr_id]
                .data
                .original_expr
                .as_ref()
                .ok_or(CubeError::internal(format!(
                    "Original expr wasn't prepared for {:?}",
                    expr_id
                )));

            if let Ok(expr) = res {
                let inner_expr_id = subst[inner_expr_var];
                let res =
                    egraph[inner_expr_id]
                        .data
                        .original_expr
                        .as_ref()
                        .ok_or(CubeError::internal(format!(
                            "Original expr wasn't prepared for {:?}",
                            inner_expr_id
                        )));

                if let Ok(inner_expr) = res {
                    match inner_expr {
                        Expr::Column(_) => {
                            for data_type in
                                var_iter!(egraph[subst[data_type_var]], CastExprDataType).cloned()
                            {
                                if data_type == ArrowDataType::Date32 {
                                    // TODO unwrap
                                    let name = expr.name(&DFSchema::empty()).unwrap();

                                    let granularity_value_id = egraph.add(
                                        LogicalPlanLanguage::LiteralExprValue(LiteralExprValue(
                                            ScalarValue::Utf8(Some("day".to_string())),
                                        )),
                                    );
                                    let granularity_id =
                                        egraph.add(LogicalPlanLanguage::LiteralExpr([
                                            granularity_value_id,
                                        ]));
                                    let date_trunc_args_empty_tail_id = egraph
                                        .add(LogicalPlanLanguage::ScalarFunctionExprArgs(vec![]));
                                    let date_trunc_args_column_id = egraph.add(
                                        LogicalPlanLanguage::ScalarFunctionExprArgs(vec![
                                            subst[inner_expr_var],
                                            date_trunc_args_empty_tail_id,
                                        ]),
                                    );
                                    let date_trunc_args_id =
                                        egraph.add(LogicalPlanLanguage::ScalarFunctionExprArgs(
                                            vec![granularity_id, date_trunc_args_column_id],
                                        ));
                                    let date_trunc_name_id =
                                        egraph.add(LogicalPlanLanguage::ScalarFunctionExprFun(
                                            ScalarFunctionExprFun(BuiltinScalarFunction::DateTrunc),
                                        ));
                                    let date_trunc_id =
                                        egraph.add(LogicalPlanLanguage::ScalarFunctionExpr([
                                            date_trunc_name_id,
                                            date_trunc_args_id,
                                        ]));
                                    let alias_id = egraph.add(LogicalPlanLanguage::AliasExprAlias(
                                        AliasExprAlias(name),
                                    ));
                                    let alias_expr_id =
                                        egraph.add(LogicalPlanLanguage::AliasExpr([
                                            date_trunc_id,
                                            alias_id,
                                        ]));

                                    subst.insert(new_expr_var, alias_expr_id);
                                    return true;
                                }
                            }
                        }
                        _ => (),
                    }
                }
            }

            subst.insert(
                new_expr_var,
                egraph.add(LogicalPlanLanguage::InnerAggregateSplitReplacer([
                    subst[inner_expr_var],
                    subst[alias_to_cube_var],
                ])),
            );
            true
        }
    }

    fn transform_cast_outer(
        &self,
        expr_var: &'static str,
        alias_to_cube_var: &'static str,
        inner_expr_var: &'static str,
        data_type_var: &'static str,
        new_expr_var: &'static str,
        is_outer_projection: bool,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let expr_var = var!(expr_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let inner_expr_var = var!(inner_expr_var);
        let data_type_var = var!(data_type_var);
        let new_expr_var = var!(new_expr_var);
        move |egraph, subst| {
            let expr_id = subst[expr_var];
            let res = egraph[expr_id]
                .data
                .original_expr
                .as_ref()
                .ok_or(CubeError::internal(format!(
                    "Original expr wasn't prepared for {:?}",
                    expr_id
                )));

            if let Ok(expr) = res {
                let inner_expr_id = subst[inner_expr_var];
                let res =
                    egraph[inner_expr_id]
                        .data
                        .original_expr
                        .as_ref()
                        .ok_or(CubeError::internal(format!(
                            "Original expr wasn't prepared for {:?}",
                            inner_expr_id
                        )));

                if let Ok(inner_expr) = res {
                    match inner_expr {
                        Expr::Column(_) => {
                            for data_type in
                                var_iter!(egraph[subst[data_type_var]], CastExprDataType).cloned()
                            {
                                if data_type == ArrowDataType::Date32 {
                                    // TODO unwrap
                                    let name = expr.name(&DFSchema::empty()).unwrap();
                                    let column = Column::from_name(name.to_string());

                                    let column_id =
                                        egraph.add(LogicalPlanLanguage::ColumnExprColumn(
                                            ColumnExprColumn(column),
                                        ));
                                    let column_expr_id =
                                        egraph.add(LogicalPlanLanguage::ColumnExpr([column_id]));
                                    let cast_expr_id = egraph.add(LogicalPlanLanguage::CastExpr([
                                        column_expr_id,
                                        subst[data_type_var],
                                    ]));
                                    let alias_id = egraph.add(LogicalPlanLanguage::AliasExprAlias(
                                        AliasExprAlias(name),
                                    ));
                                    let alias_expr_id =
                                        egraph.add(LogicalPlanLanguage::AliasExpr([
                                            cast_expr_id,
                                            alias_id,
                                        ]));

                                    subst.insert(new_expr_var, alias_expr_id);
                                    return true;
                                }
                            }
                        }
                        _ => (),
                    }
                }
            }

            let split_replacer_id = if is_outer_projection {
                egraph.add(LogicalPlanLanguage::OuterProjectionSplitReplacer([
                    subst[inner_expr_var],
                    subst[alias_to_cube_var],
                ]))
            } else {
                egraph.add(LogicalPlanLanguage::OuterAggregateSplitReplacer([
                    subst[inner_expr_var],
                    subst[alias_to_cube_var],
                ]))
            };

            subst.insert(
                new_expr_var,
                egraph.add(LogicalPlanLanguage::CastExpr([
                    split_replacer_id,
                    subst[data_type_var],
                ])),
            );
            true
        }
    }

    fn transform_is_interval_of_granularity(
        &self,
        interval_var: &'static str,
        granularity: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let interval_var = var!(interval_var);
        move |egraph, subst| {
            if let Some(expected_interval) = utils::granularity_str_to_interval(granularity) {
                for interval in var_iter!(egraph[subst[interval_var]], LiteralExprValue).cloned() {
                    if expected_interval == interval {
                        return true;
                    }
                }
            }

            false
        }
    }
}
