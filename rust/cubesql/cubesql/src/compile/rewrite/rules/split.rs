use crate::{
    compile::{
        engine::provider::CubeContext,
        rewrite::{
            agg_fun_expr, aggr_aggr_expr, aggr_aggr_expr_empty_tail, aggr_group_expr,
            aggr_group_expr_empty_tail, aggregate, alias_expr, analysis::LogicalPlanAnalysis,
            binary_expr, cast_expr, column_expr, cube_scan, fun_expr,
            inner_aggregate_split_replacer, literal_expr, literal_string, original_expr_name,
            outer_aggregate_split_replacer, outer_projection_split_replacer, projection,
            projection_expr, projection_expr_empty_tail, rewrite, rewriter::RewriteRules,
            rules::members::MemberRules, transforming_chain_rewrite, transforming_rewrite,
            AggregateFunctionExprFun, AliasExprAlias, BinaryExprOp, ColumnExprColumn,
            CubeScanTableName, InnerAggregateSplitReplacerCube, LiteralExprValue,
            LogicalPlanLanguage, OuterAggregateSplitReplacerCube, OuterProjectionSplitReplacerCube,
            ProjectionAlias, TableScanSourceTableName,
        },
    },
    transport::V1CubeMetaExt,
    var, var_iter,
};
use datafusion::{
    logical_plan::{Column, Operator},
    physical_plan::aggregates::AggregateFunction,
    scalar::ScalarValue,
};
use egg::{EGraph, Rewrite, Subst};
use std::sync::Arc;

pub struct SplitRules {
    cube_context: Arc<CubeContext>,
}

impl RewriteRules for SplitRules {
    fn rewrite_rules(&self) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
        vec![
            transforming_rewrite(
                "split-projection-aggregate",
                aggregate(
                    cube_scan(
                        "?source_table_name",
                        "?members",
                        "?filters",
                        "?orders",
                        "?limit",
                        "?offset",
                        "?aliases",
                        "?table_name",
                        "CubeScanSplit:false",
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
                            "?source_table_name",
                            "?members",
                            "?filters",
                            "?orders",
                            "?limit",
                            "?offset",
                            "?aliases",
                            "?table_name",
                            "CubeScanSplit:true",
                        ),
                        inner_aggregate_split_replacer("?group_expr", "?inner_aggregate_cube"),
                        inner_aggregate_split_replacer("?aggr_expr", "?inner_aggregate_cube"),
                    ),
                    "?projection_alias",
                ),
                self.split_projection_aggregate(
                    "?source_table_name",
                    "?inner_aggregate_cube",
                    "?outer_projection_cube",
                    "?table_name",
                    "?projection_alias",
                ),
            ),
            transforming_rewrite(
                "split-projection-projection",
                projection(
                    "?expr",
                    cube_scan(
                        "?source_table_name",
                        "?members",
                        "?filters",
                        "?orders",
                        "?limit",
                        "?offset",
                        "?aliases",
                        "?table_name",
                        "CubeScanSplit:false",
                    ),
                    "?alias",
                ),
                projection(
                    outer_projection_split_replacer("?expr", "?outer_projection_cube"),
                    projection(
                        inner_aggregate_split_replacer("?expr", "?inner_aggregate_cube"),
                        cube_scan(
                            "?source_table_name",
                            "?members",
                            "?filters",
                            "?orders",
                            "?limit",
                            "?offset",
                            "?aliases",
                            "?table_name",
                            "CubeScanSplit:true",
                        ),
                        "?alias",
                    ),
                    "?projection_alias",
                ),
                self.split_projection_aggregate(
                    "?source_table_name",
                    "?inner_aggregate_cube",
                    "?outer_projection_cube",
                    "?table_name",
                    "?projection_alias",
                ),
            ),
            // TODO: Aggregation on top of Projection to re-aggregate results after projection modifying
            transforming_rewrite(
                "split-projection-projection-aggr",
                projection(
                    "?expr",
                    cube_scan(
                        "?source_table_name",
                        "?members",
                        "?filters",
                        "?orders",
                        "?limit",
                        "?offset",
                        "?aliases",
                        "?table_name",
                        "CubeScanSplit:false",
                    ),
                    "?alias",
                ),
                projection(
                    outer_aggregate_split_replacer("?expr", "?outer_aggregate_cube"),
                    projection(
                        inner_aggregate_split_replacer("?expr", "?inner_aggregate_cube"),
                        cube_scan(
                            "?source_table_name",
                            "?members",
                            "?filters",
                            "?orders",
                            "?limit",
                            "?offset",
                            "?aliases",
                            "?table_name",
                            "CubeScanSplit:true",
                        ),
                        "?projection_alias",
                    ),
                    "?alias",
                ),
                self.split_projection_projection_aggregate(
                    "?source_table_name",
                    "?inner_aggregate_cube",
                    "?outer_aggregate_cube",
                    "?table_name",
                    "?projection_alias",
                ),
            ),
            transforming_rewrite(
                "split-aggregate-aggregate",
                aggregate(
                    cube_scan(
                        "?source_table_name",
                        "?members",
                        "?filters",
                        "?orders",
                        "?limit",
                        "?offset",
                        "?aliases",
                        "?table_name",
                        "CubeScanSplit:false",
                    ),
                    "?group_expr",
                    "?aggr_expr",
                ),
                aggregate(
                    aggregate(
                        cube_scan(
                            "?source_table_name",
                            "?members",
                            "?filters",
                            "?orders",
                            "?limit",
                            "?offset",
                            "?aliases",
                            "?table_name",
                            "CubeScanSplit:true",
                        ),
                        inner_aggregate_split_replacer("?group_expr", "?inner_aggregate_cube"),
                        inner_aggregate_split_replacer("?aggr_expr", "?inner_aggregate_cube"),
                    ),
                    outer_aggregate_split_replacer("?group_expr", "?outer_aggregate_cube"),
                    outer_aggregate_split_replacer("?aggr_expr", "?outer_aggregate_cube"),
                ),
                self.split_aggregate_aggregate(
                    "?source_table_name",
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
            // Date trunc
            rewrite(
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
                    vec![literal_expr("?granularity"), column_expr("?column")],
                ),
            ),
            transforming_chain_rewrite(
                "split-push-down-date-trunc-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?cube"),
                vec![(
                    "?expr",
                    fun_expr(
                        "DateTrunc",
                        vec![literal_expr("?granularity"), column_expr("?column")],
                    ),
                )],
                "?alias".to_string(),
                MemberRules::transform_original_expr_alias("?expr", "?alias"),
            ),
            transforming_chain_rewrite(
                "split-push-down-date-trunc-outer-replacer",
                outer_projection_split_replacer("?expr", "?cube"),
                vec![(
                    "?expr",
                    fun_expr(
                        "DateTrunc",
                        vec![literal_expr("?granularity"), column_expr("?column")],
                    ),
                )],
                "?alias".to_string(),
                MemberRules::transform_original_expr_alias("?expr", "?alias"),
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
            transforming_chain_rewrite(
                "split-push-down-date-part-outer-aggr-replacer",
                outer_aggregate_split_replacer(
                    fun_expr(
                        "DatePart",
                        vec![literal_expr("?granularity"), "?expr".to_string()],
                    ),
                    "?cube",
                ),
                vec![("?expr", column_expr("?column"))],
                fun_expr(
                    "DatePart",
                    vec![
                        literal_expr("?granularity"),
                        alias_expr("?alias_column", "?alias"),
                    ],
                ),
                MemberRules::transform_original_expr_date_trunc(
                    "?expr",
                    "?granularity",
                    "?alias_column",
                    Some("?alias"),
                    false,
                ),
            ),
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
                            "?date_trunc_granularity".to_string(),
                            column_expr("?column"),
                        ],
                    ),
                )],
                alias_expr(
                    fun_expr(
                        "DateTrunc",
                        vec![
                            literal_expr("?date_part_granularity"),
                            column_expr("?column"),
                        ],
                    ),
                    "?alias",
                ),
                MemberRules::transform_original_expr_date_trunc(
                    "?original_expr",
                    Some("?simplified_expr"),
                    "?granularity",
                    "?granularity",
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
                    vec![outer_projection_split_replacer("?expr", "?cube")],
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
            rewrite(
                "split-push-down-substr-outer-aggr-replacer",
                outer_aggregate_split_replacer(
                    fun_expr("Substr", vec!["?expr", "?from", "?to"]),
                    "?cube",
                ),
                fun_expr(
                    "Substr",
                    vec![
                        outer_aggregate_split_replacer("?expr", "?cube"),
                        "?from".to_string(),
                        "?to".to_string(),
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
            // to_char
            transforming_chain_rewrite(
                "split-push-down-aggr-fun-to-char-inner-aggr-replacer",
                inner_aggregate_split_replacer(
                    cast_expr(
                        udf_expr("to_char", vec!["?expr".to_string(), literal_string("MMDD")]),
                        "?data_type",
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
                        vec![literal_string("day"), column_expr("?column")],
                    ),
                    "?alias",
                ),
                MemberRules::transform_original_expr_date_trunc(
                    "?expr",
                    "?date_trunc_granularity",
                    "?alias_column",
                    Some("?alias"),
                    true,
                ),
            ),
            transforming_chain_rewrite(
                "split-push-down-aggr-fun-to-char-outer-aggr-replacer",
                outer_aggregate_split_replacer(
                    cast_expr(
                        udf_expr("to_char", vec!["?expr".to_string(), literal_string("MMDD")]),
                        "?data_type",
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
                cast_expr(
                    udf_expr(
                        "to_char",
                        vec![
                            alias_expr("?alias_column", "?alias"),
                            literal_string("MMDD"),
                        ],
                    ),
                    "?data_type",
                ),
                MemberRules::transform_original_expr_date_trunc(
                    "?expr",
                    "?date_trunc_granularity",
                    "?alias_column",
                    Some("?alias"),
                    false,
                ),
            ),
        ]
    }
}

impl SplitRules {
    pub fn new(cube_context: Arc<CubeContext>) -> Self {
        Self {
            cube_context: cube_context,
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
            for cube in var_iter!(
                egraph[subst[cube_expr_var]],
                InnerAggregateSplitReplacerCube
            )
            .cloned()
            {
                for fun in var_iter!(egraph[subst[fun_expr_var]], AggregateFunctionExprFun).cloned()
                {
                    if fun == AggregateFunction::Min || fun == AggregateFunction::Max {
                        if let Some(cube) = meta.find_cube_with_name(&cube) {
                            for column in var_iter!(egraph[subst[column_var]], ColumnExprColumn) {
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
            for cube in var_iter!(
                egraph[subst[cube_expr_var]],
                InnerAggregateSplitReplacerCube
            )
            .cloned()
            {
                if let Some(cube) = meta.find_cube_with_name(&cube) {
                    for column in column_var
                        .map(|column_var| {
                            var_iter!(egraph[subst[column_var]], ColumnExprColumn)
                                .map(|c| c.name.to_string())
                                .collect()
                        })
                        .unwrap_or(vec![MemberRules::default_count_measure_name()])
                    {
                        if cube.lookup_measure(&column).is_some() {
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
            for cube in var_iter!(
                egraph[subst[cube_expr_var]],
                InnerAggregateSplitReplacerCube
            )
            .cloned()
            {
                if let Some(cube) = meta.find_cube_with_name(&cube) {
                    if cube
                        .lookup_measure(&MemberRules::default_count_measure_name())
                        .is_none()
                    {
                        return true;
                    }
                }
            }
            false
        }
    }

    fn split_projection_projection_aggregate(
        &self,
        cube_expr_var: &'static str,
        inner_aggregate_cube_expr_var: &'static str,
        outer_aggregate_cube_expr_var: &'static str,
        table_name_var: &'static str,
        projection_alias_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_expr_var = var!(cube_expr_var);
        let inner_aggregate_cube_expr_var = var!(inner_aggregate_cube_expr_var);
        let outer_aggregate_cube_expr_var = var!(outer_aggregate_cube_expr_var);
        let table_name_var = var!(table_name_var);
        let projection_alias_var = var!(projection_alias_var);
        move |egraph, subst| {
            for cube in var_iter!(egraph[subst[cube_expr_var]], TableScanSourceTableName).cloned() {
                for table_name in
                    var_iter!(egraph[subst[table_name_var]], CubeScanTableName).cloned()
                {
                    subst.insert(
                        projection_alias_var,
                        egraph.add(LogicalPlanLanguage::ProjectionAlias(ProjectionAlias(Some(
                            table_name.to_string(),
                        )))),
                    );

                    subst.insert(
                        inner_aggregate_cube_expr_var,
                        egraph.add(LogicalPlanLanguage::InnerAggregateSplitReplacerCube(
                            InnerAggregateSplitReplacerCube(cube.to_string()),
                        )),
                    );

                    subst.insert(
                        outer_aggregate_cube_expr_var,
                        egraph.add(LogicalPlanLanguage::OuterAggregateSplitReplacerCube(
                            OuterAggregateSplitReplacerCube(cube.to_string()),
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
        cube_expr_var: &'static str,
        inner_aggregate_cube_expr_var: &'static str,
        outer_projection_cube_expr_var: &'static str,
        table_name_var: &'static str,
        projection_alias_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_expr_var = var!(cube_expr_var);
        let inner_aggregate_cube_expr_var = var!(inner_aggregate_cube_expr_var);
        let outer_projection_cube_expr_var = var!(outer_projection_cube_expr_var);
        let table_name_var = var!(table_name_var);
        let projection_alias_var = var!(projection_alias_var);
        move |egraph, subst| {
            for cube in var_iter!(egraph[subst[cube_expr_var]], TableScanSourceTableName).cloned() {
                for table_name in
                    var_iter!(egraph[subst[table_name_var]], CubeScanTableName).cloned()
                {
                    subst.insert(
                        projection_alias_var,
                        egraph.add(LogicalPlanLanguage::ProjectionAlias(ProjectionAlias(Some(
                            table_name.to_string(),
                        )))),
                    );

                    subst.insert(
                        inner_aggregate_cube_expr_var,
                        egraph.add(LogicalPlanLanguage::InnerAggregateSplitReplacerCube(
                            InnerAggregateSplitReplacerCube(cube.to_string()),
                        )),
                    );

                    subst.insert(
                        outer_projection_cube_expr_var,
                        egraph.add(LogicalPlanLanguage::OuterProjectionSplitReplacerCube(
                            OuterProjectionSplitReplacerCube(cube.to_string()),
                        )),
                    );
                    return true;
                }
            }
            false
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
        cube_expr_var: &'static str,
        inner_aggregate_cube_expr_var: &'static str,
        outer_aggregate_cube_expr_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_expr_var = var!(cube_expr_var);
        let inner_aggregate_cube_expr_var = var!(inner_aggregate_cube_expr_var);
        let outer_aggregate_cube_expr_var = var!(outer_aggregate_cube_expr_var);
        move |egraph, subst| {
            for cube in var_iter!(egraph[subst[cube_expr_var]], TableScanSourceTableName).cloned() {
                subst.insert(
                    inner_aggregate_cube_expr_var,
                    egraph.add(LogicalPlanLanguage::InnerAggregateSplitReplacerCube(
                        InnerAggregateSplitReplacerCube(cube.to_string()),
                    )),
                );

                subst.insert(
                    outer_aggregate_cube_expr_var,
                    egraph.add(LogicalPlanLanguage::OuterAggregateSplitReplacerCube(
                        OuterAggregateSplitReplacerCube(cube.to_string()),
                    )),
                );

                return true;
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
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_var = var!(cube_var);
        let original_expr_var = var!(original_expr_var);
        let fun_expr_var = var!(fun_expr_var);
        let arg_var = var!(arg_var);
        let column_var = column_var.map(|column_var| var!(column_var));
        let alias_expr_var = var!(alias_expr_var);
        let outer_alias_expr_var = var!(outer_alias_expr_var);
        let output_fun_var = var!(output_fun_var);
        let meta = self.cube_context.meta.clone();
        move |egraph, subst| {
            for fun in var_iter!(egraph[subst[fun_expr_var]], AggregateFunctionExprFun) {
                let output_fun = match fun {
                    AggregateFunction::Count => AggregateFunction::Sum,
                    AggregateFunction::Sum => AggregateFunction::Sum,
                    AggregateFunction::Min => AggregateFunction::Min,
                    AggregateFunction::Max => AggregateFunction::Max,
                    _ => continue,
                };

                for cube in
                    var_iter!(egraph[subst[cube_var]], OuterAggregateSplitReplacerCube).cloned()
                {
                    let (name, cube) = match (
                        original_expr_name(egraph, subst[original_expr_var]),
                        meta.find_cube_with_name(&cube),
                    ) {
                        (Some(name), Some(cube)) => (name, cube),
                        _ => continue,
                    };

                    let inner_and_outer_alias: Option<(String, String)> = if column_var.is_none() {
                        if cube
                            .lookup_measure(&MemberRules::default_count_measure_name())
                            .is_some()
                        {
                            Some((name.to_string(), name.to_string()))
                        } else {
                            None
                        }
                    } else {
                        let mut aliases = None;
                        for column in
                            var_iter!(egraph[subst[column_var.unwrap()]], ColumnExprColumn).cloned()
                        {
                            if cube.lookup_measure(&column.name).is_some() {
                                aliases = Some((name.to_string(), name.to_string()));
                                break;
                            } else if cube.lookup_dimension(&column.name).is_some() {
                                aliases = original_expr_name(egraph, subst[arg_var])
                                    .map(|inner| (inner, name.to_string()));
                                break;
                            }
                        }

                        aliases
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
                            output_fun_var,
                            egraph.add(LogicalPlanLanguage::AggregateFunctionExprFun(
                                AggregateFunctionExprFun(output_fun),
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
                    for cube in
                        var_iter!(egraph[subst[cube_var]], OuterAggregateSplitReplacerCube).cloned()
                    {
                        if let Some(cube) = meta.find_cube_with_name(&cube) {
                            if cube
                                .lookup_measure(&MemberRules::default_count_measure_name())
                                .is_none()
                            {
                                return true;
                            }
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
            for cube in var_iter!(egraph[subst[cube_var]], OuterAggregateSplitReplacerCube).cloned()
            {
                if let Some(name) = original_expr_name(egraph, subst[original_expr_var]) {
                    if let Some(cube) = meta.find_cube_with_name(&cube) {
                        for column in
                            var_iter!(egraph[subst[column_var]], ColumnExprColumn).cloned()
                        {
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
}
