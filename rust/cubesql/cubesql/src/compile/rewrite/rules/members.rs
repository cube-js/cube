use crate::{
    compile::{
        engine::provider::CubeContext,
        rewrite::{
            agg_fun_expr, aggr_aggr_expr, aggr_aggr_expr_empty_tail, aggr_group_expr,
            aggr_group_expr_empty_tail, aggregate, alias_expr, all_members,
            analysis::LogicalPlanAnalysis,
            binary_expr, cast_expr, change_user_expr, column_expr,
            column_name_to_member_to_aliases, column_name_to_member_vec, cross_join, cube_scan,
            cube_scan_filters_empty_tail, cube_scan_members, cube_scan_members_empty_tail,
            cube_scan_order_empty_tail, dimension_expr, expr_column_name, fun_expr, join,
            like_expr, limit, list_concat_pushdown_replacer, list_concat_pushup_replacer,
            literal_expr, literal_member, measure_expr, member_pushdown_replacer, member_replacer,
            merged_members_replacer, original_expr_name, projection, projection_expr,
            projection_expr_empty_tail, referenced_columns, rewrite,
            rewriter::RewriteRules,
            rules::{replacer_push_down_node, replacer_push_down_node_substitute_rules, utils},
            segment_expr, table_scan, time_dimension_expr, transforming_chain_rewrite,
            transforming_rewrite, udaf_expr, udf_expr, virtual_field_expr,
            AggregateFunctionExprDistinct, AggregateFunctionExprFun, AliasExprAlias,
            AllMembersAlias, AllMembersCube, BinaryExprOp, CastExprDataType, ChangeUserCube,
            ColumnExprColumn, CubeScanAliasToCube, CubeScanAliases, CubeScanCanPushdownJoin,
            CubeScanLimit, CubeScanOffset, DimensionName, JoinLeftOn, JoinRightOn,
            LikeExprEscapeChar, LikeExprLikeType, LikeExprNegated, LikeType, LimitFetch, LimitSkip,
            LiteralExprValue, LiteralMemberRelation, LiteralMemberValue, LogicalPlanLanguage,
            MeasureName, MemberErrorAliasToCube, MemberErrorError, MemberErrorPriority,
            MemberPushdownReplacerAliasToCube, MemberReplacerAliasToCube, MemberReplacerAliases,
            ProjectionAlias, SegmentName, TableScanSourceTableName, TableScanTableName,
            TimeDimensionDateRange, TimeDimensionGranularity, TimeDimensionName, VirtualFieldCube,
            VirtualFieldName,
        },
    },
    transport::{V1CubeMetaDimensionExt, V1CubeMetaExt, V1CubeMetaMeasureExt},
    var, var_iter, var_list_iter, CubeError,
};
use cubeclient::models::V1CubeMetaMeasure;
use datafusion::{
    arrow::datatypes::DataType,
    logical_plan::{Column, DFSchema, Expr, Operator},
    physical_plan::aggregates::AggregateFunction,
    scalar::ScalarValue,
};
use egg::{EGraph, Id, Rewrite, Subst, Var};
use itertools::{EitherOrBoth, Itertools};
use std::{
    collections::{HashMap, HashSet},
    ops::Index,
    sync::Arc,
};

pub struct MemberRules {
    cube_context: Arc<CubeContext>,
}

impl RewriteRules for MemberRules {
    fn rewrite_rules(&self) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
        let mut rules = vec![
            transforming_rewrite(
                "cube-scan",
                table_scan(
                    "?source_table_name",
                    "?table_name",
                    "?projection",
                    "?filters",
                    "?fetch",
                ),
                cube_scan(
                    "?alias_to_cube",
                    // TODO we might not need tail anymore here
                    cube_scan_members("?cube_scan_members", cube_scan_members_empty_tail()),
                    cube_scan_filters_empty_tail(),
                    cube_scan_order_empty_tail(),
                    "CubeScanLimit:None",
                    "CubeScanOffset:None",
                    "?cube_scan_aliases",
                    "CubeScanSplit:false",
                    "CubeScanCanPushdownJoin:true",
                    "CubeScanWrapped:false",
                ),
                self.transform_table_scan(
                    "?source_table_name",
                    "?table_name",
                    "?alias_to_cube",
                    "?cube_scan_aliases",
                    "?cube_scan_members",
                ),
            ),
            rewrite(
                "member-replacer-aggr-tail",
                member_replacer(aggr_aggr_expr_empty_tail(), "?alias_to_cube", "?aliases"),
                cube_scan_members_empty_tail(),
            ),
            rewrite(
                "member-replacer-group-tail",
                member_replacer(aggr_group_expr_empty_tail(), "?alias_to_cube", "?aliases"),
                cube_scan_members_empty_tail(),
            ),
            rewrite(
                "dimension-replacer-tail-proj",
                member_replacer(projection_expr_empty_tail(), "?alias_to_cube", "?aliases"),
                cube_scan_members_empty_tail(),
            ),
            rewrite(
                "member-replacer-aggr",
                member_replacer(
                    aggr_aggr_expr("?left", "?right"),
                    "?alias_to_cube",
                    "?aliases",
                ),
                cube_scan_members(
                    member_replacer("?left", "?alias_to_cube", "?aliases"),
                    member_replacer("?right", "?alias_to_cube", "?aliases"),
                ),
            ),
            rewrite(
                "member-replacer-group",
                member_replacer(
                    aggr_group_expr("?left", "?right"),
                    "?alias_to_cube",
                    "?aliases",
                ),
                cube_scan_members(
                    member_replacer("?left", "?alias_to_cube", "?aliases"),
                    member_replacer("?right", "?alias_to_cube", "?aliases"),
                ),
            ),
            rewrite(
                "member-replacer-projection",
                member_replacer(
                    projection_expr("?left", "?right"),
                    "?alias_to_cube",
                    "?aliases",
                ),
                cube_scan_members(
                    member_replacer("?left", "?alias_to_cube", "?aliases"),
                    member_replacer("?right", "?alias_to_cube", "?aliases"),
                ),
            ),
            self.measure_rewrite(
                "simple-count",
                agg_fun_expr("?aggr_fun", vec![literal_expr("?literal")], "?distinct"),
                None,
                Some("?distinct"),
                Some("?aggr_fun"),
                None,
            ),
            self.measure_rewrite(
                "named",
                agg_fun_expr("?aggr_fun", vec![column_expr("?column")], "?distinct"),
                Some("?column"),
                Some("?distinct"),
                Some("?aggr_fun"),
                None,
            ),
            // TODO There're two approaches for CAST arg problem: push down to CubeScan and remove casts / cast inside CubeScan or
            // TODO split it and implement limit, order, filter, etc. pushdown to projection nodes with casts.
            // TODO First approach is simpler and much faster compute-wise.
            // TODO Second approach is much more generalized.
            // TODO We need to weigh in on generalization vs performance tradeoffs here.
            self.measure_rewrite(
                "with-cast",
                agg_fun_expr(
                    "?aggr_fun",
                    vec![cast_expr(column_expr("?column"), "?data_type")],
                    "?distinct",
                ),
                Some("?column"),
                Some("?distinct"),
                Some("?aggr_fun"),
                Some("?data_type"),
            ),
            self.measure_rewrite(
                "measure-fun",
                udaf_expr("?aggr_fun", vec![column_expr("?column")]),
                Some("?column"),
                None,
                None,
                None,
            ),
            transforming_rewrite(
                "projection-columns-with-alias",
                member_replacer(
                    alias_expr(column_expr("?column"), "?alias"),
                    "?alias_to_cube",
                    "?aliases",
                ),
                "?member".to_string(),
                self.transform_projection_member(
                    "?alias_to_cube",
                    "?aliases",
                    "?column",
                    Some("?alias"),
                    "?member",
                ),
            ),
            transforming_rewrite(
                "default-member-error",
                member_replacer("?expr", "?alias_to_cube", "?aliases"),
                "?member_error".to_string(),
                self.transform_default_member_error("?alias_to_cube", "?expr", "?member_error"),
            ),
            transforming_rewrite(
                "projection-columns",
                member_replacer(column_expr("?column"), "?alias_to_cube", "?aliases"),
                "?member".to_string(),
                self.transform_projection_member(
                    "?alias_to_cube",
                    "?aliases",
                    "?column",
                    None,
                    "?member",
                ),
            ),
            transforming_rewrite(
                "literal-member",
                member_replacer(literal_expr("?value"), "?alias_to_cube", "?aliases"),
                literal_member("?literal_member_value", literal_expr("?value"), "?relation"),
                self.transform_literal_member(
                    "?value",
                    "?literal_member_value",
                    "?alias_to_cube",
                    "?relation",
                ),
            ),
            transforming_rewrite(
                "literal-member-alias",
                member_replacer(
                    alias_expr(literal_expr("?value"), "?alias"),
                    "?alias_to_cube",
                    "?aliases",
                ),
                literal_member(
                    "?literal_member_value",
                    alias_expr(literal_expr("?value"), "?alias"),
                    "?relation",
                ),
                self.transform_literal_member(
                    "?value",
                    "?literal_member_value",
                    "?alias_to_cube",
                    "?relation",
                ),
            ),
            transforming_chain_rewrite(
                "date-trunc",
                member_replacer("?original_expr", "?alias_to_cube", "?aliases"),
                vec![(
                    "?original_expr",
                    fun_expr(
                        "DateTrunc",
                        vec![literal_expr("?granularity"), column_expr("?column")],
                    ),
                )],
                time_dimension_expr(
                    "?time_dimension_name",
                    "?time_dimension_granularity",
                    "?date_range",
                    "?alias",
                ),
                self.transform_time_dimension(
                    "?alias_to_cube",
                    "?aliases",
                    "?column",
                    "?time_dimension_name",
                    "?granularity",
                    "?time_dimension_granularity",
                    "?date_range",
                    "?original_expr",
                    "?alias",
                ),
            ),
            // TODO make cast split work
            transforming_chain_rewrite(
                "date-trunc-unwrap-cast",
                member_replacer("?original_expr", "?alias_to_cube", "?aliases"),
                vec![(
                    "?original_expr",
                    cast_expr(
                        fun_expr(
                            "DateTrunc",
                            vec![literal_expr("?granularity"), column_expr("?column")],
                        ),
                        "?date_type",
                    ),
                )],
                time_dimension_expr(
                    "?time_dimension_name",
                    "?time_dimension_granularity",
                    "?date_range",
                    "?alias",
                ),
                self.transform_time_dimension(
                    "?alias_to_cube",
                    "?aliases",
                    "?column",
                    "?time_dimension_name",
                    "?granularity",
                    "?time_dimension_granularity",
                    "?date_range",
                    "?original_expr",
                    "?alias",
                ),
            ),
            // TODO duplicate of previous rule with aliasing. Extract aliasing as separate step?
            transforming_chain_rewrite(
                "date-trunc-alias",
                member_replacer("?original_expr", "?alias_to_cube", "?aliases"),
                vec![(
                    "?original_expr",
                    alias_expr(
                        fun_expr(
                            "DateTrunc",
                            vec![literal_expr("?granularity"), column_expr("?column")],
                        ),
                        "?alias",
                    ),
                )],
                time_dimension_expr(
                    "?time_dimension_name",
                    "?time_dimension_granularity",
                    "?date_range",
                    "?alias",
                ),
                self.transform_time_dimension(
                    "?alias_to_cube",
                    "?aliases",
                    "?column",
                    "?time_dimension_name",
                    "?granularity",
                    "?time_dimension_granularity",
                    "?date_range",
                    "?original_expr",
                    "?alias",
                ),
            ),
            transforming_rewrite(
                "push-down-aggregate-to-empty-scan",
                aggregate(
                    cube_scan(
                        "?alias_to_cube",
                        cube_scan_members_empty_tail(),
                        "?filters",
                        "?orders",
                        "?limit",
                        "?offset",
                        "?aliases",
                        "?split",
                        "?can_pushdown_join",
                        "CubeScanWrapped:false",
                    ),
                    "?group_expr",
                    "?aggr_expr",
                    "?agg_split",
                ),
                cube_scan(
                    "?alias_to_cube",
                    cube_scan_members(
                        member_replacer(
                            "?group_expr",
                            "?member_replacer_alias_to_cube",
                            "?member_replacer_aliases",
                        ),
                        member_replacer(
                            "?aggr_expr",
                            "?member_replacer_alias_to_cube",
                            "?member_replacer_aliases",
                        ),
                    ),
                    "?filters",
                    "?orders",
                    "?limit",
                    "?offset",
                    "?aliases",
                    "?split",
                    "?new_pushdown_join",
                    "CubeScanWrapped:false",
                ),
                self.push_down_aggregate_to_empty_scan(
                    "?alias_to_cube",
                    "?aliases",
                    "?aggr_expr",
                    "?can_pushdown_join",
                    "?member_replacer_alias_to_cube",
                    "?member_replacer_aliases",
                    "?new_pushdown_join",
                ),
            ),
            transforming_rewrite(
                "push-down-aggregate",
                aggregate(
                    cube_scan(
                        "?alias_to_cube",
                        "?old_members",
                        "?filters",
                        "?orders",
                        "?limit",
                        "?offset",
                        "?aliases",
                        "?split",
                        "?can_pushdown_join",
                        "CubeScanWrapped:false",
                    ),
                    "?group_expr",
                    "?aggr_expr",
                    "?agg_split",
                ),
                cube_scan(
                    "?alias_to_cube",
                    cube_scan_members(
                        member_pushdown_replacer(
                            "?group_expr",
                            list_concat_pushdown_replacer("?old_members"),
                            "?member_pushdown_replacer_alias_to_cube",
                        ),
                        member_pushdown_replacer(
                            "?aggr_expr",
                            list_concat_pushdown_replacer("?old_members"),
                            "?member_pushdown_replacer_alias_to_cube",
                        ),
                    ),
                    "?filters",
                    "?orders",
                    "?limit",
                    "?offset",
                    "?aliases",
                    "?split",
                    "?new_pushdown_join",
                    "CubeScanWrapped:false",
                ),
                self.push_down_non_empty_aggregate(
                    "?alias_to_cube",
                    "?group_expr",
                    "?aggr_expr",
                    "?old_members",
                    "?can_pushdown_join",
                    "?member_pushdown_replacer_alias_to_cube",
                    "?new_pushdown_join",
                ),
            ),
            transforming_rewrite(
                "push-down-projection-to-empty-scan",
                projection(
                    "?expr",
                    cube_scan(
                        "?alias_to_cube",
                        cube_scan_members_empty_tail(),
                        "?filters",
                        "?orders",
                        "?limit",
                        "?offset",
                        "?aliases",
                        "?split",
                        "?can_pushdown_join",
                        "CubeScanWrapped:false",
                    ),
                    "?alias",
                    "?projection_split",
                ),
                cube_scan(
                    "?new_alias_to_cube",
                    member_replacer(
                        "?expr",
                        "?member_replacer_alias_to_cube",
                        "?member_replacer_aliases",
                    ),
                    "?filters",
                    "?orders",
                    "?limit",
                    "?offset",
                    "?aliases_none",
                    "?split",
                    "?can_pushdown_join",
                    "CubeScanWrapped:false",
                ),
                self.push_down_projection_to_empty_scan(
                    "?alias",
                    "?alias_to_cube",
                    "?aliases",
                    Some("?aliases_none"),
                    "?new_alias_to_cube",
                    "?member_replacer_alias_to_cube",
                    "?member_replacer_aliases",
                ),
            ),
            transforming_rewrite(
                "push-down-projection",
                projection(
                    "?expr",
                    cube_scan(
                        "?alias_to_cube",
                        "?members",
                        "?filters",
                        "?orders",
                        "?limit",
                        "?offset",
                        "?cube_aliases",
                        "?split",
                        "?can_pushdown_join",
                        "CubeScanWrapped:false",
                    ),
                    "?alias",
                    "?projection_split",
                ),
                cube_scan(
                    "?new_alias_to_cube",
                    member_pushdown_replacer(
                        "?expr",
                        list_concat_pushdown_replacer("?members"),
                        "?member_pushdown_replacer_alias_to_cube",
                    ),
                    "?filters",
                    "?orders",
                    "?limit",
                    "?offset",
                    "?aliases",
                    "?split",
                    "?can_pushdown_join",
                    "CubeScanWrapped:false",
                ),
                self.push_down_projection(
                    "?expr",
                    "?members",
                    "?aliases",
                    "?alias",
                    "?alias_to_cube",
                    "?new_alias_to_cube",
                    "?member_pushdown_replacer_alias_to_cube",
                ),
            ),
            // TODO: this rule could benefit from more specific searcher but this would require
            // extending CubeScanAliases or introducing a placeholder node
            transforming_rewrite(
                "cube-scan-resolve-aliases",
                cube_scan(
                    "?alias_to_cube",
                    "?members",
                    "?filters",
                    "?orders",
                    "?limit",
                    "?offset",
                    "?aliases",
                    "?split",
                    "?can_pushdown_join",
                    "CubeScanWrapped:false",
                ),
                cube_scan(
                    "?alias_to_cube",
                    "?members",
                    "?filters",
                    "?orders",
                    "?limit",
                    "?offset",
                    "?new_aliases",
                    "?split",
                    "?can_pushdown_join",
                    "CubeScanWrapped:false",
                ),
                self.cube_scan_resolve_aliases("?members", "?aliases", "?new_aliases"),
            ),
            transforming_rewrite(
                "limit-push-down",
                limit(
                    "?skip",
                    "?fetch",
                    cube_scan(
                        "?alias_to_cube",
                        "?members",
                        "?filters",
                        "?orders",
                        "?cube_fetch",
                        "?offset",
                        "?aliases",
                        "?split",
                        "?can_pushdown_join",
                        "CubeScanWrapped:false",
                    ),
                ),
                cube_scan(
                    "?alias_to_cube",
                    "?members",
                    "?filters",
                    "?orders",
                    "?new_fetch",
                    "?new_skip",
                    "?aliases",
                    "?split",
                    "?can_pushdown_join",
                    "CubeScanWrapped:false",
                ),
                self.push_down_limit("?skip", "?fetch", "?new_skip", "?new_fetch"),
            ),
            // Binary expression associative properties
            rewrite(
                "binary-expr-addition-assoc",
                binary_expr(binary_expr("?a", "+", "?b"), "+", "?c"),
                binary_expr("?a", "+", binary_expr("?b", "+", "?c")),
            ),
            rewrite(
                "binary-expr-multi-assoc",
                binary_expr(binary_expr("?a", "*", "?b"), "*", "?c"),
                binary_expr("?a", "*", binary_expr("?b", "*", "?c")),
            ),
            // MOD function to binary expr
            rewrite(
                "mod-fun-to-binary-expr",
                udf_expr("mod", vec!["?a", "?b"]),
                binary_expr("?a", "%", "?b"),
            ),
            // LIKE expr to binary expr
            transforming_rewrite(
                "like-expr-to-binary-expr",
                like_expr(
                    "?like_type",
                    "?negated",
                    "?expr",
                    "?pattern",
                    "?escape_char",
                ),
                binary_expr("?expr", "?op", "?pattern"),
                self.transform_like_expr("?like_type", "?negated", "?escape_char", "?op"),
            ),
            // Join
            transforming_rewrite(
                "push-down-cross-join-to-empty-scan",
                cross_join(
                    cube_scan(
                        "?left_alias_to_cube",
                        cube_scan_members_empty_tail(),
                        cube_scan_filters_empty_tail(),
                        cube_scan_order_empty_tail(),
                        "?limit",
                        "?offset",
                        "?aliases",
                        "CubeScanSplit:false",
                        "CubeScanCanPushdownJoin:true",
                        "CubeScanWrapped:false",
                    ),
                    cube_scan(
                        "?right_alias_to_cube",
                        cube_scan_members_empty_tail(),
                        cube_scan_filters_empty_tail(),
                        cube_scan_order_empty_tail(),
                        "?limit",
                        "?offset",
                        "?aliases",
                        "CubeScanSplit:false",
                        "CubeScanCanPushdownJoin:true",
                        "CubeScanWrapped:false",
                    ),
                ),
                cube_scan(
                    "?joined_alias_to_cube",
                    cube_scan_members_empty_tail(),
                    cube_scan_filters_empty_tail(),
                    cube_scan_order_empty_tail(),
                    "?limit",
                    "?offset",
                    "?aliases",
                    "CubeScanSplit:false",
                    "CubeScanCanPushdownJoin:true",
                    "CubeScanWrapped:false",
                ),
                self.push_down_cross_join_to_empty_scan(
                    "?left_alias_to_cube",
                    "?right_alias_to_cube",
                    "?joined_alias_to_cube",
                ),
            ),
            self.push_down_cross_join_to_cubescan_rewrite(
                "not-merged-cubescans",
                "?left_members".to_string(),
                "?right_members".to_string(),
                "?left_members",
                "?right_members",
            ),
            self.push_down_cross_join_to_cubescan_rewrite(
                "merged-cubescan-left",
                merged_members_replacer("?left_members"),
                "?right_members".to_string(),
                "?left_members",
                "?right_members",
            ),
            self.push_down_cross_join_to_cubescan_rewrite(
                "merged-cubescan-right",
                "?left_members".to_string(),
                merged_members_replacer("?right_members"),
                "?left_members",
                "?right_members",
            ),
            self.push_down_cross_join_to_cubescan_rewrite(
                "merged-cubescans-both-sides",
                merged_members_replacer("?left_members"),
                merged_members_replacer("?right_members"),
                "?left_members",
                "?right_members",
            ),
            transforming_rewrite(
                "join-to-cross-join",
                join(
                    cube_scan(
                        "?left_alias_to_cube",
                        "?left_members",
                        "?left_filters",
                        "?left_orders",
                        "?left_limit",
                        "?left_offset",
                        "?left_aliases",
                        "?left_split",
                        "?left_can_pushdown_join",
                        "CubeScanWrapped:false",
                    ),
                    cube_scan(
                        "?right_alias_to_cube",
                        "?right_members",
                        "?right_filters",
                        "?right_orders",
                        "?right_limit",
                        "?right_offset",
                        "?right_aliases",
                        "?right_split",
                        "?right_can_pushdown_join",
                        "CubeScanWrapped:false",
                    ),
                    "?left_on",
                    "?right_on",
                    "?join_type",
                    "?join_constraint",
                ),
                cross_join(
                    cube_scan(
                        "?left_alias_to_cube",
                        "?left_members",
                        "?left_filters",
                        "?left_orders",
                        "?left_limit",
                        "?left_offset",
                        "?left_aliases",
                        "?left_split",
                        "?left_can_pushdown_join",
                        "CubeScanWrapped:false",
                    ),
                    cube_scan(
                        "?right_alias_to_cube",
                        "?right_members",
                        "?right_filters",
                        "?right_orders",
                        "?right_limit",
                        "?right_offset",
                        "?right_aliases",
                        "?right_split",
                        "?right_can_pushdown_join",
                        "CubeScanWrapped:false",
                    ),
                ),
                self.join_to_cross_join("?left_on", "?right_on", "?left_aliases", "?right_aliases"),
            ),
        ];

        rules.extend(self.member_pushdown_rules());
        rules
    }
}

impl MemberRules {
    pub fn new(cube_context: Arc<CubeContext>) -> Self {
        Self { cube_context }
    }

    fn member_column_pushdown(
        &self,
        name: &str,
        member_fn: impl Fn(&str) -> String,
        relation: Option<&'static str>,
    ) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
        vec![
            transforming_rewrite(
                &format!("member-pushdown-replacer-column-{}", name),
                member_pushdown_replacer(
                    column_expr("?column"),
                    member_fn("?old_alias"),
                    "?member_pushdown_replacer_alias_to_cube",
                ),
                member_fn("?output_column"),
                self.transform_column_alias(
                    "?member_pushdown_replacer_alias_to_cube",
                    "?column",
                    "?output_column",
                    relation,
                ),
            ),
            transforming_rewrite(
                &format!("member-pushdown-replacer-column-{}-alias", name),
                member_pushdown_replacer(
                    alias_expr(column_expr("?column"), "?alias"),
                    member_fn("?old_alias"),
                    "?member_pushdown_replacer_alias_to_cube",
                ),
                member_fn("?output_column"),
                self.transform_alias(
                    "?member_pushdown_replacer_alias_to_cube",
                    "?alias",
                    "?output_column",
                    relation,
                ),
            ),
        ]
    }

    fn member_pushdown_rules(&self) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
        let mut rules = Vec::new();
        let member_replacer_fn = |members| {
            member_pushdown_replacer(
                members,
                "?old_members",
                "?member_pushdown_replacer_alias_to_cube",
            )
        };

        let find_matching_old_member_with_count =
            |name: &str, column_expr: String, default_count: bool| {
                vec![
                transforming_rewrite(
                    &format!(
                        "member-pushdown-replacer-column-find-matching-old-member-{}",
                        name
                    ),
                    member_pushdown_replacer(
                        column_expr.clone(),
                        list_concat_pushup_replacer("?old_members"),
                        "?member_pushdown_replacer_alias_to_cube",
                    ),
                    member_pushdown_replacer(
                        column_expr.clone(),
                        "?terminal_member",
                        "?filtered_member_pushdown_replacer_alias_to_cube",
                    ),
                    self.find_matching_old_member(
                        "?member_pushdown_replacer_alias_to_cube",
                        "?column",
                        "?old_members",
                        "?terminal_member",
                        "?filtered_member_pushdown_replacer_alias_to_cube",
                        default_count,
                    ),
                ),
                transforming_rewrite(
                    &format!(
                        "member-pushdown-replacer-column-find-matching-old-member-{}-select-member-from-all-members",
                        name
                    ),
                    member_pushdown_replacer(
                        column_expr.clone(),
                        all_members("?cube", "?all_members_alias"),
                        "?member_pushdown_replacer_alias_to_cube",
                    ),
                    member_pushdown_replacer(
                        column_expr.clone(),
                        "?terminal_member",
                        "?member_pushdown_replacer_alias_to_cube",
                    ),
                    self.select_from_all_member_by_column(
                        "?cube",
                        "?member_pushdown_replacer_alias_to_cube",
                        "?column",
                        "?terminal_member",
                        default_count
                    ),
                ),
            ]
            };

        let find_matching_old_member = |name: &str, column_expr: String| {
            find_matching_old_member_with_count(name, column_expr, false)
        };

        rules.extend(replacer_push_down_node_substitute_rules(
            "member-pushdown-replacer-aggregate-group",
            "AggregateGroupExpr",
            "CubeScanMembers",
            member_replacer_fn.clone(),
        ));
        rules.extend(replacer_push_down_node_substitute_rules(
            "member-pushdown-replacer-aggregate-aggr",
            "AggregateAggrExpr",
            "CubeScanMembers",
            member_replacer_fn.clone(),
        ));
        rules.extend(replacer_push_down_node_substitute_rules(
            "member-pushdown-replacer-projection",
            "ProjectionExpr",
            "CubeScanMembers",
            member_replacer_fn.clone(),
        ));
        rules.extend(find_matching_old_member("column", column_expr("?column")));
        rules.extend(find_matching_old_member(
            "alias",
            alias_expr(column_expr("?column"), "?alias"),
        ));
        rules.extend(find_matching_old_member(
            "agg-fun",
            agg_fun_expr("?fun_name", vec![column_expr("?column")], "?distinct"),
        ));
        rules.extend(find_matching_old_member(
            "udaf-fun",
            udaf_expr("?fun_name", vec![column_expr("?column")]),
        ));
        rules.extend(find_matching_old_member_with_count(
            "agg-fun-default-count",
            agg_fun_expr(
                "Count",
                vec![literal_expr("?any")],
                "AggregateFunctionExprDistinct:false",
            ),
            true,
        ));
        rules.extend(find_matching_old_member_with_count(
            "agg-fun-default-count-alias",
            alias_expr(
                agg_fun_expr(
                    "Count",
                    vec![literal_expr("?any")],
                    "AggregateFunctionExprDistinct:false",
                ),
                "?alias",
            ),
            true,
        ));
        rules.extend(find_matching_old_member(
            "agg-fun-with-cast",
            // TODO need to check data_type if we can remove the cast
            agg_fun_expr(
                "?fun_name",
                vec![cast_expr(column_expr("?column"), "?data_type")],
                "?distinct",
            ),
        ));
        rules.extend(find_matching_old_member(
            "date-trunc",
            fun_expr(
                "DateTrunc",
                vec![literal_expr("?granularity"), column_expr("?column")],
            ),
        ));
        rules.extend(find_matching_old_member(
            "date-trunc-with-alias",
            // TODO need to check data_type if we can remove the cast
            alias_expr(
                fun_expr(
                    "DateTrunc",
                    vec![literal_expr("?granularity"), column_expr("?column")],
                ),
                "?original_alias",
            ),
        ));
        let pushdown_measure_rewrite =
            |name: &str,
             aggr_expr: String,
             measure_expr: String,
             fun_name: Option<&'static str>,
             distinct: Option<&'static str>,
             cast_data_type: Option<&'static str>| {
                transforming_chain_rewrite(
                    name,
                    member_pushdown_replacer(
                        "?aggr_expr",
                        measure_expr,
                        "?member_pushdown_replacer_alias_to_cube",
                    ),
                    vec![("?aggr_expr", aggr_expr)],
                    "?measure".to_string(),
                    self.pushdown_measure(
                        "?member_pushdown_replacer_alias_to_cube",
                        "?name",
                        fun_name,
                        distinct,
                        "?aggr_expr",
                        cast_data_type,
                        "?measure",
                    ),
                )
            };
        rules.push(pushdown_measure_rewrite(
            "member-pushdown-replacer-agg-fun",
            agg_fun_expr("?fun_name", vec![column_expr("?column")], "?distinct"),
            measure_expr("?name", "?old_alias"),
            Some("?fun_name"),
            Some("?distinct"),
            None,
        ));
        rules.push(pushdown_measure_rewrite(
            "member-pushdown-replacer-agg-fun-cast",
            agg_fun_expr(
                "?fun_name",
                vec![cast_expr(column_expr("?column"), "?data_type")],
                "?distinct",
            ),
            measure_expr("?name", "?old_alias"),
            Some("?fun_name"),
            Some("?distinct"),
            Some("?data_type"),
        ));
        rules.push(pushdown_measure_rewrite(
            "member-pushdown-replacer-agg-fun-on-dimension",
            agg_fun_expr("?fun_name", vec![column_expr("?column")], "?distinct"),
            dimension_expr("?name", "?old_alias"),
            Some("?fun_name"),
            Some("?distinct"),
            None,
        ));
        rules.push(pushdown_measure_rewrite(
            "member-pushdown-replacer-udaf-fun",
            udaf_expr("?fun_name", vec![column_expr("?column")]),
            measure_expr("?name", "?old_alias"),
            None,
            None,
            None,
        ));
        rules.push(pushdown_measure_rewrite(
            "member-pushdown-replacer-udaf-fun-on-dimension",
            udaf_expr("?fun_name", vec![column_expr("?column")]),
            dimension_expr("?name", "?old_alias"),
            None,
            None,
            None,
        ));
        rules.push(pushdown_measure_rewrite(
            "member-pushdown-replacer-agg-fun-default-count",
            agg_fun_expr("?fun_name", vec![literal_expr("?any")], "?distinct"),
            measure_expr("?name", "?old_alias"),
            Some("?fun_name"),
            Some("?distinct"),
            None,
        ));
        rules.push(pushdown_measure_rewrite(
            "member-pushdown-replacer-agg-fun-default-count-alias",
            alias_expr(
                agg_fun_expr("?fun_name", vec![literal_expr("?any")], "?distinct"),
                "?alias",
            ),
            measure_expr("?name", "?old_alias"),
            Some("?fun_name"),
            Some("?distinct"),
            None,
        ));

        rules.push(transforming_chain_rewrite(
            "member-pushdown-date-trunc",
            member_pushdown_replacer(
                "?original_expr",
                dimension_expr("?dimension_name", "?dimension_alias"),
                "?alias_to_cube",
            ),
            vec![(
                "?original_expr",
                fun_expr(
                    "DateTrunc",
                    vec![literal_expr("?granularity"), column_expr("?column")],
                ),
            )],
            time_dimension_expr(
                "?time_dimension_name",
                "?time_dimension_granularity",
                "?date_range",
                "?alias",
            ),
            self.pushdown_time_dimension(
                "?alias_to_cube",
                "?dimension_name",
                "?time_dimension_name",
                "?granularity",
                "?time_dimension_granularity",
                "?date_range",
                "?original_expr",
                "?alias",
            ),
        ));
        // TODO duplicate of previous rule with aliasing. Extract aliasing as separate step?
        rules.push(transforming_chain_rewrite(
            "member-pushdown-date-trunc-alias",
            member_pushdown_replacer(
                "?original_expr",
                dimension_expr("?dimension_name", "?dimension_alias"),
                "?alias_to_cube",
            ),
            vec![(
                "?original_expr",
                alias_expr(
                    fun_expr(
                        "DateTrunc",
                        vec![literal_expr("?granularity"), column_expr("?column")],
                    ),
                    "?original_alias",
                ),
            )],
            time_dimension_expr(
                "?time_dimension_name",
                "?time_dimension_granularity",
                "?date_range",
                "?alias",
            ),
            self.pushdown_time_dimension(
                "?alias_to_cube",
                "?dimension_name",
                "?time_dimension_name",
                "?granularity",
                "?time_dimension_granularity",
                "?date_range",
                "?original_expr",
                "?alias",
            ),
        ));
        rules.push(transforming_chain_rewrite(
            "member-pushdown-time-dimension-date-trunc",
            member_pushdown_replacer(
                "?original_expr",
                time_dimension_expr(
                    "?dimension_name",
                    "?original_granularity",
                    "?original_date_range",
                    "?dimension_alias",
                ),
                "?alias_to_cube",
            ),
            vec![(
                "?original_expr",
                fun_expr(
                    "DateTrunc",
                    vec![literal_expr("?granularity"), column_expr("?column")],
                ),
            )],
            time_dimension_expr(
                "?time_dimension_name",
                "?time_dimension_granularity",
                "?date_range",
                "?alias",
            ),
            self.pushdown_time_dimension(
                "?alias_to_cube",
                "?dimension_name",
                "?time_dimension_name",
                "?granularity",
                "?time_dimension_granularity",
                "?date_range",
                "?original_expr",
                "?alias",
            ),
        ));
        // TODO duplicate of previous rule with aliasing. Extract aliasing as separate step?
        rules.push(transforming_chain_rewrite(
            "member-pushdown-time-dimension-date-trunc-alias",
            member_pushdown_replacer(
                "?original_expr",
                time_dimension_expr(
                    "?dimension_name",
                    "?original_granularity",
                    "?original_date_range",
                    "?dimension_alias",
                ),
                "?alias_to_cube",
            ),
            vec![(
                "?original_expr",
                alias_expr(
                    fun_expr(
                        "DateTrunc",
                        vec![literal_expr("?granularity"), column_expr("?column")],
                    ),
                    "?original_alias",
                ),
            )],
            time_dimension_expr(
                "?time_dimension_name",
                "?time_dimension_granularity",
                "?date_range",
                "?alias",
            ),
            self.pushdown_time_dimension(
                "?alias_to_cube",
                "?dimension_name",
                "?time_dimension_name",
                "?granularity",
                "?time_dimension_granularity",
                "?date_range",
                "?original_expr",
                "?alias",
            ),
        ));
        // TODO make cast split work
        // rules.push(transforming_chain_rewrite(
        //     "date-trunc-unwrap-cast",
        //     member_replacer("?original_expr", "?alias_to_cube", "?aliases"),
        //     vec![(
        //         "?original_expr",
        //         cast_expr(
        //             fun_expr(
        //                 "DateTrunc",
        //                 vec![literal_expr("?granularity"), column_expr("?column")],
        //             ),
        //             "?date_type",
        //         ),
        //     )],
        //     time_dimension_expr(
        //         "?time_dimension_name",
        //         "?time_dimension_granularity",
        //         "?date_range",
        //         "?alias",
        //     ),
        //     self.transform_time_dimension(
        //         "?alias_to_cube",
        //         "?aliases",
        //         "?column",
        //         "?time_dimension_name",
        //         "?granularity",
        //         "?time_dimension_granularity",
        //         "?date_range",
        //         "?original_expr",
        //         "?alias",
        //     ),
        // ));

        rules.extend(self.member_column_pushdown(
            "measure",
            |column| measure_expr("?name", column),
            None,
        ));
        rules.extend(self.member_column_pushdown(
            "dimension",
            |column| dimension_expr("?name", column),
            None,
        ));
        rules.extend(self.member_column_pushdown(
            "segment",
            |column| segment_expr("?name", column),
            None,
        ));
        rules.extend(self.member_column_pushdown(
            "change-user",
            |column| change_user_expr("?change_user_cube", column),
            None,
        ));
        rules.extend(self.member_column_pushdown(
            "virtual-field",
            |column| virtual_field_expr("?name", "?virtual_field_cube", column),
            None,
        ));
        rules.extend(self.member_column_pushdown(
            "time-dimension",
            |column| time_dimension_expr("?name", "?granularity", "?date_range", column),
            None,
        ));
        rules.push(transforming_rewrite(
            "pushdown-literal-member",
            member_pushdown_replacer(literal_expr("?value"), "?old_members", "?alias_to_cube"),
            literal_member("?literal_member_value", literal_expr("?value"), "?relation"),
            self.pushdown_literal_member(
                "?value",
                "?literal_member_value",
                "?alias_to_cube",
                "?relation",
            ),
        ));
        rules.push(transforming_rewrite(
            "pushdown-literal-member-alias",
            member_pushdown_replacer(
                alias_expr(literal_expr("?value"), "?alias"),
                "?old_members",
                "?alias_to_cube",
            ),
            literal_member(
                "?literal_member_value",
                alias_expr(literal_expr("?value"), "?alias"),
                "?relation",
            ),
            self.pushdown_literal_member(
                "?value",
                "?literal_member_value",
                "?alias_to_cube",
                "?relation",
            ),
        ));
        rules.push(transforming_rewrite(
            "member-pushdown-replacer-column-literal-member",
            member_pushdown_replacer(
                column_expr("?column"),
                literal_member("?value", alias_expr("?expr", "?alias"), "?relation"),
                "?alias_to_cube",
            ),
            literal_member("?value", alias_expr("?expr", "?new_alias"), "?new_relation"),
            self.transform_literal_member_alias(
                "?alias_to_cube",
                "?column",
                "?new_alias",
                "?new_relation",
                false,
            ),
        ));
        rules.push(transforming_rewrite(
            "member-pushdown-replacer-column-literal-member-alias",
            member_pushdown_replacer(
                alias_expr("?outer_expr", "?outer_alias"),
                literal_member("?value", alias_expr("?expr", "?alias"), "?relation"),
                "?alias_to_cube",
            ),
            literal_member("?value", alias_expr("?expr", "?new_alias"), "?new_relation"),
            self.transform_literal_member_alias(
                "?alias_to_cube",
                "?outer_alias",
                "?new_alias",
                "?new_relation",
                true,
            ),
        ));

        fn list_concat_terminal(
            name: &str,
            member_fn: String,
        ) -> Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis> {
            rewrite(
                &format!("list-concat-terminal-{}", name),
                list_concat_pushdown_replacer(member_fn.to_string()),
                list_concat_pushup_replacer(member_fn),
            )
        }

        // List concat replacer -- concats CubeScanMembers into big single node to provide
        // O(n*2) CubeScanMembers complexity instead of O(n^2) for old member search
        // TODO check why overall graph size is increased most of the times
        rules.extend(replacer_push_down_node(
            "list-concat-replacer",
            "CubeScanMembers",
            list_concat_pushdown_replacer,
            false,
        ));
        rules.push(list_concat_terminal(
            "measure",
            measure_expr("?name", "?expr"),
        ));
        rules.push(list_concat_terminal(
            "dimension",
            dimension_expr("?name", "?expr"),
        ));
        rules.push(list_concat_terminal(
            "segment",
            segment_expr("?name", "?expr"),
        ));
        rules.push(list_concat_terminal(
            "change-user",
            change_user_expr("?change_user_cube", "?expr"),
        ));
        rules.push(list_concat_terminal(
            "virtual-field",
            virtual_field_expr("?name", "?virtual_field_cube", "?expr"),
        ));
        rules.push(list_concat_terminal(
            "time-dimension",
            time_dimension_expr("?name", "?granularity", "?date_range", "?expr"),
        ));
        rules.push(list_concat_terminal(
            "all-members",
            all_members("?cube", "?all_members_alias"),
        ));
        rules.push(list_concat_terminal(
            "literal-member",
            literal_member("?value", "?expr", "?relation"),
        ));
        rules.push(list_concat_terminal(
            "empty-tail",
            cube_scan_members_empty_tail(),
        ));
        rules.push(transforming_rewrite(
            "list-concat-replacer-merge",
            cube_scan_members(
                list_concat_pushup_replacer("?left"),
                list_concat_pushup_replacer("?right"),
            ),
            list_concat_pushup_replacer("?concat_output"),
            self.concat_cube_scan_members("?left", "?right", "?concat_output"),
        ));

        rules
    }

    fn concat_cube_scan_members(
        &self,
        left_var: &'static str,
        right_var: &'static str,
        concat_output_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let left_var = var!(left_var);
        let right_var = var!(right_var);
        let concat_output_var = var!(concat_output_var);
        move |egraph, subst| {
            let left_list = var_list_iter!(egraph[subst[left_var]], CubeScanMembers)
                .cloned()
                .collect::<Vec<_>>();
            let left_list = if left_list.is_empty() {
                vec![vec![subst[left_var]]]
            } else {
                left_list
            };
            for left in left_list {
                let right_list = var_list_iter!(egraph[subst[right_var]], CubeScanMembers)
                    .cloned()
                    .collect::<Vec<_>>();
                let right_list = if right_list.is_empty() {
                    vec![vec![subst[right_var]]]
                } else {
                    right_list
                };
                for right in right_list {
                    let output = egraph.add(LogicalPlanLanguage::CubeScanMembers(
                        left.into_iter().chain(right.into_iter()).collect(),
                    ));
                    subst.insert(concat_output_var, output);
                    return true;
                }
            }
            false
        }
    }

    fn transform_table_scan(
        &self,
        source_table_name_var: &'static str,
        table_name_var: &'static str,
        alias_to_cube_var: &'static str,
        cube_scan_aliases_var: &'static str,
        cube_scan_members_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let source_table_name_var = var!(source_table_name_var);
        let table_name_var = var!(table_name_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let cube_scan_aliases_var = var!(cube_scan_aliases_var);
        let cube_scan_members_var = var!(cube_scan_members_var);
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for name in var_iter!(
                egraph[subst[source_table_name_var]],
                TableScanSourceTableName
            ) {
                if let Some(cube) = meta_context
                    .cubes
                    .iter()
                    .find(|c| c.name.eq_ignore_ascii_case(name))
                {
                    for table_name in
                        var_iter!(egraph[subst[table_name_var]], TableScanTableName).cloned()
                    {
                        subst.insert(
                            alias_to_cube_var,
                            egraph.add(LogicalPlanLanguage::CubeScanAliasToCube(
                                CubeScanAliasToCube(vec![(
                                    table_name.to_string(),
                                    cube.name.to_string(),
                                )]),
                            )),
                        );

                        subst.insert(
                            cube_scan_aliases_var,
                            egraph.add(LogicalPlanLanguage::CubeScanAliases(CubeScanAliases(None))),
                        );

                        let all_members_cube = egraph.add(LogicalPlanLanguage::AllMembersCube(
                            AllMembersCube(cube.name.to_string()),
                        ));

                        let all_members_alias = egraph.add(LogicalPlanLanguage::AllMembersAlias(
                            AllMembersAlias(table_name.to_string()),
                        ));

                        subst.insert(
                            cube_scan_members_var,
                            egraph.add(LogicalPlanLanguage::AllMembers([
                                all_members_cube,
                                all_members_alias,
                            ])),
                        );

                        return true;
                    }
                }
            }
            false
        }
    }

    pub fn transform_original_expr_nested_date_trunc(
        original_expr_var: &'static str,
        // Original granularity from date_part/date_trunc
        outer_granularity_var: &'static str,
        // Original nested granularity from date_trunc
        inner_granularity_var: &'static str,
        // Var for substr which is used to pass value to Date_Trunc
        date_trunc_granularity_var: &'static str,
        column_expr_var: &'static str,
        alias_expr_var: Option<&'static str>,
        inner_replacer: bool,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool + Clone
    {
        let original_expr_var = var!(original_expr_var);
        let outer_granularity_var = var!(outer_granularity_var);
        let inner_granularity_var = var!(inner_granularity_var);
        let date_trunc_granularity_var = var!(date_trunc_granularity_var);
        let column_expr_var = var!(column_expr_var);
        let alias_expr_var = alias_expr_var.map(|alias_expr_var| var!(alias_expr_var));

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

            for granularity in var_iter!(egraph[subst[outer_granularity_var]], LiteralExprValue) {
                let outer_granularity = match utils::parse_granularity(granularity, inner_replacer)
                {
                    Some(granularity) => granularity,
                    None => continue,
                };

                let inner_granularity = if outer_granularity_var == inner_granularity_var {
                    outer_granularity.clone()
                } else {
                    var_iter!(egraph[subst[inner_granularity_var]], LiteralExprValue)
                        .find_map(|g| utils::parse_granularity(g, inner_replacer))
                        .unwrap_or(outer_granularity.clone())
                };

                let date_trunc_granularity =
                    match min_granularity(&outer_granularity, &inner_granularity) {
                        Some(granularity) => {
                            if granularity.to_lowercase() == inner_granularity.to_lowercase() {
                                outer_granularity
                            } else {
                                inner_granularity
                            }
                        }
                        None => continue,
                    };

                if let Ok(expr) = res {
                    // TODO unwrap
                    let name = expr.name(&DFSchema::empty()).unwrap();
                    let suffix_alias = format!("{}_{}", name, granularity);
                    let alias = egraph.add(LogicalPlanLanguage::ColumnExprColumn(
                        ColumnExprColumn(Column::from_name(suffix_alias.to_string())),
                    ));
                    subst.insert(
                        column_expr_var,
                        egraph.add(LogicalPlanLanguage::ColumnExpr([alias])),
                    );
                    subst.insert(
                        date_trunc_granularity_var,
                        egraph.add(LogicalPlanLanguage::LiteralExprValue(LiteralExprValue(
                            ScalarValue::Utf8(Some(date_trunc_granularity)),
                        ))),
                    );
                    if let Some(alias_expr_var) = alias_expr_var {
                        subst.insert(
                            alias_expr_var,
                            egraph.add(LogicalPlanLanguage::AliasExprAlias(AliasExprAlias(
                                if inner_replacer {
                                    suffix_alias.to_string()
                                } else {
                                    name
                                },
                            ))),
                        );
                    }

                    return true;
                }
            }

            false
        }
    }

    pub fn transform_original_expr_date_trunc(
        original_expr_var: &'static str,
        // Original granularity from date_part/date_trunc
        granularity_var: &'static str,
        // Var for substr which is used to pass value to Date_Trunc
        date_trunc_granularity_var: &'static str,
        column_expr_var: &'static str,
        alias_expr_var: Option<&'static str>,
        inner_replacer: bool,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool + Clone
    {
        Self::transform_original_expr_nested_date_trunc(
            original_expr_var,
            granularity_var,
            granularity_var,
            date_trunc_granularity_var,
            column_expr_var,
            alias_expr_var,
            inner_replacer,
        )
    }

    fn push_down_projection(
        &self,
        projection_expr_var: &'static str,
        members_var: &'static str,
        cube_aliases_var: &'static str,
        alias_var: &'static str,
        alias_to_cube_var: &'static str,
        new_alias_to_cube_var: &'static str,
        member_pushdown_replacer_alias_to_cube_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let projection_expr_var = var!(projection_expr_var);
        let members_var = var!(members_var);
        let cube_aliases_var = var!(cube_aliases_var);
        let alias_var = var!(alias_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let new_alias_to_cube_var = var!(new_alias_to_cube_var);
        let member_pushdown_replacer_alias_to_cube_var =
            var!(member_pushdown_replacer_alias_to_cube_var);
        move |egraph, subst| {
            if let Some(referenced_expr) = &egraph
                .index(subst[projection_expr_var])
                .data
                .referenced_expr
            {
                for alias_to_cube in
                    var_iter!(egraph[subst[alias_to_cube_var]], CubeScanAliasToCube).cloned()
                {
                    if let Some(member_name_to_expr) = egraph
                        .index(subst[members_var])
                        .data
                        .member_name_to_expr
                        .clone()
                    {
                        let column_name_to_member_name =
                            column_name_to_member_vec(member_name_to_expr);

                        for projection_alias in
                            var_iter!(egraph[subst[alias_var]], ProjectionAlias).cloned()
                        {
                            let mut columns = HashSet::new();
                            columns.extend(referenced_columns(referenced_expr.clone()).into_iter());
                            if columns.iter().all(|c| {
                                column_name_to_member_name
                                    .iter()
                                    .find(|(cn, _)| c == cn)
                                    .is_some()
                            }) {
                                let cube_aliases = egraph.add(
                                    LogicalPlanLanguage::CubeScanAliases(CubeScanAliases(None)),
                                );
                                subst.insert(cube_aliases_var, cube_aliases);

                                let replaced_alias_to_cube =
                                    Self::replace_alias(&alias_to_cube, &projection_alias);
                                let new_alias_to_cube =
                                    egraph.add(LogicalPlanLanguage::CubeScanAliasToCube(
                                        CubeScanAliasToCube(replaced_alias_to_cube.clone()),
                                    ));
                                subst.insert(new_alias_to_cube_var, new_alias_to_cube.clone());

                                let member_pushdown_replacer_alias_to_cube = egraph.add(
                                    LogicalPlanLanguage::MemberPushdownReplacerAliasToCube(
                                        MemberPushdownReplacerAliasToCube(
                                            Self::member_replacer_alias_to_cube(
                                                &alias_to_cube,
                                                &projection_alias,
                                            ),
                                        ),
                                    ),
                                );
                                subst.insert(
                                    member_pushdown_replacer_alias_to_cube_var,
                                    member_pushdown_replacer_alias_to_cube,
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

    fn cube_scan_resolve_aliases(
        &self,
        members_var: &'static str,
        aliases_var: &'static str,
        new_aliases_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let members_var = var!(members_var);
        let aliases_var = var!(aliases_var);
        let new_aliases_var = var!(new_aliases_var);
        move |egraph, subst| {
            for aliases in var_iter!(egraph[subst[aliases_var]], CubeScanAliases) {
                if aliases.is_some() {
                    continue;
                }

                if let Some(member_name_to_expr) = egraph
                    .index(subst[members_var])
                    .data
                    .member_name_to_expr
                    .clone()
                {
                    let column_name_to_member_name = column_name_to_member_vec(member_name_to_expr);
                    subst.insert(
                        new_aliases_var,
                        egraph.add(LogicalPlanLanguage::CubeScanAliases(CubeScanAliases(Some(
                            column_name_to_member_to_aliases(column_name_to_member_name),
                        )))),
                    );
                    return true;
                }
            }
            false
        }
    }

    fn replace_alias(
        alias_to_cube: &Vec<(String, String)>,
        projection_alias: &Option<String>,
    ) -> Vec<(String, String)> {
        // Multiple cubes can have same alias in case it's just wrapped by projection
        projection_alias
            .as_ref()
            .map(|new_alias| {
                alias_to_cube
                    .iter()
                    .map(|(_, cube)| (new_alias.clone(), cube.to_string()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or(alias_to_cube.clone())
    }

    fn member_replacer_alias_to_cube(
        alias_to_cube: &Vec<(String, String)>,
        projection_alias: &Option<String>,
    ) -> Vec<((String, String), String)> {
        // Multiple cubes can have same alias in case it's just wrapped by projection
        projection_alias
            .as_ref()
            .map(|new_alias| {
                alias_to_cube
                    .iter()
                    .map(|(old_alias, cube)| {
                        ((old_alias.clone(), new_alias.clone()), cube.to_string())
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or(
                alias_to_cube
                    .iter()
                    .map(|(old_alias, cube)| {
                        ((old_alias.clone(), old_alias.clone()), cube.to_string())
                    })
                    .collect::<Vec<_>>(),
            )
    }

    fn update_cube_aliases_relation(
        cube_aliases: Vec<(String, String)>,
        old_relations: &Vec<String>,
        new_relation: &Option<String>,
    ) -> Vec<(String, String)> {
        if let Some(new_relation) = new_relation {
            cube_aliases
                .into_iter()
                .map(|(old_alias, member)| {
                    let old_relation = old_alias.split(".").next().unwrap().to_string();
                    if old_relations.contains(&old_relation) {
                        (
                            format!(
                                "{}{}",
                                new_relation,
                                old_alias.strip_prefix(&old_relation).unwrap()
                            ),
                            member,
                        )
                    } else {
                        (old_alias, member)
                    }
                })
                .collect::<Vec<_>>()
        } else {
            cube_aliases
        }
    }

    fn push_down_aggregate_to_empty_scan(
        &self,
        alias_to_cube_var: &'static str,
        cube_aliases_var: &'static str,
        aggr_expr_var: &'static str,
        can_pushdown_join_var: &'static str,
        member_replacer_alias_to_cube_var: &'static str,
        member_replacer_aliases_var: &'static str,
        new_pushdown_join_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let cube_aliases_var = var!(cube_aliases_var);
        let aggr_expr_var = var!(aggr_expr_var);
        let can_pushdown_join_var = var!(can_pushdown_join_var);
        let member_replacer_alias_to_cube_var = var!(member_replacer_alias_to_cube_var);
        let member_replacer_aliases_var = var!(member_replacer_aliases_var);
        let new_pushdown_join_var = var!(new_pushdown_join_var);
        move |egraph, subst| {
            for alias_to_cube in
                var_iter!(egraph[subst[alias_to_cube_var]], CubeScanAliasToCube).cloned()
            {
                for cube_aliases in
                    var_iter!(egraph[subst[cube_aliases_var]], CubeScanAliases).cloned()
                {
                    for can_pushdown_join in var_iter!(
                        egraph[subst[can_pushdown_join_var]],
                        CubeScanCanPushdownJoin
                    )
                    .cloned()
                    {
                        if cube_aliases.is_none() {
                            continue;
                        }

                        let member_replacer_alias_to_cube =
                            egraph.add(LogicalPlanLanguage::MemberReplacerAliasToCube(
                                MemberReplacerAliasToCube(Self::member_replacer_alias_to_cube(
                                    &alias_to_cube,
                                    &None,
                                )),
                            ));
                        subst.insert(
                            member_replacer_alias_to_cube_var,
                            member_replacer_alias_to_cube,
                        );

                        let member_replacer_aliases =
                            egraph.add(LogicalPlanLanguage::MemberReplacerAliases(
                                MemberReplacerAliases(cube_aliases.unwrap_or(vec![])),
                            ));
                        subst.insert(member_replacer_aliases_var, member_replacer_aliases);

                        let new_pushdown_join = if let Some(referenced_aggr_expr) =
                            &egraph.index(subst[aggr_expr_var]).data.referenced_expr
                        {
                            referenced_aggr_expr.is_empty()
                        } else {
                            true
                        };

                        let new_pushdown_join =
                            egraph.add(LogicalPlanLanguage::CubeScanCanPushdownJoin(
                                CubeScanCanPushdownJoin(new_pushdown_join && can_pushdown_join),
                            ));
                        subst.insert(new_pushdown_join_var, new_pushdown_join);

                        return true;
                    }
                }
            }

            false
        }
    }

    fn push_down_projection_to_empty_scan(
        &self,
        alias_var: &'static str,
        alias_to_cube_var: &'static str,
        cube_aliases_var: &'static str,
        cube_aliases_none_var: Option<&'static str>,
        new_alias_to_cube_var: &'static str,
        member_replacer_alias_to_cube_var: &'static str,
        member_replacer_aliases_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let alias_var = var!(alias_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let cube_aliases_var = var!(cube_aliases_var);
        let cube_aliases_none_var = cube_aliases_none_var.map(|v| var!(v));
        let new_alias_to_cube_var = var!(new_alias_to_cube_var);
        let member_replacer_alias_to_cube_var = var!(member_replacer_alias_to_cube_var);
        let member_replacer_aliases_var = var!(member_replacer_aliases_var);
        move |egraph, subst| {
            for alias_to_cube in
                var_iter!(egraph[subst[alias_to_cube_var]], CubeScanAliasToCube).cloned()
            {
                for projection_alias in
                    var_iter!(egraph[subst[alias_var]], ProjectionAlias).cloned()
                {
                    for cube_aliases in
                        var_iter!(egraph[subst[cube_aliases_var]], CubeScanAliases).cloned()
                    {
                        let replaced_alias_to_cube =
                            Self::replace_alias(&alias_to_cube, &projection_alias);
                        let new_alias_to_cube =
                            egraph.add(LogicalPlanLanguage::CubeScanAliasToCube(
                                CubeScanAliasToCube(replaced_alias_to_cube.clone()),
                            ));
                        subst.insert(new_alias_to_cube_var, new_alias_to_cube);

                        let member_replacer_alias_to_cube =
                            egraph.add(LogicalPlanLanguage::MemberReplacerAliasToCube(
                                MemberReplacerAliasToCube(Self::member_replacer_alias_to_cube(
                                    &alias_to_cube,
                                    &projection_alias,
                                )),
                            ));
                        subst.insert(
                            member_replacer_alias_to_cube_var,
                            member_replacer_alias_to_cube,
                        );

                        let old_relations = alias_to_cube.iter().map(|(a, _)| a.clone()).collect();

                        let member_replacer_aliases =
                            egraph.add(LogicalPlanLanguage::MemberReplacerAliases(
                                MemberReplacerAliases(Self::update_cube_aliases_relation(
                                    cube_aliases.unwrap_or(vec![]),
                                    &old_relations,
                                    &projection_alias,
                                )),
                            ));
                        subst.insert(member_replacer_aliases_var, member_replacer_aliases);

                        if let Some(cube_aliases_none_var) = cube_aliases_none_var {
                            let cube_aliases_none = egraph
                                .add(LogicalPlanLanguage::CubeScanAliases(CubeScanAliases(None)));
                            subst.insert(cube_aliases_none_var, cube_aliases_none);
                        }

                        return true;
                    }
                }
            }
            false
        }
    }

    fn push_down_non_empty_aggregate(
        &self,
        alias_to_cube_var: &'static str,
        group_expr_var: &'static str,
        aggregate_expr_var: &'static str,
        members_var: &'static str,
        can_pushdown_join_var: &'static str,
        member_pushdown_replacer_alias_to_cube_var: &'static str,
        new_pushdown_join_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let group_expr_var = var!(group_expr_var);
        let aggregate_expr_var = var!(aggregate_expr_var);
        let members_var = var!(members_var);
        let can_pushdown_join_var = var!(can_pushdown_join_var);
        let member_pushdown_replacer_alias_to_cube_var =
            var!(member_pushdown_replacer_alias_to_cube_var);
        let new_pushdown_join_var = var!(new_pushdown_join_var);
        move |egraph, subst| {
            for alias_to_cube in
                var_iter!(egraph[subst[alias_to_cube_var]], CubeScanAliasToCube).cloned()
            {
                if let Some(referenced_group_expr) =
                    &egraph.index(subst[group_expr_var]).data.referenced_expr
                {
                    if let Some(referenced_aggr_expr) =
                        &egraph.index(subst[aggregate_expr_var]).data.referenced_expr
                    {
                        let new_pushdown_join = referenced_aggr_expr.is_empty();

                        for can_pushdown_join in var_iter!(
                            egraph[subst[can_pushdown_join_var]],
                            CubeScanCanPushdownJoin
                        )
                        .cloned()
                        {
                            if let Some(member_name_to_expr) = egraph
                                .index(subst[members_var])
                                .data
                                .member_name_to_expr
                                .clone()
                            {
                                let member_column_names =
                                    column_name_to_member_vec(member_name_to_expr);

                                let mut columns = HashSet::new();
                                columns.extend(
                                    referenced_columns(referenced_group_expr.clone()).into_iter(),
                                );
                                columns.extend(
                                    referenced_columns(referenced_aggr_expr.clone()).into_iter(),
                                );
                                // TODO default count member is not in the columns set but it should be there

                                if columns.iter().all(|c| {
                                    member_column_names.iter().find(|(cn, _)| c == cn).is_some()
                                }) {
                                    let member_pushdown_replacer_alias_to_cube = egraph.add(
                                        LogicalPlanLanguage::MemberPushdownReplacerAliasToCube(
                                            MemberPushdownReplacerAliasToCube(
                                                Self::member_replacer_alias_to_cube(
                                                    &alias_to_cube,
                                                    &None,
                                                ),
                                            ),
                                        ),
                                    );

                                    subst.insert(
                                        member_pushdown_replacer_alias_to_cube_var,
                                        member_pushdown_replacer_alias_to_cube,
                                    );

                                    let new_pushdown_join =
                                        egraph.add(LogicalPlanLanguage::CubeScanCanPushdownJoin(
                                            CubeScanCanPushdownJoin(
                                                new_pushdown_join && can_pushdown_join,
                                            ),
                                        ));
                                    subst.insert(new_pushdown_join_var, new_pushdown_join);

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

    fn push_down_limit(
        &self,
        skip_var: &'static str,
        fetch_var: &'static str,
        new_skip_var: &'static str,
        new_fetch_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let skip_var = var!(skip_var);
        let fetch_var = var!(fetch_var);
        let new_skip_var = var!(new_skip_var);
        let new_fetch_var = var!(new_fetch_var);
        move |egraph, subst| {
            let mut skip_value = None;
            for skip in var_iter!(egraph[subst[skip_var]], LimitSkip) {
                if skip.unwrap_or_default() > 0 {
                    skip_value = *skip;
                    break;
                }
            }
            let mut fetch_value = None;
            for fetch in var_iter!(egraph[subst[fetch_var]], LimitFetch) {
                if fetch.unwrap_or_default() > 0 {
                    fetch_value = *fetch;
                    break;
                }
            }

            if skip_value.is_some() || fetch_value.is_some() {
                subst.insert(
                    new_skip_var,
                    egraph.add(LogicalPlanLanguage::CubeScanOffset(CubeScanOffset(
                        skip_value,
                    ))),
                );
                subst.insert(
                    new_fetch_var,
                    egraph.add(LogicalPlanLanguage::CubeScanLimit(CubeScanLimit(
                        fetch_value,
                    ))),
                );

                return true;
            }

            false
        }
    }

    fn transform_default_member_error(
        &self,
        cube_var: &'static str,
        expr_var: &'static str,
        member_error_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let expr_var = var!(expr_var);
        let cube_var = var!(cube_var);
        let member_error_var = var!(member_error_var);
        move |egraph, subst| {
            for alias_to_cube in
                var_iter!(egraph[subst[cube_var]], MemberReplacerAliasToCube).cloned()
            {
                if let Some(expr_name) = original_expr_name(egraph, subst[expr_var]) {
                    let alias_to_cube = alias_to_cube.clone();
                    let member_error = add_member_error(egraph, format!(
                        "'{}' expression can't be coerced to any members of following cubes: {}. It may be this type of expression is not supported.",
                        expr_name,
                        alias_to_cube.iter().map(|(_, cube)| cube).join(", ")
                    ), 0, subst[expr_var], alias_to_cube);
                    subst.insert(member_error_var, member_error);
                    return true;
                }
            }
            false
        }
    }

    fn pushdown_literal_member(
        &self,
        literal_value_var: &'static str,
        literal_member_value_var: &'static str,
        alias_to_cube_var: &'static str,
        relation_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let literal_value_var = var!(literal_value_var);
        let literal_member_value_var = var!(literal_member_value_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let relation_var = var!(relation_var);
        move |egraph, subst| {
            for value in var_iter!(egraph[subst[literal_value_var]], LiteralExprValue).cloned() {
                for alias_to_cube in var_iter!(
                    egraph[subst[alias_to_cube_var]],
                    MemberPushdownReplacerAliasToCube
                )
                .cloned()
                {
                    let cube_aliases = alias_to_cube
                        .into_iter()
                        .map(|((_, cube_alias), _)| cube_alias.clone())
                        .unique()
                        .collect::<Vec<_>>();
                    let cube_alias = if cube_aliases.len() == 1 {
                        Some(cube_aliases.first().unwrap().to_owned())
                    } else {
                        None
                    };

                    let literal_member_value = egraph.add(LogicalPlanLanguage::LiteralMemberValue(
                        LiteralMemberValue(value),
                    ));
                    subst.insert(literal_member_value_var, literal_member_value);

                    let literal_member_relation =
                        egraph.add(LogicalPlanLanguage::LiteralMemberRelation(
                            LiteralMemberRelation(cube_alias),
                        ));
                    subst.insert(relation_var, literal_member_relation);
                    return true;
                }
            }
            false
        }
    }

    fn transform_literal_member(
        &self,
        literal_value_var: &'static str,
        literal_member_value_var: &'static str,
        alias_to_cube_var: &'static str,
        relation_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let literal_value_var = var!(literal_value_var);
        let literal_member_value_var = var!(literal_member_value_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let relation_var = var!(relation_var);
        move |egraph, subst| {
            for value in var_iter!(egraph[subst[literal_value_var]], LiteralExprValue).cloned() {
                for alias_to_cube in
                    var_iter!(egraph[subst[alias_to_cube_var]], MemberReplacerAliasToCube).cloned()
                {
                    let cube_aliases = alias_to_cube
                        .into_iter()
                        .map(|((_, cube_alias), _)| cube_alias.clone())
                        .unique()
                        .collect::<Vec<_>>();
                    let cube_alias = if cube_aliases.len() == 1 {
                        Some(cube_aliases.first().unwrap().to_owned())
                    } else {
                        None
                    };

                    let literal_member_value = egraph.add(LogicalPlanLanguage::LiteralMemberValue(
                        LiteralMemberValue(value),
                    ));
                    subst.insert(literal_member_value_var, literal_member_value);

                    let literal_member_relation =
                        egraph.add(LogicalPlanLanguage::LiteralMemberRelation(
                            LiteralMemberRelation(cube_alias),
                        ));
                    subst.insert(relation_var, literal_member_relation);
                    return true;
                }
            }
            false
        }
    }

    fn transform_projection_member(
        &self,
        cube_var: &'static str,
        aliases_var: &'static str,
        column_var: &'static str,
        alias_var: Option<&'static str>,
        member_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_var = cube_var.parse().unwrap();
        let aliases_var = aliases_var.parse().unwrap();
        let column_var = column_var.parse().unwrap();
        let alias_var = alias_var.map(|alias_var| alias_var.parse().unwrap());
        let member_var = member_var.parse().unwrap();
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for column in var_iter!(egraph[subst[column_var]], ColumnExprColumn).cloned() {
                for alias_to_cube in var_iter!(egraph[subst[cube_var]], MemberReplacerAliasToCube) {
                    for aliases in var_iter!(egraph[subst[aliases_var]], MemberReplacerAliases) {
                        for ((_, cube_alias), cube) in
                            meta_context.find_cube_by_column_for_replacer(&alias_to_cube, &column)
                        {
                            let column_names = if let Some(alias_var) = &alias_var {
                                var_iter!(egraph[subst[*alias_var]], AliasExprAlias)
                                    .map(|s| s.to_string())
                                    .collect::<Vec<_>>()
                            } else {
                                vec![column.name.to_string()]
                            };
                            for column_name in column_names {
                                let member_name =
                                    get_member_name(&cube.name, &column.name, aliases, &cube_alias);
                                if let Some(dimension) = cube
                                    .dimensions
                                    .iter()
                                    .find(|d| d.name.eq_ignore_ascii_case(&member_name))
                                {
                                    let dimension_name =
                                        egraph.add(LogicalPlanLanguage::DimensionName(
                                            DimensionName(dimension.name.to_string()),
                                        ));
                                    let alias = egraph.add(LogicalPlanLanguage::ColumnExprColumn(
                                        ColumnExprColumn(Column {
                                            relation: Some(cube_alias),
                                            name: column_name,
                                        }),
                                    ));
                                    let alias_expr =
                                        egraph.add(LogicalPlanLanguage::ColumnExpr([alias]));

                                    subst.insert(
                                        member_var,
                                        egraph.add(LogicalPlanLanguage::Dimension([
                                            dimension_name,
                                            alias_expr,
                                        ])),
                                    );
                                    return true;
                                }

                                if let Some(measure) = cube
                                    .measures
                                    .iter()
                                    .find(|d| d.name.eq_ignore_ascii_case(&member_name))
                                {
                                    let measure_name =
                                        egraph.add(LogicalPlanLanguage::MeasureName(MeasureName(
                                            measure.name.to_string(),
                                        )));
                                    let alias = egraph.add(LogicalPlanLanguage::ColumnExprColumn(
                                        ColumnExprColumn(Column {
                                            relation: Some(cube_alias),
                                            name: column_name,
                                        }),
                                    ));
                                    let alias_expr =
                                        egraph.add(LogicalPlanLanguage::ColumnExpr([alias]));
                                    subst.insert(
                                        member_var,
                                        egraph.add(LogicalPlanLanguage::Measure([
                                            measure_name,
                                            alias_expr,
                                        ])),
                                    );
                                    return true;
                                }

                                if let Some(segment) = cube
                                    .segments
                                    .iter()
                                    .find(|d| d.name.eq_ignore_ascii_case(&member_name))
                                {
                                    let measure_name =
                                        egraph.add(LogicalPlanLanguage::SegmentName(SegmentName(
                                            segment.name.to_string(),
                                        )));
                                    let alias = egraph.add(LogicalPlanLanguage::ColumnExprColumn(
                                        ColumnExprColumn(Column {
                                            relation: Some(cube_alias),
                                            name: column_name,
                                        }),
                                    ));
                                    let alias_expr =
                                        egraph.add(LogicalPlanLanguage::ColumnExpr([alias]));
                                    subst.insert(
                                        member_var,
                                        egraph.add(LogicalPlanLanguage::Segment([
                                            measure_name,
                                            alias_expr,
                                        ])),
                                    );
                                    return true;
                                }

                                let member_name =
                                    member_name.split(".").last().unwrap().to_string();

                                if member_name.eq_ignore_ascii_case(&"__user") {
                                    let alias = egraph.add(LogicalPlanLanguage::ColumnExprColumn(
                                        ColumnExprColumn(Column {
                                            relation: Some(cube_alias),
                                            name: column_name,
                                        }),
                                    ));
                                    let alias_expr =
                                        egraph.add(LogicalPlanLanguage::ColumnExpr([alias]));
                                    let cube = egraph.add(LogicalPlanLanguage::ChangeUserCube(
                                        ChangeUserCube(cube.name.to_string()),
                                    ));
                                    subst.insert(
                                        member_var,
                                        egraph.add(LogicalPlanLanguage::ChangeUser([
                                            cube, alias_expr,
                                        ])),
                                    );
                                    return true;
                                }

                                if member_name.eq_ignore_ascii_case(&"__cubeJoinField") {
                                    let alias = egraph.add(LogicalPlanLanguage::ColumnExprColumn(
                                        ColumnExprColumn(Column {
                                            relation: Some(cube_alias),
                                            name: column_name,
                                        }),
                                    ));
                                    let alias_expr =
                                        egraph.add(LogicalPlanLanguage::ColumnExpr([alias]));

                                    let field_name =
                                        egraph.add(LogicalPlanLanguage::VirtualFieldName(
                                            VirtualFieldName(column.name.to_string()),
                                        ));
                                    let cube = egraph.add(LogicalPlanLanguage::VirtualFieldCube(
                                        VirtualFieldCube(cube.name.to_string()),
                                    ));
                                    subst.insert(
                                        member_var,
                                        egraph.add(LogicalPlanLanguage::VirtualField([
                                            field_name, cube, alias_expr,
                                        ])),
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

    fn pushdown_time_dimension(
        &self,
        cube_var: &'static str,
        dimension_var: &'static str,
        time_dimension_name_var: &'static str,
        granularity_var: &'static str,
        time_dimension_granularity_var: &'static str,
        date_range_var: &'static str,
        original_expr_var: &'static str,
        alias_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_var = cube_var.parse().unwrap();
        let dimension_var = dimension_var.parse().unwrap();
        let time_dimension_name_var = time_dimension_name_var.parse().unwrap();
        let granularity_var = granularity_var.parse().unwrap();
        let time_dimension_granularity_var = time_dimension_granularity_var.parse().unwrap();
        let date_range_var = date_range_var.parse().unwrap();
        let original_expr_var = var!(original_expr_var);
        let alias_var = var!(alias_var);
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for alias_to_cube in
                var_iter!(egraph[subst[cube_var]], MemberPushdownReplacerAliasToCube)
            {
                let cube_alias = alias_to_cube.iter().next().unwrap().0 .1.to_string();
                for time_dimension_name in var_iter!(egraph[subst[dimension_var]], DimensionName)
                    .chain(var_iter!(egraph[subst[dimension_var]], TimeDimensionName))
                {
                    if let Some(cube) = meta_context
                        .find_cube_with_name(time_dimension_name.split(".").next().unwrap())
                    {
                        if let Some(time_dimension) = cube.dimensions.iter().find(|d| {
                            d._type == "time" && d.name.eq_ignore_ascii_case(&time_dimension_name)
                        }) {
                            for granularity in
                                var_iter!(egraph[subst[granularity_var]], LiteralExprValue)
                            {
                                let alias = if let Some(alias) =
                                    original_expr_name(egraph, subst[original_expr_var])
                                {
                                    alias
                                } else {
                                    continue;
                                };

                                let granularity_value =
                                    match utils::parse_granularity(granularity, false) {
                                        Some(g) => g,
                                        None => continue,
                                    };

                                subst.insert(
                                    time_dimension_name_var,
                                    egraph.add(LogicalPlanLanguage::TimeDimensionName(
                                        TimeDimensionName(time_dimension.name.to_string()),
                                    )),
                                );
                                subst.insert(
                                    date_range_var,
                                    egraph.add(LogicalPlanLanguage::TimeDimensionDateRange(
                                        TimeDimensionDateRange(None), // TODO
                                    )),
                                );
                                subst.insert(
                                    time_dimension_granularity_var,
                                    egraph.add(LogicalPlanLanguage::TimeDimensionGranularity(
                                        TimeDimensionGranularity(Some(granularity_value)),
                                    )),
                                );

                                let alias_expr =
                                    Self::add_alias_column(egraph, alias, Some(cube_alias));
                                subst.insert(alias_var, alias_expr);

                                return true;
                            }
                        }
                    }
                }
            }
            false
        }
    }

    fn transform_time_dimension(
        &self,
        cube_var: &'static str,
        aliases_var: &'static str,
        dimension_var: &'static str,
        time_dimension_name_var: &'static str,
        granularity_var: &'static str,
        time_dimension_granularity_var: &'static str,
        date_range_var: &'static str,
        original_expr_var: &'static str,
        alias_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_var = cube_var.parse().unwrap();
        let aliases_var = aliases_var.parse().unwrap();
        let dimension_var = dimension_var.parse().unwrap();
        let time_dimension_name_var = time_dimension_name_var.parse().unwrap();
        let granularity_var = granularity_var.parse().unwrap();
        let time_dimension_granularity_var = time_dimension_granularity_var.parse().unwrap();
        let date_range_var = date_range_var.parse().unwrap();
        let original_expr_var = var!(original_expr_var);
        let alias_var = var!(alias_var);
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for column in var_iter!(egraph[subst[dimension_var]], ColumnExprColumn).cloned() {
                for alias_to_cube in var_iter!(egraph[subst[cube_var]], MemberReplacerAliasToCube) {
                    for aliases in var_iter!(egraph[subst[aliases_var]], MemberReplacerAliases) {
                        for ((_, cube_alias), cube) in
                            meta_context.find_cube_by_column_for_replacer(&alias_to_cube, &column)
                        {
                            let time_dimension_name =
                                get_member_name(&cube.name, &column.name, aliases, &cube_alias);
                            if let Some(time_dimension) = cube.dimensions.iter().find(|d| {
                                d._type == "time"
                                    && d.name.eq_ignore_ascii_case(&time_dimension_name)
                            }) {
                                for granularity in
                                    var_iter!(egraph[subst[granularity_var]], LiteralExprValue)
                                {
                                    let alias = if let Some(alias) =
                                        original_expr_name(egraph, subst[original_expr_var])
                                    {
                                        alias
                                    } else {
                                        continue;
                                    };

                                    let granularity_value =
                                        match utils::parse_granularity(granularity, false) {
                                            Some(g) => g,
                                            None => continue,
                                        };

                                    subst.insert(
                                        time_dimension_name_var,
                                        egraph.add(LogicalPlanLanguage::TimeDimensionName(
                                            TimeDimensionName(time_dimension.name.to_string()),
                                        )),
                                    );
                                    subst.insert(
                                        date_range_var,
                                        egraph.add(LogicalPlanLanguage::TimeDimensionDateRange(
                                            TimeDimensionDateRange(None), // TODO
                                        )),
                                    );
                                    subst.insert(
                                        time_dimension_granularity_var,
                                        egraph.add(LogicalPlanLanguage::TimeDimensionGranularity(
                                            TimeDimensionGranularity(Some(granularity_value)),
                                        )),
                                    );

                                    let alias_expr =
                                        Self::add_alias_column(egraph, alias, Some(cube_alias));
                                    subst.insert(alias_var, alias_expr);

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

    fn add_alias_column(
        egraph: &mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
        alias: String,
        cube_alias: Option<String>,
    ) -> Id {
        let alias = egraph.add(LogicalPlanLanguage::ColumnExprColumn(ColumnExprColumn(
            Column {
                name: alias,
                relation: cube_alias,
            },
        )));
        egraph.add(LogicalPlanLanguage::ColumnExpr([alias]))
    }

    fn measure_rewrite(
        &self,
        name: &str,
        aggr_expr: String,
        measure_var: Option<&'static str>,
        distinct_var: Option<&'static str>,
        fun_var: Option<&'static str>,
        cast_data_type_var: Option<&'static str>,
    ) -> Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis> {
        transforming_chain_rewrite(
            &format!("measure-{}", name),
            member_replacer("?aggr_expr", "?alias_to_cube", "?aliases"),
            vec![("?aggr_expr", aggr_expr)],
            "?measure".to_string(),
            self.transform_measure(
                "?alias_to_cube",
                measure_var,
                distinct_var,
                fun_var,
                cast_data_type_var,
                "?aggr_expr",
                "?measure",
            ),
        )
    }

    fn find_matching_old_member(
        &self,
        member_pushdown_replacer_alias_to_cube_var: &'static str,
        column_var: &'static str,
        old_members_var: &'static str,
        terminal_member: &'static str,
        filtered_member_pushdown_replacer_alias_to_cube_var: &'static str,
        default_count: bool,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let member_pushdown_replacer_alias_to_cube_var =
            var!(member_pushdown_replacer_alias_to_cube_var);
        let column_var = var!(column_var);
        let old_members_var = var!(old_members_var);
        let terminal_member = var!(terminal_member);
        let filtered_member_pushdown_replacer_alias_to_cube_var =
            var!(filtered_member_pushdown_replacer_alias_to_cube_var);
        move |egraph, subst| {
            for alias_to_cube in var_iter!(
                egraph[subst[member_pushdown_replacer_alias_to_cube_var]],
                MemberPushdownReplacerAliasToCube
            )
            .cloned()
            {
                let column_iter = if default_count {
                    vec![Column::from_name(Self::default_count_measure_name())]
                } else {
                    var_iter!(egraph[subst[column_var]], ColumnExprColumn)
                        .cloned()
                        .collect()
                };
                for alias_column in column_iter {
                    let alias_name = expr_column_name(Expr::Column(alias_column), &None);

                    if let Some(left_member_name_to_expr) = egraph
                        .index(subst[old_members_var])
                        .data
                        .member_name_to_expr
                        .clone()
                    {
                        let column_name_to_member =
                            column_name_to_member_vec(left_member_name_to_expr);
                        if let Some(member) = column_name_to_member
                            .iter()
                            .find(|(member_alias, _)| member_alias == &alias_name)
                        {
                            let cube_to_filter = if member.1.is_some() {
                                Some(
                                    member
                                        .1
                                        .as_ref()
                                        .unwrap()
                                        .split(".")
                                        .next()
                                        .unwrap()
                                        .to_string(),
                                )
                            } else {
                                alias_to_cube
                                    .iter()
                                    .find(|((alias, _), _)| alias == &member.0)
                                    .map(|(_, cube)| cube.to_string())
                            };
                            let filtered_alias_to_cube = if cube_to_filter.is_some() {
                                alias_to_cube
                                    .clone()
                                    .into_iter()
                                    .filter(|(_, cube)| cube == cube_to_filter.as_ref().unwrap())
                                    .collect()
                            } else {
                                alias_to_cube.clone()
                            };
                            for old_members in
                                var_list_iter!(egraph[subst[old_members_var]], CubeScanMembers)
                                    .cloned()
                            {
                                let old_member = old_members.iter().find(|m| {
                                    if let Some(member_to_name_expr) =
                                        egraph.index(**m).data.member_name_to_expr.clone()
                                    {
                                        let column_name_to_member =
                                            column_name_to_member_vec(member_to_name_expr);
                                        column_name_to_member
                                            .iter()
                                            .any(|(member_alias, _)| member_alias == &alias_name)
                                    } else {
                                        false
                                    }
                                });
                                if let Some(old_member) = old_member {
                                    subst.insert(terminal_member, *old_member);

                                    let filtered_member_pushdown_replacer_alias_to_cube = egraph
                                        .add(
                                            LogicalPlanLanguage::MemberPushdownReplacerAliasToCube(
                                                MemberPushdownReplacerAliasToCube(
                                                    filtered_alias_to_cube,
                                                ),
                                            ),
                                        );

                                    subst.insert(
                                        filtered_member_pushdown_replacer_alias_to_cube_var,
                                        filtered_member_pushdown_replacer_alias_to_cube,
                                    );

                                    return true;
                                } else {
                                    log::error!("Unexpected state: can't find {} during member iteration in {:?}", alias_name, column_name_to_member);
                                    return false;
                                }
                            }
                        }
                    }
                }
            }
            false
        }
    }

    fn select_from_all_member_by_column(
        &self,
        cube_var: &'static str,
        member_pushdown_replacer_alias_to_cube_var: &'static str,
        column_var: &'static str,
        member_var: &'static str,
        default_count: bool,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_var = var!(cube_var);
        let member_pushdown_replacer_alias_to_cube_var =
            var!(member_pushdown_replacer_alias_to_cube_var);
        let column_var = var!(column_var);
        let member_var = var!(member_var);
        let cube_context = self.cube_context.clone();
        move |egraph, subst| {
            for alias_to_cube in var_iter!(
                egraph[subst[member_pushdown_replacer_alias_to_cube_var]],
                MemberPushdownReplacerAliasToCube
            )
            .cloned()
            {
                // alias_to_cube at this point is already filtered to a single cube
                let cube_alias = alias_to_cube.iter().next().unwrap().0 .1.to_string();
                for cube in var_iter!(egraph[subst[cube_var]], AllMembersCube).cloned() {
                    let column_iter = if default_count {
                        vec![Column::from_name(Self::default_count_measure_name())]
                    } else {
                        var_iter!(egraph[subst[column_var]], ColumnExprColumn)
                            .cloned()
                            .collect()
                    };
                    for column in column_iter {
                        if let Some(cube) = cube_context.meta.find_cube_with_name(&cube) {
                            let alias_expr = |egraph| {
                                Self::add_alias_column(
                                    egraph,
                                    column.name.to_string(),
                                    Some(cube_alias.clone()),
                                )
                            };

                            if let Some(dimension) = cube.lookup_dimension(&column.name) {
                                let dimension_name =
                                    egraph.add(LogicalPlanLanguage::DimensionName(DimensionName(
                                        dimension.name.to_string(),
                                    )));

                                let alias = alias_expr(egraph);
                                subst.insert(
                                    member_var,
                                    egraph.add(LogicalPlanLanguage::Dimension([
                                        dimension_name,
                                        alias,
                                    ])),
                                );
                                return true;
                            }

                            if let Some(measure) = cube.lookup_measure(&column.name) {
                                let measure_name = egraph.add(LogicalPlanLanguage::MeasureName(
                                    MeasureName(measure.name.to_string()),
                                ));
                                let alias = alias_expr(egraph);
                                subst.insert(
                                    member_var,
                                    egraph.add(LogicalPlanLanguage::Measure([measure_name, alias])),
                                );
                                return true;
                            }

                            if let Some(segment) = cube.lookup_segment(&column.name) {
                                let segment_name = egraph.add(LogicalPlanLanguage::SegmentName(
                                    SegmentName(segment.name.to_string()),
                                ));
                                let alias = alias_expr(egraph);
                                subst.insert(
                                    member_var,
                                    egraph.add(LogicalPlanLanguage::Segment([segment_name, alias])),
                                );
                                return true;
                            }

                            let member_name = column.name.to_string();

                            if member_name.eq_ignore_ascii_case(&"__user") {
                                let cube = egraph.add(LogicalPlanLanguage::ChangeUserCube(
                                    ChangeUserCube(cube.name.to_string()),
                                ));
                                let alias = alias_expr(egraph);
                                subst.insert(
                                    member_var,
                                    egraph.add(LogicalPlanLanguage::ChangeUser([cube, alias])),
                                );
                                return true;
                            }

                            if member_name.eq_ignore_ascii_case(&"__cubeJoinField") {
                                let field_name = egraph.add(LogicalPlanLanguage::VirtualFieldName(
                                    VirtualFieldName(column.name.to_string()),
                                ));
                                let cube = egraph.add(LogicalPlanLanguage::VirtualFieldCube(
                                    VirtualFieldCube(cube.name.to_string()),
                                ));
                                let alias = alias_expr(egraph);
                                subst.insert(
                                    member_var,
                                    egraph.add(LogicalPlanLanguage::VirtualField([
                                        field_name, cube, alias,
                                    ])),
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

    fn pushdown_measure(
        &self,
        member_pushdown_replacer_alias_to_cube_var: &'static str,
        measure_name_var: &'static str,
        fun_var: Option<&'static str>,
        distinct_var: Option<&'static str>,
        original_expr_var: &'static str,
        cast_data_type_var: Option<&'static str>,
        measure_out_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let member_pushdown_replacer_alias_to_cube_var =
            var!(member_pushdown_replacer_alias_to_cube_var);
        let measure_name_var = var!(measure_name_var);
        let fun_var = fun_var.map(|fun_var| var!(fun_var));
        let distinct_var = distinct_var.map(|distinct_var| var!(distinct_var));
        let original_expr_var = var!(original_expr_var);
        let cast_data_type_var = cast_data_type_var.map(|var| var!(var));
        let measure_out_var = var!(measure_out_var);
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            if let Some(alias) = original_expr_name(egraph, subst[original_expr_var]) {
                for measure_name in var_iter!(egraph[subst[measure_name_var]], MeasureName)
                    .cloned()
                    .chain(var_iter!(egraph[subst[measure_name_var]], DimensionName).cloned())
                {
                    for fun in fun_var
                        .map(|fun_var| {
                            var_iter!(egraph[subst[fun_var]], AggregateFunctionExprFun)
                                .map(|fun| Some(fun))
                                .collect()
                        })
                        .unwrap_or(vec![None])
                    {
                        for distinct in distinct_var
                            .map(|distinct_var| {
                                var_iter!(
                                    egraph[subst[distinct_var]],
                                    AggregateFunctionExprDistinct
                                )
                                .map(|d| *d)
                                .collect()
                            })
                            .unwrap_or(vec![false])
                        {
                            let call_agg_type = Self::get_agg_type(fun, distinct);

                            for alias_to_cube in var_iter!(
                                egraph[subst[member_pushdown_replacer_alias_to_cube_var]],
                                MemberPushdownReplacerAliasToCube
                            )
                            .cloned()
                            {
                                if let Some(measure) =
                                    meta_context.find_measure_with_name(measure_name.to_string())
                                {
                                    let measure_cube_name = measure_name.split(".").next().unwrap();
                                    if let Some(((_, cube_alias), _)) = alias_to_cube
                                        .iter()
                                        .find(|(_, cube)| cube == measure_cube_name)
                                    {
                                        let can_remove_cast = cast_data_type_var
                                            .map(|cast_data_type_var| {
                                                var_iter!(
                                                    egraph[subst[cast_data_type_var]],
                                                    CastExprDataType
                                                )
                                                .any(|dt| match dt {
                                                    DataType::Decimal(_, _) => true,
                                                    _ => false,
                                                })
                                            })
                                            .unwrap_or(true);

                                        if can_remove_cast {
                                            Self::measure_output(
                                                egraph,
                                                subst,
                                                &measure,
                                                call_agg_type,
                                                alias,
                                                measure_out_var,
                                                cube_alias.to_string(),
                                                subst[original_expr_var],
                                                alias_to_cube.clone(),
                                            );
                                            return true;
                                        }
                                    }
                                }

                                if let Some(dimension) =
                                    meta_context.find_dimension_with_name(measure_name.to_string())
                                {
                                    let alias_to_cube = alias_to_cube.clone();
                                    subst.insert(
                                        measure_out_var,
                                        add_member_error(egraph, format!(
                                            "Dimension '{}' was used with the aggregate function '{}()'. Please use a measure instead",
                                            dimension.get_real_name(),
                                            call_agg_type.unwrap_or("MEASURE".to_string()).to_uppercase(),
                                        ), 1, subst[original_expr_var], alias_to_cube),
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

    fn transform_measure(
        &self,
        alias_to_cube_var: &'static str,
        measure_var: Option<&'static str>,
        distinct_var: Option<&'static str>,
        fun_var: Option<&'static str>,
        cast_data_type_var: Option<&'static str>,
        aggr_expr_var: &'static str,
        measure_out_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let distinct_var = distinct_var.map(|var| var.parse().unwrap());
        let fun_var = fun_var.map(|var| var.parse().unwrap());
        let measure_var = measure_var.map(|var| var.parse().unwrap());
        let aggr_expr_var = aggr_expr_var.parse().unwrap();
        let cast_data_type_var = cast_data_type_var.map(|var| var!(var));
        let measure_out_var = measure_out_var.parse().unwrap();
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for column in measure_var
                .map(|measure_var| {
                    var_iter!(egraph[subst[measure_var]], ColumnExprColumn)
                        .map(|c| c.clone())
                        .collect()
                })
                .unwrap_or(vec![Column::from_name(Self::default_count_measure_name())])
            {
                for alias_to_cube in
                    var_iter!(egraph[subst[alias_to_cube_var]], MemberReplacerAliasToCube)
                {
                    for ((_, cube_alias), cube) in
                        meta_context.find_cube_by_column_for_replacer(&alias_to_cube, &column)
                    {
                        for distinct in distinct_var
                            .map(|distinct_var| {
                                var_iter!(
                                    egraph[subst[distinct_var]],
                                    AggregateFunctionExprDistinct
                                )
                                .map(|d| *d)
                                .collect()
                            })
                            .unwrap_or(vec![false])
                        {
                            for fun in fun_var
                                .map(|fun_var| {
                                    var_iter!(egraph[subst[fun_var]], AggregateFunctionExprFun)
                                        .map(|fun| Some(fun))
                                        .collect()
                                })
                                .unwrap_or(vec![None])
                            {
                                let call_agg_type = Self::get_agg_type(fun, distinct);

                                if let Some(measure) = cube.lookup_measure(&column.name) {
                                    if let Some(alias) =
                                        original_expr_name(egraph, subst[aggr_expr_var])
                                    {
                                        let alias_to_cube = alias_to_cube.clone();
                                        let can_remove_cast = cast_data_type_var
                                            .map(|cast_data_type_var| {
                                                var_iter!(
                                                    egraph[subst[cast_data_type_var]],
                                                    CastExprDataType
                                                )
                                                .any(|dt| match dt {
                                                    DataType::Decimal(_, _) => true,
                                                    _ => false,
                                                })
                                            })
                                            .unwrap_or(true);

                                        if can_remove_cast {
                                            Self::measure_output(
                                                egraph,
                                                subst,
                                                measure,
                                                call_agg_type,
                                                alias,
                                                measure_out_var,
                                                cube_alias,
                                                subst[aggr_expr_var],
                                                alias_to_cube,
                                            );

                                            return true;
                                        }
                                    }
                                }

                                if let Some(dimension) = cube.lookup_dimension(&column.name) {
                                    let alias_to_cube = alias_to_cube.clone();
                                    subst.insert(
                                        measure_out_var,
                                        add_member_error(egraph, format!(
                                            "Dimension '{}' was used with the aggregate function '{}()'. Please use a measure instead",
                                            dimension.get_real_name(),
                                            call_agg_type.unwrap_or("MEASURE".to_string()).to_uppercase(),
                                        ), 1, subst[aggr_expr_var], alias_to_cube),
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

    fn measure_output(
        egraph: &mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
        subst: &mut Subst,
        measure: &V1CubeMetaMeasure,
        call_agg_type: Option<String>,
        alias: String,
        measure_out_var: Var,
        cube_alias: String,
        expr: Id,
        alias_to_cube: Vec<((String, String), String)>,
    ) {
        if call_agg_type.is_some() && !measure.is_same_agg_type(call_agg_type.as_ref().unwrap()) {
            let mut agg_type = measure
                .agg_type
                .as_ref()
                .unwrap_or(&"unknown".to_string())
                .to_uppercase();
            if agg_type == "NUMBER"
                || agg_type == "STRING"
                || agg_type == "TIME"
                || agg_type == "BOOLEAN"
            {
                agg_type = "MEASURE".to_string();
            }
            subst.insert(
                measure_out_var,
                add_member_error(egraph, format!(
                    "Measure aggregation type doesn't match. The aggregation type for '{}' is '{}()' but '{}()' was provided",
                    measure.get_real_name(),
                    agg_type,
                    call_agg_type.unwrap().to_uppercase(),
                ), 1, expr, alias_to_cube),
            );
        } else {
            let measure_name = egraph.add(LogicalPlanLanguage::MeasureName(MeasureName(
                measure.name.to_string(),
            )));

            let alias_expr = Self::add_alias_column(egraph, alias, Some(cube_alias));

            subst.insert(
                measure_out_var,
                egraph.add(LogicalPlanLanguage::Measure([measure_name, alias_expr])),
            );
        }
    }

    fn transform_column_alias(
        &self,
        member_pushdown_replacer_alias_to_cube_var: &'static str,
        column_var: &'static str,
        output_column_var: &'static str,
        relation_var: Option<&'static str>,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let member_pushdown_replacer_alias_to_cube_var =
            var!(member_pushdown_replacer_alias_to_cube_var);
        let column_var = var!(column_var);
        let output_column_var = var!(output_column_var);
        let relation_var = relation_var.map(|relation_var| var!(relation_var));
        move |egraph, subst| {
            for alias_to_cube in var_iter!(
                egraph[subst[member_pushdown_replacer_alias_to_cube_var]],
                MemberPushdownReplacerAliasToCube
            )
            .cloned()
            {
                for column in var_iter!(egraph[subst[column_var]], ColumnExprColumn).cloned() {
                    // alias_to_cube at this point is already filtered to a single cube
                    let alias = alias_to_cube.iter().next().unwrap().0 .1.to_string();
                    let alias_expr = Self::add_alias_column(
                        egraph,
                        column.name.to_string(),
                        Some(alias.clone()),
                    );
                    subst.insert(output_column_var, alias_expr);

                    if let Some(relation_var) = relation_var {
                        let literal_member_relation =
                            egraph.add(LogicalPlanLanguage::LiteralMemberRelation(
                                LiteralMemberRelation(Some(alias)),
                            ));
                        subst.insert(relation_var, literal_member_relation);
                    }
                    return true;
                }
            }
            false
        }
    }

    fn transform_literal_member_alias(
        &self,
        alias_to_cube_var: &'static str,
        column_var: &'static str,
        new_alias_var: &'static str,
        new_relation_var: &'static str,
        is_alias: bool,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let column_var = var!(column_var);
        let new_alias_var = var!(new_alias_var);
        let new_relation_var = var!(new_relation_var);
        move |egraph, subst| {
            for alias_to_cube in var_iter!(
                egraph[subst[alias_to_cube_var]],
                MemberPushdownReplacerAliasToCube
            )
            .cloned()
            {
                // alias_to_cube at this point is already filtered to a single cube
                let ((_, alias), _) = &alias_to_cube[0];
                if is_alias {
                    for outer_alias in var_iter!(egraph[subst[column_var]], AliasExprAlias).cloned()
                    {
                        subst.insert(
                            new_alias_var,
                            egraph.add(LogicalPlanLanguage::AliasExprAlias(AliasExprAlias(
                                outer_alias,
                            ))),
                        );
                        subst.insert(
                            new_relation_var,
                            egraph.add(LogicalPlanLanguage::LiteralMemberRelation(
                                LiteralMemberRelation(Some(alias.clone())),
                            )),
                        );
                        return true;
                    }
                } else {
                    for column in var_iter!(egraph[subst[column_var]], ColumnExprColumn).cloned() {
                        subst.insert(
                            new_alias_var,
                            egraph.add(LogicalPlanLanguage::AliasExprAlias(AliasExprAlias(
                                column.name,
                            ))),
                        );
                        subst.insert(
                            new_relation_var,
                            egraph.add(LogicalPlanLanguage::LiteralMemberRelation(
                                LiteralMemberRelation(Some(alias.clone())),
                            )),
                        );
                        return true;
                    }
                }
            }
            false
        }
    }

    fn transform_alias(
        &self,
        member_pushdown_replacer_alias_to_cube_var: &'static str,
        alias_var: &'static str,
        output_column_var: &'static str,
        relation_var: Option<&'static str>,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let member_pushdown_replacer_alias_to_cube_var =
            var!(member_pushdown_replacer_alias_to_cube_var);
        let alias_var = var!(alias_var);
        let output_column_var = var!(output_column_var);
        let relation_var = relation_var.map(|relation_var| var!(relation_var));
        move |egraph, subst| {
            for alias_to_cube in var_iter!(
                egraph[subst[member_pushdown_replacer_alias_to_cube_var]],
                MemberPushdownReplacerAliasToCube
            )
            .cloned()
            {
                for alias in var_iter!(egraph[subst[alias_var]], AliasExprAlias).cloned() {
                    // alias_to_cube at this point is already filtered to a single cube
                    let cube_alias = alias_to_cube.iter().next().unwrap().0 .1.to_string();
                    let alias_expr =
                        Self::add_alias_column(egraph, alias.to_string(), Some(cube_alias.clone()));
                    subst.insert(output_column_var, alias_expr);

                    if let Some(relation_var) = relation_var {
                        let literal_member_relation =
                            egraph.add(LogicalPlanLanguage::LiteralMemberRelation(
                                LiteralMemberRelation(Some(cube_alias)),
                            ));
                        subst.insert(relation_var, literal_member_relation);
                    }

                    return true;
                }
            }

            false
        }
    }

    fn transform_like_expr(
        &self,
        like_type_var: &'static str,
        negated_var: &'static str,
        escape_char_var: &'static str,
        op_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let like_type_var = var!(like_type_var);
        let negated_var = var!(negated_var);
        let escape_char_var = var!(escape_char_var);
        let op_var = var!(op_var);
        move |egraph, subst| {
            for escape_char in var_iter!(egraph[subst[escape_char_var]], LikeExprEscapeChar) {
                if escape_char.is_some() {
                    continue;
                }

                for like_type in var_iter!(egraph[subst[like_type_var]], LikeExprLikeType) {
                    for negated in var_iter!(egraph[subst[negated_var]], LikeExprNegated) {
                        let operator = match (like_type, negated) {
                            (LikeType::Like, false) => Operator::Like,
                            (LikeType::Like, true) => Operator::NotLike,
                            (LikeType::ILike, false) => Operator::ILike,
                            (LikeType::ILike, true) => Operator::NotILike,
                            _ => continue,
                        };

                        subst.insert(
                            op_var,
                            egraph.add(LogicalPlanLanguage::BinaryExprOp(BinaryExprOp(operator))),
                        );

                        return true;
                    }
                }
            }

            false
        }
    }

    pub fn get_agg_type(fun: Option<&AggregateFunction>, distinct: bool) -> Option<String> {
        fun.map(|fun| {
            match fun {
                AggregateFunction::Count => {
                    if distinct {
                        "countDistinct"
                    } else {
                        "count"
                    }
                }
                AggregateFunction::Sum => "sum",
                AggregateFunction::Min => "min",
                AggregateFunction::Max => "max",
                AggregateFunction::Avg => "avg",
                AggregateFunction::ApproxDistinct => "countDistinctApprox",
                // TODO: Fix me
                _ => "unknown_aggregation_type_hardcoded",
            }
            .to_string()
        })
    }

    pub fn default_count_measure_name() -> String {
        "count".to_string()
    }

    fn push_down_cross_join_to_empty_scan(
        &self,
        left_alias_to_cube_var: &'static str,
        right_alias_to_cube_var: &'static str,
        joined_alias_to_cube_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let left_alias_to_cube_var = var!(left_alias_to_cube_var);
        let right_alias_to_cube_var = var!(right_alias_to_cube_var);
        let joined_alias_to_cube_var = var!(joined_alias_to_cube_var);
        move |egraph, subst| {
            for left_alias_to_cube in
                var_iter!(egraph[subst[left_alias_to_cube_var]], CubeScanAliasToCube).cloned()
            {
                for right_alias_to_cube in
                    var_iter!(egraph[subst[right_alias_to_cube_var]], CubeScanAliasToCube).cloned()
                {
                    let joined_alias_to_cube = egraph.add(
                        LogicalPlanLanguage::CubeScanAliasToCube(CubeScanAliasToCube(
                            left_alias_to_cube
                                .into_iter()
                                .chain(right_alias_to_cube.into_iter())
                                .collect(),
                        )),
                    );
                    subst.insert(joined_alias_to_cube_var, joined_alias_to_cube);

                    return true;
                }
            }

            false
        }
    }

    fn push_down_cross_join_to_cube_scan(
        &self,
        left_alias_to_cube_var: &'static str,
        right_alias_to_cube_var: &'static str,
        joined_alias_to_cube_var: &'static str,
        left_members_var: &'static str,
        right_members_var: &'static str,
        joined_members_var: &'static str,
        left_filters_var: &'static str,
        right_filters_var: &'static str,
        new_filters_var: &'static str,
        left_aliases_var: &'static str,
        right_aliases_var: &'static str,
        joined_aliases_var: &'static str,
        left_order_var: &'static str,
        right_order_var: &'static str,
        new_order_var: &'static str,
        left_limit_var: &'static str,
        right_limit_var: &'static str,
        new_limit_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let left_alias_to_cube_var = var!(left_alias_to_cube_var);
        let right_alias_to_cube_var = var!(right_alias_to_cube_var);
        let joined_alias_to_cube_var = var!(joined_alias_to_cube_var);
        let left_members_var = var!(left_members_var);
        let right_members_var = var!(right_members_var);
        let joined_members_var = var!(joined_members_var);
        let left_filters_var = var!(left_filters_var);
        let right_filters_var = var!(right_filters_var);
        let new_filters_var = var!(new_filters_var);
        let left_aliases_var = var!(left_aliases_var);
        let right_aliases_var = var!(right_aliases_var);
        let joined_aliases_var = var!(joined_aliases_var);
        let left_order_var = var!(left_order_var);
        let right_order_var = var!(right_order_var);
        let new_order_var = var!(new_order_var);
        let left_limit_var = var!(left_limit_var);
        let right_limit_var = var!(right_limit_var);
        let new_limit_var = var!(new_limit_var);
        move |egraph, subst| {
            for left_alias_to_cube in
                var_iter!(egraph[subst[left_alias_to_cube_var]], CubeScanAliasToCube).cloned()
            {
                for right_alias_to_cube in
                    var_iter!(egraph[subst[right_alias_to_cube_var]], CubeScanAliasToCube).cloned()
                {
                    for left_members in
                        var_list_iter!(egraph[subst[left_members_var]], CubeScanMembers).cloned()
                    {
                        for right_members in
                            var_list_iter!(egraph[subst[right_members_var]], CubeScanMembers)
                                .cloned()
                        {
                            // push_down_cross_join_to_empty_scan works in this case
                            if left_members.is_empty() && right_members.is_empty() {
                                continue;
                            }

                            let left_limit =
                                match var_iter!(egraph[subst[left_limit_var]], CubeScanLimit)
                                    .cloned()
                                    .next()
                                {
                                    Some(limit) => limit,
                                    None => continue,
                                };

                            let right_limit =
                                match var_iter!(egraph[subst[right_limit_var]], CubeScanLimit)
                                    .cloned()
                                    .next()
                                {
                                    Some(limit) => limit,
                                    None => continue,
                                };

                            // TODO handle the case when limit set on non multiplied cube. It's possible to push down the limit in this case.
                            if left_limit.is_some() || right_limit.is_some() {
                                continue;
                            }

                            for left_aliases in
                                var_iter!(egraph[subst[left_aliases_var]], CubeScanAliases).cloned()
                            {
                                if left_aliases.is_none() {
                                    continue;
                                }

                                for right_aliases in
                                    var_iter!(egraph[subst[right_aliases_var]], CubeScanAliases)
                                        .cloned()
                                {
                                    if right_aliases.is_none() {
                                        continue;
                                    }

                                    let is_left_order_empty = Some(true)
                                        == egraph[subst[left_order_var]].data.is_empty_list.clone();

                                    let is_right_order_empty = Some(true)
                                        == egraph[subst[right_order_var]]
                                            .data
                                            .is_empty_list
                                            .clone();

                                    if !is_left_order_empty && !is_right_order_empty {
                                        continue;
                                    }

                                    subst.insert(
                                        joined_alias_to_cube_var,
                                        egraph.add(LogicalPlanLanguage::CubeScanAliasToCube(
                                            CubeScanAliasToCube(
                                                left_alias_to_cube
                                                    .into_iter()
                                                    .chain(right_alias_to_cube.into_iter())
                                                    .collect(),
                                            ),
                                        )),
                                    );

                                    let joined_members =
                                        egraph.add(LogicalPlanLanguage::CubeScanMembers(vec![
                                            subst[left_members_var],
                                            subst[right_members_var],
                                        ]));

                                    subst.insert(joined_members_var, joined_members);

                                    subst.insert(
                                        new_filters_var,
                                        egraph.add(LogicalPlanLanguage::CubeScanFilters(vec![
                                            subst[left_filters_var],
                                            subst[right_filters_var],
                                        ])),
                                    );

                                    subst.insert(
                                        joined_aliases_var,
                                        egraph.add(LogicalPlanLanguage::CubeScanAliases(
                                            CubeScanAliases(Some(
                                                left_aliases
                                                    .unwrap_or(vec![])
                                                    .into_iter()
                                                    .chain(
                                                        right_aliases.unwrap_or(vec![]).into_iter(),
                                                    )
                                                    .collect(),
                                            )),
                                        )),
                                    );

                                    let orders = if is_left_order_empty {
                                        subst[right_order_var]
                                    } else {
                                        subst[left_order_var]
                                    };

                                    subst.insert(
                                        new_limit_var,
                                        egraph.add(LogicalPlanLanguage::CubeScanLimit(
                                            CubeScanLimit(None),
                                        )),
                                    );

                                    subst.insert(new_order_var, orders);

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

    fn join_to_cross_join(
        &self,
        left_on_var: &'static str,
        right_on_var: &'static str,
        left_aliases_var: &'static str,
        right_aliases_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let left_on_var = var!(left_on_var);
        let right_on_var = var!(right_on_var);
        let left_aliases_var = var!(left_aliases_var);
        let right_aliases_var = var!(right_aliases_var);
        move |egraph, subst| {
            for left_aliases in var_iter!(egraph[subst[left_aliases_var]], CubeScanAliases).cloned()
            {
                if left_aliases.is_none() {
                    continue;
                }

                let left_aliases = left_aliases.unwrap();
                for right_aliases in
                    var_iter!(egraph[subst[right_aliases_var]], CubeScanAliases).cloned()
                {
                    if right_aliases.is_none() {
                        continue;
                    }

                    let right_aliases = right_aliases.unwrap();
                    for left_join_on in var_iter!(egraph[subst[left_on_var]], JoinLeftOn) {
                        for join_on in left_join_on.iter() {
                            let mut column_name = join_on.name.clone();
                            if let Some(name) = find_column_by_alias(
                                &column_name,
                                &left_aliases,
                                &join_on.relation.clone().unwrap_or_default(),
                            ) {
                                column_name = name.split(".").last().unwrap().to_string();
                            }

                            if column_name == "__cubeJoinField" {
                                for right_join_on in
                                    var_iter!(egraph[subst[right_on_var]], JoinRightOn)
                                {
                                    for join_on in right_join_on.iter() {
                                        let mut column_name = join_on.name.clone();
                                        if let Some(name) = find_column_by_alias(
                                            &column_name,
                                            &right_aliases,
                                            &join_on.relation.clone().unwrap_or_default(),
                                        ) {
                                            column_name =
                                                name.split(".").last().unwrap().to_string();
                                        }

                                        if column_name == "__cubeJoinField" {
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

    fn push_down_cross_join_to_cubescan_rewrite(
        &self,
        name: &str,
        left_members_expr: String,
        right_members_expr: String,
        left_members: &'static str,
        right_members: &'static str,
    ) -> Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis> {
        transforming_rewrite(
            &format!("push-down-cross-join-to-cube-scan-{}", name),
            cross_join(
                cube_scan(
                    "?left_alias_to_cube",
                    left_members_expr,
                    "?left_filters",
                    "?left_order",
                    "?left_limit",
                    "CubeScanOffset:None",
                    "?left_aliases",
                    "CubeScanSplit:false",
                    "CubeScanCanPushdownJoin:true",
                    "CubeScanWrapped:false",
                ),
                cube_scan(
                    "?right_alias_to_cube",
                    right_members_expr,
                    "?right_filters",
                    "?right_order",
                    "?right_limit",
                    "CubeScanOffset:None",
                    "?right_aliases",
                    "CubeScanSplit:false",
                    "CubeScanCanPushdownJoin:true",
                    "CubeScanWrapped:false",
                ),
            ),
            cube_scan(
                "?joined_alias_to_cube",
                "?joined_members",
                "?joined_filters",
                "?new_order",
                "?new_limit",
                "CubeScanOffset:None",
                "?joined_aliases",
                "CubeScanSplit:false",
                "CubeScanCanPushdownJoin:true",
                "CubeScanWrapped:false",
            ),
            self.push_down_cross_join_to_cube_scan(
                "?left_alias_to_cube",
                "?right_alias_to_cube",
                "?joined_alias_to_cube",
                left_members,
                right_members,
                "?joined_members",
                "?left_filters",
                "?right_filters",
                "?joined_filters",
                "?left_aliases",
                "?right_aliases",
                "?joined_aliases",
                "?left_order",
                "?right_order",
                "?new_order",
                "?left_limit",
                "?right_limit",
                "?new_limit",
            ),
        )
    }
}

pub fn add_member_error(
    egraph: &mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
    member_error: String,
    priority: usize,
    expr: Id,
    alias_to_cube: Vec<((String, String), String)>,
) -> Id {
    let member_error = egraph.add(LogicalPlanLanguage::MemberErrorError(MemberErrorError(
        member_error,
    )));

    let member_priority = egraph.add(LogicalPlanLanguage::MemberErrorPriority(
        MemberErrorPriority(priority),
    ));

    // We need this so MemberError can be unique within specific CubeScan.
    // Otherwise it'll provide transitive equivalence bridge between CubeScan members
    let alias_to_cube = egraph.add(LogicalPlanLanguage::MemberErrorAliasToCube(
        MemberErrorAliasToCube(alias_to_cube),
    ));

    egraph.add(LogicalPlanLanguage::MemberError([
        member_error,
        member_priority,
        expr,
        alias_to_cube,
    ]))
}

lazy_static! {
    static ref STANDARD_GRANULARITIES_PARENTS: HashMap<&'static str, Vec<&'static str>> = [
        (
            "year",
            vec!["year", "quarter", "month", "day", "hour", "minute", "second"]
        ),
        (
            "quarter",
            vec!["quarter", "month", "day", "hour", "minute", "second"]
        ),
        ("month", vec!["month", "day", "hour", "minute", "second"]),
        ("week", vec!["week", "day", "hour", "minute", "second"]),
        ("day", vec!["day", "hour", "minute", "second"]),
        ("hour", vec!["hour", "minute", "second"]),
        ("minute", vec!["minute", "second"]),
        ("second", vec!["second"]),
    ]
    .iter()
    .cloned()
    .collect();
}

fn min_granularity(granularity_a: &String, granularity_b: &String) -> Option<String> {
    let granularity_a = granularity_a.to_lowercase();
    let granularity_b = granularity_b.to_lowercase();

    if granularity_a == granularity_b {
        return Some(granularity_a);
    }
    if !STANDARD_GRANULARITIES_PARENTS.contains_key(granularity_a.as_str())
        || !STANDARD_GRANULARITIES_PARENTS.contains_key(granularity_b.as_str())
    {
        return None;
    }

    let a_hierarchy = STANDARD_GRANULARITIES_PARENTS[granularity_a.as_str()].clone();
    let b_hierarchy = STANDARD_GRANULARITIES_PARENTS[granularity_b.as_str()].clone();

    let last_index = a_hierarchy
        .iter()
        .rev()
        .zip_longest(b_hierarchy.iter().rev())
        .enumerate()
        .find_map(|(i, val)| match val {
            EitherOrBoth::Both(a, b) if a == b => None,
            _ => Some(i as i32),
        })
        .unwrap_or(-1);

    if last_index <= 0 {
        None
    } else {
        Some(a_hierarchy[a_hierarchy.len() - last_index as usize].to_string())
    }
}

fn get_member_name(
    cube_name: &String,
    column_name: &String,
    aliases: &Vec<(String, String)>,
    cube_alias: &String,
) -> String {
    find_column_by_alias(column_name, aliases, cube_alias)
        .unwrap_or(format!("{}.{}", cube_name, column_name))
}

fn find_column_by_alias(
    column_name: &String,
    aliases: &Vec<(String, String)>,
    cube_alias: &String,
) -> Option<String> {
    if let Some((_, name)) = aliases
        .iter()
        .find(|(a, _)| a == &format!("{}.{}", cube_alias, column_name))
    {
        return Some(name.to_string());
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_min_granularity() {
        assert_eq!(
            min_granularity(&"month".to_string(), &"week".to_string()),
            Some("day".to_string())
        );

        assert_eq!(
            min_granularity(&"week".to_string(), &"month".to_string()),
            Some("day".to_string())
        );

        assert_eq!(
            min_granularity(&"year".to_string(), &"year".to_string()),
            Some("year".to_string())
        );

        assert_eq!(
            min_granularity(&"YEAR".to_string(), &"year".to_string()),
            Some("year".to_string())
        );

        assert_eq!(
            min_granularity(&"week".to_string(), &"second".to_string()),
            Some("second".to_string())
        );

        assert_eq!(
            min_granularity(&"minute".to_string(), &"quarter".to_string()),
            Some("minute".to_string())
        );

        assert_eq!(
            min_granularity(&"NULL".to_string(), &"quarter".to_string()),
            None,
        );
    }
}
