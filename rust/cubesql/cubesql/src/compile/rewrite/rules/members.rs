use crate::{
    compile::{
        engine::udf::MEASURE_UDAF_NAME,
        rewrite::{
            agg_fun_expr, agg_fun_expr_within_group_empty_tail, aggregate, alias_expr, all_members,
            analysis::{ConstantFolding, LogicalPlanData, Member, MemberNamesToExpr, OriginalExpr},
            binary_expr, cast_expr, change_user_expr, column_expr, cross_join, cube_scan,
            cube_scan_filters, cube_scan_filters_empty_tail, cube_scan_members,
            cube_scan_members_empty_tail, cube_scan_order_empty_tail, dimension_expr, distinct,
            expr_column_name, fun_expr, join, like_expr, limit, list_concat_pushdown_replacer,
            list_concat_pushup_replacer, literal_expr, literal_member, measure_expr,
            member_pushdown_replacer, member_replacer, original_expr_name, projection,
            referenced_columns, rewrite,
            rewriter::{CubeEGraph, CubeRewrite, RewriteRules},
            rules::{
                replacer_flat_push_down_node_substitute_rules, replacer_push_down_node,
                replacer_push_down_node_substitute_rules, utils,
            },
            segment_expr, table_scan, time_dimension_expr, transform_original_expr_to_alias,
            transforming_chain_rewrite, transforming_rewrite, transforming_rewrite_with_root,
            udaf_expr, udf_expr, virtual_field_expr, AggregateFunctionExprDistinct,
            AggregateFunctionExprFun, AliasExprAlias, AllMembersAlias, AllMembersCube,
            BinaryExprOp, CastExprDataType, ColumnExprColumn, CubeScanAliasToCube,
            CubeScanCanPushdownJoin, CubeScanJoinHints, CubeScanLimit, CubeScanOffset,
            CubeScanUngrouped, DimensionName, JoinLeftOn, JoinRightOn, LikeExprEscapeChar,
            LikeExprLikeType, LikeExprNegated, LikeType, LimitFetch, LimitSkip, ListType,
            LiteralExprValue, LiteralMemberRelation, LiteralMemberValue, LogicalPlanLanguage,
            MeasureName, MemberErrorAliasToCube, MemberErrorError, MemberErrorPriority,
            MemberPushdownReplacerAliasToCube, MemberReplacerAliasToCube, ProjectionAlias,
            TableScanFetch, TableScanProjection, TableScanSourceTableName, TableScanTableName,
            TimeDimensionDateRange, TimeDimensionGranularity, TimeDimensionName,
        },
    },
    config::ConfigObj,
    singular_eclass,
    sql::ColumnType,
    transport::{MetaContext, V1CubeMetaDimensionExt, V1CubeMetaExt, V1CubeMetaMeasureExt},
    var, var_iter, var_list_iter, CubeError,
};
use cubeclient::models::V1CubeMetaMeasure;
use datafusion::{
    arrow::datatypes::DataType,
    logical_plan::{Column, DFSchema, Expr, Operator},
    physical_plan::aggregates::AggregateFunction,
    scalar::ScalarValue,
};
use egg::{Id, Subst, Var};
use itertools::{EitherOrBoth, Itertools};
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    iter,
    ops::{Index, IndexMut},
    sync::{Arc, LazyLock},
};

pub struct MemberRules {
    meta_context: Arc<MetaContext>,
    config_obj: Arc<dyn ConfigObj>,
    enable_ungrouped: bool,
}

impl RewriteRules for MemberRules {
    fn rewrite_rules(&self) -> Vec<CubeRewrite> {
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
                    "CubeScanSplit:false",
                    "CubeScanCanPushdownJoin:true",
                    "CubeScanWrapped:false",
                    format!("CubeScanUngrouped:{}", self.enable_ungrouped),
                    "?cube_scan_join_hints",
                ),
                self.transform_table_scan(
                    "?source_table_name",
                    "?table_name",
                    "?projection",
                    "?filters",
                    "?fetch",
                    "?alias_to_cube",
                    "?cube_scan_members",
                    "?cube_scan_join_hints",
                ),
            ),
            self.measure_rewrite(
                "simple-count",
                agg_fun_expr(
                    "?aggr_fun",
                    vec![literal_expr("?literal")],
                    "?distinct",
                    agg_fun_expr_within_group_empty_tail(),
                ),
                None,
                Some("?distinct"),
                Some("?aggr_fun"),
                None,
            ),
            self.measure_rewrite(
                "named",
                agg_fun_expr(
                    "?aggr_fun",
                    vec![column_expr("?column")],
                    "?distinct",
                    agg_fun_expr_within_group_empty_tail(),
                ),
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
                    agg_fun_expr_within_group_empty_tail(),
                ),
                Some("?column"),
                Some("?distinct"),
                Some("?aggr_fun"),
                Some("?data_type"),
            ),
            self.measure_rewrite(
                "measure-fun",
                udaf_expr(MEASURE_UDAF_NAME, vec![column_expr("?column")]),
                Some("?column"),
                None,
                None,
                None,
            ),
            transforming_rewrite(
                "push-down-aggregate",
                aggregate(
                    cube_scan(
                        "?alias_to_cube",
                        "?old_members",
                        "?filters",
                        "?orders",
                        // If CubeScan already have limit and offset it would be incorrect to push aggregation into it
                        // Aggregate(CubeScan(limit, offset)) would run aggregation over limited rows
                        // CubeScan(aggregation, limit, offset) would return limited groups
                        "CubeScanLimit:None",
                        "CubeScanOffset:None",
                        "?split",
                        "?can_pushdown_join",
                        "CubeScanWrapped:false",
                        "?ungrouped",
                        "?join_hints",
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
                            "?old_members",
                            "?member_pushdown_replacer_alias_to_cube",
                        ),
                        member_pushdown_replacer(
                            "?aggr_expr",
                            "?old_members",
                            "?member_pushdown_replacer_alias_to_cube",
                        ),
                    ),
                    "?filters",
                    "?orders",
                    "CubeScanLimit:None",
                    "CubeScanOffset:None",
                    "?split",
                    "?new_pushdown_join",
                    "CubeScanWrapped:false",
                    "CubeScanUngrouped:false",
                    "?join_hints",
                ),
                self.push_down_non_empty_aggregate(
                    "?alias_to_cube",
                    "?group_expr",
                    "?aggr_expr",
                    "?old_members",
                    "?can_pushdown_join",
                    "?member_pushdown_replacer_alias_to_cube",
                    "?new_pushdown_join",
                    "?ungrouped",
                    "?filters",
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
                        "?split",
                        "?can_pushdown_join",
                        "CubeScanWrapped:false",
                        "?ungrouped",
                        "?join_hints",
                    ),
                    "?alias",
                    "?projection_split",
                ),
                cube_scan(
                    "?new_alias_to_cube",
                    member_pushdown_replacer(
                        "?expr",
                        "?members",
                        "?member_pushdown_replacer_alias_to_cube",
                    ),
                    "?filters",
                    "?orders",
                    "?limit",
                    "?offset",
                    "?split",
                    "?can_pushdown_join",
                    "CubeScanWrapped:false",
                    "?ungrouped",
                    "?join_hints",
                ),
                self.push_down_projection(
                    "?expr",
                    "?members",
                    "?alias",
                    "?alias_to_cube",
                    "?new_alias_to_cube",
                    "?member_pushdown_replacer_alias_to_cube",
                ),
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
                        "?inner_fetch",
                        "?inner_skip",
                        "?split",
                        "?can_pushdown_join",
                        "CubeScanWrapped:false",
                        "?ungrouped",
                        "?join_hints",
                    ),
                ),
                cube_scan(
                    "?alias_to_cube",
                    "?members",
                    "?filters",
                    "?orders",
                    "?new_fetch",
                    "?new_skip",
                    "?split",
                    "?can_pushdown_join",
                    "CubeScanWrapped:false",
                    "?ungrouped",
                    "?join_hints",
                ),
                self.push_down_limit(
                    "?skip",
                    "?fetch",
                    "?inner_skip",
                    "?inner_fetch",
                    "?new_skip",
                    "?new_fetch",
                ),
            ),
            transforming_rewrite(
                "select-distinct-dimensions",
                distinct(cube_scan(
                    "?alias_to_cube",
                    "?members",
                    "?filters",
                    "?orders",
                    "CubeScanLimit:None",
                    "CubeScanOffset:None",
                    "?split",
                    "?can_pushdown_join",
                    "CubeScanWrapped:false",
                    "?left_ungrouped",
                    "?join_hints",
                )),
                cube_scan(
                    "?alias_to_cube",
                    "?members",
                    "?filters",
                    "?orders",
                    "CubeScanLimit:None",
                    "CubeScanOffset:None",
                    "?split",
                    "?can_pushdown_join",
                    "CubeScanWrapped:false",
                    "CubeScanUngrouped:false",
                    "?join_hints",
                ),
                self.select_distinct_dimensions(
                    "?alias_to_cube",
                    "?members",
                    "?filters",
                    "?left_ungrouped",
                ),
            ),
            // MOD function to binary expr
            transforming_rewrite_with_root(
                "mod-fun-to-binary-expr",
                udf_expr("mod", vec!["?a", "?b"]),
                alias_expr(binary_expr("?a", "%", "?b"), "?alias"),
                transform_original_expr_to_alias("?alias"),
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
            self.push_down_cross_join_to_cubescan_rewrite(
                "not-merged-cubescans",
                "?left_members".to_string(),
                "?right_members".to_string(),
                "?left_members",
                "?right_members",
            ),
            // It is incorrect to merge two CubeScan's into one after grouping
            // __cubeJoinField is a representation of join from data model,
            // and it is valid only for ungrouped queries to data source
            // So CubeScanCanPushdownJoin and CubeScanUngrouped are fixed to true
            // Limit and offset are not allowed
            // Consider plan like Join(CubeScan(limit = 1), CubeScan(limit = 1))
            // Join would check only one row from left scan and only one from right
            // And if they mismatch it will produce empty table
            // There's no way to represent this as a single CubeScan
            // Join does not guarantee ordering, so there's no point in keeping orders in RHS
            transforming_rewrite(
                "push-down-cube-join",
                join(
                    cube_scan(
                        "?left_alias_to_cube",
                        "?left_members",
                        "?left_filters",
                        "?left_orders",
                        "CubeScanLimit:None",
                        "CubeScanOffset:None",
                        "?left_split",
                        "CubeScanCanPushdownJoin:true",
                        "CubeScanWrapped:false",
                        "CubeScanUngrouped:true",
                        "?left_join_hints",
                    ),
                    cube_scan(
                        "?right_alias_to_cube",
                        "?right_members",
                        "?right_filters",
                        "?right_orders",
                        "CubeScanLimit:None",
                        "CubeScanOffset:None",
                        "?right_split",
                        "CubeScanCanPushdownJoin:true",
                        "CubeScanWrapped:false",
                        "CubeScanUngrouped:true",
                        "?right_join_hints",
                    ),
                    "?left_on",
                    "?right_on",
                    "?join_type",
                    "?join_constraint",
                    "?null_equals_null",
                ),
                cube_scan(
                    "?out_alias_to_cube",
                    cube_scan_members("?left_members", "?right_members"),
                    cube_scan_filters("?left_filters", "?right_filters"),
                    cube_scan_order_empty_tail(),
                    "CubeScanLimit:None",
                    "CubeScanOffset:None",
                    // New CubeScan is treated as "not yet split", to give split rules one more chance
                    "CubeScanSplit:false",
                    "CubeScanCanPushdownJoin:true",
                    "CubeScanWrapped:false",
                    "CubeScanUngrouped:true",
                    "?out_join_hints",
                ),
                self.push_down_cube_join(
                    "?left_alias_to_cube",
                    "?right_alias_to_cube",
                    "?out_alias_to_cube",
                    "?left_members",
                    "?right_members",
                    "?left_on",
                    "?right_on",
                    "?left_join_hints",
                    "?right_join_hints",
                    "?out_join_hints",
                ),
            ),
        ];

        rules.extend(self.member_pushdown_rules());
        rules
    }
}

enum ColumnToSearch {
    Var(&'static str),
    DefaultCount,
}

impl MemberRules {
    pub fn new(
        meta_context: Arc<MetaContext>,
        config_obj: Arc<dyn ConfigObj>,
        enable_ungrouped: bool,
    ) -> Self {
        Self {
            meta_context,
            config_obj,
            enable_ungrouped,
        }
    }

    fn fun_expr(&self, fun_name: impl Display, args: Vec<impl Display>) -> String {
        fun_expr(fun_name, args, self.config_obj.push_down_pull_up_split())
    }

    fn member_column_pushdown(
        &self,
        name: &str,
        member_fn: impl Fn(&str) -> String,
        relation: Option<&'static str>,
    ) -> Vec<CubeRewrite> {
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
            // Cast without alias will not generate stable name in schema, so there's no rule like that for now
            // TODO implement it anyway, to be able to remove Projection on top of CubeScan completely
            transforming_rewrite(
                &format!("member-pushdown-replacer-column-{}-cast-alias", name),
                member_pushdown_replacer(
                    alias_expr(cast_expr(column_expr("?column"), "?cast_type"), "?alias"),
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

    fn member_pushdown_rules(&self) -> Vec<CubeRewrite> {
        let mut rules = Vec::new();
        let member_replacer_fn = |members| {
            member_pushdown_replacer(
                members,
                "?old_members",
                "?member_pushdown_replacer_alias_to_cube",
            )
        };

        let find_matching_old_member =
            |name: &str,
             column_expr: String,
             column_to_search: ColumnToSearch,
             cast_type_var: Option<&'static str>| {
                transforming_rewrite(
                    &format!(
                        "member-pushdown-replacer-column-find-matching-old-member-{}",
                        name
                    ),
                    member_pushdown_replacer(
                        column_expr.clone(),
                        "?old_members",
                        "?member_pushdown_replacer_alias_to_cube",
                    ),
                    member_pushdown_replacer(
                        column_expr.clone(),
                        "?terminal_member",
                        "?filtered_member_pushdown_replacer_alias_to_cube",
                    ),
                    self.transform_find_matching_old_member(
                        "?member_pushdown_replacer_alias_to_cube",
                        column_to_search,
                        cast_type_var,
                        "?old_members",
                        "?terminal_member",
                        "?filtered_member_pushdown_replacer_alias_to_cube",
                    ),
                )
            };

        if self.config_obj.push_down_pull_up_split() {
            rules.extend(replacer_flat_push_down_node_substitute_rules(
                "member-pushdown-replacer-aggregate-group",
                ListType::AggregateGroupExpr,
                ListType::CubeScanMembers,
                member_replacer_fn,
            ));
            rules.extend(replacer_flat_push_down_node_substitute_rules(
                "member-pushdown-replacer-aggregate-aggr",
                ListType::AggregateAggrExpr,
                ListType::CubeScanMembers,
                member_replacer_fn,
            ));
            rules.extend(replacer_flat_push_down_node_substitute_rules(
                "member-pushdown-replacer-projection",
                ListType::ProjectionExpr,
                ListType::CubeScanMembers,
                member_replacer_fn,
            ));
        } else {
            rules.extend(replacer_push_down_node_substitute_rules(
                "member-pushdown-replacer-aggregate-group",
                "AggregateGroupExpr",
                "CubeScanMembers",
                member_replacer_fn,
            ));
            rules.extend(replacer_push_down_node_substitute_rules(
                "member-pushdown-replacer-aggregate-aggr",
                "AggregateAggrExpr",
                "CubeScanMembers",
                member_replacer_fn,
            ));
            rules.extend(replacer_push_down_node_substitute_rules(
                "member-pushdown-replacer-projection",
                "ProjectionExpr",
                "CubeScanMembers",
                member_replacer_fn,
            ));
        }
        rules.push(find_matching_old_member(
            "column",
            column_expr("?column"),
            ColumnToSearch::Var("?column"),
            None,
        ));
        rules.push(find_matching_old_member(
            "column-cast",
            cast_expr(column_expr("?column"), "?cast_type"),
            ColumnToSearch::Var("?column"),
            Some("?cast_type"),
        ));
        rules.push(find_matching_old_member(
            "alias",
            alias_expr(column_expr("?column"), "?alias"),
            ColumnToSearch::Var("?column"),
            None,
        ));
        rules.push(find_matching_old_member(
            "alias-cast",
            alias_expr(cast_expr(column_expr("?column"), "?cast_type"), "?alias"),
            ColumnToSearch::Var("?column"),
            Some("?cast_type"),
        ));
        rules.push(find_matching_old_member(
            "agg-fun",
            agg_fun_expr(
                "?fun_name",
                vec![column_expr("?column")],
                "?distinct",
                agg_fun_expr_within_group_empty_tail(),
            ),
            ColumnToSearch::Var("?column"),
            None,
        ));
        rules.push(find_matching_old_member(
            "agg-fun-alias",
            alias_expr(
                agg_fun_expr(
                    "?fun_name",
                    vec![column_expr("?column")],
                    "?distinct",
                    agg_fun_expr_within_group_empty_tail(),
                ),
                "?alias",
            ),
            ColumnToSearch::Var("?column"),
            None,
        ));
        rules.push(find_matching_old_member(
            "udaf-fun",
            udaf_expr(MEASURE_UDAF_NAME, vec![column_expr("?column")]),
            ColumnToSearch::Var("?column"),
            None,
        ));
        rules.push(find_matching_old_member(
            "agg-fun-default-count",
            agg_fun_expr(
                "Count",
                vec![literal_expr("?any")],
                "AggregateFunctionExprDistinct:false",
                agg_fun_expr_within_group_empty_tail(),
            ),
            ColumnToSearch::DefaultCount,
            None,
        ));
        rules.push(find_matching_old_member(
            "agg-fun-default-count-alias",
            alias_expr(
                agg_fun_expr(
                    "Count",
                    vec![literal_expr("?any")],
                    "AggregateFunctionExprDistinct:false",
                    agg_fun_expr_within_group_empty_tail(),
                ),
                "?alias",
            ),
            ColumnToSearch::DefaultCount,
            None,
        ));
        rules.push(find_matching_old_member(
            "agg-fun-with-cast",
            // TODO need to check data_type if we can remove the cast
            agg_fun_expr(
                "?fun_name",
                vec![cast_expr(column_expr("?column"), "?data_type")],
                "?distinct",
                agg_fun_expr_within_group_empty_tail(),
            ),
            ColumnToSearch::Var("?column"),
            None,
        ));
        rules.push(find_matching_old_member(
            "date-trunc",
            self.fun_expr(
                "DateTrunc",
                vec![literal_expr("?granularity"), column_expr("?column")],
            ),
            ColumnToSearch::Var("?column"),
            None,
        ));
        rules.push(find_matching_old_member(
            "date-trunc-with-alias",
            // TODO need to check data_type if we can remove the cast
            alias_expr(
                self.fun_expr(
                    "DateTrunc",
                    vec![literal_expr("?granularity"), column_expr("?column")],
                ),
                "?original_alias",
            ),
            ColumnToSearch::Var("?column"),
            None,
        ));
        Self::measure_rewrites(
            &mut rules,
            |name: &'static str,
             aggr_expr: String,
             measure_expr: String,
             fun_name: Option<&'static str>,
             distinct: Option<&'static str>,
             cast_data_type: Option<&'static str>,
             _column: Option<&'static str>| {
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
            },
        );

        rules.push(transforming_chain_rewrite(
            "member-pushdown-date-trunc",
            member_pushdown_replacer(
                "?original_expr",
                dimension_expr("?dimension_name", "?dimension_alias"),
                "?alias_to_cube",
            ),
            vec![(
                "?original_expr",
                self.fun_expr(
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
                    self.fun_expr(
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
                self.fun_expr(
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
                    self.fun_expr(
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
        //             self.fun_expr(
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
            member_pushdown_replacer("?value", "?old_members", "?alias_to_cube"),
            literal_member("?literal_member_value", "?value", "?relation"),
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

        fn list_concat_terminal(name: &str, member_fn: String) -> CubeRewrite {
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

    pub fn measure_rewrites(
        rules: &mut Vec<CubeRewrite>,
        pushdown_measure_rewrite: impl Fn(
            &'static str,
            String,
            String,
            Option<&'static str>,
            Option<&'static str>,
            Option<&'static str>,
            Option<&'static str>,
        ) -> CubeRewrite,
    ) {
        rules.push(pushdown_measure_rewrite(
            "member-pushdown-replacer-agg-fun",
            agg_fun_expr(
                "?fun_name",
                vec![column_expr("?column")],
                "?distinct",
                agg_fun_expr_within_group_empty_tail(),
            ),
            measure_expr("?name", "?old_alias"),
            Some("?fun_name"),
            Some("?distinct"),
            None,
            Some("?column"),
        ));
        rules.push(pushdown_measure_rewrite(
            "member-pushdown-replacer-agg-fun-alias",
            alias_expr(
                agg_fun_expr(
                    "?fun_name",
                    vec![column_expr("?column")],
                    "?distinct",
                    agg_fun_expr_within_group_empty_tail(),
                ),
                "?alias",
            ),
            measure_expr("?name", "?old_alias"),
            Some("?fun_name"),
            Some("?distinct"),
            None,
            Some("?column"),
        ));
        rules.push(pushdown_measure_rewrite(
            "member-pushdown-replacer-agg-fun-cast",
            agg_fun_expr(
                "?fun_name",
                vec![cast_expr(column_expr("?column"), "?data_type")],
                "?distinct",
                agg_fun_expr_within_group_empty_tail(),
            ),
            measure_expr("?name", "?old_alias"),
            Some("?fun_name"),
            Some("?distinct"),
            Some("?data_type"),
            Some("?column"),
        ));
        rules.push(pushdown_measure_rewrite(
            "member-pushdown-replacer-agg-fun-on-dimension",
            agg_fun_expr(
                "?fun_name",
                vec![column_expr("?column")],
                "?distinct",
                agg_fun_expr_within_group_empty_tail(),
            ),
            dimension_expr("?name", "?old_alias"),
            Some("?fun_name"),
            Some("?distinct"),
            None,
            Some("?column"),
        ));
        rules.push(pushdown_measure_rewrite(
            "member-pushdown-replacer-udaf-fun",
            udaf_expr(MEASURE_UDAF_NAME, vec![column_expr("?column")]),
            measure_expr("?name", "?old_alias"),
            None,
            None,
            None,
            Some("?column"),
        ));
        rules.push(pushdown_measure_rewrite(
            "member-pushdown-replacer-udaf-fun-on-dimension",
            udaf_expr(MEASURE_UDAF_NAME, vec![column_expr("?column")]),
            dimension_expr("?name", "?old_alias"),
            None,
            None,
            None,
            Some("?column"),
        ));
        rules.push(pushdown_measure_rewrite(
            "member-pushdown-replacer-agg-fun-default-count",
            agg_fun_expr(
                "?fun_name",
                vec![literal_expr("?any")],
                "?distinct",
                agg_fun_expr_within_group_empty_tail(),
            ),
            measure_expr("?name", "?old_alias"),
            Some("?fun_name"),
            Some("?distinct"),
            None,
            None,
        ));
        rules.push(pushdown_measure_rewrite(
            "member-pushdown-replacer-agg-fun-default-count-alias",
            alias_expr(
                agg_fun_expr(
                    "?fun_name",
                    vec![literal_expr("?any")],
                    "?distinct",
                    agg_fun_expr_within_group_empty_tail(),
                ),
                "?alias",
            ),
            measure_expr("?name", "?old_alias"),
            Some("?fun_name"),
            Some("?distinct"),
            None,
            None,
        ));
    }

    fn concat_cube_scan_members(
        &self,
        left_var: &'static str,
        right_var: &'static str,
        concat_output_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
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
        table_scan_projection_var: &'static str,
        table_scan_filters_var: &'static str,
        table_scan_fetch_var: &'static str,
        alias_to_cube_var: &'static str,
        cube_scan_members_var: &'static str,
        cube_scan_join_hints_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let source_table_name_var = var!(source_table_name_var);
        let table_name_var = var!(table_name_var);
        let table_scan_projection_var = var!(table_scan_projection_var);
        let table_scan_filters_var = var!(table_scan_filters_var);
        let table_scan_fetch_var = var!(table_scan_fetch_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let cube_scan_members_var = var!(cube_scan_members_var);
        let cube_scan_join_hints_var = var!(cube_scan_join_hints_var);
        let meta_context = self.meta_context.clone();
        move |egraph, subst| {
            for table_projection in var_iter!(
                egraph[subst[table_scan_projection_var]],
                TableScanProjection
            ) {
                if table_projection.is_some() {
                    // This rule always inserts AllMembersCube, so it does not support projection in TableScan
                    // TODO support this and enable "push projection to scan" optimizer from DF, it should help many CubeScans without members
                    return false;
                }
            }

            for table_filters in
                var_list_iter!(egraph[subst[table_scan_filters_var]], TableScanFilters)
            {
                if !table_filters.is_empty() {
                    // This rule always inserts empty filters, so it does not support filters in TableScan
                    return false;
                }
            }

            for table_fetch in var_iter!(egraph[subst[table_scan_fetch_var]], TableScanFetch) {
                if table_fetch.is_some() {
                    // This rule always inserts limit:None, so it does not support fetch in TableScan
                    // TODO support this
                    return false;
                }
            }

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

                        subst.insert(
                            cube_scan_join_hints_var,
                            egraph.add(LogicalPlanLanguage::CubeScanJoinHints(CubeScanJoinHints(
                                vec![],
                            ))),
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
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool + Clone {
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

                if let Ok(OriginalExpr::Expr(expr)) = res {
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
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool + Clone {
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
        alias_var: &'static str,
        alias_to_cube_var: &'static str,
        new_alias_to_cube_var: &'static str,
        member_pushdown_replacer_alias_to_cube_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let projection_expr_var = var!(projection_expr_var);
        let members_var = var!(members_var);
        let alias_var = var!(alias_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let new_alias_to_cube_var = var!(new_alias_to_cube_var);
        let member_pushdown_replacer_alias_to_cube_var =
            var!(member_pushdown_replacer_alias_to_cube_var);
        move |egraph, subst| {
            let members_id: Id = subst[members_var];
            if let Some(referenced_expr) = &egraph
                .index(subst[projection_expr_var])
                .data
                .referenced_expr
            {
                if egraph.index(members_id).data.member_name_to_expr.is_some() {
                    let aliases_to_cube: Vec<_> =
                        var_iter!(egraph[subst[alias_to_cube_var]], CubeScanAliasToCube)
                            .cloned()
                            .collect();
                    let projection_aliases: Vec<_> =
                        var_iter!(egraph[subst[alias_var]], ProjectionAlias)
                            .cloned()
                            .collect();

                    if !aliases_to_cube.is_empty() && !projection_aliases.is_empty() {
                        // TODO: We could, more generally, cache referenced_columns(referenced_expr), which calls expr_column_name.
                        let mut columns = HashSet::new();
                        columns.extend(referenced_columns(referenced_expr));

                        for alias_to_cube in aliases_to_cube {
                            for projection_alias in &projection_aliases {
                                let all_some = {
                                    let member_name_to_exprs: &mut MemberNamesToExpr = &mut egraph
                                        .index_mut(members_id)
                                        .data
                                        .member_name_to_expr
                                        .as_mut()
                                        .unwrap();
                                    columns.iter().all(|c| {
                                        LogicalPlanData::do_find_member_by_alias(
                                            member_name_to_exprs,
                                            c,
                                        )
                                        .is_some()
                                    })
                                };
                                if all_some {
                                    let replaced_alias_to_cube =
                                        Self::replace_alias(&alias_to_cube, &projection_alias);
                                    let new_alias_to_cube =
                                        egraph.add(LogicalPlanLanguage::CubeScanAliasToCube(
                                            CubeScanAliasToCube(replaced_alias_to_cube.clone()),
                                        ));
                                    subst.insert(new_alias_to_cube_var, new_alias_to_cube);

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
            }
            false
        }
    }

    pub fn replace_alias(
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

    fn select_distinct_dimensions(
        &self,
        alias_to_cube_var: &'static str,
        members_var: &'static str,
        filters_var: &'static str,
        left_ungrouped_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let members_var = var!(members_var);
        let filters_var = var!(filters_var);
        let left_ungrouped_var = var!(left_ungrouped_var);
        let meta_context = self.meta_context.clone();

        move |egraph, subst| {
            let empty_filters = &egraph[subst[filters_var]]
                .data
                .is_empty_list
                .unwrap_or(true);
            let ungrouped =
                var_iter!(egraph[subst[left_ungrouped_var]], CubeScanUngrouped).any(|v| *v);

            if !empty_filters && ungrouped {
                return false;
            }

            let res = match egraph
                .index(subst[members_var])
                .data
                .member_name_to_expr
                .as_ref()
            {
                Some(names_to_expr) => {
                    names_to_expr.list.iter().all(|(_, member, _)| {
                        // we should allow transform for queries with dimensions only,
                        // as it doesn't make sense for measures
                        match member {
                            Member::Dimension { .. } => true,
                            Member::VirtualField { .. } => true,
                            Member::LiteralMember { .. } => true,
                            _ => false,
                        }
                    })
                }
                None => {
                    // this might be the case of `SELECT DISTINCT *`
                    // we need to check that there are only dimensions defined in the referenced cube(s)
                    var_iter!(egraph[subst[alias_to_cube_var]], CubeScanAliasToCube)
                        .cloned()
                        .all(|alias_to_cube| {
                            alias_to_cube.iter().all(|(_, cube_name)| {
                                if let Some(cube) = meta_context.find_cube_with_name(&cube_name) {
                                    cube.measures.len() == 0 && cube.segments.len() == 0
                                } else {
                                    false
                                }
                            })
                        })
                }
            };

            res
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
        ungrouped_var: &'static str,
        filters_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let group_expr_var = var!(group_expr_var);
        let aggregate_expr_var = var!(aggregate_expr_var);
        let members_var = var!(members_var);
        let can_pushdown_join_var = var!(can_pushdown_join_var);
        let member_pushdown_replacer_alias_to_cube_var =
            var!(member_pushdown_replacer_alias_to_cube_var);
        let new_pushdown_join_var = var!(new_pushdown_join_var);
        let ungrouped_var = var!(ungrouped_var);
        let filters_var = var!(filters_var);
        let enable_ungrouped = self.enable_ungrouped;
        move |egraph, subst| {
            let Some(referenced_group_expr) =
                &egraph.index(subst[group_expr_var]).data.referenced_expr
            else {
                return false;
            };

            let Some(referenced_aggr_expr) =
                &egraph.index(subst[aggregate_expr_var]).data.referenced_expr
            else {
                return false;
            };

            if enable_ungrouped {
                // Pushing down members might eliminate dimensions, so if the query is grouped
                // and contains filters over measures, the results will be incorrect.
                for ungrouped in var_iter!(egraph[subst[ungrouped_var]], CubeScanUngrouped) {
                    if *ungrouped {
                        continue;
                    }
                    let Some(filter_operators) =
                        &egraph.index(subst[filters_var]).data.filter_operators
                    else {
                        return false;
                    };
                    let only_allowed_filters = filter_operators.iter().all(|(member, _op)| {
                        // TODO this should allow even more, like dimensions and segments
                        member == "__user"
                    });
                    if !only_allowed_filters {
                        return false;
                    }
                    if referenced_aggr_expr.len() == 0 {
                        continue;
                    }
                    return false;
                }
            }

            let mut columns = HashSet::new();
            columns.extend(referenced_columns(referenced_group_expr));
            columns.extend(referenced_columns(referenced_aggr_expr));

            let new_pushdown_join = referenced_aggr_expr.is_empty();

            let aliases_to_cube: Vec<_> =
                var_iter!(egraph[subst[alias_to_cube_var]], CubeScanAliasToCube)
                    .cloned()
                    .collect();

            for alias_to_cube in aliases_to_cube {
                let can_pushdown_joins: Vec<_> = var_iter!(
                    egraph[subst[can_pushdown_join_var]],
                    CubeScanCanPushdownJoin
                )
                .cloned()
                .collect();

                for can_pushdown_join in can_pushdown_joins {
                    if let Some(member_names_to_expr) = &mut egraph
                        .index_mut(subst[members_var])
                        .data
                        .member_name_to_expr
                    {
                        // TODO default count member is not in the columns set but it should be there

                        if columns.iter().all(|c| {
                            LogicalPlanData::do_find_member_by_alias(member_names_to_expr, c)
                                .is_some()
                        }) {
                            let member_pushdown_replacer_alias_to_cube =
                                egraph.add(LogicalPlanLanguage::MemberPushdownReplacerAliasToCube(
                                    MemberPushdownReplacerAliasToCube(
                                        Self::member_replacer_alias_to_cube(&alias_to_cube, &None),
                                    ),
                                ));

                            subst.insert(
                                member_pushdown_replacer_alias_to_cube_var,
                                member_pushdown_replacer_alias_to_cube,
                            );

                            let new_pushdown_join =
                                egraph.add(LogicalPlanLanguage::CubeScanCanPushdownJoin(
                                    CubeScanCanPushdownJoin(new_pushdown_join && can_pushdown_join),
                                ));
                            subst.insert(new_pushdown_join_var, new_pushdown_join);

                            return true;
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
        inner_skip_var: &'static str,
        inner_fetch_var: &'static str,
        new_skip_var: &'static str,
        new_fetch_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let skip_var = var!(skip_var);
        let fetch_var = var!(fetch_var);
        let inner_skip_var = var!(inner_skip_var);
        let inner_fetch_var = var!(inner_fetch_var);
        let new_skip_var = var!(new_skip_var);
        let new_fetch_var = var!(new_fetch_var);
        move |egraph, subst| {
            // This transform expects only single value in every (eclass, kind)
            // No two different values of fetch or skip should ever get unified

            let mut skip_value = None;
            for skip in var_iter!(egraph[subst[skip_var]], LimitSkip) {
                skip_value = *skip;
                break;
            }
            let mut fetch_value = None;
            for fetch in var_iter!(egraph[subst[fetch_var]], LimitFetch) {
                fetch_value = *fetch;
                break;
            }
            // TODO support this case
            if fetch_value == Some(0) {
                // Broken and unsupported case for now
                return false;
            }

            let mut inner_skip_value = None;
            for inner_skip in var_iter!(egraph[subst[inner_skip_var]], CubeScanOffset) {
                inner_skip_value = *inner_skip;
                break;
            }

            let mut inner_fetch_value = None;
            for inner_fetch in var_iter!(egraph[subst[inner_fetch_var]], CubeScanLimit) {
                inner_fetch_value = *inner_fetch;
                break;
            }

            let new_skip = match (skip_value, inner_skip_value) {
                (None, None) => None,
                (Some(skip), None) | (None, Some(skip)) => Some(skip),
                (Some(outer_skip), Some(inner_skip)) => Some(outer_skip + inner_skip),
            };
            // No need to set offset=0, it's same as no offset
            let new_skip = if new_skip != Some(0) { new_skip } else { None };
            let new_fetch = match (fetch_value, inner_fetch_value) {
                (None, None) => None,
                // Inner node have no limit, maybe just offset, result limit is same as for outer node
                (Some(outer_fetch), None) => Some(outer_fetch),
                // Outer node have no limit, but may have offset
                // First, inner offset would apply
                // Then inner node would limit rows
                // Then outer offset would apply, which would yield no more than `inner_fetch - outer_skip` rows
                (None, Some(inner_fetch)) => {
                    Some(inner_fetch.saturating_sub(skip_value.unwrap_or(0)))
                }
                // Both nodes have a limit
                // First, inner offset would apply
                // Then inner node would limit rows
                // Then outer offset would apply, which would yield no more than `in_limit - out_offset` rows
                // Then outer limit would apply, which would yield no more than minimal of two
                (Some(outer_fetch), Some(inner_fetch)) => Some(usize::min(
                    inner_fetch.saturating_sub(skip_value.unwrap_or(0)),
                    outer_fetch,
                )),
            };

            subst.insert(
                new_skip_var,
                egraph.add(LogicalPlanLanguage::CubeScanOffset(CubeScanOffset(
                    new_skip,
                ))),
            );
            subst.insert(
                new_fetch_var,
                egraph.add(LogicalPlanLanguage::CubeScanLimit(CubeScanLimit(new_fetch))),
            );

            true
        }
    }

    fn pushdown_literal_member(
        &self,
        literal_value_var: &'static str,
        literal_member_value_var: &'static str,
        alias_to_cube_var: &'static str,
        relation_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let literal_value_var = var!(literal_value_var);
        let literal_member_value_var = var!(literal_member_value_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let relation_var = var!(relation_var);
        move |egraph, subst| {
            if let Some(ConstantFolding::Scalar(value)) =
                egraph[subst[literal_value_var]].data.constant.clone()
            {
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
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let cube_var = cube_var.parse().unwrap();
        let dimension_var = dimension_var.parse().unwrap();
        let time_dimension_name_var = time_dimension_name_var.parse().unwrap();
        let granularity_var = granularity_var.parse().unwrap();
        let time_dimension_granularity_var = time_dimension_granularity_var.parse().unwrap();
        let date_range_var = date_range_var.parse().unwrap();
        let original_expr_var = var!(original_expr_var);
        let alias_var = var!(alias_var);
        let meta_context = self.meta_context.clone();
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
                            d.r#type == "time" && d.name.eq_ignore_ascii_case(&time_dimension_name)
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

    pub fn add_alias_column(
        egraph: &mut CubeEGraph,
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
    ) -> CubeRewrite {
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

    fn can_remove_cast(
        meta: &MetaContext,
        member: &Member,
        cast_types: Option<&Vec<DataType>>,
    ) -> bool {
        let cube = member.cube();
        match cast_types {
            // No cast, nothing to check
            None => true,
            // Need to check that cast is trivial
            Some(cast_types) => {
                // For now, allow trivial casts only for cube members, not literals
                let Some(cube) = &cube else {
                    return false;
                };
                let Some(name) = member.name() else {
                    return false;
                };
                let Some(cube) = meta.find_cube_with_name(cube) else {
                    return false;
                };
                // For now, allow trivial casts only for dimensions
                let Some(dimension) = cube.lookup_dimension_by_member_name(name) else {
                    return false;
                };

                cast_types
                    .iter()
                    .any(|dt| match (dimension.get_sql_type(), dt) {
                        (ColumnType::String, DataType::Utf8) => true,
                        _ => false,
                    })
            }
        }
    }

    fn transform_find_matching_old_member(
        &self,
        member_pushdown_replacer_alias_to_cube_var: &'static str,
        column_to_search: ColumnToSearch,
        cast_type_var: Option<&'static str>,
        old_members_var: &'static str,
        terminal_member: &'static str,
        filtered_member_pushdown_replacer_alias_to_cube_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let member_pushdown_replacer_alias_to_cube_var =
            var!(member_pushdown_replacer_alias_to_cube_var);
        let column_var = match &column_to_search {
            ColumnToSearch::Var(column_var) => Some(var!(column_var)),
            ColumnToSearch::DefaultCount => None,
        };
        let cast_type_var = cast_type_var.map(|cast_type_var| var!(cast_type_var));
        let old_members_var = var!(old_members_var);
        let terminal_member = var!(terminal_member);
        let filtered_member_pushdown_replacer_alias_to_cube_var =
            var!(filtered_member_pushdown_replacer_alias_to_cube_var);
        let flat_list = self.config_obj.push_down_pull_up_split();
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            let alias_to_cubes: Vec<_> = var_iter!(
                egraph[subst[member_pushdown_replacer_alias_to_cube_var]],
                MemberPushdownReplacerAliasToCube
            )
            .cloned()
            .collect();

            let cast_types = cast_type_var.map(|cast_type_var| {
                var_iter!(egraph[subst[cast_type_var]], CastExprDataType)
                    .cloned()
                    .collect::<Vec<_>>()
            });

            for alias_to_cube in alias_to_cubes {
                // Do not push down COUNT(*) if there are joined cubes
                if matches!(column_to_search, ColumnToSearch::DefaultCount) {
                    let joined_cubes = alias_to_cube
                        .iter()
                        .map(|(_, cube_name)| cube_name)
                        .collect::<HashSet<_>>();
                    if joined_cubes.len() > 1 {
                        continue;
                    }
                }

                let column_iter = match column_var {
                    Some(column_var) => var_iter!(egraph[subst[column_var]], ColumnExprColumn)
                        .cloned()
                        .collect(),
                    None => vec![Column::from_name(Self::default_count_measure_name())],
                };
                for alias_column in column_iter {
                    let alias_name = expr_column_name(&Expr::Column(alias_column), &None);

                    if let Some((member, member_alias)) = &egraph
                        .index_mut(subst[old_members_var])
                        .data
                        .find_member_by_alias(&alias_name)
                    {
                        let member = &member.1;

                        let cube_to_filter = if let Some(cube) = member.cube() {
                            Some(cube)
                        } else {
                            alias_to_cube
                                .iter()
                                .find(|((alias, _), _)| alias == member_alias)
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

                        if !Self::can_remove_cast(&meta, member, cast_types.as_ref()) {
                            continue;
                        }

                        // TODO remove unwrap
                        let old_member = member.clone().add_to_egraph(egraph, flat_list).unwrap();
                        subst.insert(terminal_member, old_member);

                        let filtered_member_pushdown_replacer_alias_to_cube =
                            egraph.add(LogicalPlanLanguage::MemberPushdownReplacerAliasToCube(
                                MemberPushdownReplacerAliasToCube(filtered_alias_to_cube),
                            ));

                        subst.insert(
                            filtered_member_pushdown_replacer_alias_to_cube_var,
                            filtered_member_pushdown_replacer_alias_to_cube,
                        );

                        return true;
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
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let member_pushdown_replacer_alias_to_cube_var =
            var!(member_pushdown_replacer_alias_to_cube_var);
        let measure_name_var = var!(measure_name_var);
        let fun_var = fun_var.map(|fun_var| var!(fun_var));
        let distinct_var = distinct_var.map(|distinct_var| var!(distinct_var));
        let original_expr_var = var!(original_expr_var);
        let cast_data_type_var = cast_data_type_var.map(|var| var!(var));
        let measure_out_var = var!(measure_out_var);
        let meta_context = self.meta_context.clone();
        let disable_strict_agg_type_match = self.config_obj.disable_strict_agg_type_match();
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
                                    meta_context.find_measure_with_name(&measure_name)
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
                                                disable_strict_agg_type_match,
                                            );
                                            return true;
                                        }
                                    }
                                }

                                if let Some(dimension) =
                                    meta_context.find_dimension_with_name(&measure_name)
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
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let distinct_var = distinct_var.map(|var| var.parse().unwrap());
        let fun_var = fun_var.map(|var| var.parse().unwrap());
        let measure_var = measure_var.map(|var| var.parse().unwrap());
        let aggr_expr_var = aggr_expr_var.parse().unwrap();
        let cast_data_type_var = cast_data_type_var.map(|var| var!(var));
        let measure_out_var = measure_out_var.parse().unwrap();
        let meta_context = self.meta_context.clone();
        let disable_strict_agg_type_match = self.config_obj.disable_strict_agg_type_match();
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
                                                cube_alias.to_string(),
                                                subst[aggr_expr_var],
                                                alias_to_cube,
                                                disable_strict_agg_type_match,
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
        egraph: &mut CubeEGraph,
        subst: &mut Subst,
        measure: &V1CubeMetaMeasure,
        call_agg_type: Option<String>,
        alias: String,
        measure_out_var: Var,
        cube_alias: String,
        expr: Id,
        alias_to_cube: Vec<((String, String), String)>,
        disable_strict_agg_type_match: bool,
    ) {
        if call_agg_type.is_some()
            && !measure.is_same_agg_type(
                call_agg_type.as_ref().unwrap(),
                disable_strict_agg_type_match,
            )
        {
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
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
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
                    let alias = alias_to_cube.first().unwrap().0 .1.to_string();
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
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
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
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
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
                    let cube_alias = alias_to_cube.first().unwrap().0 .1.to_string();
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
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
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

    fn push_down_cross_join_to_cube_scan(
        &self,
        left_alias_to_cube_var: &'static str,
        right_alias_to_cube_var: &'static str,
        joined_alias_to_cube_var: &'static str,
        left_ungrouped_var: &'static str,
        right_ungrouped_var: &'static str,
        new_ungrouped_var: &'static str,
        left_join_hints_var: &'static str,
        right_join_hints_var: &'static str,
        out_join_hints_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let left_alias_to_cube_var = var!(left_alias_to_cube_var);
        let right_alias_to_cube_var = var!(right_alias_to_cube_var);
        let joined_alias_to_cube_var = var!(joined_alias_to_cube_var);
        let left_ungrouped_var = var!(left_ungrouped_var);
        let right_ungrouped_var = var!(right_ungrouped_var);
        let new_ungrouped_var = var!(new_ungrouped_var);
        let left_join_hints_var = var!(left_join_hints_var);
        let right_join_hints_var = var!(right_join_hints_var);
        let out_join_hints_var = var!(out_join_hints_var);
        move |egraph, subst| {
            let Some(left_ungrouped) =
                singular_eclass!(egraph[subst[left_ungrouped_var]], CubeScanUngrouped).copied()
            else {
                return false;
            };
            let Some(right_ungrouped) =
                singular_eclass!(egraph[subst[right_ungrouped_var]], CubeScanUngrouped).copied()
            else {
                return false;
            };

            for left_alias_to_cube in
                var_iter!(egraph[subst[left_alias_to_cube_var]], CubeScanAliasToCube).cloned()
            {
                for right_alias_to_cube in
                    var_iter!(egraph[subst[right_alias_to_cube_var]], CubeScanAliasToCube).cloned()
                {
                    for left_join_hints in
                        var_iter!(egraph[subst[left_join_hints_var]], CubeScanJoinHints)
                    {
                        for right_join_hints in
                            var_iter!(egraph[subst[right_join_hints_var]], CubeScanJoinHints)
                        {
                            // This is CrossJoin(CubeScan,CubeScan), so there's no way to determine proper join hint
                            // It means that when there are several cubes on each side, we have to choose one
                            // When there are several cubes on the left, we should just choose last
                            // For a chained join query (cube1 CROSS JOIN cube2 CROSS JOIN ...) right CubeScan would always have single cube
                            // So this would choose last cube from last join hint on the left, and first cube on the right

                            let Some(left_cube) = left_join_hints
                                .iter()
                                .filter(|hint| !hint.is_empty())
                                .last()
                                .and_then(|hint| hint.last())
                                .or_else(|| left_alias_to_cube.first().map(|(_, cube)| cube))
                                .cloned()
                            else {
                                continue;
                            };
                            let Some(right_cube) =
                                right_alias_to_cube.first().map(|(_, cube)| cube).cloned()
                            else {
                                continue;
                            };

                            let out_join_hints = CubeScanJoinHints(
                                left_join_hints
                                    .iter()
                                    .chain(right_join_hints.iter())
                                    .cloned()
                                    .chain(iter::once(vec![left_cube, right_cube]))
                                    .collect(),
                            );

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

                            let joined_ungrouped =
                                egraph.add(LogicalPlanLanguage::CubeScanUngrouped(
                                    CubeScanUngrouped(left_ungrouped && right_ungrouped),
                                ));
                            subst.insert(new_ungrouped_var, joined_ungrouped);

                            subst.insert(
                                out_join_hints_var,
                                egraph.add(LogicalPlanLanguage::CubeScanJoinHints(out_join_hints)),
                            );

                            return true;
                        }
                    }
                }
            }

            false
        }
    }

    fn push_down_cube_join(
        &self,
        left_alias_to_cube_var: &'static str,
        right_alias_to_cube_var: &'static str,
        out_alias_to_cube_var: &'static str,
        left_members_var: &'static str,
        right_members_var: &'static str,
        left_on_var: &'static str,
        right_on_var: &'static str,
        left_join_hints_var: &'static str,
        right_join_hints_var: &'static str,
        out_join_hints_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let left_alias_to_cube_var = var!(left_alias_to_cube_var);
        let right_alias_to_cube_var = var!(right_alias_to_cube_var);
        let out_alias_to_cube_var = var!(out_alias_to_cube_var);
        let left_members_var = var!(left_members_var);
        let right_members_var = var!(right_members_var);
        let left_on_var = var!(left_on_var);
        let right_on_var = var!(right_on_var);
        let left_join_hints_var = var!(left_join_hints_var);
        let right_join_hints_var = var!(right_join_hints_var);
        let out_join_hints_var = var!(out_join_hints_var);
        move |egraph, subst| {
            let Some((left_cube, right_cube)) = is_proper_cube_join_condition(
                egraph,
                subst,
                left_members_var,
                left_on_var,
                right_members_var,
                right_on_var,
            ) else {
                return false;
            };

            for left_alias_to_cube in
                var_iter!(egraph[subst[left_alias_to_cube_var]], CubeScanAliasToCube)
            {
                for right_alias_to_cube in
                    var_iter!(egraph[subst[right_alias_to_cube_var]], CubeScanAliasToCube)
                {
                    for left_join_hints in
                        var_iter!(egraph[subst[left_join_hints_var]], CubeScanJoinHints)
                    {
                        for right_join_hints in
                            var_iter!(egraph[subst[right_join_hints_var]], CubeScanJoinHints)
                        {
                            let out_alias_to_cube = CubeScanAliasToCube(
                                left_alias_to_cube
                                    .iter()
                                    .chain(right_alias_to_cube.iter())
                                    .cloned()
                                    .collect(),
                            );

                            let out_join_hints = CubeScanJoinHints(
                                left_join_hints
                                    .iter()
                                    .chain(right_join_hints.iter())
                                    .cloned()
                                    .chain(iter::once(vec![left_cube, right_cube]))
                                    .collect(),
                            );

                            subst.insert(
                                out_alias_to_cube_var,
                                egraph.add(LogicalPlanLanguage::CubeScanAliasToCube(
                                    out_alias_to_cube,
                                )),
                            );

                            subst.insert(
                                out_join_hints_var,
                                egraph.add(LogicalPlanLanguage::CubeScanJoinHints(out_join_hints)),
                            );

                            return true;
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
    ) -> CubeRewrite {
        // TODO handle the case when limit set on non multiplied cube. It's possible to push down the limit in this case.
        transforming_rewrite(
            &format!("push-down-cross-join-to-cube-scan-{}", name),
            cross_join(
                cube_scan(
                    "?left_alias_to_cube",
                    left_members_expr,
                    "?left_filters",
                    "?left_order",
                    "CubeScanLimit:None",
                    "CubeScanOffset:None",
                    "CubeScanSplit:false",
                    "CubeScanCanPushdownJoin:true",
                    "CubeScanWrapped:false",
                    "?left_ungrouped",
                    "?left_join_hints",
                ),
                cube_scan(
                    "?right_alias_to_cube",
                    right_members_expr,
                    "?right_filters",
                    "?right_order",
                    "CubeScanLimit:None",
                    "CubeScanOffset:None",
                    "CubeScanSplit:false",
                    "CubeScanCanPushdownJoin:true",
                    "CubeScanWrapped:false",
                    "?right_ungrouped",
                    "?right_join_hints",
                ),
            ),
            cube_scan(
                "?joined_alias_to_cube",
                cube_scan_members(left_members, right_members),
                cube_scan_filters("?left_filters", "?right_filters"),
                cube_scan_order_empty_tail(),
                "CubeScanLimit:None",
                "CubeScanOffset:None",
                "CubeScanSplit:false",
                "CubeScanCanPushdownJoin:true",
                "CubeScanWrapped:false",
                "?new_ungrouped",
                "?out_join_hints",
            ),
            self.push_down_cross_join_to_cube_scan(
                "?left_alias_to_cube",
                "?right_alias_to_cube",
                "?joined_alias_to_cube",
                "?left_ungrouped",
                "?right_ungrouped",
                "?new_ungrouped",
                "?left_join_hints",
                "?right_join_hints",
                "?out_join_hints",
            ),
        )
    }
}

pub fn add_member_error(
    egraph: &mut CubeEGraph,
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

static STANDARD_GRANULARITIES_PARENTS: LazyLock<HashMap<&'static str, Vec<&'static str>>> =
    LazyLock::new(|| {
        [
            (
                "year",
                vec![
                    "year", "quarter", "month", "day", "hour", "minute", "second",
                ],
            ),
            (
                "quarter",
                vec!["quarter", "month", "day", "hour", "minute", "second"],
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
        .collect()
    });

pub fn min_granularity(granularity_a: &String, granularity_b: &String) -> Option<String> {
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

// Return None if `join_on` is not a __cubeJoinField
// Return Some(cube_name) if it is
fn is_join_on_cube_join_field(
    egraph: &mut CubeEGraph,
    subst: &Subst,
    cube_members_var: Var,
    join_on: &[Column],
) -> Option<String> {
    if join_on.len() != 1 {
        return None;
    }
    let join_on = &join_on[0];
    let ((_, join_member, _), _) = egraph[subst[cube_members_var]]
        .data
        .find_member_by_column(join_on)?;
    let Member::VirtualField { name, cube, .. } = join_member else {
        return None;
    };
    if name != "__cubeJoinField" {
        return None;
    }
    Some(cube.clone())
}

// Return None if condition is not a left.__cubeJoinField = right.__cubeJoinField
// Return Some((left_cube_name, right_cube_name)) if it is
fn is_proper_cube_join_condition(
    egraph: &mut CubeEGraph,
    subst: &Subst,
    left_cube_members_var: Var,
    left_on_var: Var,
    right_cube_members_var: Var,
    right_on_var: Var,
) -> Option<(String, String)> {
    egraph[subst[left_cube_members_var]]
        .data
        .member_name_to_expr
        .as_ref()?;

    egraph[subst[right_cube_members_var]]
        .data
        .member_name_to_expr
        .as_ref()?;

    let left_join_ons = var_iter!(egraph[subst[left_on_var]], JoinLeftOn)
        .cloned()
        .collect::<Vec<_>>();
    let right_join_ons = var_iter!(egraph[subst[right_on_var]], JoinRightOn)
        .cloned()
        .collect::<Vec<_>>();

    // For now this allows only exact left.__cubeJoinField = right.__cubeJoinField
    // TODO implement more complex conditions

    let left_cube = left_join_ons
        .iter()
        .filter_map(|left_join_on| {
            is_join_on_cube_join_field(egraph, subst, left_cube_members_var, left_join_on)
        })
        .next()?;

    let right_cube = right_join_ons
        .iter()
        .filter_map(|right_join_on| {
            is_join_on_cube_join_field(egraph, subst, right_cube_members_var, right_join_on)
        })
        .next()?;

    Some((left_cube, right_cube))
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
