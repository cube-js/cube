use crate::{
    compile::{
        engine::provider::CubeContext,
        rewrite::{
            agg_fun_expr, aggr_aggr_expr, aggr_aggr_expr_empty_tail, aggr_group_expr,
            aggr_group_expr_empty_tail, aggregate, alias_expr,
            analysis::LogicalPlanAnalysis,
            binary_expr, cast_expr, change_user_expr, column_expr, column_name_to_member_vec,
            cross_join, cube_scan, cube_scan_filters_empty_tail, cube_scan_members,
            cube_scan_members_empty_tail, cube_scan_order_empty_tail, dimension_expr,
            expr_column_name, expr_column_name_with_relation, fun_expr, limit,
            list_concat_pushdown_replacer, list_concat_pushup_replacer, literal_expr,
            literal_member, measure_expr, member_pushdown_replacer, member_replacer,
            original_expr_name, projection, projection_expr, projection_expr_empty_tail,
            referenced_columns, rewrite,
            rewriter::RewriteRules,
            rules::{replacer_push_down_node, replacer_push_down_node_substitute_rules, utils},
            segment_expr, table_scan, time_dimension_expr, transforming_chain_rewrite,
            transforming_rewrite, udaf_expr, AggregateFunctionExprDistinct,
            AggregateFunctionExprFun, AliasExprAlias, CastExprDataType, ChangeUserCube,
            ColumnExprColumn, CubeScanAliasToCube, CubeScanAliases, CubeScanLimit, CubeScanOffset,
            DimensionName, LimitFetch, LimitSkip, LiteralExprValue, LiteralMemberValue,
            LogicalPlanLanguage, MeasureName, MemberErrorAliasToCube, MemberErrorError,
            MemberErrorPriority, MemberPushdownReplacerAliasToCube, MemberReplacerAliasToCube,
            ProjectionAlias, SegmentName, TableScanSourceTableName, TableScanTableName,
            TimeDimensionDateRange, TimeDimensionGranularity, TimeDimensionName,
            WithColumnRelation,
        },
    },
    transport::{V1CubeMetaDimensionExt, V1CubeMetaExt, V1CubeMetaMeasureExt},
    var, var_iter, var_list_iter, CubeError,
};
use cubeclient::models::V1CubeMetaMeasure;
use datafusion::{
    arrow::datatypes::DataType,
    logical_plan::{Column, DFSchema, Expr},
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
                    cube_scan_members_empty_tail(),
                    cube_scan_filters_empty_tail(),
                    cube_scan_order_empty_tail(),
                    "CubeScanLimit:None",
                    "CubeScanOffset:None",
                    "CubeScanAliases:None",
                    "CubeScanSplit:false",
                ),
                self.transform_table_scan("?source_table_name", "?table_name", "?alias_to_cube"),
            ),
            rewrite(
                "member-replacer-aggr-tail",
                member_replacer(aggr_aggr_expr_empty_tail(), "?alias_to_cube"),
                cube_scan_members_empty_tail(),
            ),
            rewrite(
                "member-replacer-group-tail",
                member_replacer(aggr_group_expr_empty_tail(), "?alias_to_cube"),
                cube_scan_members_empty_tail(),
            ),
            rewrite(
                "dimension-replacer-tail-proj",
                member_replacer(projection_expr_empty_tail(), "?alias_to_cube"),
                cube_scan_members_empty_tail(),
            ),
            rewrite(
                "member-replacer-aggr",
                member_replacer(aggr_aggr_expr("?left", "?right"), "?alias_to_cube"),
                cube_scan_members(
                    member_replacer("?left", "?alias_to_cube"),
                    member_replacer("?right", "?alias_to_cube"),
                ),
            ),
            rewrite(
                "member-replacer-group",
                member_replacer(aggr_group_expr("?left", "?right"), "?alias_to_cube"),
                cube_scan_members(
                    member_replacer("?left", "?alias_to_cube"),
                    member_replacer("?right", "?alias_to_cube"),
                ),
            ),
            rewrite(
                "member-replacer-projection",
                member_replacer(projection_expr("?left", "?right"), "?alias_to_cube"),
                cube_scan_members(
                    member_replacer("?left", "?alias_to_cube"),
                    member_replacer("?right", "?alias_to_cube"),
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
                ),
                "?member".to_string(),
                self.transform_projection_member(
                    "?alias_to_cube",
                    "?column",
                    Some("?alias"),
                    "?member",
                ),
            ),
            transforming_rewrite(
                "default-member-error",
                member_replacer("?expr", "?alias_to_cube"),
                "?member_error".to_string(),
                self.transform_default_member_error("?alias_to_cube", "?expr", "?member_error"),
            ),
            transforming_rewrite(
                "projection-columns",
                member_replacer(column_expr("?column"), "?alias_to_cube"),
                "?member".to_string(),
                self.transform_projection_member("?alias_to_cube", "?column", None, "?member"),
            ),
            transforming_rewrite(
                "literal-member",
                member_replacer(literal_expr("?value"), "?alias_to_cube"),
                literal_member("?literal_member_value", literal_expr("?value")),
                self.transform_literal_member("?value", "?literal_member_value"),
            ),
            transforming_rewrite(
                "literal-member-alias",
                member_replacer(
                    alias_expr(literal_expr("?value"), "?alias"),
                    "?alias_to_cube",
                ),
                literal_member(
                    "?literal_member_value",
                    alias_expr(literal_expr("?value"), "?alias"),
                ),
                self.transform_literal_member("?value", "?literal_member_value"),
            ),
            transforming_chain_rewrite(
                "date-trunc",
                member_replacer("?original_expr", "?alias_to_cube"),
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
                member_replacer("?original_expr", "?alias_to_cube"),
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
                member_replacer("?original_expr", "?alias_to_cube"),
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
                    ),
                    "?group_expr",
                    "?aggr_expr",
                ),
                cube_scan(
                    "?alias_to_cube",
                    cube_scan_members(
                        member_replacer("?group_expr", "?member_replacer_alias_to_cube"),
                        member_replacer("?aggr_expr", "?member_replacer_alias_to_cube"),
                    ),
                    "?filters",
                    "?orders",
                    "?limit",
                    "?offset",
                    "?aliases",
                    "?split",
                ),
                self.push_down_aggregate_to_empty_scan(
                    "?alias_to_cube",
                    "?member_replacer_alias_to_cube",
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
                        "CubeScanSplit:false",
                    ),
                    "?group_expr",
                    "?aggr_expr",
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
                    "CubeScanSplit:false",
                ),
                self.push_down_non_empty_aggregate(
                    "?alias_to_cube",
                    "?group_expr",
                    "?aggr_expr",
                    "?old_members",
                    "?member_pushdown_replacer_alias_to_cube",
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
                    ),
                    "?alias",
                ),
                cube_scan(
                    "?new_alias_to_cube",
                    member_replacer("?expr", "?member_replacer_alias_to_cube"),
                    "?filters",
                    "?orders",
                    "?limit",
                    "?offset",
                    "?aliases",
                    "?split",
                ),
                self.push_down_projection_to_empty_scan(
                    "?alias",
                    "?alias_to_cube",
                    "?new_alias_to_cube",
                    "?member_replacer_alias_to_cube",
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
                        "CubeScanSplit:false",
                    ),
                    "?alias",
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
                    "?cube_aliases",
                    "CubeScanSplit:false",
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
                ),
                self.push_down_limit("?skip", "?fetch", "?new_skip", "?new_fetch"),
            ),
            // Empty tail merges
            rewrite(
                "merge-member-empty-tails",
                cube_scan_members(
                    cube_scan_members_empty_tail(),
                    cube_scan_members_empty_tail(),
                ),
                cube_scan_members_empty_tail(),
            ),
            rewrite(
                "merge-member-empty-tails-right",
                cube_scan_members(
                    cube_scan_members_empty_tail(),
                    cube_scan_members("?left", "?right"),
                ),
                cube_scan_members("?left", "?right"),
            ),
            rewrite(
                "merge-member-empty-tails-left",
                cube_scan_members(
                    cube_scan_members("?left", "?right"),
                    cube_scan_members_empty_tail(),
                ),
                cube_scan_members("?left", "?right"),
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
                ),
                self.push_down_cross_join_to_empty_scan(
                    "?left_alias_to_cube",
                    "?right_alias_to_cube",
                    "?joined_alias_to_cube",
                ),
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
                ),
            ),
            transforming_rewrite(
                &format!("member-pushdown-replacer-column-{}-alias", name),
                member_pushdown_replacer(
                    alias_expr("?expr", "?alias"),
                    member_fn("?old_alias"),
                    "?member_pushdown_replacer_alias_to_cube",
                ),
                member_fn("?output_column"),
                self.transform_alias(
                    "?member_pushdown_replacer_alias_to_cube",
                    "?alias",
                    "?output_column",
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

        let find_matching_old_member = |name: &str, column_expr: String| {
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
                    column_expr,
                    "?terminal_member",
                    "?filtered_member_pushdown_replacer_alias_to_cube",
                ),
                self.find_matching_old_member(
                    "?member_pushdown_replacer_alias_to_cube",
                    "?column",
                    "?old_members",
                    "?terminal_member",
                    "?filtered_member_pushdown_replacer_alias_to_cube",
                ),
            )
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
        rules.push(find_matching_old_member("column", column_expr("?column")));
        rules.push(find_matching_old_member(
            "alias",
            alias_expr(column_expr("?column"), "?alias"),
        ));
        rules.push(find_matching_old_member(
            "agg-fun",
            agg_fun_expr("?fun_name", vec![column_expr("?column")], "?distinct"),
        ));
        rules.push(find_matching_old_member(
            "agg-fun-with-cast",
            // TODO need to check data_type if we can remove the cast
            agg_fun_expr(
                "?fun_name",
                vec![cast_expr(column_expr("?column"), "?data_type")],
                "?distinct",
            ),
        ));
        rules.push(transforming_chain_rewrite(
            "member-pushdown-replacer-agg-fun",
            member_pushdown_replacer(
                "?aggr_expr",
                measure_expr("?name", "?old_alias"),
                "?member_pushdown_replacer_alias_to_cube",
            ),
            vec![(
                "?aggr_expr",
                agg_fun_expr("?fun_name", vec![column_expr("?column")], "?distinct"),
            )],
            "?measure".to_string(),
            self.pushdown_measure(
                "?member_pushdown_replacer_alias_to_cube",
                "?name",
                Some("?fun_name"),
                Some("?distinct"),
                "?aggr_expr",
                "?measure",
            ),
        ));
        rules
            .extend(self.member_column_pushdown("measure", |column| measure_expr("?name", column)));
        rules.extend(
            self.member_column_pushdown("dimension", |column| dimension_expr("?name", column)),
        );
        rules
            .extend(self.member_column_pushdown("segment", |column| segment_expr("?name", column)));
        rules.extend(self.member_column_pushdown("change-user", |column| {
            change_user_expr("?change_user_cube", column)
        }));
        rules.extend(self.member_column_pushdown("time-dimension", |column| {
            time_dimension_expr("?name", "?granularity", "?date_range", column)
        }));
        rules.extend(
            self.member_column_pushdown("literal", |column| literal_member("?value", column)),
        );

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
            "time-dimension",
            time_dimension_expr("?name", "?granularity", "?date_range", "?expr"),
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
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let source_table_name_var = var!(source_table_name_var);
        let table_name_var = var!(table_name_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
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
                                CubeScanAliasToCube(vec![(table_name, cube.name.to_string())]),
                            )),
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
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
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
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
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
            if let Some(expr_to_alias) =
                &egraph.index(subst[projection_expr_var]).data.expr_to_alias
            {
                for alias_to_cube in
                    var_iter!(egraph[subst[alias_to_cube_var]], CubeScanAliasToCube).cloned()
                {
                    let mut relation = WithColumnRelation(None);
                    let column_name_to_alias = expr_to_alias
                        .clone()
                        .into_iter()
                        .map(|(e, a)| (expr_column_name_with_relation(e, &mut relation), a))
                        .collect::<Vec<_>>();
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
                            if column_name_to_alias.iter().all(|(c, _)| {
                                column_name_to_member_name
                                    .iter()
                                    .find(|(cn, _)| c == cn)
                                    .is_some()
                            }) {
                                let cube_aliases = egraph.add(
                                    LogicalPlanLanguage::CubeScanAliases(CubeScanAliases(Some(
                                        column_name_to_alias
                                            .iter()
                                            .map(|(_, alias)| alias.to_string())
                                            .collect::<Vec<_>>(),
                                    ))),
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

    fn push_down_aggregate_to_empty_scan(
        &self,
        alias_to_cube_var: &'static str,
        member_replacer_alias_to_cube_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let member_replacer_alias_to_cube_var = var!(member_replacer_alias_to_cube_var);
        move |egraph, subst| {
            for alias_to_cube in
                var_iter!(egraph[subst[alias_to_cube_var]], CubeScanAliasToCube).cloned()
            {
                let member_replacer_alias_to_cube = egraph.add(
                    LogicalPlanLanguage::MemberReplacerAliasToCube(MemberReplacerAliasToCube(
                        Self::member_replacer_alias_to_cube(&alias_to_cube, &None),
                    )),
                );
                subst.insert(
                    member_replacer_alias_to_cube_var,
                    member_replacer_alias_to_cube,
                );

                return true;
            }

            false
        }
    }

    fn push_down_projection_to_empty_scan(
        &self,
        alias_var: &'static str,
        alias_to_cube_var: &'static str,
        new_alias_to_cube_var: &'static str,
        member_replacer_alias_to_cube_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let alias_var = var!(alias_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let new_alias_to_cube_var = var!(new_alias_to_cube_var);
        let member_replacer_alias_to_cube_var = var!(member_replacer_alias_to_cube_var);
        move |egraph, subst| {
            for alias_to_cube in
                var_iter!(egraph[subst[alias_to_cube_var]], CubeScanAliasToCube).cloned()
            {
                for projection_alias in
                    var_iter!(egraph[subst[alias_var]], ProjectionAlias).cloned()
                {
                    let replaced_alias_to_cube =
                        Self::replace_alias(&alias_to_cube, &projection_alias);
                    let new_alias_to_cube = egraph.add(LogicalPlanLanguage::CubeScanAliasToCube(
                        CubeScanAliasToCube(replaced_alias_to_cube.clone()),
                    ));
                    subst.insert(new_alias_to_cube_var, new_alias_to_cube);

                    let member_replacer_alias_to_cube = egraph.add(
                        LogicalPlanLanguage::MemberReplacerAliasToCube(MemberReplacerAliasToCube(
                            Self::member_replacer_alias_to_cube(&alias_to_cube, &projection_alias),
                        )),
                    );
                    subst.insert(
                        member_replacer_alias_to_cube_var,
                        member_replacer_alias_to_cube,
                    );

                    return true;
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
        member_pushdown_replacer_alias_to_cube_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let group_expr_var = var!(group_expr_var);
        let aggregate_expr_var = var!(aggregate_expr_var);
        let members_var = var!(members_var);
        let member_pushdown_replacer_alias_to_cube_var =
            var!(member_pushdown_replacer_alias_to_cube_var);
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

                                return true;
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

    fn transform_literal_member(
        &self,
        literal_value_var: &'static str,
        literal_member_value_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let literal_value_var = var!(literal_value_var);
        let literal_member_value_var = var!(literal_member_value_var);
        move |egraph, subst| {
            for value in var_iter!(egraph[subst[literal_value_var]], LiteralExprValue).cloned() {
                let literal_member_value = egraph.add(LogicalPlanLanguage::LiteralMemberValue(
                    LiteralMemberValue(value),
                ));
                subst.insert(literal_member_value_var, literal_member_value);
                return true;
            }
            false
        }
    }

    fn transform_projection_member(
        &self,
        cube_var: &'static str,
        column_var: &'static str,
        alias_var: Option<&'static str>,
        member_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_var = cube_var.parse().unwrap();
        let column_var = column_var.parse().unwrap();
        let alias_var = alias_var.map(|alias_var| alias_var.parse().unwrap());
        let member_var = member_var.parse().unwrap();
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for column in var_iter!(egraph[subst[column_var]], ColumnExprColumn).cloned() {
                for alias_to_cube in var_iter!(egraph[subst[cube_var]], MemberReplacerAliasToCube) {
                    if let Some(((_, cube_alias), cube)) =
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
                            let member_name = format!("{}.{}", cube.name, column.name);
                            if let Some(dimension) = cube
                                .dimensions
                                .iter()
                                .find(|d| d.name.eq_ignore_ascii_case(&member_name))
                            {
                                let dimension_name =
                                    egraph.add(LogicalPlanLanguage::DimensionName(DimensionName(
                                        dimension.name.to_string(),
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
                                let measure_name = egraph.add(LogicalPlanLanguage::MeasureName(
                                    MeasureName(measure.name.to_string()),
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
                                let measure_name = egraph.add(LogicalPlanLanguage::SegmentName(
                                    SegmentName(segment.name.to_string()),
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
                                    egraph.add(LogicalPlanLanguage::Segment([
                                        measure_name,
                                        alias_expr,
                                    ])),
                                );
                                return true;
                            }

                            if column.name.eq_ignore_ascii_case(&"__user") {
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
                                    egraph.add(LogicalPlanLanguage::ChangeUser([cube, alias_expr])),
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

    fn transform_time_dimension(
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
            for column in var_iter!(egraph[subst[dimension_var]], ColumnExprColumn).cloned() {
                for alias_to_cube in var_iter!(egraph[subst[cube_var]], MemberReplacerAliasToCube) {
                    if let Some(((_, cube_alias), cube)) =
                        meta_context.find_cube_by_column_for_replacer(&alias_to_cube, &column)
                    {
                        let time_dimension_name = format!("{}.{}", cube.name, column.name);
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
            member_replacer("?aggr_expr", "?alias_to_cube"),
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
                for alias_column in var_iter!(egraph[subst[column_var]], ColumnExprColumn).cloned()
                {
                    let alias_name = expr_column_name(Expr::Column(alias_column), &None);

                    if let Some(left_member_name_to_expr) = egraph
                        .index(subst[old_members_var])
                        .data
                        .member_name_to_expr
                        .clone()
                    {
                        let column_name_to_member =
                            column_name_to_member_vec(left_member_name_to_expr);
                        if let Some((index, member)) = column_name_to_member
                            .iter()
                            .find_position(|(member_alias, _)| member_alias == &alias_name)
                        {
                            let filtered_alias_to_cube = alias_to_cube
                                .into_iter()
                                .filter(|(_, cube)| cube == member.1.split(".").next().unwrap())
                                .collect();
                            // Members are represented in pairs: fully qualified name and unqualified name
                            let index = index / 2;
                            for old_members in
                                var_list_iter!(egraph[subst[old_members_var]], CubeScanMembers)
                                    .cloned()
                            {
                                subst.insert(terminal_member, old_members[index]);
                            }

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
        measure_out_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let member_pushdown_replacer_alias_to_cube_var =
            var!(member_pushdown_replacer_alias_to_cube_var);
        let measure_name_var = var!(measure_name_var);
        let fun_var = fun_var.map(|fun_var| var!(fun_var));
        let distinct_var = distinct_var.map(|distinct_var| var!(distinct_var));
        let original_expr_var = var!(original_expr_var);
        let measure_out_var = var!(measure_out_var);
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            if let Some(alias) = original_expr_name(egraph, subst[original_expr_var]) {
                for measure_name in var_iter!(egraph[subst[measure_name_var]], MeasureName).cloned()
                {
                    if let Some(measure) =
                        meta_context.find_measure_with_name(measure_name.to_string())
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
                                for alias_to_cube in var_iter!(
                                    egraph[subst[member_pushdown_replacer_alias_to_cube_var]],
                                    MemberPushdownReplacerAliasToCube
                                )
                                .cloned()
                                {
                                    let measure_cube_name = measure_name.split(".").next().unwrap();
                                    if let Some(((_, cube_alias), _)) = alias_to_cube
                                        .iter()
                                        .find(|(_, cube)| cube == measure_cube_name)
                                    {
                                        let call_agg_type = Self::get_agg_type(fun, distinct);
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
                    if let Some(((_, cube_alias), cube)) =
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
            subst.insert(
                measure_out_var,
                add_member_error(egraph, format!(
                    "Measure aggregation type doesn't match. The aggregation type for '{}' is '{}()' but '{}()' was provided",
                    measure.get_real_name(),
                    measure.agg_type.as_ref().unwrap_or(&"unknown".to_string()).to_uppercase(),
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
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let member_pushdown_replacer_alias_to_cube_var =
            var!(member_pushdown_replacer_alias_to_cube_var);
        let column_var = var!(column_var);
        let output_column_var = var!(output_column_var);
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
                    let alias_expr =
                        Self::add_alias_column(egraph, column.name.to_string(), Some(alias));
                    subst.insert(output_column_var, alias_expr);
                    return true;
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
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let member_pushdown_replacer_alias_to_cube_var =
            var!(member_pushdown_replacer_alias_to_cube_var);
        let alias_var = var!(alias_var);
        let output_column_var = var!(output_column_var);
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
                        Self::add_alias_column(egraph, alias.to_string(), Some(cube_alias));
                    subst.insert(output_column_var, alias_expr);
                    return true;
                }
            }

            false
        }
    }

    fn get_agg_type(fun: Option<&AggregateFunction>, distinct: bool) -> Option<String> {
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
