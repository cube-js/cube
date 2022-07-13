use crate::{
    compile::{
        engine::provider::CubeContext,
        rewrite::{
            agg_fun_expr, aggr_aggr_expr, aggr_aggr_expr_empty_tail, aggr_group_expr,
            aggr_group_expr_empty_tail, aggregate, alias_expr,
            analysis::LogicalPlanAnalysis,
            binary_expr, cast_expr, column_expr, column_name_to_member_name,
            column_name_to_member_vec, cube_scan, cube_scan_filters_empty_tail, cube_scan_members,
            cube_scan_members_empty_tail, cube_scan_order_empty_tail, dimension_expr,
            expr_column_name, expr_column_name_with_relation, fun_expr, limit,
            list_concat_pushdown_replacer, list_concat_pushup_replacer, literal_expr,
            literal_member, measure_expr, member_pushdown_replacer, member_replacer,
            original_expr_name, projection, projection_expr, projection_expr_empty_tail,
            referenced_columns, rewrite,
            rewriter::RewriteRules,
            rules::{replacer_push_down_node, replacer_push_down_node_substitute_rules},
            segment_expr, table_scan, time_dimension_expr, transforming_chain_rewrite,
            transforming_rewrite, udaf_expr, AggregateFunctionExprDistinct,
            AggregateFunctionExprFun, AliasExprAlias, ColumnExprColumn, CubeScanAliases,
            CubeScanLimit, CubeScanTableName, DimensionName, LimitN, LiteralExprValue,
            LiteralMemberValue, LogicalPlanLanguage, MeasureName, MemberErrorError,
            MemberErrorPriority, MemberPushdownReplacerTableName,
            MemberPushdownReplacerTargetTableName, ProjectionAlias, SegmentName,
            TableScanSourceTableName, TableScanTableName, TimeDimensionDateRange,
            TimeDimensionGranularity, TimeDimensionName, WithColumnRelation,
        },
    },
    transport::{V1CubeMetaDimensionExt, V1CubeMetaMeasureExt},
    var, var_iter, var_list_iter, CubeError,
};
use cubeclient::models::V1CubeMetaMeasure;
use datafusion::{
    logical_plan::{Column, DFSchema, Expr},
    physical_plan::aggregates::AggregateFunction,
    scalar::ScalarValue,
};
use egg::{EGraph, Id, Rewrite, Subst, Var};
use itertools::Itertools;
use std::{collections::HashSet, ops::Index, sync::Arc};

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
                    "?limit",
                ),
                cube_scan(
                    "?source_table_name",
                    cube_scan_members_empty_tail(),
                    cube_scan_filters_empty_tail(),
                    cube_scan_order_empty_tail(),
                    "CubeScanLimit:None",
                    "CubeScanOffset:None",
                    "CubeScanAliases:None",
                    "?cube_table_name",
                    "CubeScanSplit:false",
                ),
                self.transform_table_scan("?source_table_name", "?table_name", "?cube_table_name"),
            ),
            rewrite(
                "member-replacer-aggr-tail",
                member_replacer(aggr_aggr_expr_empty_tail(), "?source_table_name"),
                cube_scan_members_empty_tail(),
            ),
            rewrite(
                "member-replacer-group-tail",
                member_replacer(aggr_group_expr_empty_tail(), "?source_table_name"),
                cube_scan_members_empty_tail(),
            ),
            rewrite(
                "dimension-replacer-tail-proj",
                member_replacer(projection_expr_empty_tail(), "?source_table_name"),
                cube_scan_members_empty_tail(),
            ),
            rewrite(
                "member-replacer-aggr",
                member_replacer(aggr_aggr_expr("?left", "?right"), "?source_table_name"),
                cube_scan_members(
                    member_replacer("?left", "?source_table_name"),
                    member_replacer("?right", "?source_table_name"),
                ),
            ),
            rewrite(
                "member-replacer-group",
                member_replacer(aggr_group_expr("?left", "?right"), "?source_table_name"),
                cube_scan_members(
                    member_replacer("?left", "?source_table_name"),
                    member_replacer("?right", "?source_table_name"),
                ),
            ),
            rewrite(
                "member-replacer-projection",
                member_replacer(projection_expr("?left", "?right"), "?source_table_name"),
                cube_scan_members(
                    member_replacer("?left", "?source_table_name"),
                    member_replacer("?right", "?source_table_name"),
                ),
            ),
            self.measure_rewrite(
                "simple-count",
                agg_fun_expr("?aggr_fun", vec![literal_expr("?literal")], "?distinct"),
                None,
                Some("?distinct"),
                Some("?aggr_fun"),
            ),
            self.measure_rewrite(
                "named",
                agg_fun_expr("?aggr_fun", vec![column_expr("?column")], "?distinct"),
                Some("?column"),
                Some("?distinct"),
                Some("?aggr_fun"),
            ),
            self.measure_rewrite(
                "measure-fun",
                udaf_expr("?aggr_fun", vec![column_expr("?column")]),
                Some("?column"),
                None,
                None,
            ),
            transforming_rewrite(
                "projection-columns-with-alias",
                member_replacer(
                    alias_expr(column_expr("?column"), "?alias"),
                    "?source_table_name",
                ),
                "?member".to_string(),
                self.transform_projection_member(
                    "?source_table_name",
                    "?column",
                    Some("?alias"),
                    "?member",
                ),
            ),
            transforming_rewrite(
                "default-member-error",
                member_replacer("?expr", "?source_table_name"),
                "?member_error".to_string(),
                self.transform_default_member_error("?source_table_name", "?expr", "?member_error"),
            ),
            transforming_rewrite(
                "projection-columns",
                member_replacer(column_expr("?column"), "?source_table_name"),
                "?member".to_string(),
                self.transform_projection_member("?source_table_name", "?column", None, "?member"),
            ),
            transforming_rewrite(
                "literal-member",
                member_replacer(literal_expr("?value"), "?source_table_name"),
                literal_member("?literal_member_value", literal_expr("?value")),
                self.transform_literal_member("?value", "?literal_member_value"),
            ),
            transforming_rewrite(
                "literal-member-alias",
                member_replacer(
                    alias_expr(literal_expr("?value"), "?alias"),
                    "?source_table_name",
                ),
                literal_member(
                    "?literal_member_value",
                    alias_expr(literal_expr("?value"), "?alias"),
                ),
                self.transform_literal_member("?value", "?literal_member_value"),
            ),
            transforming_chain_rewrite(
                "date-trunc",
                member_replacer("?original_expr", "?source_table_name"),
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
                    "?source_table_name",
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
                member_replacer("?original_expr", "?source_table_name"),
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
                    "?source_table_name",
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
                member_replacer("?original_expr", "?source_table_name"),
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
                    "?source_table_name",
                    "?column",
                    "?time_dimension_name",
                    "?granularity",
                    "?time_dimension_granularity",
                    "?date_range",
                    "?original_expr",
                    "?alias",
                ),
            ),
            rewrite(
                "push-down-aggregate-to-empty-scan",
                aggregate(
                    cube_scan(
                        "?source_table_name",
                        cube_scan_members_empty_tail(),
                        "?filters",
                        "?orders",
                        "?limit",
                        "?offset",
                        "?aliases",
                        "?table_name",
                        "?split",
                    ),
                    "?group_expr",
                    "?aggr_expr",
                ),
                cube_scan(
                    "?source_table_name",
                    cube_scan_members(
                        member_replacer("?group_expr", "?source_table_name"),
                        member_replacer("?aggr_expr", "?source_table_name"),
                    ),
                    "?filters",
                    "?orders",
                    "?limit",
                    "?offset",
                    "?aliases",
                    "?table_name",
                    "?split",
                ),
            ),
            transforming_rewrite(
                "push-down-aggregate",
                aggregate(
                    cube_scan(
                        "?source_table_name",
                        "?old_members",
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
                cube_scan(
                    "?source_table_name",
                    cube_scan_members(
                        member_pushdown_replacer(
                            "?group_expr",
                            list_concat_pushdown_replacer("?old_members"),
                            "?member_pushdown_replacer_table_name",
                            "?member_pushdown_replacer_target_table_name",
                        ),
                        member_pushdown_replacer(
                            "?aggr_expr",
                            list_concat_pushdown_replacer("?old_members"),
                            "?member_pushdown_replacer_table_name",
                            "?member_pushdown_replacer_target_table_name",
                        ),
                    ),
                    "?filters",
                    "?orders",
                    "?limit",
                    "?offset",
                    "?aliases",
                    "?table_name",
                    "CubeScanSplit:false",
                ),
                self.push_down_non_empty_aggregate(
                    "?table_name",
                    "?group_expr",
                    "?aggr_expr",
                    "?old_members",
                    "?member_pushdown_replacer_table_name",
                    "?member_pushdown_replacer_target_table_name",
                ),
            ),
            transforming_rewrite(
                "push-down-projection-to-empty-scan",
                projection(
                    "?expr",
                    cube_scan(
                        "?source_table_name",
                        cube_scan_members_empty_tail(),
                        "?filters",
                        "?orders",
                        "?limit",
                        "?offset",
                        "?aliases",
                        "?table_name",
                        "?split",
                    ),
                    "?alias",
                ),
                cube_scan(
                    "?source_table_name",
                    member_replacer("?expr", "?source_table_name"),
                    "?filters",
                    "?orders",
                    "?limit",
                    "?offset",
                    "?aliases",
                    "?new_table_name",
                    "?split",
                ),
                self.push_down_projection_to_empty_scan("?alias", "?table_name", "?new_table_name"),
            ),
            transforming_rewrite(
                "push-down-projection",
                projection(
                    "?expr",
                    cube_scan(
                        "?source_table_name",
                        "?members",
                        "?filters",
                        "?orders",
                        "?limit",
                        "?offset",
                        "?cube_aliases",
                        "?table_name",
                        "CubeScanSplit:false",
                    ),
                    "?alias",
                ),
                cube_scan(
                    "?source_table_name",
                    member_pushdown_replacer(
                        "?expr",
                        list_concat_pushdown_replacer("?members"),
                        "?member_pushdown_table_name",
                        "?target_table_name",
                    ),
                    "?filters",
                    "?orders",
                    "?limit",
                    "?offset",
                    "?cube_aliases",
                    "?new_table_name",
                    "CubeScanSplit:false",
                ),
                self.push_down_projection(
                    "?expr",
                    "?members",
                    "?aliases",
                    "?alias",
                    "?table_name",
                    "?new_table_name",
                    "?member_pushdown_table_name",
                    "?target_table_name",
                ),
            ),
            transforming_rewrite(
                "limit-push-down",
                limit(
                    "?limit",
                    cube_scan(
                        "?source_table_name",
                        "?members",
                        "?filters",
                        "?orders",
                        "?cube_limit",
                        "?offset",
                        "?aliases",
                        "?table_name",
                        "?split",
                    ),
                ),
                cube_scan(
                    "?source_table_name",
                    "?members",
                    "?filters",
                    "?orders",
                    "?new_limit",
                    "?offset",
                    "?aliases",
                    "?table_name",
                    "?split",
                ),
                self.push_down_limit("?limit", "?new_limit"),
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
        ];

        rules.extend(self.member_pushdown_rules());
        rules
    }
}

impl MemberRules {
    pub fn new(cube_context: Arc<CubeContext>) -> Self {
        Self { cube_context }
    }

    fn member_pushdown_rules(&self) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
        let mut rules = Vec::new();
        let member_replacer_fn = |members| {
            member_pushdown_replacer(members, "?old_members", "?table_name", "?target_table_name")
        };

        fn member_column_pushdown(
            name: &str,
            member_fn: impl Fn(&str) -> String,
        ) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
            vec![
                transforming_rewrite(
                    &format!("member-pushdown-replacer-column-{}", name),
                    member_pushdown_replacer(
                        column_expr("?column"),
                        member_fn("?old_alias"),
                        "?table_name",
                        "?target_table_name",
                    ),
                    member_fn("?output_column"),
                    MemberRules::transform_column_alias("?column", "?output_column"),
                ),
                transforming_rewrite(
                    &format!("member-pushdown-replacer-column-{}-alias", name),
                    member_pushdown_replacer(
                        alias_expr("?expr", "?alias"),
                        member_fn("?old_alias"),
                        "?table_name",
                        "?target_table_name",
                    ),
                    member_fn("?output_column"),
                    MemberRules::transform_alias("?alias", "?output_column"),
                ),
            ]
        }

        let find_matching_old_member = |name: &str, column_expr: String| {
            transforming_rewrite(
                &format!(
                    "member-pushdown-replacer-column-find-matching-old-member-{}",
                    name
                ),
                member_pushdown_replacer(
                    column_expr.clone(),
                    list_concat_pushup_replacer("?old_members"),
                    "?table_name",
                    "?target_table_name",
                ),
                member_pushdown_replacer(
                    column_expr,
                    "?terminal_member",
                    "?table_name",
                    "?target_table_name",
                ),
                self.find_matching_old_member(
                    "?column",
                    "?old_members",
                    "?table_name",
                    "?terminal_member",
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
        rules.push(transforming_chain_rewrite(
            "member-pushdown-replacer-agg-fun",
            member_pushdown_replacer(
                "?aggr_expr",
                measure_expr("?name", "?old_alias"),
                "?table_name",
                "?target_table_name",
            ),
            vec![(
                "?aggr_expr",
                agg_fun_expr("?fun_name", vec![column_expr("?column")], "?distinct"),
            )],
            "?measure".to_string(),
            self.pushdown_measure(
                "?name",
                Some("?fun_name"),
                Some("?distinct"),
                "?aggr_expr",
                "?measure",
            ),
        ));
        rules.extend(member_column_pushdown("measure", |column| {
            measure_expr("?name", column)
        }));
        rules.extend(member_column_pushdown("dimension", |column| {
            dimension_expr("?name", column)
        }));
        rules.extend(member_column_pushdown("segment", |column| {
            segment_expr("?name", column)
        }));
        rules.extend(member_column_pushdown("time-dimension", |column| {
            time_dimension_expr("?name", "?granularity", "?date_range", column)
        }));
        rules.extend(member_column_pushdown("literal", |column| {
            literal_member("?value", column)
        }));

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
        var: &'static str,
        table_name_var: &'static str,
        cube_table_name_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let var = var!(var);
        let table_name_var = var!(table_name_var);
        let cube_table_name_var = var!(cube_table_name_var);
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for name in var_iter!(egraph[subst[var]], TableScanSourceTableName) {
                if meta_context
                    .cubes
                    .iter()
                    .any(|c| c.name.eq_ignore_ascii_case(name))
                {
                    for table_name in
                        var_iter!(egraph[subst[table_name_var]], TableScanTableName).cloned()
                    {
                        subst.insert(
                            cube_table_name_var,
                            egraph.add(LogicalPlanLanguage::CubeScanTableName(CubeScanTableName(
                                table_name,
                            ))),
                        );
                        return true;
                    }
                }
            }
            false
        }
    }

    pub fn transform_original_expr_alias(
        original_expr_var: &'static str,
        alias_expr_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let original_expr_var = original_expr_var.parse().unwrap();
        let alias_expr_var = alias_expr_var.parse().unwrap();
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
                let alias = egraph.add(LogicalPlanLanguage::ColumnExprColumn(ColumnExprColumn(
                    Column::from_name(name),
                )));
                subst.insert(
                    alias_expr_var,
                    egraph.add(LogicalPlanLanguage::ColumnExpr([alias])),
                );
                return true;
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
                let outer_granularity = match Self::parse_granularity(granularity, inner_replacer) {
                    Some(granularity) => granularity,
                    None => continue,
                };
                let inner_granularity = if outer_granularity_var == inner_granularity_var {
                    outer_granularity.clone()
                } else {
                    var_iter!(egraph[subst[inner_granularity_var]], LiteralExprValue)
                        .find_map(|g| Self::parse_granularity(g, inner_replacer))
                        .unwrap_or(outer_granularity.clone())
                };

                let date_trunc_granularity =
                    match Self::get_least_granularity(&outer_granularity, &inner_granularity) {
                        Some(granularity) => granularity,
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
        table_name_var: &'static str,
        new_table_name_var: &'static str,
        member_pushdown_replacer_table_name_var: &'static str,
        member_pushdown_replacer_target_table_name_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let projection_expr_var = var!(projection_expr_var);
        let members_var = var!(members_var);
        let cube_aliases_var = var!(cube_aliases_var);
        let alias_var = var!(alias_var);
        let table_name_var = var!(table_name_var);
        let new_table_name_var = var!(new_table_name_var);
        let member_pushdown_replacer_table_name_var = var!(member_pushdown_replacer_table_name_var);
        let member_pushdown_replacer_target_table_name_var =
            var!(member_pushdown_replacer_target_table_name_var);
        move |egraph, subst| {
            if let Some(expr_to_alias) =
                &egraph.index(subst[projection_expr_var]).data.expr_to_alias
            {
                for table_name in
                    var_iter!(egraph[subst[table_name_var]], CubeScanTableName).cloned()
                {
                    let mut relation = WithColumnRelation(table_name.to_string());
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
                            column_name_to_member_name(member_name_to_expr, table_name.to_string());

                        for projection_alias in
                            var_iter!(egraph[subst[alias_var]], ProjectionAlias).cloned()
                        {
                            if column_name_to_alias
                                .iter()
                                .all(|(c, _)| column_name_to_member_name.contains_key(c))
                            {
                                let cube_aliases = egraph.add(
                                    LogicalPlanLanguage::CubeScanAliases(CubeScanAliases(Some(
                                        column_name_to_alias
                                            .iter()
                                            .map(|(_, alias)| alias.to_string())
                                            .collect::<Vec<_>>(),
                                    ))),
                                );
                                subst.insert(cube_aliases_var, cube_aliases);

                                let final_table_name =
                                    projection_alias.clone().unwrap_or(table_name.to_string());
                                let new_table_name =
                                    egraph.add(LogicalPlanLanguage::CubeScanTableName(
                                        CubeScanTableName(final_table_name.to_string()),
                                    ));
                                subst.insert(new_table_name_var, new_table_name);

                                let replacer_table_name = egraph.add(
                                    LogicalPlanLanguage::MemberPushdownReplacerTableName(
                                        MemberPushdownReplacerTableName(table_name.to_string()),
                                    ),
                                );
                                subst.insert(
                                    member_pushdown_replacer_table_name_var,
                                    replacer_table_name,
                                );

                                let target_table_name = egraph.add(
                                    LogicalPlanLanguage::MemberPushdownReplacerTargetTableName(
                                        MemberPushdownReplacerTargetTableName(
                                            final_table_name.to_string(),
                                        ),
                                    ),
                                );
                                subst.insert(
                                    member_pushdown_replacer_target_table_name_var,
                                    target_table_name,
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

    fn push_down_projection_to_empty_scan(
        &self,
        alias_var: &'static str,
        table_name_var: &'static str,
        new_table_name_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let alias_var = var!(alias_var);
        let table_name_var = var!(table_name_var);
        let new_table_name_var = var!(new_table_name_var);
        move |egraph, subst| {
            for table_name in var_iter!(egraph[subst[table_name_var]], CubeScanTableName).cloned() {
                for projection_alias in
                    var_iter!(egraph[subst[alias_var]], ProjectionAlias).cloned()
                {
                    let new_table_name = egraph.add(LogicalPlanLanguage::CubeScanTableName(
                        CubeScanTableName(projection_alias.unwrap_or(table_name)),
                    ));
                    subst.insert(new_table_name_var, new_table_name);

                    return true;
                }
            }
            false
        }
    }

    fn push_down_non_empty_aggregate(
        &self,
        table_name_var: &'static str,
        group_expr_var: &'static str,
        aggregate_expr_var: &'static str,
        members_var: &'static str,
        member_pushdown_replacer_table_name_var: &'static str,
        member_pushdown_replacer_target_table_name_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let table_name_var = var!(table_name_var);
        let group_expr_var = var!(group_expr_var);
        let aggregate_expr_var = var!(aggregate_expr_var);
        let members_var = var!(members_var);
        let member_pushdown_replacer_table_name_var = var!(member_pushdown_replacer_table_name_var);
        let member_pushdown_replacer_target_table_name_var =
            var!(member_pushdown_replacer_target_table_name_var);
        move |egraph, subst| {
            for table_name in var_iter!(egraph[subst[table_name_var]], CubeScanTableName).cloned() {
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
                            let member_column_names = column_name_to_member_name(
                                member_name_to_expr,
                                table_name.to_string(),
                            )
                            .keys()
                            .cloned()
                            .collect::<HashSet<_>>();
                            let mut columns = HashSet::new();
                            columns.extend(
                                referenced_columns(
                                    referenced_group_expr.clone(),
                                    table_name.to_string(),
                                )
                                .into_iter(),
                            );
                            columns.extend(
                                referenced_columns(
                                    referenced_aggr_expr.clone(),
                                    table_name.to_string(),
                                )
                                .into_iter(),
                            );
                            // TODO default count member is not in the columns set but it should be there
                            if columns.iter().all(|c| member_column_names.contains(c)) {
                                let table_name_id = egraph.add(
                                    LogicalPlanLanguage::MemberPushdownReplacerTableName(
                                        MemberPushdownReplacerTableName(table_name.to_string()),
                                    ),
                                );
                                subst
                                    .insert(member_pushdown_replacer_table_name_var, table_name_id);

                                let target_table_name_id = egraph.add(
                                    LogicalPlanLanguage::MemberPushdownReplacerTargetTableName(
                                        MemberPushdownReplacerTargetTableName(
                                            table_name.to_string(),
                                        ),
                                    ),
                                );
                                subst.insert(
                                    member_pushdown_replacer_target_table_name_var,
                                    target_table_name_id,
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
        limit_var: &'static str,
        new_limit_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let limit_var = var!(limit_var);
        let new_limit_var = var!(new_limit_var);
        move |egraph, subst| {
            for limit in var_iter!(egraph[subst[limit_var]], LimitN) {
                let limit = *limit;
                if limit > 0 {
                    subst.insert(
                        new_limit_var,
                        egraph.add(LogicalPlanLanguage::CubeScanLimit(CubeScanLimit(Some(
                            limit,
                        )))),
                    );
                    return true;
                }
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
            for cube_name in var_iter!(egraph[subst[cube_var]], TableScanSourceTableName).cloned() {
                if let Some(expr_name) = original_expr_name(egraph, subst[expr_var]) {
                    let member_error = add_member_error(egraph, format!("'{}' expression can't be coerced to any members of '{}' cube. It may be this type of expression is not supported. Please check logs for additional information.", expr_name, cube_name), 0);
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
            for member_name in
                var_iter!(egraph[subst[column_var]], ColumnExprColumn).map(|c| c.name.to_string())
            {
                for cube_name in var_iter!(egraph[subst[cube_var]], TableScanSourceTableName) {
                    if let Some(cube) = meta_context
                        .cubes
                        .iter()
                        .find(|c| c.name.eq_ignore_ascii_case(cube_name))
                    {
                        let column_names = if let Some(alias_var) = &alias_var {
                            var_iter!(egraph[subst[*alias_var]], AliasExprAlias)
                                .map(|s| s.to_string())
                                .collect::<Vec<_>>()
                        } else {
                            vec![member_name.to_string()]
                        };
                        for column_name in column_names {
                            let member_name = format!("{}.{}", cube_name, member_name);
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
                                    ColumnExprColumn(Column::from_name(column_name)),
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
                                    ColumnExprColumn(Column::from_name(column_name)),
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
                                    ColumnExprColumn(Column::from_name(column_name)),
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
            for time_dimension_name in var_iter!(egraph[subst[dimension_var]], ColumnExprColumn)
                .map(|c| c.name.to_string())
            {
                for cube_name in var_iter!(egraph[subst[cube_var]], TableScanSourceTableName) {
                    if let Some(cube) = meta_context
                        .cubes
                        .iter()
                        .find(|c| c.name.eq_ignore_ascii_case(cube_name))
                    {
                        let time_dimension_name = format!("{}.{}", cube_name, time_dimension_name);
                        if let Some(time_dimension) = cube.dimensions.iter().find(|d| {
                            d._type == "time" && d.name.eq_ignore_ascii_case(&time_dimension_name)
                        }) {
                            for granularity in
                                var_iter!(egraph[subst[granularity_var]], LiteralExprValue)
                            {
                                if let Some(alias) =
                                    original_expr_name(egraph, subst[original_expr_var])
                                {
                                    match granularity {
                                        ScalarValue::Utf8(Some(granularity_value)) => {
                                            let granularity_value =
                                                granularity_value.to_lowercase();
                                            subst.insert(
                                                time_dimension_name_var,
                                                egraph.add(LogicalPlanLanguage::TimeDimensionName(
                                                    TimeDimensionName(
                                                        time_dimension.name.to_string(),
                                                    ),
                                                )),
                                            );
                                            subst.insert(
                                                date_range_var,
                                                egraph.add(
                                                    LogicalPlanLanguage::TimeDimensionDateRange(
                                                        TimeDimensionDateRange(None), // TODO
                                                    ),
                                                ),
                                            );
                                            subst.insert(
                                                time_dimension_granularity_var,
                                                egraph.add(
                                                    LogicalPlanLanguage::TimeDimensionGranularity(
                                                        TimeDimensionGranularity(Some(
                                                            granularity_value,
                                                        )),
                                                    ),
                                                ),
                                            );

                                            let alias_expr = Self::add_alias_column(egraph, alias);
                                            subst.insert(alias_var, alias_expr);

                                            return true;
                                        }
                                        _ => {}
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

    fn add_alias_column(
        egraph: &mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
        alias: String,
    ) -> Id {
        let alias = egraph.add(LogicalPlanLanguage::ColumnExprColumn(ColumnExprColumn(
            Column::from_name(alias),
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
    ) -> Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis> {
        transforming_chain_rewrite(
            &format!("measure-{}", name),
            member_replacer("?aggr_expr", "?source_table_name"),
            vec![("?aggr_expr", aggr_expr)],
            "?measure".to_string(),
            self.transform_measure(
                "?source_table_name",
                measure_var,
                distinct_var,
                fun_var,
                "?aggr_expr",
                "?measure",
            ),
        )
    }

    fn find_matching_old_member(
        &self,
        column_var: &'static str,
        old_members_var: &'static str,
        table_name_var: &'static str,
        terminal_member: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let column_var = var!(column_var);
        let old_members_var = var!(old_members_var);
        let table_name_var = var!(table_name_var);
        let terminal_member = var!(terminal_member);
        move |egraph, subst| {
            for table_name in var_iter!(
                egraph[subst[table_name_var]],
                MemberPushdownReplacerTableName
            )
            .cloned()
            {
                for alias_column in var_iter!(egraph[subst[column_var]], ColumnExprColumn).cloned()
                {
                    let alias_name =
                        expr_column_name(Expr::Column(alias_column), &Some(table_name.to_string()));

                    if let Some(left_member_name_to_expr) = egraph
                        .index(subst[old_members_var])
                        .data
                        .member_name_to_expr
                        .clone()
                    {
                        let column_name_to_member = column_name_to_member_vec(
                            left_member_name_to_expr,
                            table_name.to_string(),
                        );
                        if let Some((index, _)) = column_name_to_member
                            .iter()
                            .find_position(|(member_alias, _)| member_alias == &alias_name)
                        {
                            for old_members in
                                var_list_iter!(egraph[subst[old_members_var]], CubeScanMembers)
                                    .cloned()
                            {
                                subst.insert(terminal_member, old_members[index]);
                            }

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
        measure_name_var: &'static str,
        fun_var: Option<&'static str>,
        distinct_var: Option<&'static str>,
        original_expr_var: &'static str,
        measure_out_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
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
                    if let Some(measure) = meta_context.find_measure_with_name(measure_name) {
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
                                Self::measure_output(
                                    egraph,
                                    subst,
                                    &measure,
                                    call_agg_type,
                                    alias,
                                    measure_out_var,
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

    fn transform_measure(
        &self,
        cube_var: &'static str,
        measure_var: Option<&'static str>,
        distinct_var: Option<&'static str>,
        fun_var: Option<&'static str>,
        aggr_expr_var: &'static str,
        measure_out_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let var = cube_var.parse().unwrap();
        let distinct_var = distinct_var.map(|var| var.parse().unwrap());
        let fun_var = fun_var.map(|var| var.parse().unwrap());
        let measure_var = measure_var.map(|var| var.parse().unwrap());
        let aggr_expr_var = aggr_expr_var.parse().unwrap();
        let measure_out_var = measure_out_var.parse().unwrap();
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for measure_name in measure_var
                .map(|measure_var| {
                    var_iter!(egraph[subst[measure_var]], ColumnExprColumn)
                        .map(|c| c.name.to_string())
                        .collect()
                })
                .unwrap_or(vec![Self::default_count_measure_name()])
            {
                for cube_name in var_iter!(egraph[subst[var]], TableScanSourceTableName) {
                    if let Some(cube) = meta_context
                        .cubes
                        .iter()
                        .find(|c| c.name.eq_ignore_ascii_case(cube_name))
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

                                let measure_name = format!("{}.{}", cube_name, measure_name);
                                if let Some(measure) = cube
                                    .measures
                                    .iter()
                                    .find(|m| measure_name.eq_ignore_ascii_case(&m.name))
                                {
                                    if let Some(alias) =
                                        original_expr_name(egraph, subst[aggr_expr_var])
                                    {
                                        Self::measure_output(
                                            egraph,
                                            subst,
                                            measure,
                                            call_agg_type,
                                            alias,
                                            measure_out_var,
                                        );
                                    }

                                    return true;
                                }

                                if let Some(dimension) = cube
                                    .dimensions
                                    .iter()
                                    .find(|m| measure_name.eq_ignore_ascii_case(&m.name))
                                {
                                    subst.insert(
                                        measure_out_var,
                                        add_member_error(egraph, format!(
                                            "Dimension '{}' was used with the aggregate function '{}()'. Please use a measure instead",
                                            dimension.get_real_name(),
                                            call_agg_type.unwrap_or("MEASURE".to_string()).to_uppercase(),
                                        ), 1),
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
    ) {
        if call_agg_type.is_some() && !measure.is_same_agg_type(call_agg_type.as_ref().unwrap()) {
            subst.insert(
                measure_out_var,
                add_member_error(egraph, format!(
                    "Measure aggregation type doesn't match. The aggregation type for '{}' is '{}()' but '{}()' was provided",
                    measure.get_real_name(),
                    measure.agg_type.as_ref().unwrap_or(&"unknown".to_string()).to_uppercase(),
                    call_agg_type.unwrap().to_uppercase(),
                ), 1),
            );
        } else {
            let measure_name = egraph.add(LogicalPlanLanguage::MeasureName(MeasureName(
                measure.name.to_string(),
            )));

            let alias_expr = Self::add_alias_column(egraph, alias);

            subst.insert(
                measure_out_var,
                egraph.add(LogicalPlanLanguage::Measure([measure_name, alias_expr])),
            );
        }
    }

    fn transform_column_alias(
        column_var: &'static str,
        output_column_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let column_var = var!(column_var);
        let output_column_var = var!(output_column_var);
        move |egraph, subst| {
            for column in var_iter!(egraph[subst[column_var]], ColumnExprColumn).cloned() {
                let alias_expr = Self::add_alias_column(egraph, column.name.to_string());
                subst.insert(output_column_var, alias_expr);
                return true;
            }
            false
        }
    }

    fn transform_alias(
        alias_var: &'static str,
        output_column_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let alias_var = var!(alias_var);
        let output_column_var = var!(output_column_var);
        move |egraph, subst| {
            for alias in var_iter!(egraph[subst[alias_var]], AliasExprAlias).cloned() {
                let alias_expr = Self::add_alias_column(egraph, alias.to_string());
                subst.insert(output_column_var, alias_expr);
                return true;
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

    fn get_least_granularity(
        first_granularity: &String,
        second_granularity: &String,
    ) -> Option<String> {
        if first_granularity == second_granularity {
            return Some(first_granularity.clone());
        }

        match (
            CubeTimeGranularity::from_str(first_granularity),
            CubeTimeGranularity::from_str(second_granularity),
        ) {
            (Some(first), Some(second)) => {
                Some(CubeTimeGranularity::larger_of_two(first, second).to_str())
            }
            _ => None,
        }
    }

    fn parse_granularity(granularity: &ScalarValue, to_normalize: bool) -> Option<String> {
        match granularity {
            ScalarValue::Utf8(Some(granularity)) => {
                if to_normalize {
                    match granularity.to_lowercase().as_str() {
                        "dow" | "doy" => Some("day".to_string()),
                        _ => Some(granularity.clone()),
                    }
                } else {
                    Some(granularity.clone())
                }
            }
            _ => None,
        }
    }
}

pub fn add_member_error(
    egraph: &mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
    member_error: String,
    priority: usize,
) -> Id {
    let member_error = egraph.add(LogicalPlanLanguage::MemberErrorError(MemberErrorError(
        member_error,
    )));

    let member_priority = egraph.add(LogicalPlanLanguage::MemberErrorPriority(
        MemberErrorPriority(priority),
    ));

    egraph.add(LogicalPlanLanguage::MemberError([
        member_error,
        member_priority,
    ]))
}

#[derive(Debug, PartialEq, Clone, Copy)]
enum CubeTimeGranularity {
    Second = 0,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

impl CubeTimeGranularity {
    fn from_str(str: &String) -> Option<CubeTimeGranularity> {
        match str.to_lowercase().as_str() {
            "year" => Some(CubeTimeGranularity::Year),
            "quarter" => Some(CubeTimeGranularity::Quarter),
            "month" => Some(CubeTimeGranularity::Month),
            "week" => Some(CubeTimeGranularity::Week),
            "day" => Some(CubeTimeGranularity::Day),
            "hour" => Some(CubeTimeGranularity::Hour),
            "minute" => Some(CubeTimeGranularity::Minute),
            "second" => Some(CubeTimeGranularity::Second),
            _ => None,
        }
    }

    fn to_str(&self) -> String {
        match &self {
            CubeTimeGranularity::Year => "year".to_string(),
            CubeTimeGranularity::Quarter => "quarter".to_string(),
            CubeTimeGranularity::Month => "month".to_string(),
            CubeTimeGranularity::Week => "week".to_string(),
            CubeTimeGranularity::Day => "day".to_string(),
            CubeTimeGranularity::Hour => "hour".to_string(),
            CubeTimeGranularity::Minute => "minute".to_string(),
            CubeTimeGranularity::Second => "second".to_string(),
        }
    }

    fn larger_of_two(
        first_granularity: CubeTimeGranularity,
        second_granularity: CubeTimeGranularity,
    ) -> CubeTimeGranularity {
        if first_granularity == second_granularity {
            first_granularity
        } else if first_granularity == CubeTimeGranularity::Week
            || second_granularity == CubeTimeGranularity::Week
        {
            CubeTimeGranularity::Day
        } else {
            if first_granularity as i32 > second_granularity as i32 {
                first_granularity
            } else {
                second_granularity
            }
        }
    }
}
