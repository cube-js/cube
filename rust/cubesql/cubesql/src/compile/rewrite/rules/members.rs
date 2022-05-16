use crate::{
    compile::{
        engine::provider::CubeContext,
        rewrite::{
            agg_fun_expr, aggr_aggr_expr, aggr_aggr_expr_empty_tail, aggr_group_expr,
            aggr_group_expr_empty_tail, aggregate, alias_expr, analysis::LogicalPlanAnalysis,
            binary_expr, cast_expr, column_alias_replacer, column_expr, column_name_to_member_name,
            cube_scan, cube_scan_filters_empty_tail, cube_scan_members,
            cube_scan_members_empty_tail, cube_scan_order_empty_tail, dimension_expr,
            expr_column_name, expr_column_name_with_relation, fun_expr, limit, literal_expr,
            measure_expr, member_replacer, original_expr_name, projection, projection_expr,
            projection_expr_empty_tail, rewrite, rewriter::RewriteRules, table_scan,
            time_dimension_expr, transforming_chain_rewrite, transforming_rewrite, udaf_expr,
            AggregateFunctionExprDistinct, AggregateFunctionExprFun, AliasExprAlias,
            ColumnAliasReplacerAliases, ColumnAliasReplacerTableName, ColumnExprColumn,
            CubeScanAliases, CubeScanLimit, CubeScanTableName, DimensionName, LimitN,
            LiteralExprValue, LogicalPlanLanguage, MeasureName, MemberErrorError,
            MemberErrorPriority, ProjectionAlias, TableScanSourceTableName, TableScanTableName,
            TimeDimensionDateRange, TimeDimensionGranularity, TimeDimensionName,
            WithColumnRelation,
        },
    },
    transport::{V1CubeMetaDimensionExt, V1CubeMetaMeasureExt, V1CubeMetaSegmentExt},
    var, var_iter, CubeError,
};
use datafusion::{
    logical_plan::{Column, DFSchema},
    physical_plan::aggregates::AggregateFunction,
    scalar::ScalarValue,
};
use egg::{EGraph, Id, Rewrite, Subst};
use std::{ops::Index, sync::Arc};

pub struct MemberRules {
    cube_context: Arc<CubeContext>,
}

impl RewriteRules for MemberRules {
    fn rewrite_rules(&self) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
        vec![
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
                "projection-segment",
                member_replacer(
                    projection_expr(column_expr("?column"), "?tail_group_expr"),
                    "?source_table_name",
                ),
                member_replacer("?tail_group_expr", "?source_table_name"),
                self.transform_segment("?source_table_name", "?column"),
            ),
            // TODO this rule only for group by segment error
            transforming_chain_rewrite(
                "member-replacer-dimension",
                member_replacer(
                    aggr_group_expr("?aggr_expr", "?tail_group_expr"),
                    "?source_table_name",
                ),
                vec![("?aggr_expr", column_expr("?column"))],
                cube_scan_members(
                    "?dimension",
                    member_replacer("?tail_group_expr", "?source_table_name"),
                ),
                self.transform_dimension(
                    "?source_table_name",
                    "?column",
                    "?aggr_expr",
                    "?dimension",
                ),
            ),
            transforming_rewrite(
                "date-trunc",
                member_replacer(
                    fun_expr(
                        "DateTrunc",
                        vec![literal_expr("?granularity"), column_expr("?column")],
                    ),
                    "?source_table_name",
                ),
                time_dimension_expr(
                    "?time_dimension_name",
                    "?time_dimension_granularity",
                    "?date_range",
                    fun_expr(
                        "DateTrunc",
                        vec![literal_expr("?granularity"), column_expr("?column")],
                    ),
                ),
                self.transform_time_dimension(
                    "?source_table_name",
                    "?column",
                    "?time_dimension_name",
                    "?granularity",
                    "?time_dimension_granularity",
                    "?date_range",
                ),
            ),
            // TODO make cast split work
            transforming_rewrite(
                "date-trunc-unwrap-cast",
                member_replacer(
                    cast_expr(
                        fun_expr(
                            "DateTrunc",
                            vec![literal_expr("?granularity"), column_expr("?column")],
                        ),
                        "?date_type",
                    ),
                    "?source_table_name",
                ),
                time_dimension_expr(
                    "?time_dimension_name",
                    "?time_dimension_granularity",
                    "?date_range",
                    cast_expr(
                        fun_expr(
                            "DateTrunc",
                            vec![literal_expr("?granularity"), column_expr("?column")],
                        ),
                        "?date_type",
                    ),
                ),
                self.transform_time_dimension(
                    "?source_table_name",
                    "?column",
                    "?time_dimension_name",
                    "?granularity",
                    "?time_dimension_granularity",
                    "?date_range",
                ),
            ),
            // TODO duplicate of previous rule with aliasing. Extract aliasing as separate step?
            transforming_rewrite(
                "date-trunc-alias",
                member_replacer(
                    alias_expr(
                        fun_expr(
                            "DateTrunc",
                            vec![literal_expr("?granularity"), column_expr("?column")],
                        ),
                        "?alias",
                    ),
                    "?source_table_name",
                ),
                time_dimension_expr(
                    "?time_dimension_name",
                    "?time_dimension_granularity",
                    "?date_range",
                    alias_expr(
                        fun_expr(
                            "DateTrunc",
                            vec![literal_expr("?granularity"), column_expr("?column")],
                        ),
                        "?alias",
                    ),
                ),
                self.transform_time_dimension(
                    "?source_table_name",
                    "?column",
                    "?time_dimension_name",
                    "?granularity",
                    "?time_dimension_granularity",
                    "?date_range",
                ),
            ),
            transforming_rewrite(
                "time-dimension-alias",
                time_dimension_expr(
                    "?time_dimension_name",
                    "?time_dimension_granularity",
                    "?date_range",
                    "?original_expr",
                ),
                time_dimension_expr(
                    "?time_dimension_name",
                    "?time_dimension_granularity",
                    "?date_range",
                    "?alias",
                ),
                Self::transform_original_expr_alias("?original_expr", "?alias"),
            ),
            transforming_rewrite(
                "measure-alias",
                measure_expr("?measure", "?original_expr"),
                measure_expr("?measure", "?alias"),
                Self::transform_original_expr_alias("?original_expr", "?alias"),
            ),
            transforming_rewrite(
                "dimension-alias",
                dimension_expr("?dimension", "?original_expr"),
                dimension_expr("?dimension", "?alias"),
                Self::transform_original_expr_alias("?original_expr", "?alias"),
            ),
            rewrite(
                "push-down-aggregate",
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
            rewrite(
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
                    "?table_name",
                    "?split",
                ),
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
                        "?split",
                    ),
                    "?alias",
                ),
                cube_scan(
                    "?source_table_name",
                    column_alias_replacer("?members", "?aliases", "?cube"),
                    "?filters",
                    "?orders",
                    "?limit",
                    "?offset",
                    "?cube_aliases",
                    "?new_table_name",
                    "?split",
                ),
                self.push_down_projection(
                    "?expr",
                    "?members",
                    "?aliases",
                    "?cube",
                    "?cube_aliases",
                    "?alias",
                    "?table_name",
                    "?new_table_name",
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
            rewrite(
                "alias-replacer-split",
                column_alias_replacer(
                    cube_scan_members(
                        cube_scan_members("?members_left", "?tail_left"),
                        cube_scan_members("?members_right", "?tail_right"),
                    ),
                    "?aliases",
                    "?cube",
                ),
                cube_scan_members(
                    column_alias_replacer(
                        cube_scan_members("?members_left", "?tail_left"),
                        "?aliases",
                        "?cube",
                    ),
                    column_alias_replacer(
                        cube_scan_members("?members_right", "?tail_right"),
                        "?aliases",
                        "?cube",
                    ),
                ),
            ),
            rewrite(
                "alias-replacer-split-left-empty",
                column_alias_replacer(
                    cube_scan_members(
                        cube_scan_members_empty_tail(),
                        cube_scan_members("?members_right", "?tail_right"),
                    ),
                    "?aliases",
                    "?cube",
                ),
                cube_scan_members(
                    cube_scan_members_empty_tail(),
                    column_alias_replacer(
                        cube_scan_members("?members_right", "?tail_right"),
                        "?aliases",
                        "?cube",
                    ),
                ),
            ),
            rewrite(
                "alias-replacer-split-right-empty",
                column_alias_replacer(
                    cube_scan_members(
                        cube_scan_members("?members_left", "?tail_left"),
                        cube_scan_members_empty_tail(),
                    ),
                    "?aliases",
                    "?cube",
                ),
                cube_scan_members(
                    column_alias_replacer(
                        cube_scan_members("?members_left", "?tail_left"),
                        "?aliases",
                        "?cube",
                    ),
                    cube_scan_members_empty_tail(),
                ),
            ),
            transforming_rewrite(
                "alias-replacer-measure",
                column_alias_replacer(
                    cube_scan_members(measure_expr("?measure", "?expr"), "?tail_group_expr"),
                    "?aliases",
                    "?cube",
                ),
                cube_scan_members(
                    measure_expr("?measure", "?replaced_alias_expr"),
                    column_alias_replacer("?tail_group_expr", "?aliases", "?cube"),
                ),
                self.replace_projection_alias("?expr", "?aliases", "?cube", "?replaced_alias_expr"),
            ),
            transforming_rewrite(
                "alias-replacer-dimension",
                column_alias_replacer(
                    cube_scan_members(dimension_expr("?dimension", "?expr"), "?tail_group_expr"),
                    "?aliases",
                    "?cube",
                ),
                cube_scan_members(
                    dimension_expr("?dimension", "?replaced_alias_expr"),
                    column_alias_replacer("?tail_group_expr", "?aliases", "?cube"),
                ),
                self.replace_projection_alias("?expr", "?aliases", "?cube", "?replaced_alias_expr"),
            ),
            transforming_rewrite(
                "alias-replacer-time-dimension",
                column_alias_replacer(
                    cube_scan_members(
                        time_dimension_expr(
                            "?time_dimension_name",
                            "?time_dimension_granularity",
                            "?date_range",
                            "?expr",
                        ),
                        "?tail_group_expr",
                    ),
                    "?aliases",
                    "?cube",
                ),
                cube_scan_members(
                    time_dimension_expr(
                        "?time_dimension_name",
                        "?time_dimension_granularity",
                        "?date_range",
                        "?replaced_alias_expr",
                    ),
                    column_alias_replacer("?tail_group_expr", "?aliases", "?cube"),
                ),
                self.replace_projection_alias("?expr", "?aliases", "?cube", "?replaced_alias_expr"),
            ),
            rewrite(
                "alias-replacer-tail",
                column_alias_replacer(cube_scan_members_empty_tail(), "?aliases", "?cube"),
                cube_scan_members_empty_tail(),
            ),
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
        ]
    }
}

impl MemberRules {
    pub fn new(cube_context: Arc<CubeContext>) -> Self {
        Self { cube_context }
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

    pub fn transform_original_expr_date_trunc(
        original_expr_var: &'static str,
        granularity_var: &'static str,
        column_expr_var: &'static str,
        alias_expr_var: Option<&'static str>,
        inner_replacer: bool,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let original_expr_var = var!(original_expr_var);
        let granularity_var = var!(granularity_var);
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
            for granularity in var_iter!(egraph[subst[granularity_var]], LiteralExprValue) {
                match granularity {
                    ScalarValue::Utf8(Some(granularity)) => {
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
                            if let Some(alias_expr_var) = alias_expr_var {
                                subst.insert(
                                    alias_expr_var,
                                    egraph.add(LogicalPlanLanguage::AliasExprAlias(
                                        AliasExprAlias(if inner_replacer {
                                            suffix_alias.to_string()
                                        } else {
                                            name
                                        }),
                                    )),
                                );
                            }
                            return true;
                        }
                    }
                    _ => {}
                }
            }
            false
        }
    }

    fn push_down_projection(
        &self,
        projection_expr_var: &'static str,
        members_var: &'static str,
        aliases_var: &'static str,
        cube_var: &'static str,
        cube_aliases_var: &'static str,
        alias_var: &'static str,
        table_name_var: &'static str,
        new_table_name_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let projection_expr_var = var!(projection_expr_var);
        let members_var = var!(members_var);
        let aliases_var = var!(aliases_var);
        let cube_var = var!(cube_var);
        let cube_aliases_var = var!(cube_aliases_var);
        let alias_var = var!(alias_var);
        let table_name_var = var!(table_name_var);
        let new_table_name_var = var!(new_table_name_var);
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
                        if column_name_to_alias
                            .iter()
                            .all(|(c, _)| column_name_to_member_name.contains_key(c))
                        {
                            for projection_alias in
                                var_iter!(egraph[subst[alias_var]], ProjectionAlias).cloned()
                            {
                                let aliases =
                                    egraph.add(LogicalPlanLanguage::ColumnAliasReplacerAliases(
                                        ColumnAliasReplacerAliases(column_name_to_alias.clone()),
                                    ));
                                subst.insert(aliases_var, aliases);

                                let cube_aliases = egraph.add(
                                    LogicalPlanLanguage::CubeScanAliases(CubeScanAliases(Some(
                                        column_name_to_alias
                                            .iter()
                                            .map(|(_, alias)| alias.to_string())
                                            .collect::<Vec<_>>(),
                                    ))),
                                );
                                subst.insert(cube_aliases_var, cube_aliases);

                                let cube =
                                    egraph.add(LogicalPlanLanguage::ColumnAliasReplacerTableName(
                                        ColumnAliasReplacerTableName(Some(table_name.to_string())),
                                    ));
                                subst.insert(cube_var, cube);

                                let new_table_name =
                                    egraph.add(LogicalPlanLanguage::CubeScanTableName(
                                        CubeScanTableName(projection_alias.unwrap_or(table_name)),
                                    ));
                                subst.insert(new_table_name_var, new_table_name);

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

    fn replace_projection_alias(
        &self,
        expr_var: &'static str,
        column_name_to_alias: &'static str,
        cube_var: &'static str,
        replaced_alias_expr: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let expr_var = expr_var.parse().unwrap();
        let column_name_to_alias = column_name_to_alias.parse().unwrap();
        let cube_var = cube_var.parse().unwrap();
        let replaced_alias_expr = replaced_alias_expr.parse().unwrap();
        move |egraph, subst| {
            let expr = egraph[subst[expr_var]]
                .data
                .original_expr
                .as_ref()
                .expect(&format!(
                    "Original expr wasn't prepared for {:?}",
                    egraph[subst[expr_var]]
                ));
            for table_name in var_iter!(egraph[subst[cube_var]], ColumnAliasReplacerTableName) {
                let column_name = expr_column_name(expr.clone(), &table_name);
                for column_name_to_alias in var_iter!(
                    egraph[subst[column_name_to_alias]],
                    ColumnAliasReplacerAliases
                ) {
                    if let Some((_, new_alias)) =
                        column_name_to_alias.iter().find(|(c, _)| c == &column_name)
                    {
                        let new_alias = new_alias.clone();
                        let alias = egraph.add(LogicalPlanLanguage::ColumnExprColumn(
                            ColumnExprColumn(Column::from_name(new_alias.to_string())),
                        ));
                        subst.insert(
                            replaced_alias_expr,
                            egraph.add(LogicalPlanLanguage::ColumnExpr([alias])),
                        );
                        return true;
                    }
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
                        }
                    }
                }
            }
            false
        }
    }

    fn transform_segment(
        &self,
        cube_var: &'static str,
        column_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_var = cube_var.parse().unwrap();
        let column_var = column_var.parse().unwrap();
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
                        let member_name = format!("{}.{}", cube_name, member_name);
                        if let Some(_) = cube
                            .segments
                            .iter()
                            .find(|d| d.name.eq_ignore_ascii_case(&member_name))
                        {
                            return true;
                        }
                    }
                }
            }
            false
        }
    }

    fn transform_dimension(
        &self,
        cube_var: &'static str,
        column_var: &'static str,
        aggr_expr_var: &'static str,
        dimension_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_var = var!(cube_var);
        let column_var = var!(column_var);
        let aggr_expr_var = var!(aggr_expr_var);
        let dimension_var = var!(dimension_var);
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for dimension_name in
                var_iter!(egraph[subst[column_var]], ColumnExprColumn).map(|c| c.name.to_string())
            {
                for cube_name in var_iter!(egraph[subst[cube_var]], TableScanSourceTableName) {
                    if let Some(cube) = meta_context.find_cube_with_name(cube_name.to_string()) {
                        let dimension_name = format!("{}.{}", cube_name, dimension_name);
                        if let Some(dimension) = cube
                            .dimensions
                            .iter()
                            .find(|d| d.name.eq_ignore_ascii_case(&dimension_name))
                        {
                            let dimension_name = egraph.add(LogicalPlanLanguage::DimensionName(
                                DimensionName(dimension.name.to_string()),
                            ));

                            subst.insert(
                                dimension_var,
                                egraph.add(LogicalPlanLanguage::Dimension([
                                    dimension_name,
                                    subst[aggr_expr_var],
                                ])),
                            );

                            return true;
                        }

                        if let Some(s) = cube
                            .segments
                            .iter()
                            .find(|d| d.name.eq_ignore_ascii_case(&dimension_name))
                        {
                            subst.insert(
                                dimension_var,
                                add_member_error(
                                    egraph,
                                    format!(
                                        "Unable to use segment '{}' in GROUP BY",
                                        s.get_real_name()
                                    ),
                                    1,
                                ),
                            );

                            return true;
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
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_var = cube_var.parse().unwrap();
        let dimension_var = dimension_var.parse().unwrap();
        let time_dimension_name_var = time_dimension_name_var.parse().unwrap();
        let granularity_var = granularity_var.parse().unwrap();
        let time_dimension_granularity_var = time_dimension_granularity_var.parse().unwrap();
        let date_range_var = date_range_var.parse().unwrap();
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
                                match granularity {
                                    ScalarValue::Utf8(Some(granularity_value)) => {
                                        let granularity_value = granularity_value.to_lowercase();
                                        subst.insert(
                                            time_dimension_name_var,
                                            egraph.add(LogicalPlanLanguage::TimeDimensionName(
                                                TimeDimensionName(time_dimension.name.to_string()),
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
                                        return true;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
            false
        }
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
                                let call_agg_type = {
                                    fun.map(|fun| match fun {
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
                                    })
                                };

                                let measure_name = format!("{}.{}", cube_name, measure_name);
                                if let Some(measure) = cube
                                    .measures
                                    .iter()
                                    .find(|m| measure_name.eq_ignore_ascii_case(&m.name))
                                {
                                    if call_agg_type.is_some()
                                        && !measure
                                            .is_same_agg_type(call_agg_type.as_ref().unwrap())
                                    {
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
                                        let measure_name =
                                            egraph.add(LogicalPlanLanguage::MeasureName(
                                                MeasureName(measure.name.to_string()),
                                            ));

                                        subst.insert(
                                            measure_out_var,
                                            egraph.add(LogicalPlanLanguage::Measure([
                                                measure_name,
                                                subst[aggr_expr_var],
                                            ])),
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
                                            call_agg_type.unwrap().to_uppercase(),
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

    pub fn default_count_measure_name() -> String {
        "count".to_string()
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
