use crate::compile::engine::provider::CubeContext;
use crate::compile::rewrite::analysis::LogicalPlanAnalysis;
use crate::compile::rewrite::cube_scan_order_empty_tail;
use crate::compile::rewrite::rewriter::RewriteRules;
use crate::compile::rewrite::table_scan;
use crate::compile::rewrite::AggregateFunctionExprDistinct;
use crate::compile::rewrite::AggregateFunctionExprFun;
use crate::compile::rewrite::AliasExprAlias;
use crate::compile::rewrite::ColumnAliasReplacerAliases;
use crate::compile::rewrite::ColumnAliasReplacerCube;
use crate::compile::rewrite::ColumnExprColumn;
use crate::compile::rewrite::CubeScanLimit;
use crate::compile::rewrite::DimensionName;
use crate::compile::rewrite::LimitN;
use crate::compile::rewrite::LiteralExprValue;
use crate::compile::rewrite::LogicalPlanLanguage;
use crate::compile::rewrite::MeasureName;
use crate::compile::rewrite::TableScanSourceTableName;
use crate::compile::rewrite::TimeDimensionDateRange;
use crate::compile::rewrite::TimeDimensionGranularity;
use crate::compile::rewrite::TimeDimensionName;
use crate::compile::rewrite::{
    agg_fun_expr, aggr_aggr_expr, aggr_aggr_expr_empty_tail, aggr_group_expr,
    aggr_group_expr_empty_tail, aggregate, alias_expr, column_alias_replacer,
    column_name_to_member_name, cube_scan_members_empty_tail, expr_column_name,
    expr_column_name_with_relation, fun_expr, limit, member_replacer, projection, projection_expr,
    projection_expr_empty_tail, sort_expr, udaf_expr, WithColumnRelation,
};
use crate::compile::rewrite::{
    binary_expr, column_expr, cube_scan, literal_expr, rewrite, transforming_rewrite,
};
use crate::compile::rewrite::{
    cube_scan_filters_empty_tail, cube_scan_members, dimension_expr, measure_expr,
    time_dimension_expr,
};
use crate::var_iter;
use crate::{var, CubeError};
use datafusion::logical_plan::{Column, DFSchema};
use datafusion::physical_plan::aggregates::AggregateFunction;
use datafusion::scalar::ScalarValue;
use egg::{EGraph, Rewrite, Subst};
use std::ops::Index;
use std::sync::Arc;

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
                ),
                self.is_cube_table("?source_table_name"),
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
            transforming_rewrite(
                "simple-count",
                member_replacer(
                    aggr_aggr_expr(
                        agg_fun_expr("?aggr_fun", vec![literal_expr("?literal")], "?distinct"),
                        "?tail_aggr_expr",
                    ),
                    "?source_table_name",
                ),
                cube_scan_members(
                    measure_expr(
                        "?measure_name",
                        agg_fun_expr("?aggr_fun", vec![literal_expr("?literal")], "?distinct"),
                    ),
                    member_replacer("?tail_aggr_expr", "?source_table_name"),
                ),
                self.transform_measure(
                    "?source_table_name",
                    None,
                    Some("?distinct"),
                    Some("?aggr_fun"),
                ),
            ),
            transforming_rewrite(
                "named-aggr",
                member_replacer(
                    aggr_aggr_expr(
                        agg_fun_expr("?aggr_fun", vec![column_expr("?column")], "?distinct"),
                        "?tail_aggr_expr",
                    ),
                    "?source_table_name",
                ),
                cube_scan_members(
                    measure_expr(
                        "?measure_name",
                        agg_fun_expr("?aggr_fun", vec![column_expr("?column")], "?distinct"),
                    ),
                    member_replacer("?tail_aggr_expr", "?source_table_name"),
                ),
                self.transform_measure(
                    "?source_table_name",
                    Some("?column"),
                    Some("?distinct"),
                    Some("?aggr_fun"),
                ),
            ),
            transforming_rewrite(
                "measure-fun-aggr",
                member_replacer(
                    aggr_aggr_expr(
                        udaf_expr("?aggr_fun", vec![column_expr("?column")]),
                        "?tail_aggr_expr",
                    ),
                    "?source_table_name",
                ),
                cube_scan_members(
                    measure_expr(
                        "?measure_name",
                        udaf_expr("?aggr_fun", vec![column_expr("?column")]),
                    ),
                    member_replacer("?tail_aggr_expr", "?source_table_name"),
                ),
                self.transform_measure("?source_table_name", Some("?column"), None, None),
            ),
            transforming_rewrite(
                "projection-columns-with-alias",
                member_replacer(
                    projection_expr(
                        alias_expr(column_expr("?column"), "?alias"),
                        "?tail_group_expr",
                    ),
                    "?source_table_name",
                ),
                cube_scan_members(
                    "?member",
                    member_replacer("?tail_group_expr", "?source_table_name"),
                ),
                self.transform_projection_member(
                    "?source_table_name",
                    "?column",
                    Some("?alias"),
                    "?member",
                ),
            ),
            transforming_rewrite(
                "projection-columns",
                member_replacer(
                    projection_expr(column_expr("?column"), "?tail_group_expr"),
                    "?source_table_name",
                ),
                cube_scan_members(
                    "?member",
                    member_replacer("?tail_group_expr", "?source_table_name"),
                ),
                self.transform_projection_member("?source_table_name", "?column", None, "?member"),
            ),
            transforming_rewrite(
                "date-trunc",
                member_replacer(
                    aggr_group_expr(
                        fun_expr(
                            "DateTrunc",
                            vec![literal_expr("?granularity"), column_expr("?column")],
                        ),
                        "?tail_group_expr",
                    ),
                    "?source_table_name",
                ),
                cube_scan_members(
                    time_dimension_expr(
                        "?time_dimension_name",
                        "?time_dimension_granularity",
                        "?date_range",
                        fun_expr(
                            "DateTrunc",
                            vec![literal_expr("?granularity"), column_expr("?column")],
                        ),
                    ),
                    member_replacer("?tail_group_expr", "?source_table_name"),
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
                "date-trunc-projection",
                member_replacer(
                    projection_expr(
                        fun_expr(
                            "DateTrunc",
                            vec![literal_expr("?granularity"), column_expr("?column")],
                        ),
                        "?tail_group_expr",
                    ),
                    "?source_table_name",
                ),
                cube_scan_members(
                    time_dimension_expr(
                        "?time_dimension_name",
                        "?time_dimension_granularity",
                        "?date_range",
                        fun_expr(
                            "DateTrunc",
                            vec![literal_expr("?granularity"), column_expr("?column")],
                        ),
                    ),
                    member_replacer("?tail_group_expr", "?source_table_name"),
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
                self.transform_original_expr_alias("?original_expr", "?alias"),
            ),
            transforming_rewrite(
                "measure-alias",
                measure_expr("?measure", "?original_expr"),
                measure_expr("?measure", "?alias"),
                self.transform_original_expr_alias("?original_expr", "?alias"),
            ),
            transforming_rewrite(
                "dimension-alias",
                dimension_expr("?dimension", "?original_expr"),
                dimension_expr("?dimension", "?alias"),
                self.transform_original_expr_alias("?original_expr", "?alias"),
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
                ),
                self.push_down_projection(
                    "?source_table_name",
                    "?expr",
                    "?members",
                    "?aliases",
                    "?cube",
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
                    ),
                ),
                cube_scan(
                    "?source_table_name",
                    "?members",
                    "?filters",
                    "?orders",
                    "?new_limit",
                    "?offset",
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
            transforming_rewrite(
                "sort-expr-column-name",
                sort_expr("?expr", "?asc", "?nulls_first"),
                sort_expr("?alias", "?asc", "?nulls_first"),
                self.transform_original_expr_alias("?expr", "?alias"),
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

    fn is_cube_table(
        &self,
        var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let var = var.parse().unwrap();
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for name in var_iter!(egraph[subst[var]], TableScanSourceTableName) {
                if meta_context
                    .cubes
                    .iter()
                    .any(|c| c.name.eq_ignore_ascii_case(name))
                {
                    return true;
                }
            }
            false
        }
    }

    fn transform_original_expr_alias(
        &self,
        original_expr_var: &'static str,
        alias_expr_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let original_expr_var = original_expr_var.parse().unwrap();
        let alias_expr_var = alias_expr_var.parse().unwrap();
        move |egraph, subst| {
            let original_expr_id = subst[original_expr_var];
            if !egraph[original_expr_id]
                .nodes
                .iter()
                .any(|node| match node {
                    LogicalPlanLanguage::ColumnExpr(_) => true,
                    _ => false,
                })
            {
                let res = egraph[original_expr_id].data.original_expr.as_ref().ok_or(
                    CubeError::internal(format!(
                        "Original expr wasn't prepared for {:?}",
                        original_expr_id
                    )),
                );
                if let Ok(expr) = res {
                    // TODO unwrap
                    let name = expr.name(&DFSchema::empty()).unwrap();
                    let alias = egraph.add(LogicalPlanLanguage::ColumnExprColumn(
                        ColumnExprColumn(Column::from_name(name)),
                    ));
                    subst.insert(
                        alias_expr_var,
                        egraph.add(LogicalPlanLanguage::ColumnExpr([alias])),
                    );
                    return true;
                }
            }
            false
        }
    }

    fn push_down_projection(
        &self,
        table_name_var: &'static str,
        projection_expr_var: &'static str,
        members_var: &'static str,
        aliases_var: &'static str,
        cube_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let table_name_var = table_name_var.parse().unwrap();
        let projection_expr_var = projection_expr_var.parse().unwrap();
        let members_var = members_var.parse().unwrap();
        let aliases_var = aliases_var.parse().unwrap();
        let cube_var = cube_var.parse().unwrap();
        move |egraph, subst| {
            for table_name in var_iter!(egraph[subst[table_name_var]], TableScanSourceTableName) {
                if let Some(expr_to_alias) =
                    &egraph.index(subst[projection_expr_var]).data.expr_to_alias
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
                        let table_name = table_name.to_string();
                        if column_name_to_alias
                            .iter()
                            .all(|(c, _)| column_name_to_member_name.contains_key(c))
                        {
                            let aliases =
                                egraph.add(LogicalPlanLanguage::ColumnAliasReplacerAliases(
                                    ColumnAliasReplacerAliases(column_name_to_alias.clone()),
                                ));
                            subst.insert(aliases_var, aliases);

                            let cube = egraph.add(LogicalPlanLanguage::ColumnAliasReplacerCube(
                                ColumnAliasReplacerCube(Some(table_name)),
                            ));
                            subst.insert(cube_var, cube);
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
        limit_var: &'static str,
        new_limit_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let limit_var = var!(limit_var);
        let new_limit_var = var!(new_limit_var);
        move |egraph, subst| {
            for limit in var_iter!(egraph[subst[limit_var]], LimitN) {
                let limit = *limit;
                subst.insert(
                    new_limit_var,
                    egraph.add(LogicalPlanLanguage::CubeScanLimit(CubeScanLimit(Some(
                        limit,
                    )))),
                );
                return true;
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
            for cube in var_iter!(egraph[subst[cube_var]], ColumnAliasReplacerCube) {
                let column_name = expr_column_name(expr.clone(), &cube);
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
                                        let granularity_value = granularity_value.to_string();
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

    fn transform_measure(
        &self,
        cube_var: &'static str,
        measure_var: Option<&'static str>,
        distinct_var: Option<&'static str>,
        fun_var: Option<&'static str>,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let var = cube_var.parse().unwrap();
        let distinct_var = distinct_var.map(|var| var.parse().unwrap());
        let fun_var = fun_var.map(|var| var.parse().unwrap());
        let measure_var = measure_var.map(|var| var.parse().unwrap());
        let measure_name_var = "?measure_name".parse().unwrap();
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for measure_name in measure_var
                .map(|measure_var| {
                    var_iter!(egraph[subst[measure_var]], ColumnExprColumn)
                        .map(|c| c.name.to_string())
                        .collect()
                })
                .unwrap_or(vec!["count".to_string()])
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
                                let measure_name = format!("{}.{}", cube_name, measure_name);
                                if let Some(measure) = cube.measures.iter().find(|m| {
                                    measure_name.eq_ignore_ascii_case(&m.name) && {
                                        if let Some(agg_type) = &m.agg_type {
                                            match fun {
                                                Some(AggregateFunction::Count) => {
                                                    if distinct {
                                                        agg_type == "countDistinct"
                                                            || agg_type == "countDistinctApprox"
                                                    } else {
                                                        agg_type == "count"
                                                    }
                                                }
                                                Some(AggregateFunction::Sum) => agg_type == "sum",
                                                Some(AggregateFunction::Min) => agg_type == "min",
                                                Some(AggregateFunction::Max) => agg_type == "max",
                                                Some(AggregateFunction::Avg) => agg_type == "avg",
                                                Some(AggregateFunction::ApproxDistinct) => {
                                                    agg_type == "countDistinctApprox"
                                                }
                                                None => true,
                                            }
                                        } else {
                                            false
                                        }
                                    }
                                }) {
                                    subst.insert(
                                        measure_name_var,
                                        egraph.add(LogicalPlanLanguage::MeasureName(MeasureName(
                                            measure.name.to_string(),
                                        ))),
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
