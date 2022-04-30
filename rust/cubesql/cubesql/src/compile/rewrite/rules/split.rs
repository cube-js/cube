use crate::compile::engine::provider::CubeContext;
use crate::compile::rewrite::analysis::{LogicalPlanAnalysis, SplitType};
use crate::compile::rewrite::rewriter::RewriteRules;
use crate::compile::rewrite::rules::members::MemberRules;
use crate::compile::rewrite::OuterAggregateSplitReplacerCube;
use crate::compile::rewrite::OuterProjectionSplitReplacerCube;
use crate::compile::rewrite::TableScanSourceTableName;
use crate::compile::rewrite::{agg_fun_expr, alias_expr, transforming_chain_rewrite};
use crate::compile::rewrite::{
    aggr_aggr_expr, aggr_aggr_expr_empty_tail, aggr_group_expr, aggr_group_expr_empty_tail,
    aggregate, fun_expr, projection, projection_expr,
};
use crate::compile::rewrite::{cast_expr, projection_expr_empty_tail};
use crate::compile::rewrite::{column_expr, cube_scan, literal_expr, rewrite};
use crate::compile::rewrite::{inner_aggregate_split_replacer, outer_projection_split_replacer};
use crate::compile::rewrite::{literal_string, ColumnExprColumn};
use crate::compile::rewrite::{original_expr_name, InnerAggregateSplitReplacerCube};
use crate::compile::rewrite::{outer_aggregate_split_replacer, LogicalPlanLanguage};
use crate::compile::rewrite::{transforming_rewrite, AliasExprAlias};
use crate::compile::rewrite::{transforming_rewrite_with_root, AggregateFunctionExprFun};
use crate::{var, var_iter};
use datafusion::logical_plan::Column;
use datafusion::physical_plan::aggregates::AggregateFunction;
use egg::{EGraph, Id, Rewrite, Subst};
use std::sync::Arc;

pub struct SplitRules {
    cube_context: Arc<CubeContext>,
}

impl RewriteRules for SplitRules {
    fn rewrite_rules(&self) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
        vec![
            transforming_rewrite_with_root(
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
                        ),
                        inner_aggregate_split_replacer("?group_expr", "?inner_aggregate_cube"),
                        inner_aggregate_split_replacer("?aggr_expr", "?inner_aggregate_cube"),
                    ),
                    "ProjectionAlias:None",
                ),
                self.split_projection_aggregate(
                    "?source_table_name",
                    "?inner_aggregate_cube",
                    "?outer_projection_cube",
                ),
            ),
            transforming_rewrite_with_root(
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
            // Inner aggregate replacers
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
                "split-push-down-group-outer-aggr-replacer-tail",
                outer_aggregate_split_replacer(aggr_group_expr_empty_tail(), "?cube"),
                aggr_group_expr_empty_tail(),
            ),
            rewrite(
                "split-push-down-aggr-outer-aggr-replacer-tail",
                outer_aggregate_split_replacer(aggr_aggr_expr_empty_tail(), "?cube"),
                aggr_aggr_expr_empty_tail(),
            ),
            // Members
            // Column rules
            transforming_chain_rewrite(
                "split-push-down-column-inner-replacer",
                inner_aggregate_split_replacer("?expr", "?cube"),
                vec![("?expr", column_expr("?column"))],
                alias_expr(column_expr("?column"), "?alias"),
                self.transform_inner_column("?expr", "?alias"),
            ),
            transforming_chain_rewrite(
                "split-push-down-column-outer-replacer",
                outer_projection_split_replacer("?expr", "?cube"),
                vec![("?expr", column_expr("?column"))],
                "?alias".to_string(),
                MemberRules::transform_original_expr_alias("?expr", "?alias"),
            ),
            transforming_chain_rewrite(
                "split-push-down-column-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?cube"),
                vec![("?expr", column_expr("?column"))],
                "?alias".to_string(),
                MemberRules::transform_original_expr_alias("?expr", "?alias"),
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
                        vec![literal_expr("?granularity"), column_expr("?column")],
                    ),
                    "?alias",
                ),
                MemberRules::transform_original_expr_date_trunc(
                    "?expr",
                    "?granularity",
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
            // Aggregate function
            transforming_rewrite(
                "split-push-down-aggr-fun-inner-replacer",
                inner_aggregate_split_replacer(
                    agg_fun_expr("?fun", vec!["?arg"], "?distinct"),
                    "?cube",
                ),
                agg_fun_expr("?fun", vec!["?arg"], "?distinct"),
                self.transform_inner_measure("?cube", "?arg"),
            ),
            transforming_chain_rewrite(
                "split-push-down-aggr-fun-outer-replacer",
                outer_projection_split_replacer("?expr", "?cube"),
                vec![("?expr", agg_fun_expr("?fun", vec!["?arg"], "?distinct"))],
                "?alias".to_string(),
                MemberRules::transform_original_expr_alias("?expr", "?alias"),
            ),
            transforming_chain_rewrite(
                "split-push-down-aggr-fun-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr", "?cube"),
                vec![("?expr", agg_fun_expr("?fun", vec!["?arg"], "?distinct"))],
                alias_expr(
                    agg_fun_expr("?output_fun", vec!["?alias".to_string()], "?distinct"),
                    "?outer_alias",
                ),
                self.transform_outer_aggr_fun(
                    "?cube",
                    "?expr",
                    "?fun",
                    "?arg",
                    "?alias",
                    "?outer_alias",
                    "?output_fun",
                ),
            ),
            // TODO It replaces aggregate function with scalar one. This breaks Aggregate consistency.
            // Works because push down aggregate rule doesn't care about if it's in group by or aggregate.
            // Member types detected by column names.
            transforming_rewrite(
                "split-push-down-aggr-min-max-date-trunc-fun-inner-replacer",
                inner_aggregate_split_replacer(
                    agg_fun_expr("?fun", vec!["?arg"], "?distinct"),
                    "?cube",
                ),
                alias_expr(
                    fun_expr(
                        "DateTrunc",
                        vec![literal_string("month"), "?arg".to_string()],
                    ),
                    "?alias",
                ),
                self.transform_min_max_dimension("?cube", "?fun", "?arg", "?alias", true),
            ),
            transforming_rewrite(
                "split-push-down-aggr-min-max-dimension-fun-inner-replacer",
                inner_aggregate_split_replacer(
                    agg_fun_expr("?fun", vec!["?arg"], "?distinct"),
                    "?cube",
                ),
                alias_expr("?arg", "?alias"),
                self.transform_min_max_dimension("?cube", "?fun", "?arg", "?alias", false),
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
        alias_var: &'static str,
        is_time_dimension: bool,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_expr_var = var!(cube_expr_var);
        let fun_expr_var = var!(fun_expr_var);
        let arg_expr_var = var!(arg_expr_var);
        let alias_var = var!(alias_var);
        let meta = self.cube_context.meta.clone();
        move |egraph, subst| {
            for cube in var_iter!(
                egraph[subst[cube_expr_var]],
                InnerAggregateSplitReplacerCube
            )
            .cloned()
            {
                if let Some(cube) = meta.find_cube_with_name(cube) {
                    if let Some(can_split) = &egraph[subst[arg_expr_var]].data.can_split {
                        for fun in var_iter!(egraph[subst[fun_expr_var]], AggregateFunctionExprFun)
                            .cloned()
                        {
                            if fun == AggregateFunction::Min || fun == AggregateFunction::Max {
                                let alternatives_with_cube =
                                    can_split.narrow_down_alternatives_with_meta(&cube);

                                if let Some(true) = alternatives_with_cube.map(|a| {
                                    if is_time_dimension {
                                        a.has_time_dimension()
                                    } else {
                                        !a.has_time_dimension() && a.has_dimension()
                                    }
                                }) {
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
            false
        }
    }

    fn transform_inner_measure(
        &self,
        cube_expr_var: &'static str,
        arg_expr_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_expr_var = var!(cube_expr_var);
        let arg_expr_var = var!(arg_expr_var);
        let meta = self.cube_context.meta.clone();
        move |egraph, subst| {
            for cube in var_iter!(
                egraph[subst[cube_expr_var]],
                InnerAggregateSplitReplacerCube
            )
            .cloned()
            {
                if let Some(cube) = meta.find_cube_with_name(cube) {
                    if let Some(can_split) = &egraph[subst[arg_expr_var]].data.can_split {
                        let alternatives_with_cube =
                            can_split.narrow_down_alternatives_with_meta(&cube);

                        if let Some(true) = alternatives_with_cube.map(|a| a.has_measure()) {
                            return true;
                        }
                    }
                }
            }
            false
        }
    }

    fn transform_inner_column(
        &self,
        expr_var: &'static str,
        alias_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let expr_var = var!(expr_var);
        let alias_var = var!(alias_var);
        move |egraph, subst| {
            if let Some(alias) = original_expr_name(egraph, subst[expr_var]) {
                subst.insert(
                    alias_var,
                    egraph.add(LogicalPlanLanguage::AliasExprAlias(AliasExprAlias(alias))),
                );

                return true;
            }
            false
        }
    }

    fn split_projection_aggregate(
        &self,
        cube_expr_var: &'static str,
        inner_aggregate_cube_expr_var: &'static str,
        outer_projection_cube_expr_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, Id, &mut Subst) -> bool
    {
        let cube_expr_var = var!(cube_expr_var);
        let inner_aggregate_cube_expr_var = var!(inner_aggregate_cube_expr_var);
        let outer_projection_cube_expr_var = var!(outer_projection_cube_expr_var);
        move |egraph, root, subst| {
            for cube in var_iter!(egraph[subst[cube_expr_var]], TableScanSourceTableName).cloned() {
                if let Some(SplitType::Projection) = &egraph[root]
                    .data
                    .can_split
                    .as_ref()
                    .and_then(|c| c.min_split_type())
                {
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

    fn split_aggregate_aggregate(
        &self,
        cube_expr_var: &'static str,
        inner_aggregate_cube_expr_var: &'static str,
        outer_aggregate_cube_expr_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, Id, &mut Subst) -> bool
    {
        let cube_expr_var = var!(cube_expr_var);
        let inner_aggregate_cube_expr_var = var!(inner_aggregate_cube_expr_var);
        let outer_aggregate_cube_expr_var = var!(outer_aggregate_cube_expr_var);
        move |egraph, root, subst| {
            for cube in var_iter!(egraph[subst[cube_expr_var]], TableScanSourceTableName).cloned() {
                if let Some(SplitType::Aggregation) = &egraph[root]
                    .data
                    .can_split
                    .as_ref()
                    .and_then(|c| c.min_split_type())
                {
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

    pub fn transform_outer_aggr_fun(
        &self,
        cube_var: &'static str,
        original_expr_var: &'static str,
        fun_expr_var: &'static str,
        arg_var: &'static str,
        alias_expr_var: &'static str,
        outer_alias_expr_var: &'static str,
        output_fun_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_var = var!(cube_var);
        let original_expr_var = var!(original_expr_var);
        let fun_expr_var = var!(fun_expr_var);
        let arg_var = var!(arg_var);
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
                    if let Some(name) = original_expr_name(egraph, subst[original_expr_var]) {
                        if let Some(cube) = meta.find_cube_with_name(cube) {
                            if let Some(can_split) = &egraph[subst[arg_var]].data.can_split {
                                let alternatives_with_cube =
                                    can_split.narrow_down_alternatives_with_meta(&cube);

                                let inner_and_outer_alias = if let Some(true) =
                                    alternatives_with_cube.as_ref().map(|a| a.has_measure())
                                {
                                    Some((name.to_string(), name.to_string()))
                                } else if let Some(true) =
                                    alternatives_with_cube.as_ref().map(|a| {
                                        a.has_time_dimension()
                                            || !a.has_time_dimension() && a.has_dimension()
                                    })
                                {
                                    original_expr_name(egraph, subst[arg_var])
                                        .map(|inner| (inner, name.to_string()))
                                } else {
                                    None
                                };

                                if let Some((inner_alias, outer_alias)) = inner_and_outer_alias {
                                    let alias = egraph.add(LogicalPlanLanguage::ColumnExprColumn(
                                        ColumnExprColumn(Column::from_name(
                                            inner_alias.to_string(),
                                        )),
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
