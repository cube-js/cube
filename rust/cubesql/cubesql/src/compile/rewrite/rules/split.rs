use crate::compile::engine::provider::CubeContext;
use crate::compile::rewrite::analysis::{LogicalPlanAnalysis, SplitType};
use crate::compile::rewrite::rewriter::RewriteRules;
use crate::compile::rewrite::rules::members::MemberRules;
use crate::compile::rewrite::AggregateFunctionExprFun;
use crate::compile::rewrite::AliasExprAlias;
use crate::compile::rewrite::ColumnExprColumn;
use crate::compile::rewrite::{agg_fun_expr, alias_expr, transforming_chain_rewrite};
use crate::compile::rewrite::{
    aggr_aggr_expr, aggr_aggr_expr_empty_tail, aggr_group_expr, aggr_group_expr_empty_tail,
    aggregate, fun_expr, projection, projection_expr,
};
use crate::compile::rewrite::{cast_expr, projection_expr_empty_tail};
use crate::compile::rewrite::{
    column_expr, cube_scan, literal_expr, rewrite, transforming_rewrite,
};
use crate::compile::rewrite::{inner_aggregate_split_replacer, outer_projection_split_replacer};
use crate::compile::rewrite::{outer_aggregate_split_replacer, LogicalPlanLanguage};
use crate::{var, var_iter, CubeError};
use datafusion::logical_plan::{Column, DFSchema};
use datafusion::physical_plan::aggregates::AggregateFunction;
use egg::{EGraph, Rewrite, Subst};
use std::sync::Arc;

pub struct SplitRules {
    _cube_context: Arc<CubeContext>,
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
                    ),
                    "?group_expr",
                    "?aggr_expr",
                ),
                projection(
                    projection_expr(
                        outer_projection_split_replacer("?group_expr"),
                        outer_projection_split_replacer("?aggr_expr"),
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
                        inner_aggregate_split_replacer("?group_expr"),
                        inner_aggregate_split_replacer("?aggr_expr"),
                    ),
                    "ProjectionAlias:None",
                ),
                self.split_projection_aggregate("?group_expr", "?aggr_expr"),
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
                        inner_aggregate_split_replacer("?group_expr"),
                        inner_aggregate_split_replacer("?aggr_expr"),
                    ),
                    outer_aggregate_split_replacer("?group_expr"),
                    outer_aggregate_split_replacer("?aggr_expr"),
                ),
                self.split_aggregate_aggregate("?group_expr", "?aggr_expr"),
            ),
            // Inner aggregate replacers
            rewrite(
                "split-push-down-group-inner-replacer",
                inner_aggregate_split_replacer(aggr_group_expr("?left", "?right")),
                aggr_group_expr(
                    inner_aggregate_split_replacer("?left"),
                    inner_aggregate_split_replacer("?right"),
                ),
            ),
            rewrite(
                "split-push-down-aggr-inner-replacer",
                inner_aggregate_split_replacer(aggr_aggr_expr("?left", "?right")),
                aggr_aggr_expr(
                    inner_aggregate_split_replacer("?left"),
                    inner_aggregate_split_replacer("?right"),
                ),
            ),
            rewrite(
                "split-push-down-group-inner-replacer-tail",
                inner_aggregate_split_replacer(aggr_group_expr_empty_tail()),
                aggr_group_expr_empty_tail(),
            ),
            rewrite(
                "split-push-down-aggr-inner-replacer-tail",
                inner_aggregate_split_replacer(aggr_aggr_expr_empty_tail()),
                aggr_aggr_expr_empty_tail(),
            ),
            rewrite(
                "split-push-down-column-inner-replacer",
                inner_aggregate_split_replacer(column_expr("?column")),
                column_expr("?column"),
            ),
            rewrite(
                "split-push-down-date-trunc-inner-replacer",
                inner_aggregate_split_replacer(fun_expr(
                    "DateTrunc",
                    vec![literal_expr("?granularity"), column_expr("?column")],
                )),
                fun_expr(
                    "DateTrunc",
                    vec![literal_expr("?granularity"), column_expr("?column")],
                ),
            ),
            transforming_chain_rewrite(
                "split-push-down-date-part-inner-replacer",
                inner_aggregate_split_replacer(fun_expr(
                    "DatePart",
                    vec![literal_expr("?granularity"), "?expr".to_string()],
                )),
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
            rewrite(
                "split-push-down-aggr-fun-inner-replacer",
                inner_aggregate_split_replacer(agg_fun_expr("?fun", vec!["?arg"], "?distinct")),
                agg_fun_expr("?fun", vec!["?arg"], "?distinct"),
            ),
            rewrite(
                "split-push-down-cast-inner-replacer",
                inner_aggregate_split_replacer(cast_expr("?expr", "?data_type")),
                inner_aggregate_split_replacer("?expr"),
            ),
            rewrite(
                "split-push-down-trunc-inner-replacer",
                inner_aggregate_split_replacer(fun_expr("Trunc", vec!["?expr"])),
                inner_aggregate_split_replacer("?expr"),
            ),
            // Outer projection replacer
            rewrite(
                "split-push-down-group-outer-replacer",
                outer_projection_split_replacer(aggr_group_expr("?left", "?right")),
                projection_expr(
                    outer_projection_split_replacer("?left"),
                    outer_projection_split_replacer("?right"),
                ),
            ),
            rewrite(
                "split-push-down-aggr-outer-replacer",
                outer_projection_split_replacer(aggr_aggr_expr("?left", "?right")),
                projection_expr(
                    outer_projection_split_replacer("?left"),
                    outer_projection_split_replacer("?right"),
                ),
            ),
            rewrite(
                "split-push-down-group-outer-replacer-tail",
                outer_projection_split_replacer(aggr_group_expr_empty_tail()),
                projection_expr_empty_tail(),
            ),
            rewrite(
                "split-push-down-aggr-outer-replacer-tail",
                outer_projection_split_replacer(aggr_aggr_expr_empty_tail()),
                projection_expr_empty_tail(),
            ),
            transforming_chain_rewrite(
                "split-push-down-column-outer-replacer",
                outer_projection_split_replacer("?expr"),
                vec![("?expr", column_expr("?column"))],
                "?alias".to_string(),
                MemberRules::transform_original_expr_alias("?expr", "?alias"),
            ),
            transforming_chain_rewrite(
                "split-push-down-date-trunc-outer-replacer",
                outer_projection_split_replacer("?expr"),
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
                "split-push-down-aggr-fun-outer-replacer",
                outer_projection_split_replacer("?expr"),
                vec![("?expr", agg_fun_expr("?fun", vec!["?arg"], "?distinct"))],
                "?alias".to_string(),
                MemberRules::transform_original_expr_alias("?expr", "?alias"),
            ),
            rewrite(
                "split-push-down-cast-outer-replacer",
                outer_projection_split_replacer(cast_expr("?expr", "?data_type")),
                cast_expr(outer_projection_split_replacer("?expr"), "?data_type"),
            ),
            // Outer aggregate replacer
            rewrite(
                "split-push-down-group-outer-aggr-replacer",
                outer_aggregate_split_replacer(aggr_group_expr("?left", "?right")),
                aggr_group_expr(
                    outer_aggregate_split_replacer("?left"),
                    outer_aggregate_split_replacer("?right"),
                ),
            ),
            rewrite(
                "split-push-down-aggr-outer-aggr-replacer",
                outer_aggregate_split_replacer(aggr_aggr_expr("?left", "?right")),
                aggr_aggr_expr(
                    outer_aggregate_split_replacer("?left"),
                    outer_aggregate_split_replacer("?right"),
                ),
            ),
            rewrite(
                "split-push-down-group-outer-aggr-replacer-tail",
                outer_aggregate_split_replacer(aggr_group_expr_empty_tail()),
                aggr_group_expr_empty_tail(),
            ),
            rewrite(
                "split-push-down-aggr-outer-aggr-replacer-tail",
                outer_aggregate_split_replacer(aggr_aggr_expr_empty_tail()),
                aggr_aggr_expr_empty_tail(),
            ),
            transforming_chain_rewrite(
                "split-push-down-column-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr"),
                vec![("?expr", column_expr("?column"))],
                "?alias".to_string(),
                MemberRules::transform_original_expr_alias("?expr", "?alias"),
            ),
            transforming_chain_rewrite(
                "split-push-down-date-trunc-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr"),
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
                "split-push-down-date-part-outer-aggr-replacer",
                outer_aggregate_split_replacer(fun_expr(
                    "DatePart",
                    vec![literal_expr("?granularity"), "?expr".to_string()],
                )),
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
                "split-push-down-aggr-fun-outer-aggr-replacer",
                outer_aggregate_split_replacer("?expr"),
                vec![("?expr", agg_fun_expr("?fun", vec!["?arg"], "?distinct"))],
                alias_expr(
                    agg_fun_expr("?output_fun", vec!["?alias".to_string()], "?distinct"),
                    "?outer_alias",
                ),
                SplitRules::transform_outer_aggr_fun(
                    "?expr",
                    "?fun",
                    "?alias",
                    "?outer_alias",
                    "?output_fun",
                ),
            ),
            rewrite(
                "split-push-down-cast-outer-aggr-replacer",
                outer_aggregate_split_replacer(cast_expr("?expr", "?data_type")),
                cast_expr(outer_aggregate_split_replacer("?expr"), "?data_type"),
            ),
            rewrite(
                "split-push-down-trunc-outer-aggr-replacer",
                outer_aggregate_split_replacer(fun_expr("Trunc", vec!["?expr"])),
                fun_expr("Trunc", vec![outer_aggregate_split_replacer("?expr")]),
            ),
        ]
    }
}

impl SplitRules {
    pub fn new(cube_context: Arc<CubeContext>) -> Self {
        Self {
            _cube_context: cube_context,
        }
    }

    fn split_projection_aggregate(
        &self,
        group_expr_var: &'static str,
        aggr_expr_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let group_expr_var = var!(group_expr_var);
        let aggr_expr_var = var!(aggr_expr_var);
        move |egraph, subst| {
            if let Some(SplitType::Projection) = &egraph[subst[group_expr_var]].data.can_split {
                if let Some(SplitType::Projection) = &egraph[subst[aggr_expr_var]].data.can_split {
                    return true;
                }
            }
            false
        }
    }

    fn split_aggregate_aggregate(
        &self,
        group_expr_var: &'static str,
        aggr_expr_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let group_expr_var = var!(group_expr_var);
        let aggr_expr_var = var!(aggr_expr_var);
        move |egraph, subst| {
            let can_split = vec![
                egraph[subst[group_expr_var]].data.can_split.clone(),
                egraph[subst[aggr_expr_var]].data.can_split.clone(),
            ]
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .and_then(|s| s.into_iter().min());
            if let Some(SplitType::Aggregation) = can_split {
                return true;
            }
            false
        }
    }

    pub fn transform_outer_aggr_fun(
        original_expr_var: &'static str,
        fun_expr_var: &'static str,
        alias_expr_var: &'static str,
        outer_alias_expr_var: &'static str,
        output_fun_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let original_expr_var = var!(original_expr_var);
        let fun_expr_var = var!(fun_expr_var);
        let alias_expr_var = var!(alias_expr_var);
        let outer_alias_expr_var = var!(outer_alias_expr_var);
        let output_fun_var = var!(output_fun_var);
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
            for fun in var_iter!(egraph[subst[fun_expr_var]], AggregateFunctionExprFun) {
                let output_fun = match fun {
                    AggregateFunction::Count => AggregateFunction::Sum,
                    AggregateFunction::Sum => AggregateFunction::Sum,
                    AggregateFunction::Min => AggregateFunction::Min,
                    AggregateFunction::Max => AggregateFunction::Max,
                    _ => continue,
                };

                if let Ok(expr) = res {
                    // TODO unwrap
                    let name = expr.name(&DFSchema::empty()).unwrap();
                    let alias = egraph.add(LogicalPlanLanguage::ColumnExprColumn(
                        ColumnExprColumn(Column::from_name(name.to_string())),
                    ));
                    subst.insert(
                        alias_expr_var,
                        egraph.add(LogicalPlanLanguage::ColumnExpr([alias])),
                    );
                    subst.insert(
                        outer_alias_expr_var,
                        egraph.add(LogicalPlanLanguage::AliasExprAlias(AliasExprAlias(
                            name.to_string(),
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
            false
        }
    }
}
