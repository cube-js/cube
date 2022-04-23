use crate::compile::engine::provider::CubeContext;
use crate::compile::rewrite::analysis::{LogicalPlanAnalysis, SplitType};
use crate::compile::rewrite::rewriter::RewriteRules;
use crate::compile::rewrite::rules::members::MemberRules;
use crate::compile::rewrite::LogicalPlanLanguage;
use crate::compile::rewrite::{agg_fun_expr, transforming_chain_rewrite};
use crate::compile::rewrite::{
    aggr_aggr_expr, aggr_aggr_expr_empty_tail, aggr_group_expr, aggr_group_expr_empty_tail,
    aggregate, fun_expr, projection, projection_expr,
};
use crate::compile::rewrite::{cast_expr, projection_expr_empty_tail};
use crate::compile::rewrite::{
    column_expr, cube_scan, literal_expr, rewrite, transforming_rewrite,
};
use crate::compile::rewrite::{inner_projection_split_replacer, outer_projection_split_replacer};
use crate::var;
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
                        inner_projection_split_replacer("?group_expr"),
                        inner_projection_split_replacer("?aggr_expr"),
                    ),
                    "ProjectionAlias:None",
                ),
                self.split_projection_aggregate("?group_expr", "?aggr_expr"),
            ),
            // Inner replacers
            rewrite(
                "split-push-down-group-inner-replacer",
                inner_projection_split_replacer(aggr_group_expr("?left", "?right")),
                aggr_group_expr(
                    inner_projection_split_replacer("?left"),
                    inner_projection_split_replacer("?right"),
                ),
            ),
            rewrite(
                "split-push-down-aggr-inner-replacer",
                inner_projection_split_replacer(aggr_aggr_expr("?left", "?right")),
                aggr_aggr_expr(
                    inner_projection_split_replacer("?left"),
                    inner_projection_split_replacer("?right"),
                ),
            ),
            rewrite(
                "split-push-down-group-inner-replacer-tail",
                inner_projection_split_replacer(aggr_group_expr_empty_tail()),
                aggr_group_expr_empty_tail(),
            ),
            rewrite(
                "split-push-down-aggr-inner-replacer-tail",
                inner_projection_split_replacer(aggr_aggr_expr_empty_tail()),
                aggr_aggr_expr_empty_tail(),
            ),
            rewrite(
                "split-push-down-column-inner-replacer",
                inner_projection_split_replacer(column_expr("?column")),
                column_expr("?column"),
            ),
            rewrite(
                "split-push-down-date-trunc-inner-replacer",
                inner_projection_split_replacer(fun_expr(
                    "DateTrunc",
                    vec![literal_expr("?granularity"), column_expr("?column")],
                )),
                fun_expr(
                    "DateTrunc",
                    vec![literal_expr("?granularity"), column_expr("?column")],
                ),
            ),
            rewrite(
                "split-push-down-aggr-fun-inner-replacer",
                inner_projection_split_replacer(agg_fun_expr("?fun", vec!["?arg"], "?distinct")),
                agg_fun_expr("?fun", vec!["?arg"], "?distinct"),
            ),
            rewrite(
                "split-push-down-cast-inner-replacer",
                inner_projection_split_replacer(cast_expr("?expr", "?data_type")),
                inner_projection_split_replacer("?expr"),
            ),
            // Outer replacers
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
}
