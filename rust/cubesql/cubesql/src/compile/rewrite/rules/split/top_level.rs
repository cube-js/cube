use crate::{
    compile::rewrite::{
        aggr_aggr_expr_empty_tail, aggr_group_expr_empty_tail, aggregate,
        aggregate_split_pullup_replacer, aggregate_split_pushdown_replacer, cube_scan, projection,
        projection_expr_empty_tail, projection_split_pullup_replacer,
        projection_split_pushdown_replacer, rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::split::SplitRules,
        transforming_rewrite, AggregateSplitPushDownReplacerAliasToCube, CubeScanAliasToCube,
        ListType, LogicalPlanLanguage, ProjectionAlias, ProjectionSplitPushDownReplacerAliasToCube,
    },
    var, var_iter,
};
use egg::Subst;

impl SplitRules {
    pub fn top_level_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.push(transforming_rewrite(
            "split-projection-aggregate",
            aggregate(
                cube_scan(
                    "?alias_to_cube",
                    "?members",
                    "?filters",
                    "?orders",
                    "?limit",
                    "?offset",
                    "CubeScanSplit:false",
                    "?can_pushdown_join",
                    "CubeScanWrapped:false",
                    "?ungrouped",
                ),
                "?group_expr",
                "?aggr_expr",
                "AggregateSplit:false",
            ),
            aggregate(
                cube_scan(
                    "?alias_to_cube",
                    "?members",
                    "?filters",
                    "?orders",
                    "?limit",
                    "?offset",
                    "CubeScanSplit:true",
                    "?can_pushdown_join",
                    "CubeScanWrapped:false",
                    "?ungrouped",
                ),
                projection_split_pushdown_replacer(
                    "?group_expr",
                    aggr_group_expr_empty_tail(),
                    "?split_alias_to_cube",
                ),
                projection_split_pushdown_replacer(
                    "?aggr_expr",
                    aggr_aggr_expr_empty_tail(),
                    "?split_alias_to_cube",
                ),
                "AggregateSplit:true",
            ),
            self.transform_projection_aggregate(
                "?alias_to_cube",
                "?split_alias_to_cube",
                Some("?group_expr"),
                Some("?aggr_expr"),
                None,
            ),
        ));

        rules.push(transforming_rewrite(
            "split-projection-aggregate-pull-up",
            aggregate(
                cube_scan(
                    "?alias_to_cube",
                    "?members",
                    "?filters",
                    "?orders",
                    "?limit",
                    "?offset",
                    "CubeScanSplit:true",
                    "?can_pushdown_join",
                    "CubeScanWrapped:false",
                    "?ungrouped",
                ),
                projection_split_pullup_replacer(
                    "?inner_group_expr",
                    "?outer_group_expr",
                    aggr_group_expr_empty_tail(),
                    "?split_alias_to_cube",
                ),
                projection_split_pullup_replacer(
                    "?inner_aggr_expr",
                    "?outer_aggr_expr",
                    aggr_aggr_expr_empty_tail(),
                    "?split_alias_to_cube",
                ),
                "AggregateSplit:true",
            ),
            projection(
                "?projection_expr",
                aggregate(
                    cube_scan(
                        "?alias_to_cube",
                        "?members",
                        "?filters",
                        "?orders",
                        "?limit",
                        "?offset",
                        "CubeScanSplit:true",
                        "?can_pushdown_join",
                        "CubeScanWrapped:false",
                        "?ungrouped",
                    ),
                    "?inner_group_expr",
                    "?inner_aggr_expr",
                    "AggregateSplit:true",
                ),
                "?projection_alias",
                "ProjectionSplit:true",
            ),
            self.transform_projection_aggregate_pull_up(
                "?projection_alias",
                "?outer_group_expr",
                "?outer_aggr_expr",
                "?projection_expr",
            ),
        ));

        rules.push(transforming_rewrite(
            "split-aggregate-aggregate",
            aggregate(
                cube_scan(
                    "?alias_to_cube",
                    "?members",
                    "?filters",
                    "?orders",
                    "?limit",
                    "?offset",
                    "CubeScanSplit:false",
                    "?can_pushdown_join",
                    "CubeScanWrapped:false",
                    "?ungrouped",
                ),
                "?group_expr",
                "?aggr_expr",
                "AggregateSplit:false",
            ),
            aggregate(
                cube_scan(
                    "?alias_to_cube",
                    "?members",
                    "?filters",
                    "?orders",
                    "?limit",
                    "?offset",
                    "CubeScanSplit:true",
                    "?can_pushdown_join",
                    "CubeScanWrapped:false",
                    "?ungrouped",
                ),
                aggregate_split_pushdown_replacer(
                    "?group_expr",
                    aggr_group_expr_empty_tail(),
                    "?split_alias_to_cube",
                ),
                aggregate_split_pushdown_replacer(
                    "?aggr_expr",
                    aggr_aggr_expr_empty_tail(),
                    "?split_alias_to_cube",
                ),
                "AggregateSplit:true",
            ),
            self.transform_aggregate_aggregate(
                "?alias_to_cube",
                "?split_alias_to_cube",
                "?group_expr",
                "?aggr_expr",
            ),
        ));

        rules.push(rewrite(
            "split-aggregate-aggregate-pull-up",
            aggregate(
                cube_scan(
                    "?alias_to_cube",
                    "?members",
                    "?filters",
                    "?orders",
                    "?limit",
                    "?offset",
                    "CubeScanSplit:true",
                    "?can_pushdown_join",
                    "CubeScanWrapped:false",
                    "?ungrouped",
                ),
                aggregate_split_pullup_replacer(
                    "?inner_group_expr",
                    "?outer_group_expr",
                    aggr_group_expr_empty_tail(),
                    "?split_alias_to_cube",
                ),
                aggregate_split_pullup_replacer(
                    "?inner_aggr_expr",
                    "?outer_aggr_expr",
                    aggr_aggr_expr_empty_tail(),
                    "?split_alias_to_cube",
                ),
                "AggregateSplit:true",
            ),
            aggregate(
                aggregate(
                    cube_scan(
                        "?alias_to_cube",
                        "?members",
                        "?filters",
                        "?orders",
                        "?limit",
                        "?offset",
                        "CubeScanSplit:true",
                        "?can_pushdown_join",
                        "CubeScanWrapped:false",
                        "?ungrouped",
                    ),
                    "?inner_group_expr",
                    "?inner_aggr_expr",
                    "AggregateSplit:true",
                ),
                "?outer_group_expr",
                "?outer_aggr_expr",
                "AggregateSplit:true",
            ),
        ));

        rules.push(transforming_rewrite(
            "split-projection-projection-ungrouped",
            projection(
                "?projection_expr",
                cube_scan(
                    "?alias_to_cube",
                    "?members",
                    "?filters",
                    "?orders",
                    "?limit",
                    "?offset",
                    "CubeScanSplit:false",
                    "?can_pushdown_join",
                    "CubeScanWrapped:false",
                    "CubeScanUngrouped:true",
                ),
                "?projection_alias",
                "ProjectionSplit:false",
            ),
            projection(
                cube_scan(
                    "?alias_to_cube",
                    "?members",
                    "?filters",
                    "?orders",
                    "?limit",
                    "?offset",
                    "CubeScanSplit:true",
                    "?can_pushdown_join",
                    "CubeScanWrapped:false",
                    "CubeScanUngrouped:true",
                ),
                aggregate_split_pushdown_replacer(
                    "?projection_expr",
                    projection_expr_empty_tail(),
                    "?split_alias_to_cube",
                ),
                "?projection_alias",
                "ProjectionSplit:true",
            ),
            self.transform_projection_aggregate(
                "?alias_to_cube",
                "?split_alias_to_cube",
                None,
                None,
                Some("?projection_expr"),
            ),
        ));

        rules.push(transforming_rewrite(
            "split-projection-projection-ungrouped-pull-up",
            projection(
                cube_scan(
                    "?alias_to_cube",
                    "?members",
                    "?filters",
                    "?orders",
                    "?limit",
                    "?offset",
                    "CubeScanSplit:true",
                    "?can_pushdown_join",
                    "CubeScanWrapped:false",
                    "CubeScanUngrouped:true",
                ),
                aggregate_split_pullup_replacer(
                    "?inner_expr",
                    "?outer_expr",
                    projection_expr_empty_tail(),
                    "?split_alias_to_cube",
                ),
                "?top_alias",
                "ProjectionSplit:true",
            ),
            projection(
                "?outer_expr",
                projection(
                    "?inner_expr",
                    cube_scan(
                        "?alias_to_cube",
                        "?members",
                        "?filters",
                        "?orders",
                        "?limit",
                        "?offset",
                        "CubeScanSplit:true",
                        "?can_pushdown_join",
                        "CubeScanWrapped:false",
                        "CubeScanUngrouped:true",
                    ),
                    "?projection_alias",
                    "ProjectionSplit:true",
                ),
                "?top_alias",
                "ProjectionSplit:true",
            ),
            self.transform_projection_projection_ungrouped_pull_up("?projection_alias"),
        ));

        Self::flat_list_pushdown_pullup_rules(
            "aggr-group-expr",
            ListType::AggregateGroupExpr,
            rules,
        );
        Self::flat_list_pushdown_pullup_rules("aggr-aggr-expr", ListType::AggregateAggrExpr, rules);
        Self::flat_list_pushdown_pullup_rules("projection-expr", ListType::ProjectionExpr, rules);
    }

    fn transform_projection_aggregate(
        &self,
        alias_to_cube_var: &str,
        split_alias_to_cube_var: &str,
        group_expr_var: Option<&str>,
        aggr_expr_var: Option<&str>,
        projection_expr_var: Option<&str>,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let split_alias_to_cube_var = var!(split_alias_to_cube_var);
        let group_expr_var = group_expr_var.map(|group_expr_var| var!(group_expr_var));
        let aggr_expr_var = aggr_expr_var.map(|aggr_expr_var| var!(aggr_expr_var));
        let projection_expr_var =
            projection_expr_var.map(|projection_expr_var| var!(projection_expr_var));
        move |egraph, subst| {
            if matches!(
                projection_expr_var.and_then(|v| egraph[subst[v]].data.trivial_push_down),
                Some(0) | Some(1)
            ) {
                return false;
            }
            if matches!(
                aggr_expr_var
                    .and_then(|v| egraph[subst[v]].data.trivial_push_down)
                    .zip(group_expr_var.and_then(|v| egraph[subst[v]].data.trivial_push_down))
                    .map(|(a, b)| a.max(b)),
                Some(0) | Some(1)
            ) {
                return false;
            }
            if let Some(projection_expr_var) = projection_expr_var {
                if egraph[subst[projection_expr_var]]
                    .data
                    .referenced_expr
                    .is_none()
                {
                    return false;
                }
            }
            if let Some(group_expr_var) = group_expr_var {
                if egraph[subst[group_expr_var]].data.referenced_expr.is_none() {
                    return false;
                }
            }
            if let Some(aggr_expr_var) = aggr_expr_var {
                if egraph[subst[aggr_expr_var]].data.referenced_expr.is_none() {
                    return false;
                }
            }
            for alias_to_cube in
                var_iter!(egraph[subst[alias_to_cube_var]], CubeScanAliasToCube).cloned()
            {
                let split_alias_to_cube = egraph.add(
                    LogicalPlanLanguage::ProjectionSplitPushDownReplacerAliasToCube(
                        ProjectionSplitPushDownReplacerAliasToCube(alias_to_cube),
                    ),
                );

                subst.insert(split_alias_to_cube_var, split_alias_to_cube);
                return true;
            }
            false
        }
    }

    fn transform_aggregate_aggregate(
        &self,
        alias_to_cube_var: &str,
        split_alias_to_cube_var: &str,
        group_expr_var: &str,
        aggr_expr_var: &str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let split_alias_to_cube_var = var!(split_alias_to_cube_var);
        let group_expr_var = var!(group_expr_var);
        let aggr_expr_var = var!(aggr_expr_var);
        move |egraph, subst| {
            if matches!(
                egraph[subst[aggr_expr_var]]
                    .data
                    .trivial_push_down
                    .zip(egraph[subst[group_expr_var]].data.trivial_push_down)
                    .map(|(a, b)| a.max(b)),
                Some(0) | Some(1)
            ) {
                return false;
            }
            if egraph[subst[aggr_expr_var]].data.referenced_expr.is_none() {
                return false;
            }
            if egraph[subst[group_expr_var]].data.referenced_expr.is_none() {
                return false;
            }
            for alias_to_cube in
                var_iter!(egraph[subst[alias_to_cube_var]], CubeScanAliasToCube).cloned()
            {
                let split_alias_to_cube = egraph.add(
                    LogicalPlanLanguage::AggregateSplitPushDownReplacerAliasToCube(
                        AggregateSplitPushDownReplacerAliasToCube(alias_to_cube),
                    ),
                );

                subst.insert(split_alias_to_cube_var, split_alias_to_cube);
                return true;
            }
            false
        }
    }

    fn transform_projection_projection_ungrouped_pull_up(
        &self,
        projection_alias_var: &str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let projection_alias_var = var!(projection_alias_var);
        move |egraph, subst| {
            subst.insert(
                projection_alias_var,
                // Do not put alias on inner projection so table name from cube scan can be reused
                egraph.add(LogicalPlanLanguage::ProjectionAlias(ProjectionAlias(None))),
            );
            return true;
        }
    }

    fn transform_projection_aggregate_pull_up(
        &self,
        projection_alias_var: &str,
        outer_group_expr_var: &str,
        outer_aggr_expr_var: &str,
        projection_expr_var: &str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let projection_alias_var = var!(projection_alias_var);
        let outer_group_expr_var = var!(outer_group_expr_var);
        let outer_aggr_expr_var = var!(outer_aggr_expr_var);
        let projection_expr_var = var!(projection_expr_var);
        move |egraph, subst| {
            let group_expr_node = egraph[subst[outer_group_expr_var]]
                .nodes
                .iter()
                .find(|n| matches!(n, LogicalPlanLanguage::ProjectionExpr(_)));
            let Some(LogicalPlanLanguage::ProjectionExpr(group_expr_node)) = group_expr_node else {
                return false;
            };
            let aggr_expr_node = egraph[subst[outer_aggr_expr_var]]
                .nodes
                .iter()
                .find(|n| matches!(n, LogicalPlanLanguage::ProjectionExpr(_)));
            let Some(LogicalPlanLanguage::ProjectionExpr(aggr_expr_node)) = aggr_expr_node else {
                return false;
            };
            let projection_expr = group_expr_node
                .iter()
                .chain(aggr_expr_node.iter())
                .copied()
                .collect::<Vec<_>>();
            subst.insert(
                projection_expr_var,
                egraph.add(LogicalPlanLanguage::ProjectionExpr(projection_expr)),
            );
            subst.insert(
                projection_alias_var,
                // Do not put alias on inner projection so table name from cube scan can be reused
                egraph.add(LogicalPlanLanguage::ProjectionAlias(ProjectionAlias(None))),
            );
            return true;
        }
    }
}
