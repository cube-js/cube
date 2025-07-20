use crate::{
    compile::rewrite::{
        aggregate, cube_scan, flatten_pushdown_replacer, projection,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::{flatten::FlattenRules, replacer_flat_push_down_node, replacer_push_down_node},
        transforming_chain_rewrite_with_root, FlattenPushdownReplacerInnerAlias, ListType,
        LogicalPlanLanguage, ProjectionAlias,
    },
    var, var_iter,
};
use egg::Id;

impl FlattenRules {
    pub fn top_level_rules(&self, rules: &mut Vec<CubeRewrite>) {
        // TODO use root instead for performance
        rules.extend(vec![transforming_chain_rewrite_with_root(
            "flatten-projection-pushdown",
            projection(
                "?outer_projection_expr",
                "?inner_projection",
                "?outer_projection_alias",
                "ProjectionSplit:false",
            ),
            vec![
                (
                    "?inner_projection",
                    projection(
                        "?inner_projection_expr",
                        "?cube_scan",
                        "?inner_projection_alias",
                        "ProjectionSplit:false",
                    ),
                ),
                (
                    "?cube_scan",
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
                        "?join_hints",
                    ),
                ),
            ],
            projection(
                flatten_pushdown_replacer(
                    "?outer_projection_expr",
                    "?inner_projection_expr",
                    "?inner_alias",
                    "FlattenPushdownReplacerTopLevel:true",
                ),
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
                    "?join_hints",
                ),
                "?new_projection_alias",
                "ProjectionSplit:false",
            ),
            self.flatten_projection(
                "?inner_projection",
                "?cube_scan",
                "?members",
                "?inner_projection_expr",
                "?outer_projection_expr",
                "?inner_projection_alias",
                "?outer_projection_alias",
                "?new_projection_alias",
                "?inner_alias",
            ),
        )]);

        rules.extend(vec![transforming_chain_rewrite_with_root(
            "flatten-aggregate-pushdown",
            aggregate(
                "?inner_projection",
                "?outer_group_expr",
                "?outer_aggregate_expr",
                "AggregateSplit:false",
            ),
            vec![
                (
                    "?inner_projection",
                    projection(
                        "?inner_projection_expr",
                        "?cube_scan",
                        "?inner_projection_alias",
                        "ProjectionSplit:false",
                    ),
                ),
                (
                    "?cube_scan",
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
                        "?join_hints",
                    ),
                ),
            ],
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
                    "?join_hints",
                ),
                flatten_pushdown_replacer(
                    "?outer_group_expr",
                    "?inner_projection_expr",
                    "?inner_alias",
                    "FlattenPushdownReplacerTopLevel:false",
                ),
                flatten_pushdown_replacer(
                    "?outer_aggregate_expr",
                    "?inner_projection_expr",
                    "?inner_alias",
                    "FlattenPushdownReplacerTopLevel:false",
                ),
                "AggregateSplit:false",
            ),
            self.flatten_aggregate(
                "?inner_projection",
                "?cube_scan",
                "?members",
                "?inner_projection_expr",
                "?outer_group_expr",
                "?outer_aggregate_expr",
                "?inner_projection_alias",
                "?inner_alias",
            ),
        )]);

        if self.config_obj.push_down_pull_up_split() {
            Self::flat_list_pushdown_rules(
                "flatten-projection-expr",
                ListType::ProjectionExpr,
                rules,
            );
            Self::flat_list_pushdown_rules(
                "flatten-aggregate-expr",
                ListType::AggregateAggrExpr,
                rules,
            );
            Self::flat_list_pushdown_rules(
                "flatten-group-expr",
                ListType::AggregateGroupExpr,
                rules,
            );
        } else {
            Self::list_pushdown_rules("flatten-projection-expr", "ProjectionExpr", rules);
            Self::list_pushdown_rules("flatten-aggregate-expr", "AggregateAggrExpr", rules);
            Self::list_pushdown_rules("flatten-group-expr", "AggregateGroupExpr", rules);
        }
    }

    pub fn flatten_projection(
        &self,
        inner_projection_var: &str,
        cube_scan_var: &str,
        members_var: &str,
        inner_projection_expr_var: &'static str,
        outer_projection_expr_var: &'static str,
        inner_projection_alias_var: &'static str,
        outer_projection_alias_var: &'static str,
        new_projection_alias_var: &'static str,
        inner_alias_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, Id, &mut egg::Subst) -> bool {
        let inner_projection_var = var!(inner_projection_var);
        let cube_scan_var = var!(cube_scan_var);
        let members_var = var!(members_var);
        let inner_projection_expr_var = var!(inner_projection_expr_var);
        let outer_projection_expr_var = var!(outer_projection_expr_var);
        let inner_projection_alias_var = var!(inner_projection_alias_var);
        let outer_projection_alias_var = var!(outer_projection_alias_var);
        let new_projection_alias_var = var!(new_projection_alias_var);
        let inner_alias_var = var!(inner_alias_var);
        move |egraph, root, subst| {
            if root == subst[inner_projection_var]
                || subst[inner_projection_var] == subst[cube_scan_var]
            {
                return false;
            }
            if let Some(_) = egraph[subst[members_var]].data.member_name_to_expr {
                if let Some(_) = egraph[subst[inner_projection_expr_var]].data.expr_to_alias {
                    if let Some(_) = egraph[subst[outer_projection_expr_var]].data.expr_to_alias {
                        for inner_projection_alias in
                            var_iter!(egraph[subst[inner_projection_alias_var]], ProjectionAlias)
                        {
                            for outer_projection_alias in var_iter!(
                                egraph[subst[outer_projection_alias_var]],
                                ProjectionAlias
                            ) {
                                let new_projection_alias_id = if outer_projection_alias.is_none() {
                                    subst[inner_projection_alias_var]
                                } else {
                                    subst[outer_projection_alias_var]
                                };
                                subst.insert(new_projection_alias_var, new_projection_alias_id);

                                let inner_alias = egraph.add(
                                    LogicalPlanLanguage::FlattenPushdownReplacerInnerAlias(
                                        FlattenPushdownReplacerInnerAlias(
                                            inner_projection_alias.clone(),
                                        ),
                                    ),
                                );
                                subst.insert(inner_alias_var, inner_alias);
                                return true;
                            }
                        }
                    }
                }
            }

            false
        }
    }

    pub fn flatten_aggregate(
        &self,
        inner_projection_var: &str,
        cube_scan_var: &str,
        members_var: &str,
        inner_projection_expr_var: &'static str,
        outer_group_expr_var: &'static str,
        outer_aggregate_expr_var: &'static str,
        inner_projection_alias_var: &'static str,
        inner_alias_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, Id, &mut egg::Subst) -> bool {
        let inner_projection_var = var!(inner_projection_var);
        let cube_scan_var = var!(cube_scan_var);
        let members_var = var!(members_var);
        let inner_projection_expr_var = var!(inner_projection_expr_var);
        let outer_group_expr_var = var!(outer_group_expr_var);
        let outer_aggregate_expr_var = var!(outer_aggregate_expr_var);
        let inner_projection_alias_var = var!(inner_projection_alias_var);
        let inner_alias_var = var!(inner_alias_var);
        move |egraph, root, subst| {
            if root == subst[inner_projection_var]
                || subst[inner_projection_var] == subst[cube_scan_var]
            {
                return false;
            }
            if let Some(_) = egraph[subst[members_var]].data.member_name_to_expr {
                if let Some(_) = egraph[subst[inner_projection_expr_var]]
                    .data
                    .referenced_expr
                {
                    if let Some(_) = egraph[subst[outer_group_expr_var]].data.referenced_expr {
                        if let Some(_) =
                            egraph[subst[outer_aggregate_expr_var]].data.referenced_expr
                        {
                            for inner_projection_alias in var_iter!(
                                egraph[subst[inner_projection_alias_var]],
                                ProjectionAlias
                            )
                            .cloned()
                            {
                                let inner_alias = egraph.add(
                                    LogicalPlanLanguage::FlattenPushdownReplacerInnerAlias(
                                        FlattenPushdownReplacerInnerAlias(
                                            inner_projection_alias.clone(),
                                        ),
                                    ),
                                );
                                subst.insert(inner_alias_var, inner_alias);
                                return true;
                            }
                        }
                    }
                }
            }

            false
        }
    }

    fn list_pushdown_rules(name: &str, list_node: &str, rules: &mut Vec<CubeRewrite>) {
        rules.extend(replacer_push_down_node(
            name,
            list_node,
            |node| flatten_pushdown_replacer(node, "?inner_expr", "?inner_alias", "?top_level"),
            true,
        ));
    }

    fn flat_list_pushdown_rules(name: &str, list_type: ListType, rules: &mut Vec<CubeRewrite>) {
        rules.extend(replacer_flat_push_down_node(
            name,
            list_type,
            |node| flatten_pushdown_replacer(node, "?inner_expr", "?inner_alias", "?top_level"),
            true,
        ));
    }
}
