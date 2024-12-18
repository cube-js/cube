use crate::{
    compile::rewrite::{
        cube_scan_wrapper, projection,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        subquery, transforming_rewrite, wrapped_select, wrapped_select_aggr_expr_empty_tail,
        wrapped_select_filter_expr_empty_tail, wrapped_select_group_expr_empty_tail,
        wrapped_select_having_expr_empty_tail, wrapped_select_joins_empty_tail,
        wrapped_select_order_expr_empty_tail, wrapped_select_subqueries_empty_tail,
        wrapped_select_window_expr_empty_tail, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        ListType, LogicalPlanLanguage, ProjectionAlias, WrappedSelectAlias,
        WrappedSelectPushToCube, WrappedSelectUngroupedScan,
        WrapperPullupReplacerGroupedSubqueries, WrapperPullupReplacerPushToCube,
        WrapperPushdownReplacerGroupedSubqueries, WrapperPushdownReplacerPushToCube,
    },
    copy_flag, copy_value, var, var_iter,
};
use egg::{Subst, Var};

impl WrapperRules {
    pub fn projection_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![transforming_rewrite(
            "wrapper-push-down-projection-to-cube-scan",
            projection(
                "?expr",
                cube_scan_wrapper(
                    wrapper_pullup_replacer(
                        "?cube_scan_input",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                "?projection_alias",
                "ProjectionSplit:false",
            ),
            cube_scan_wrapper(
                wrapped_select(
                    "WrappedSelectSelectType:Projection",
                    wrapper_pushdown_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?pushdown_push_to_cube",
                        "WrapperPullupReplacerInProjection:true",
                        "?cube_members",
                        "?pushdown_grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_subqueries_empty_tail(),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "WrapperPullupReplacerInProjection:true",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_group_expr_empty_tail(),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "WrapperPullupReplacerInProjection:true",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_aggr_expr_empty_tail(),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "WrapperPullupReplacerInProjection:true",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_window_expr_empty_tail(),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "WrapperPullupReplacerInProjection:true",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        "?cube_scan_input",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "WrapperPullupReplacerInProjection:true",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_joins_empty_tail(),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "WrapperPullupReplacerInProjection:true",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_filter_expr_empty_tail(),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "WrapperPullupReplacerInProjection:true",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapped_select_having_expr_empty_tail(),
                    "WrappedSelectLimit:None",
                    "WrappedSelectOffset:None",
                    wrapper_pullup_replacer(
                        wrapped_select_order_expr_empty_tail(),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "WrapperPullupReplacerInProjection:true",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    "?select_alias",
                    "WrappedSelectDistinct:false",
                    "?select_push_to_cube",
                    "?select_ungrouped_scan",
                ),
                "CubeScanWrapperFinalized:false",
            ),
            self.transform_projection(
                "?expr",
                "?projection_alias",
                "?push_to_cube",
                "?pushdown_push_to_cube",
                "?select_alias",
                "?select_push_to_cube",
                "?select_ungrouped_scan",
                "?grouped_subqueries",
                "?pushdown_grouped_subqueries",
            ),
        )]);

        if self.config_obj.push_down_pull_up_split() {
            Self::flat_list_pushdown_pullup_rules(
                rules,
                "wrapper-projection-expr",
                ListType::ProjectionExpr,
                ListType::WrappedSelectProjectionExpr,
            );
        } else {
            Self::list_pushdown_pullup_rules(
                rules,
                "wrapper-projection-expr",
                "ProjectionExpr",
                "WrappedSelectProjectionExpr",
            );
        }
    }

    pub fn projection_rules_subquery(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![transforming_rewrite(
            "wrapper-push-down-projection-and-subquery-to-cube-scan",
            projection(
                "?expr",
                subquery(
                    cube_scan_wrapper(
                        wrapper_pullup_replacer(
                            "?cube_scan_input",
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                        ),
                        "CubeScanWrapperFinalized:false",
                    ),
                    "?subqueries",
                    "?types",
                ),
                "?projection_alias",
                "ProjectionSplit:false",
            ),
            cube_scan_wrapper(
                wrapped_select(
                    "WrappedSelectSelectType:Projection",
                    wrapper_pushdown_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?pushdown_push_to_cube",
                        "WrapperPullupReplacerInProjection:true",
                        "?cube_members",
                        "?pushdown_grouped_subqueries",
                    ),
                    wrapper_pushdown_replacer(
                        "?subqueries",
                        "?alias_to_cube",
                        "?pushdown_push_to_cube",
                        "WrapperPullupReplacerInProjection:true",
                        "?cube_members",
                        "?pushdown_grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_group_expr_empty_tail(),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "WrapperPullupReplacerInProjection:true",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_aggr_expr_empty_tail(),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "WrapperPullupReplacerInProjection:true",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_window_expr_empty_tail(),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "WrapperPullupReplacerInProjection:true",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        "?cube_scan_input",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "WrapperPullupReplacerInProjection:true",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_joins_empty_tail(),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "WrapperPullupReplacerInProjection:true",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_filter_expr_empty_tail(),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "WrapperPullupReplacerInProjection:true",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapped_select_having_expr_empty_tail(),
                    "WrappedSelectLimit:None",
                    "WrappedSelectOffset:None",
                    wrapper_pullup_replacer(
                        wrapped_select_order_expr_empty_tail(),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "WrapperPullupReplacerInProjection:true",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    "?select_alias",
                    "WrappedSelectDistinct:false",
                    "?select_push_to_cube",
                    "?select_ungrouped_scan",
                ),
                "CubeScanWrapperFinalized:false",
            ),
            self.transform_projection_subquery(
                "?alias_to_cube",
                "?expr",
                "?projection_alias",
                "?push_to_cube",
                "?pushdown_push_to_cube",
                "?select_alias",
                "?select_push_to_cube",
                "?select_ungrouped_scan",
                "?grouped_subqueries",
                "?pushdown_grouped_subqueries",
            ),
        )]);
    }
    fn transform_projection(
        &self,
        expr_var: &'static str,
        projection_alias_var: &'static str,
        push_to_cube_var: &'static str,
        pushdown_push_to_cube_var: &'static str,
        select_alias_var: &'static str,
        select_push_to_cube_var: &'static str,
        select_ungrouped_scan_var: &'static str,
        grouped_subqueries_var: &'static str,
        pushdown_grouped_subqueries_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let expr_var = var!(expr_var);
        let projection_alias_var = var!(projection_alias_var);
        let push_to_cube_var = var!(push_to_cube_var);
        let pushdown_push_to_cube_var = var!(pushdown_push_to_cube_var);
        let select_alias_var = var!(select_alias_var);
        let select_push_to_cube_var = var!(select_push_to_cube_var);
        let select_ungrouped_scan_var = var!(select_ungrouped_scan_var);
        let grouped_subqueries_var = var!(grouped_subqueries_var);
        let pushdown_grouped_subqueries_var = var!(pushdown_grouped_subqueries_var);
        move |egraph, subst| {
            Self::transform_projection_impl(
                egraph,
                subst,
                expr_var,
                projection_alias_var,
                push_to_cube_var,
                pushdown_push_to_cube_var,
                select_alias_var,
                select_push_to_cube_var,
                select_ungrouped_scan_var,
                grouped_subqueries_var,
                pushdown_grouped_subqueries_var,
            )
        }
    }

    fn transform_projection_subquery(
        &self,
        alias_to_cube_var: &'static str,
        expr_var: &'static str,
        projection_alias_var: &'static str,
        push_to_cube_var: &'static str,
        pushdown_push_to_cube_var: &'static str,
        select_alias_var: &'static str,
        select_push_to_cube_var: &'static str,
        select_ungrouped_scan_var: &'static str,
        grouped_subqueries_var: &'static str,
        pushdown_grouped_subqueries_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let expr_var = var!(expr_var);
        let projection_alias_var = var!(projection_alias_var);
        let push_to_cube_var = var!(push_to_cube_var);
        let pushdown_push_to_cube_var = var!(pushdown_push_to_cube_var);
        let select_alias_var = var!(select_alias_var);
        let select_push_to_cube_var = var!(select_push_to_cube_var);
        let select_ungrouped_scan_var = var!(select_ungrouped_scan_var);
        let meta = self.meta_context.clone();
        let grouped_subqueries_var = var!(grouped_subqueries_var);
        let pushdown_grouped_subqueries_var = var!(pushdown_grouped_subqueries_var);
        move |egraph, subst| {
            if Self::transform_check_subquery_allowed(
                egraph,
                subst,
                meta.clone(),
                alias_to_cube_var,
            ) {
                Self::transform_projection_impl(
                    egraph,
                    subst,
                    expr_var,
                    projection_alias_var,
                    push_to_cube_var,
                    pushdown_push_to_cube_var,
                    select_alias_var,
                    select_push_to_cube_var,
                    select_ungrouped_scan_var,
                    grouped_subqueries_var,
                    pushdown_grouped_subqueries_var,
                )
            } else {
                false
            }
        }
    }

    fn transform_projection_impl(
        egraph: &mut CubeEGraph,
        subst: &mut Subst,
        expr_var: Var,
        projection_alias_var: Var,
        push_to_cube_var: Var,
        pushdown_push_to_cube_var: Var,
        select_alias_var: Var,
        select_push_to_cube_var: Var,
        select_ungrouped_scan_var: Var,
        grouped_subqueries_var: Var,
        pushdown_grouped_subqueries_var: Var,
    ) -> bool {
        if let Some(_) = &egraph[subst[expr_var]].data.referenced_expr {
            if !copy_flag!(
                egraph,
                subst,
                push_to_cube_var,
                WrapperPullupReplacerPushToCube,
                pushdown_push_to_cube_var,
                WrapperPushdownReplacerPushToCube
            ) {
                return false;
            }
            if !copy_value!(
                egraph,
                subst,
                Vec<String>,
                grouped_subqueries_var,
                WrapperPullupReplacerGroupedSubqueries,
                pushdown_grouped_subqueries_var,
                WrapperPushdownReplacerGroupedSubqueries
            ) {
                return false;
            }

            for projection_alias in
                var_iter!(egraph[subst[projection_alias_var]], ProjectionAlias).cloned()
            {
                for push_to_cube in var_iter!(
                    egraph[subst[push_to_cube_var]],
                    WrapperPullupReplacerPushToCube
                )
                .cloned()
                {
                    subst.insert(
                        select_push_to_cube_var,
                        egraph.add(LogicalPlanLanguage::WrappedSelectPushToCube(
                            WrappedSelectPushToCube(push_to_cube),
                        )),
                    );
                    subst.insert(
                        select_ungrouped_scan_var,
                        egraph.add(LogicalPlanLanguage::WrappedSelectUngroupedScan(
                            WrappedSelectUngroupedScan(push_to_cube),
                        )),
                    );
                    subst.insert(
                        select_alias_var,
                        egraph.add(LogicalPlanLanguage::WrappedSelectAlias(WrappedSelectAlias(
                            projection_alias,
                        ))),
                    );
                    return true;
                }
            }
        }

        false
    }
}
