use crate::{
    compile::rewrite::{
        aggregate,
        analysis::LogicalPlanData,
        cube_scan_wrapper, grouping_set_expr, original_expr_name, rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::{members::MemberRules, wrapper::WrapperRules},
        subquery, transforming_chain_rewrite, transforming_rewrite, wrapped_select,
        wrapped_select_aggr_expr_empty_tail, wrapped_select_filter_expr_empty_tail,
        wrapped_select_group_expr_empty_tail, wrapped_select_having_expr_empty_tail,
        wrapped_select_joins_empty_tail, wrapped_select_order_expr_empty_tail,
        wrapped_select_projection_expr_empty_tail, wrapped_select_subqueries_empty_tail,
        wrapped_select_window_expr_empty_tail, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        AggregateFunctionExprDistinct, AggregateFunctionExprFun, AliasExprAlias, ColumnExprColumn,
        ListType, LogicalPlanLanguage, WrappedSelectPushToCube, WrapperPullupReplacerAliasToCube,
        WrapperPullupReplacerGroupedSubqueries, WrapperPullupReplacerPushToCube,
        WrapperPushdownReplacerGroupedSubqueries, WrapperPushdownReplacerPushToCube,
    },
    copy_flag, copy_value,
    transport::V1CubeMetaMeasureExt,
    var, var_iter,
};
use datafusion::logical_plan::Column;
use egg::{Subst, Var};
use std::ops::IndexMut;

impl WrapperRules {
    pub fn aggregate_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            transforming_rewrite(
                "wrapper-push-down-aggregate-to-cube-scan",
                aggregate(
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
                    "?group_expr",
                    "?aggr_expr",
                    "AggregateSplit:false",
                ),
                cube_scan_wrapper(
                    wrapped_select(
                        "WrappedSelectSelectType:Aggregate",
                        wrapper_pullup_replacer(
                            wrapped_select_projection_expr_empty_tail(),
                            "?alias_to_cube",
                            "?push_to_cube",
                            "WrapperPullupReplacerInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_subqueries_empty_tail(),
                            "?alias_to_cube",
                            "?push_to_cube",
                            "WrapperPullupReplacerInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                        ),
                        wrapper_pushdown_replacer(
                            "?group_expr",
                            "?alias_to_cube",
                            "?pushdown_push_to_cube",
                            "WrapperPullupReplacerInProjection:false",
                            "?cube_members",
                            "?pushdown_grouped_subqueries",
                        ),
                        wrapper_pushdown_replacer(
                            "?aggr_expr",
                            "?alias_to_cube",
                            "?pushdown_push_to_cube",
                            "WrapperPullupReplacerInProjection:false",
                            "?cube_members",
                            "?pushdown_grouped_subqueries",
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_window_expr_empty_tail(),
                            "?alias_to_cube",
                            "?push_to_cube",
                            "WrapperPullupReplacerInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                        ),
                        wrapper_pullup_replacer(
                            "?cube_scan_input",
                            "?alias_to_cube",
                            "?push_to_cube",
                            "WrapperPullupReplacerInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_joins_empty_tail(),
                            "?alias_to_cube",
                            "?push_to_cube",
                            "WrapperPullupReplacerInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_filter_expr_empty_tail(),
                            "?alias_to_cube",
                            "?push_to_cube",
                            "WrapperPullupReplacerInProjection:false",
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
                            "WrapperPullupReplacerInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                        ),
                        "WrappedSelectAlias:None",
                        "WrappedSelectDistinct:false",
                        "?select_push_to_cube",
                        "WrappedSelectUngroupedScan:false",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                self.transform_aggregate(
                    "?group_expr",
                    "?aggr_expr",
                    "?push_to_cube",
                    "?pushdown_push_to_cube",
                    "?select_push_to_cube",
                    "?grouped_subqueries",
                    "?pushdown_grouped_subqueries",
                ),
            ),
            transforming_rewrite(
                "wrapper-groupping-set-push-down",
                wrapper_pushdown_replacer(
                    grouping_set_expr("?rollout_members", "?type"),
                    "?alias_to_cube",
                    "?push_to_cube",
                    "WrapperPullupReplacerInProjection:false",
                    "?cube_members",
                    "?grouped_subqueries",
                ),
                grouping_set_expr(
                    wrapper_pushdown_replacer(
                        "?rollout_members",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "WrapperPullupReplacerInProjection:false",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    "?type",
                ),
                self.check_rollup_allowed("?alias_to_cube"),
            ),
            rewrite(
                "wrapper-groupping-set-pull-up",
                grouping_set_expr(
                    wrapper_pullup_replacer(
                        "?rollout_members",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "WrapperPullupReplacerInProjection:false",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    "?type",
                ),
                wrapper_pullup_replacer(
                    grouping_set_expr("?rollout_members", "?type"),
                    "?alias_to_cube",
                    "?push_to_cube",
                    "WrapperPullupReplacerInProjection:false",
                    "?cube_members",
                    "?grouped_subqueries",
                ),
            ),
        ]);

        // TODO add flag to disable dimension rules
        MemberRules::measure_rewrites(
            rules,
            |name: &'static str,
             aggr_expr: String,
             _measure_expr: String,
             fun_name: Option<&'static str>,
             distinct: Option<&'static str>,
             cast_data_type: Option<&'static str>,
             column: Option<&'static str>| {
                transforming_chain_rewrite(
                    &format!("wrapper-{}", name),
                    wrapper_pushdown_replacer(
                        "?aggr_expr",
                        "?alias_to_cube",
                        "WrapperPushdownReplacerPushToCube:true",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    vec![("?aggr_expr", aggr_expr)],
                    wrapper_pullup_replacer(
                        "?measure",
                        "?alias_to_cube",
                        "WrapperPullupReplacerPushToCube:true",
                        "?in_projection",
                        "?cube_members",
                        "?pullup_grouped_subqueries",
                    ),
                    self.pushdown_measure(
                        "?aggr_expr",
                        column,
                        fun_name,
                        distinct,
                        cast_data_type,
                        "?cube_members",
                        "?measure",
                        "?grouped_subqueries",
                        "?pullup_grouped_subqueries",
                    ),
                )
            },
        );

        if self.config_obj.push_down_pull_up_split() {
            Self::flat_list_pushdown_pullup_rules(
                rules,
                "wrapper-aggregate-aggr-expr",
                ListType::AggregateAggrExpr,
                ListType::WrappedSelectAggrExpr,
            );

            Self::flat_list_pushdown_pullup_rules(
                rules,
                "wrapper-aggregate-group-expr",
                ListType::AggregateGroupExpr,
                ListType::WrappedSelectGroupExpr,
            );
            Self::flat_list_pushdown_pullup_rules(
                rules,
                "wrapper-grouping-set-members",
                ListType::GroupingSetExprMembers,
                ListType::GroupingSetExprMembers,
            );
        } else {
            Self::list_pushdown_pullup_rules(
                rules,
                "wrapper-aggregate-aggr-expr",
                "AggregateAggrExpr",
                "WrappedSelectAggrExpr",
            );

            Self::list_pushdown_pullup_rules(
                rules,
                "wrapper-aggregate-group-expr",
                "AggregateGroupExpr",
                "WrappedSelectGroupExpr",
            );
            Self::list_pushdown_pullup_rules(
                rules,
                "wrapper-grouping-set-members",
                "GroupingSetExprMembers",
                "GroupingSetExprMembers",
            );
        }
    }

    pub fn aggregate_rules_subquery(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![transforming_rewrite(
            "wrapper-push-down-aggregate-and-subquery-to-cube-scan",
            aggregate(
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
                "?group_expr",
                "?aggr_expr",
                "AggregateSplit:false",
            ),
            cube_scan_wrapper(
                wrapped_select(
                    "WrappedSelectSelectType:Aggregate",
                    wrapper_pullup_replacer(
                        wrapped_select_projection_expr_empty_tail(),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "WrapperPullupReplacerInProjection:false",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pushdown_replacer(
                        "?subqueries",
                        "?alias_to_cube",
                        "?pushdown_push_to_cube",
                        "WrapperPullupReplacerInProjection:false",
                        "?cube_members",
                        "?pushdown_grouped_subqueries",
                    ),
                    wrapper_pushdown_replacer(
                        "?group_expr",
                        "?alias_to_cube",
                        "?pushdown_push_to_cube",
                        "WrapperPullupReplacerInProjection:false",
                        "?cube_members",
                        "?pushdown_grouped_subqueries",
                    ),
                    wrapper_pushdown_replacer(
                        "?aggr_expr",
                        "?alias_to_cube",
                        "?pushdown_push_to_cube",
                        "WrapperPullupReplacerInProjection:false",
                        "?cube_members",
                        "?pushdown_grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_window_expr_empty_tail(),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "WrapperPullupReplacerInProjection:false",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        "?cube_scan_input",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "WrapperPullupReplacerInProjection:false",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_joins_empty_tail(),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "WrapperPullupReplacerInProjection:false",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_filter_expr_empty_tail(),
                        "?alias_to_cube",
                        "?push_to_cube",
                        "WrapperPullupReplacerInProjection:false",
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
                        "WrapperPullupReplacerInProjection:false",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    "WrappedSelectAlias:None",
                    "WrappedSelectDistinct:false",
                    "?select_push_to_cube",
                    "WrappedSelectUngroupedScan:false",
                ),
                "CubeScanWrapperFinalized:false",
            ),
            self.transform_aggregate_subquery(
                "?alias_to_cube",
                "?group_expr",
                "?aggr_expr",
                "?push_to_cube",
                "?pushdown_push_to_cube",
                "?select_push_to_cube",
                "?grouped_subqueries",
                "?pushdown_grouped_subqueries",
            ),
        )]);
    }

    pub fn aggregate_merge_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![transforming_rewrite(
            "wrapper-merge-aggregation-with-inner-wrapped-select",
            // Input is not a finished wrapper_pullup_replacer, but WrappedSelect just before pullup
            // After pullup replacer would disable push to cube, because any node on top would have WrappedSelect in `from`
            // So there would be no CubeScan to push to
            // Instead, this rule tries to catch `from` before pulling up, and merge outer Aggregate into inner WrappedSelect
            aggregate(
                cube_scan_wrapper(
                    wrapped_select(
                        "WrappedSelectSelectType:Projection",
                        wrapper_pullup_replacer(
                            wrapped_select_projection_expr_empty_tail(),
                            "?alias_to_cube",
                            "WrapperPullupReplacerPushToCube:true",
                            "WrapperPullupReplacerInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_subqueries_empty_tail(),
                            "?alias_to_cube",
                            "WrapperPullupReplacerPushToCube:true",
                            "WrapperPullupReplacerInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_group_expr_empty_tail(),
                            "?alias_to_cube",
                            "WrapperPullupReplacerPushToCube:true",
                            "WrapperPullupReplacerInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_aggr_expr_empty_tail(),
                            "?alias_to_cube",
                            "WrapperPullupReplacerPushToCube:true",
                            "WrapperPullupReplacerInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_window_expr_empty_tail(),
                            "?alias_to_cube",
                            "WrapperPullupReplacerPushToCube:true",
                            "WrapperPullupReplacerInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                        ),
                        wrapper_pullup_replacer(
                            "?inner_from",
                            "?alias_to_cube",
                            "WrapperPullupReplacerPushToCube:true",
                            "WrapperPullupReplacerInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                        ),
                        wrapper_pullup_replacer(
                            "?inner_joins",
                            "?alias_to_cube",
                            "WrapperPullupReplacerPushToCube:true",
                            "WrapperPullupReplacerInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                        ),
                        wrapper_pullup_replacer(
                            "?inner_filters",
                            "?alias_to_cube",
                            "WrapperPullupReplacerPushToCube:true",
                            "WrapperPullupReplacerInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                        ),
                        wrapped_select_having_expr_empty_tail(),
                        // Inner must not have limit and offset, because they are not commutative with aggregation
                        "WrappedSelectLimit:None",
                        "WrappedSelectOffset:None",
                        wrapper_pullup_replacer(
                            wrapped_select_order_expr_empty_tail(),
                            "?alias_to_cube",
                            "WrapperPullupReplacerPushToCube:true",
                            "WrapperPullupReplacerInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                        ),
                        "WrappedSelectAlias:None",
                        "WrappedSelectDistinct:false",
                        "WrappedSelectPushToCube:true",
                        "WrappedSelectUngroupedScan:true",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                "?group_expr",
                "?aggr_expr",
                "AggregateSplit:false",
            ),
            cube_scan_wrapper(
                wrapped_select(
                    "WrappedSelectSelectType:Aggregate",
                    wrapper_pullup_replacer(
                        wrapped_select_projection_expr_empty_tail(),
                        "?alias_to_cube",
                        "WrapperPullupReplacerPushToCube:true",
                        "WrapperPullupReplacerInProjection:false",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_subqueries_empty_tail(),
                        "?alias_to_cube",
                        "WrapperPullupReplacerPushToCube:true",
                        "WrapperPullupReplacerInProjection:false",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pushdown_replacer(
                        "?group_expr",
                        "?alias_to_cube",
                        "WrapperPushdownReplacerPushToCube:true",
                        "WrapperPullupReplacerInProjection:false",
                        "?cube_members",
                        "?pushdown_grouped_subqueries",
                    ),
                    wrapper_pushdown_replacer(
                        "?aggr_expr",
                        "?alias_to_cube",
                        "WrapperPushdownReplacerPushToCube:true",
                        "WrapperPullupReplacerInProjection:false",
                        "?cube_members",
                        "?pushdown_grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_window_expr_empty_tail(),
                        "?alias_to_cube",
                        "WrapperPullupReplacerPushToCube:true",
                        "WrapperPullupReplacerInProjection:false",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        "?inner_from",
                        "?alias_to_cube",
                        "WrapperPullupReplacerPushToCube:true",
                        "WrapperPullupReplacerInProjection:false",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        "?inner_joins",
                        "?alias_to_cube",
                        "WrapperPullupReplacerPushToCube:true",
                        "WrapperPullupReplacerInProjection:false",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapper_pullup_replacer(
                        "?inner_filters",
                        "?alias_to_cube",
                        "WrapperPullupReplacerPushToCube:true",
                        "WrapperPullupReplacerInProjection:false",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    wrapped_select_having_expr_empty_tail(),
                    "WrappedSelectLimit:None",
                    "WrappedSelectOffset:None",
                    wrapper_pullup_replacer(
                        wrapped_select_order_expr_empty_tail(),
                        "?alias_to_cube",
                        "WrapperPullupReplacerPushToCube:true",
                        "WrapperPullupReplacerInProjection:false",
                        "?cube_members",
                        "?grouped_subqueries",
                    ),
                    "WrappedSelectAlias:None",
                    "WrappedSelectDistinct:false",
                    "WrappedSelectPushToCube:true",
                    "WrappedSelectUngroupedScan:false",
                ),
                "CubeScanWrapperFinalized:false",
            ),
            self.transform_merge_aggregate("?grouped_subqueries", "?pushdown_grouped_subqueries"),
        )]);
    }

    fn transform_aggregate(
        &self,
        group_expr_var: &'static str,
        aggr_expr_var: &'static str,
        push_to_cube_var: &'static str,
        pushdown_push_to_cube_var: &'static str,
        select_push_to_cube_var: &'static str,
        grouped_subqueries_var: &'static str,
        pushdown_grouped_subqueries_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let group_expr_var = var!(group_expr_var);
        let aggr_expr_var = var!(aggr_expr_var);
        let push_to_cube_var = var!(push_to_cube_var);
        let pushdown_push_to_cube_var = var!(pushdown_push_to_cube_var);
        let select_push_to_cube_var = var!(select_push_to_cube_var);
        let grouped_subqueries_var = var!(grouped_subqueries_var);
        let pushdown_grouped_subqueries_var = var!(pushdown_grouped_subqueries_var);
        move |egraph, subst| {
            Self::transform_aggregate_impl(
                egraph,
                subst,
                group_expr_var,
                aggr_expr_var,
                push_to_cube_var,
                pushdown_push_to_cube_var,
                select_push_to_cube_var,
                grouped_subqueries_var,
                pushdown_grouped_subqueries_var,
            )
        }
    }

    fn transform_aggregate_subquery(
        &self,
        alias_to_cube_var: &'static str,
        group_expr_var: &'static str,
        aggr_expr_var: &'static str,
        push_to_cube_var: &'static str,
        pushdown_push_to_cube_var: &'static str,
        select_push_to_cube_var: &'static str,
        grouped_subqueries_var: &'static str,
        pushdown_grouped_subqueries_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let group_expr_var = var!(group_expr_var);
        let aggr_expr_var = var!(aggr_expr_var);
        let push_to_cube_var = var!(push_to_cube_var);
        let pushdown_push_to_cube_var = var!(pushdown_push_to_cube_var);
        let select_push_to_cube_var = var!(select_push_to_cube_var);
        let grouped_subqueries_var = var!(grouped_subqueries_var);
        let pushdown_grouped_subqueries_var = var!(pushdown_grouped_subqueries_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            if Self::transform_check_subquery_allowed(
                egraph,
                subst,
                meta.clone(),
                alias_to_cube_var,
            ) {
                Self::transform_aggregate_impl(
                    egraph,
                    subst,
                    group_expr_var,
                    aggr_expr_var,
                    push_to_cube_var,
                    pushdown_push_to_cube_var,
                    select_push_to_cube_var,
                    grouped_subqueries_var,
                    pushdown_grouped_subqueries_var,
                )
            } else {
                false
            }
        }
    }

    fn transform_aggregate_impl(
        egraph: &mut CubeEGraph,
        subst: &mut Subst,
        group_expr_var: Var,
        aggr_expr_var: Var,
        push_to_cube_var: Var,
        pushdown_push_to_cube_var: Var,
        select_push_to_cube_var: Var,
        grouped_subqueries_var: Var,
        pushdown_grouped_subqueries_var: Var,
    ) -> bool {
        if egraph[subst[group_expr_var]].data.referenced_expr.is_none() {
            return false;
        }
        if egraph[subst[aggr_expr_var]].data.referenced_expr.is_none() {
            return false;
        }

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
            return true;
        }
        false
    }

    fn transform_merge_aggregate(
        &self,
        grouped_subqueries_var: &'static str,
        pushdown_grouped_subqueries_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let grouped_subqueries_var = var!(grouped_subqueries_var);
        let pushdown_grouped_subqueries_var = var!(pushdown_grouped_subqueries_var);
        move |egraph, subst| {
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

            true
        }
    }

    fn check_rollup_allowed(
        &self,
        alias_to_cube_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            for alias_to_cube in var_iter!(
                egraph[subst[alias_to_cube_var]],
                WrapperPullupReplacerAliasToCube
            )
            .cloned()
            {
                if let Some(sql_generator) = meta.sql_generator_by_alias_to_cube(&alias_to_cube) {
                    if sql_generator
                        .get_sql_templates()
                        .templates
                        .contains_key("expressions/rollup")
                    {
                        return true;
                    }
                }
            }
            false
        }
    }

    fn pushdown_measure(
        &self,
        original_expr_var: &'static str,
        column_var: Option<&'static str>,
        fun_name_var: Option<&'static str>,
        distinct_var: Option<&'static str>,
        // TODO support cast push downs
        _cast_data_type_var: Option<&'static str>,
        cube_members_var: &'static str,
        measure_out_var: &'static str,
        grouped_subqueries_var: &'static str,
        pullup_grouped_subqueries_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let original_expr_var = var!(original_expr_var);
        let column_var = column_var.map(|v| var!(v));
        let fun_name_var = fun_name_var.map(|v| var!(v));
        let distinct_var = distinct_var.map(|v| var!(v));
        // let cast_data_type_var = cast_data_type_var.map(|v| var!(v));
        let cube_members_var = var!(cube_members_var);
        let measure_out_var = var!(measure_out_var);
        let grouped_subqueries_var = var!(grouped_subqueries_var);
        let pullup_grouped_subqueries_var = var!(pullup_grouped_subqueries_var);
        let meta = self.meta_context.clone();
        let disable_strict_agg_type_match = self.config_obj.disable_strict_agg_type_match();
        move |egraph, subst| {
            if let Some(alias) = original_expr_name(egraph, subst[original_expr_var]) {
                for fun in fun_name_var
                    .map(|fun_var| {
                        var_iter!(egraph[subst[fun_var]], AggregateFunctionExprFun)
                            .map(|fun| Some(fun.clone()))
                            .collect()
                    })
                    .unwrap_or(vec![None])
                {
                    for distinct in distinct_var
                        .map(|distinct_var| {
                            var_iter!(egraph[subst[distinct_var]], AggregateFunctionExprDistinct)
                                .map(|d| *d)
                                .collect()
                        })
                        .unwrap_or(vec![false])
                    {
                        let call_agg_type = MemberRules::get_agg_type(fun.as_ref(), distinct);

                        let column_iter = if let Some(column_var) = column_var {
                            var_iter!(egraph[subst[column_var]], ColumnExprColumn)
                                .cloned()
                                .collect()
                        } else {
                            vec![Column::from_name(MemberRules::default_count_measure_name())]
                        };

                        if let Some(member_names_to_expr) = &mut egraph
                            .index_mut(subst[cube_members_var])
                            .data
                            .member_name_to_expr
                        {
                            for column in column_iter {
                                if let Some((&(Some(ref member), _, _), _)) =
                                    LogicalPlanData::do_find_member_by_alias(
                                        member_names_to_expr,
                                        &column.name,
                                    )
                                {
                                    if let Some(measure) =
                                        meta.find_measure_with_name(member.to_string())
                                    {
                                        if call_agg_type.is_none()
                                            || measure.is_same_agg_type(
                                                call_agg_type.as_ref().unwrap(),
                                                disable_strict_agg_type_match,
                                            )
                                        {
                                            if !copy_value!(
                                                egraph,
                                                subst,
                                                Vec<String>,
                                                grouped_subqueries_var,
                                                WrapperPushdownReplacerGroupedSubqueries,
                                                pullup_grouped_subqueries_var,
                                                WrapperPullupReplacerGroupedSubqueries
                                            ) {
                                                return false;
                                            }

                                            let column_expr_column =
                                                egraph.add(LogicalPlanLanguage::ColumnExprColumn(
                                                    ColumnExprColumn(column.clone()),
                                                ));

                                            let column_expr =
                                                egraph.add(LogicalPlanLanguage::ColumnExpr([
                                                    column_expr_column,
                                                ]));
                                            let alias_expr_alias =
                                                egraph.add(LogicalPlanLanguage::AliasExprAlias(
                                                    AliasExprAlias(alias.clone()),
                                                ));

                                            let alias_expr =
                                                egraph.add(LogicalPlanLanguage::AliasExpr([
                                                    column_expr,
                                                    alias_expr_alias,
                                                ]));

                                            subst.insert(measure_out_var, alias_expr);

                                            return true;
                                        }
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
}
