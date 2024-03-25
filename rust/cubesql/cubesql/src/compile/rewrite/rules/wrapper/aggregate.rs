use crate::{
    compile::rewrite::{
        aggregate,
        analysis::LogicalPlanAnalysis,
        column_name_to_member_vec, cube_scan_wrapper, original_expr_name,
        rules::{members::MemberRules, wrapper::WrapperRules},
        transforming_chain_rewrite, transforming_rewrite, wrapped_select,
        wrapped_select_filter_expr_empty_tail, wrapped_select_having_expr_empty_tail,
        wrapped_select_joins_empty_tail, wrapped_select_order_expr_empty_tail,
        wrapped_select_projection_expr_empty_tail, wrapped_select_subqueries_empty_tail,
        wrapped_select_window_expr_empty_tail, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        AggregateFunctionExprDistinct, AggregateFunctionExprFun, AliasExprAlias, ColumnExprColumn,
        LogicalPlanLanguage, WrappedSelectUngrouped, WrapperPullupReplacerUngrouped,
    },
    transport::V1CubeMetaMeasureExt,
    var, var_iter,
};
use datafusion::logical_plan::Column;
use egg::{EGraph, Rewrite, Subst};

impl WrapperRules {
    pub fn aggregate_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        rules.extend(vec![transforming_rewrite(
            "wrapper-push-down-aggregate-to-cube-scan",
            aggregate(
                cube_scan_wrapper(
                    wrapper_pullup_replacer(
                        "?cube_scan_input",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
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
                        "?ungrouped",
                        "WrapperPullupReplacerInProjection:false",
                        "?cube_members",
                    ),
                    wrapped_select_subqueries_empty_tail(),
                    wrapper_pushdown_replacer(
                        "?group_expr",
                        "?alias_to_cube",
                        "?ungrouped",
                        "WrapperPullupReplacerInProjection:false",
                        "?cube_members",
                    ),
                    wrapper_pushdown_replacer(
                        "?aggr_expr",
                        "?alias_to_cube",
                        "?ungrouped",
                        "WrapperPullupReplacerInProjection:false",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_window_expr_empty_tail(),
                        "?alias_to_cube",
                        "?ungrouped",
                        "WrapperPullupReplacerInProjection:false",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        "?cube_scan_input",
                        "?alias_to_cube",
                        "?ungrouped",
                        "WrapperPullupReplacerInProjection:false",
                        "?cube_members",
                    ),
                    wrapped_select_joins_empty_tail(),
                    wrapper_pullup_replacer(
                        wrapped_select_filter_expr_empty_tail(),
                        "?alias_to_cube",
                        "?ungrouped",
                        "WrapperPullupReplacerInProjection:false",
                        "?cube_members",
                    ),
                    wrapped_select_having_expr_empty_tail(),
                    "WrappedSelectLimit:None",
                    "WrappedSelectOffset:None",
                    wrapper_pullup_replacer(
                        wrapped_select_order_expr_empty_tail(),
                        "?alias_to_cube",
                        "?ungrouped",
                        "WrapperPullupReplacerInProjection:false",
                        "?cube_members",
                    ),
                    "WrappedSelectAlias:None",
                    "WrappedSelectDistinct:false",
                    "?select_ungrouped",
                    "WrappedSelectUngroupedScan:false",
                ),
                "CubeScanWrapperFinalized:false",
            ),
            self.transform_aggregate(
                "?group_expr",
                "?aggr_expr",
                "?ungrouped",
                "?select_ungrouped",
            ),
        )]);

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
                        "WrapperPullupReplacerUngrouped:true",
                        "?in_projection",
                        "?cube_members",
                    ),
                    vec![("?aggr_expr", aggr_expr)],
                    wrapper_pullup_replacer(
                        "?measure",
                        "?alias_to_cube",
                        "WrapperPullupReplacerUngrouped:true",
                        "?in_projection",
                        "?cube_members",
                    ),
                    self.pushdown_measure(
                        "?aggr_expr",
                        column,
                        fun_name,
                        distinct,
                        cast_data_type,
                        "?cube_members",
                        "?measure",
                    ),
                )
            },
        );

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
    }

    fn transform_aggregate(
        &self,
        group_expr_var: &'static str,
        aggr_expr_var: &'static str,
        ungrouped_var: &'static str,
        select_ungrouped_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let group_expr_var = var!(group_expr_var);
        let aggr_expr_var = var!(aggr_expr_var);
        let ungrouped_var = var!(ungrouped_var);
        let select_ungrouped_var = var!(select_ungrouped_var);
        move |egraph, subst| {
            if egraph[subst[group_expr_var]].data.referenced_expr.is_none() {
                return false;
            }
            if egraph[subst[aggr_expr_var]].data.referenced_expr.is_none() {
                return false;
            }
            for ungrouped in
                var_iter!(egraph[subst[ungrouped_var]], WrapperPullupReplacerUngrouped).cloned()
            {
                subst.insert(
                    select_ungrouped_var,
                    egraph.add(LogicalPlanLanguage::WrappedSelectUngrouped(
                        WrappedSelectUngrouped(ungrouped),
                    )),
                );
                return true;
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
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let original_expr_var = var!(original_expr_var);
        let column_var = column_var.map(|v| var!(v));
        let fun_name_var = fun_name_var.map(|v| var!(v));
        let distinct_var = distinct_var.map(|v| var!(v));
        // let cast_data_type_var = cast_data_type_var.map(|v| var!(v));
        let cube_members_var = var!(cube_members_var);
        let measure_out_var = var!(measure_out_var);
        let meta = self.meta_context.clone();
        let disable_strict_agg_type_match = self.config_obj.disable_strict_agg_type_match();
        move |egraph, subst| {
            if let Some(alias) = original_expr_name(egraph, subst[original_expr_var]) {
                for fun in fun_name_var
                    .map(|fun_var| {
                        var_iter!(egraph[subst[fun_var]], AggregateFunctionExprFun)
                            .map(|fun| Some(fun))
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
                        let call_agg_type = MemberRules::get_agg_type(fun, distinct);

                        let column_iter = if let Some(column_var) = column_var {
                            var_iter!(egraph[subst[column_var]], ColumnExprColumn)
                                .cloned()
                                .collect()
                        } else {
                            vec![Column::from_name(MemberRules::default_count_measure_name())]
                        };

                        if let Some(member_name_to_expr) = egraph[subst[cube_members_var]]
                            .data
                            .member_name_to_expr
                            .clone()
                        {
                            let column_name_to_member_name =
                                column_name_to_member_vec(member_name_to_expr);
                            for column in column_iter {
                                if let Some((_, Some(member))) = column_name_to_member_name
                                    .iter()
                                    .find(|(cn, _)| cn == &column.name)
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
